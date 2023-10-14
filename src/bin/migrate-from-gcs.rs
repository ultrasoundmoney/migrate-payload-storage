use std::{
    fmt,
    fs::File,
    io::{Read, Write},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Datelike, Timelike, Utc};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use futures::{FutureExt, TryStreamExt};
use lazy_static::lazy_static;
use migrate_payload_archive::{env::ENV_CONFIG, log};
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, ObjectMeta, ObjectStore, RetryConfig,
};
use serde::Serialize;
use tokio::time::interval;
use tokio_util::io::StreamReader;
use tracing::{debug, info};

const PROGRESS_FILE_PATH: &str = "progress.json";

fn read_last_file() -> anyhow::Result<String> {
    let progress_file_path = Path::new(PROGRESS_FILE_PATH);
    if !progress_file_path.exists() {
        // No progress file found, returning empty string
        return Ok(String::new());
    }
    let mut file = File::open(progress_file_path)?;
    let mut last_file = String::new();
    file.read_to_string(&mut last_file)?;
    Ok(last_file)
}

fn write_last_file(last_file: &ObjectMeta) -> anyhow::Result<()> {
    let mut file = File::create(Path::new(PROGRESS_FILE_PATH))?;
    file.write_all(last_file.location.to_string().as_bytes())?;
    Ok(())
}

fn cleanup_last_file() -> anyhow::Result<()> {
    std::fs::remove_file(PROGRESS_FILE_PATH)?;
    Ok(())
}

fn print_migration_rate(payloads_migrated: u64, duration_secs: u64) {
    let rate = payloads_migrated as f64 / duration_secs as f64;
    info!("migration rate: {:.2} payloads per second", rate);
}

const GCS_BUCKET: &str = "execution-payload-archive";

fn get_gcs_object_store() -> anyhow::Result<impl ObjectStore> {
    // We can't read directly from the gcs_store so mimic what it does. No need to blow up if we fail.
    info!(gcs_bucket = GCS_BUCKET, "using GCS store");
    let gcs_store = GoogleCloudStorageBuilder::new()
        .with_service_account_path("./gcs_secret.json")
        .with_bucket_name(GCS_BUCKET)
        .with_retry(RetryConfig::default())
        .build()?;
    Ok(gcs_store)
}

fn get_ovh_object_store() -> anyhow::Result<impl ObjectStore> {
    // We can't read directly from the s3_store so mimic what it does. No need to blow up if we fail.
    let endpoint = std::env::var("AWS_ENDPOINT").unwrap_or("UNKNOWN".to_string());
    let s3_bucket = &ENV_CONFIG.s3_bucket;
    info!(endpoint, s3_bucket, "using OVH store");
    let s3_store = AmazonS3Builder::from_env()
        .with_bucket_name(s3_bucket)
        .with_retry(RetryConfig::default())
        .build()?;
    Ok(s3_store)
}

use std::fmt::Display;

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = "2020-12-01T12:00:23Z".parse().unwrap();
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct Slot(pub i32);

impl Display for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}", self.0)
    }
}

impl Slot {
    const SECONDS_PER_SLOT: u8 = 12;

    fn date_time(&self) -> DateTime<Utc> {
        self.into()
    }
}

impl From<&Slot> for DateTime<Utc> {
    fn from(slot: &Slot) -> Self {
        let seconds = slot.0 as i64 * Slot::SECONDS_PER_SLOT as i64;
        *GENESIS_TIMESTAMP + chrono::Duration::seconds(seconds)
    }
}

#[derive(Serialize)]
struct ExecutionPayload {
    block_hash: String,
    #[serde(skip_serializing)]
    id: String,
    inserted_at: String,
    payload: serde_json::Value,
    proposer_pubkey: String,
    slot: Slot,
    version: String,
}

impl ExecutionPayload {
    pub fn payload_block_hash(&self) -> &str {
        self.payload["block_hash"].as_str().unwrap()
    }
}

impl fmt::Debug for ExecutionPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload_block_hash = self.payload_block_hash();
        f.debug_struct("ExecutionPayload")
            .field("block_hash", &self.block_hash)
            .field("id", &self.id)
            .field("inserted_at", &self.inserted_at)
            .field("proposer_pubkey", &self.proposer_pubkey)
            .field("slot", &self.slot)
            .field("version", &self.version)
            .field(
                "payload",
                &format!("<PAYLOAD block_hash={}>", payload_block_hash),
            )
            .finish()
    }
}

async fn migrate_bundle(
    gcs: &impl ObjectStore,
    ovh: &impl ObjectStore,
    object: &ObjectMeta,
) -> anyhow::Result<()> {
    info!(object = %object.location, size_mib=object.size / 1_000_000, "migrating bundle");

    // Introduce a counter and a timer
    let mut payloads_migrated = 0u64;
    let mut timer = interval(Duration::from_secs(10));
    let start_time = SystemTime::now();

    let payload_stream = gcs.get(&object.location).await?.into_stream();
    let reader = StreamReader::new(payload_stream);

    let (decoded_tx, decoded_rx) = std::sync::mpsc::sync_channel(16);

    let handle = tokio::task::spawn_blocking(move || {
        let reader_sync = tokio_util::io::SyncIoBridge::new(reader);
        let decoder = GzDecoder::new(reader_sync);
        let mut csv_reader = csv::Reader::from_reader(decoder);
        let mut iter = csv_reader.byte_records();

        while let Some(record) = iter.next().transpose().unwrap() {
            let execution_payload = {
                unsafe {
                    ExecutionPayload {
                        block_hash: String::from_utf8_unchecked(record[4].into()),
                        id: String::from_utf8_unchecked(record[0].into()),
                        inserted_at: String::from_utf8_unchecked(record[1].into()),
                        payload: serde_json::from_slice(&record[6]).unwrap(),
                        proposer_pubkey: String::from_utf8_unchecked(record[3].into()),
                        slot: std::str::from_utf8_unchecked(&record[2])
                            .parse()
                            .map(Slot)
                            .unwrap(),
                        version: String::from_utf8_unchecked(record[5].into()),
                    }
                }
            };

            decoded_tx.send(execution_payload).unwrap();
        }
    });

    while let Ok(payload) = decoded_rx.recv() {
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("expect duration since UNIX_EPOCH to be positive regardless of clock shift")
            .as_micros();

        let block_hash = &payload.block_hash;

        let slot = &payload.slot;
        let slot_date_time = slot.date_time();
        let year = slot_date_time.year();
        let month = slot_date_time.month();
        let day = slot_date_time.day();
        let hour = slot_date_time.hour();
        let minute = slot_date_time.minute();

        let path_string =
            format!("old_formats/gcs/{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}/{timestamp_micros}-{block_hash}.json.gz");
        let path = object_store::path::Path::from(path_string);

        let bytes = serde_json::to_vec(&payload).unwrap();
        let bytes_gz = Vec::new();
        let mut encoder = GzEncoder::new(bytes_gz, Compression::default());
        encoder.write_all(&bytes).unwrap();
        let bytes_gz = encoder.finish().unwrap();

        ovh.put(&path, bytes_gz.into()).await.unwrap();

        debug!(object = %path, "migrated");

        payloads_migrated += 1; // Increment the counter for each payload migrated

        // Check if it's time to report the migration rate
        if timer.tick().now_or_never().is_some() {
            let elapsed = SystemTime::now()
                .duration_since(start_time)
                .unwrap()
                .as_secs();

            print_migration_rate(payloads_migrated, elapsed);
        }
    }

    handle.await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init();

    // Initialize GCS object store
    let gcs = get_gcs_object_store()?;

    // Initialize OVH object store
    let ovh = get_ovh_object_store()?;
    debug!("initialized object stores");

    debug!("listing day bundles");
    let day_bundle_meta_stream = gcs.list(None).await?;
    let mut day_bundle_metas = day_bundle_meta_stream.try_collect::<Vec<_>>().await?;
    debug!("found {} day bundles", day_bundle_metas.len());

    let last_file = read_last_file()?;

    day_bundle_metas.retain(|file| file.location.to_string() > last_file);

    if day_bundle_metas.is_empty() {
        info!("no new files to process");
        return Ok(());
    }

    // Sort the files by name to make sure we process them in order
    day_bundle_metas.sort_by(|a, b| a.location.to_string().cmp(&b.location.to_string()));

    for file in &day_bundle_metas {
        migrate_bundle(&gcs, &ovh, file).await?;

        write_last_file(file)?;
    }

    // Migration complete, clean up the progress file
    if last_file == day_bundle_metas.last().unwrap().location.to_string() {
        cleanup_last_file()?;
    }

    Ok(())
}
