use std::{
    collections::HashMap,
    fmt,
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Datelike, Timelike, Utc};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use futures::{channel::mpsc::channel, FutureExt, SinkExt, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use migrate_payload_archive::{env::ENV_CONFIG, log};
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, ObjectMeta, ObjectStore, RetryConfig,
};
use serde::Serialize;
use tokio::{spawn, task::spawn_blocking, time::interval};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{debug, info};

const PROGRESS_FILE_PATH: &str = "progress.json";

fn read_progress() -> anyhow::Result<Option<String>> {
    let progress_file_path = Path::new(PROGRESS_FILE_PATH);
    if !progress_file_path.exists() {
        // No progress file found, returning empty string
        debug!("no progress file found");
        return Ok(None);
    }
    let mut file = File::open(progress_file_path)?;
    let mut last_file = String::new();
    file.read_to_string(&mut last_file)?;
    debug!(last_file, "found progress file");
    Ok(Some(last_file))
}

fn write_progress(last_file: &ObjectMeta) -> anyhow::Result<()> {
    info!(last_file = %last_file.location, "writing progress");
    let mut file = File::create(Path::new(PROGRESS_FILE_PATH))?;
    let progress = format!("{}", last_file.location.to_string());
    file.write_all(progress.as_bytes())?;
    Ok(())
}

fn print_migration_rate(payloads_migrated: usize, duration_secs: u64) {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init();

    // Initialize GCS object store
    let gcs = get_gcs_object_store()?;

    // Initialize OVH object store
    let ovh = &get_ovh_object_store()?;
    debug!("initialized object stores");

    debug!("listing day bundles");
    let day_bundle_meta_stream = gcs.list(None).await?;
    let mut day_bundle_metas = day_bundle_meta_stream.try_collect::<Vec<_>>().await?;
    debug!("found {} day bundles", day_bundle_metas.len());

    let progress = read_progress()?;

    if let Some(last_file) = progress.as_ref() {
        info!(last_file = %last_file, "resuming migration");
        day_bundle_metas.retain(|file| file.location.to_string() >= *last_file);
    } else {
        info!("starting migration from scratch");
    }

    if day_bundle_metas.is_empty() {
        info!("no new files to process");
        return Ok(());
    }

    // Sort the files by name to make sure we process them in order
    day_bundle_metas.sort_by(|a, b| a.location.to_string().cmp(&b.location.to_string()));

    for object_meta in &day_bundle_metas {
        info!(object = %object_meta.location, size_mib=object_meta.size / 1_000_000, "migrating bundle");

        // Introduce a counter and a timer
        let payloads_migrated_counter = &AtomicUsize::new(0);
        let interval_10_seconds = &Arc::new(Mutex::new(interval(Duration::from_secs(10))));
        let start_time = SystemTime::now();

        let payload_stream = gcs.get(&object_meta.location).await?.into_stream();
        let reader = StreamReader::new(payload_stream);

        const DECODED_BUFFER_SIZE: usize = 128;
        let (mut decoded_tx, mut decoded_rx) = channel(DECODED_BUFFER_SIZE);

        spawn_blocking(move || {
            let reader_sync = SyncIoBridge::new(reader);
            let decoder = GzDecoder::new(reader_sync);
            let mut csv_reader = csv::Reader::from_reader(decoder);
            let mut iter = csv_reader.byte_records();

            let mut batch = Vec::new();

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

                batch.push(execution_payload);

                if batch.len() == 32 {
                    for execution_payload in batch.drain(..) {
                        futures::executor::block_on(decoded_tx.feed(execution_payload)).unwrap();
                    }
                    futures::executor::block_on(decoded_tx.flush()).unwrap();
                    batch.clear();
                }
            }
        });

        const SLOT_BUNDLE_BUFFER_SIZE: usize = 32;
        let (mut slot_bundle_tx, slot_bundle_rx) = channel(SLOT_BUNDLE_BUFFER_SIZE);

        spawn(async move {
            let mut slot_bundles = HashMap::new();

            while let Some(payload) = decoded_rx.next().await {
                let payload_slot = payload.slot.0;
                let entry = slot_bundles.entry(payload_slot).or_insert_with(Vec::new);
                entry.push(payload);

                // Whenever we have more than 2 slots of payloads in the hashmap, we flush the
                // oldest slot to the next stage of the pipeline.
                if slot_bundles.len() > 2 {
                    let oldest_slot = slot_bundles.keys().min().unwrap().clone();
                    let oldest_slot_payloads = slot_bundles.remove(&oldest_slot).unwrap();
                    slot_bundle_tx.send(oldest_slot_payloads).await.unwrap();
                }
            }
        });

        const CONCURRENT_PUT_LIMIT: usize = 16;

        slot_bundle_rx
            .map(Ok)
            .try_for_each_concurrent(CONCURRENT_PUT_LIMIT, |payloads| async move  {
                let payloads_count = payloads.len();
                let slot = &payloads.first().unwrap().slot;

                let slot_date_time = slot.date_time();
                let year = slot_date_time.year();
                let month = slot_date_time.month();
                let day = slot_date_time.day();
                let hour = slot_date_time.hour();
                let minute = slot_date_time.minute();

                let path_string =
                format!("old_formats/gcs_v3/{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}.ndjson.gz");
                let path = object_store::path::Path::from(path_string);

                let bytes_gz = spawn_blocking(move || {
                    // Iterate the payloads, and write them to a gzipped ndjson file
                    let mut bytes = Vec::new();
                    for payload in payloads {
                        let payload = serde_json::to_vec(&payload).unwrap();
                        bytes.extend_from_slice(&payload);
                        bytes.push(b'\n');
                    }
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(&bytes).unwrap();
                    encoder.finish().unwrap()
                })
                    .await
                    .unwrap();

                ovh.put(&path, bytes_gz.into()).await.unwrap();

                let payloads_migrated_count = payloads_migrated_counter.fetch_add(payloads_count, std::sync::atomic::Ordering::Relaxed);

                // Check if it's time to report the migration rate
                if interval_10_seconds.lock().unwrap().tick().now_or_never().is_some() {
                    let elapsed = SystemTime::now()
                        .duration_since(start_time)
                        .unwrap()
                        .as_secs();

                    print_migration_rate(payloads_migrated_count, elapsed);

                }


                Ok::<_, anyhow::Error>(())
            })
            .await?;

        // As we process concurrently on a sudden shut down, we may lose payloads we
        // processed before this one by skipping over them when we resume.
        write_progress(&object_meta)?;
    }

    Ok(())
}
