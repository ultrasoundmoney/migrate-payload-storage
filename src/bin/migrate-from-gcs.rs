use std::{
    fmt,
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex},
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
use tokio::{task::spawn_blocking, time::interval};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{debug, info, trace};

const PROGRESS_FILE_PATH: &str = "progress.json";

fn read_progress() -> anyhow::Result<Option<(String, String)>> {
    let progress_file_path = Path::new(PROGRESS_FILE_PATH);
    if !progress_file_path.exists() {
        // No progress file found, returning empty string
        info!("no progress file found");
        return Ok(None);
    }
    let mut file = File::open(progress_file_path)?;
    let mut last_file = String::new();
    file.read_to_string(&mut last_file)?;
    let mut iter = last_file.split(':');
    let progress = (
        iter.next().unwrap().to_string(),
        iter.next().unwrap().to_string(),
    );
    info!(last_file = %progress.0, progress_id = %progress.1, "found progress file");
    Ok(Some(progress))
}

fn write_progress(last_file: &ObjectMeta, payload_id: &str) -> anyhow::Result<()> {
    info!(last_file = %last_file.location, payload_id, "writing progress");
    let mut file = File::create(Path::new(PROGRESS_FILE_PATH))?;
    let progress = format!("{}:{}", last_file.location.to_string(), payload_id);
    file.write_all(progress.as_bytes())?;
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

    if let Some((last_file, last_id)) = progress.as_ref() {
        info!(last_file = %last_file, last_id, "resuming migration");
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
        let payloads_migrated_counter = &AtomicU64::new(0);
        let interval_10_seconds = &Arc::new(Mutex::new(interval(Duration::from_secs(10))));
        let start_time = SystemTime::now();

        let payload_stream = gcs.get(&object_meta.location).await?.into_stream();
        let reader = StreamReader::new(payload_stream);

        const DECODED_BUFFER_SIZE: usize = 32;
        let (mut decoded_tx, decoded_rx) = channel(DECODED_BUFFER_SIZE);

        let handle = spawn_blocking(move || {
            let reader_sync = SyncIoBridge::new(reader);
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

                futures::executor::block_on(decoded_tx.send(execution_payload)).unwrap();
            }
        });

        const CONCURRENT_PUT_LIMIT: usize = 1;

        // Skip payloads that have already been processed.
        decoded_rx
            .skip_while(|payload| {
                match progress.as_ref() {
                    // If there was previous progress
                    Some((_last_file, last_id)) => {
                        // And the current payload matches our last progress, process remaining payloads in
                        // the stream.
                        if payload.id == *last_id {
                            debug!(payload_id = %payload.id, "found last processed payload");
                            futures::future::ready(false)
                        } else {
                            // Otherwise, skip this one.
                            trace!(payload_id = %payload.id, "skipping payload");
                            futures::future::ready(true)
                        }
                    }
                    // If there was no previous progress (first run), process all payloads in the stream.
                    None => futures::future::ready(false),
                }
            })
            .map(Ok)
            .try_for_each_concurrent(CONCURRENT_PUT_LIMIT, |payload| async move  {
                let block_hash = payload.block_hash.clone();
                let payload_id = payload.id.clone();

                debug!(block_hash, payload_id, "storing payload");

                let slot = &payload.slot;
                let slot_date_time = slot.date_time();
                let year = slot_date_time.year();
                let month = slot_date_time.month();
                let day = slot_date_time.day();
                let hour = slot_date_time.hour();
                let minute = slot_date_time.minute();

                let path_string =
                format!("old_formats/gcs_v2/{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}/{payload_id}-{block_hash}.json.gz");
                let path = object_store::path::Path::from(path_string);

                let payload_id = payload.id.clone();

                let bytes_gz = spawn_blocking(move || {
                    let bytes = serde_json::to_vec(&payload).unwrap();
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(&bytes).unwrap();
                    encoder.finish().unwrap()
                })
                    .await
                    .unwrap();

                ovh.put(&path, bytes_gz.into()).await.unwrap();

                let payloads_migrated_count = payloads_migrated_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Check if it's time to report the migration rate
                if interval_10_seconds.lock().unwrap().tick().now_or_never().is_some() {
                    let elapsed = SystemTime::now()
                        .duration_since(start_time)
                        .unwrap()
                        .as_secs();

                    print_migration_rate(payloads_migrated_count, elapsed);

                    // As we process concurrently on a sudden shut down, we may lose payloads we
                    // processed before this one by skipping over them when we resume.
                    write_progress(&object_meta, &payload_id)?;
                }


                debug!(block_hash, payload_id, "payload stored");

                Ok::<_, anyhow::Error>(())
            })
            .await?;

        handle.await?;
    }

    // Migration complete, clean up the progress file
    if let Some((last_file, _row)) = progress {
        if last_file == day_bundle_metas.last().unwrap().location.to_string() {
            cleanup_last_file()?;
        }
    }

    Ok(())
}
