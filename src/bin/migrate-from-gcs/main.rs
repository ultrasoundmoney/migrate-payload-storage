mod execution_payloads;
mod object_stores;
mod progress;
mod slots;

use std::{
    io::{Read, Write},
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::{Duration, SystemTime},
};

use backoff::ExponentialBackoff;
use bytes::Bytes;
use chrono::{Datelike, Timelike};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    try_join, FutureExt, SinkExt, Stream, StreamExt, TryStreamExt,
};
use migrate_payload_archive::log;
use object_store::ObjectStore;
use tokio::{
    spawn,
    task::{spawn_blocking, JoinHandle},
    time::interval,
};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{debug, info};

fn print_rate(count: usize, duration_secs: u64) {
    let rate = count as f64 / duration_secs as f64;
    info!("rate: {:.2} payloads per second", rate);
}

use crate::execution_payloads::ExecutionPayload;

fn run_decode_payloads(
    payload_stream: impl Stream<Item = object_store::Result<Bytes>> + Unpin + Send + 'static,
    mut decoded_tx: Sender<ExecutionPayload>,
) -> JoinHandle<()> {
    spawn_blocking(move || {
        let reader = StreamReader::new(payload_stream);
        let reader_sync = SyncIoBridge::new(reader);
        let decoder = GzDecoder::new(reader_sync);
        let mut csv_reader = csv::Reader::from_reader(decoder);
        let mut iter = csv_reader.byte_records();

        // We send payloads in batches to avoid constant flushing of the channel. They should be
        // coming in ~1000 per second.
        let mut batch = Vec::new();

        while let Some(record) = iter.next().transpose().unwrap() {
            let execution_payload: ExecutionPayload = record.into();

            batch.push(execution_payload);

            if batch.len() == 32 {
                for execution_payload in batch.drain(..) {
                    futures::executor::block_on(decoded_tx.feed(execution_payload)).unwrap();
                }
                futures::executor::block_on(decoded_tx.flush()).unwrap();
                batch.clear();
            }
        }
    })
}

// Here we take payloads as we decode them and bundle them by slot.
// Unfortunately it appears the payloads are not always ordered by slot, and so bundles cannot be
// considered complete until the full file is processed. We write all slot bundles to file, and
// append continuously.
fn run_bundle_payloads(
    day_bundle_id: String,
    mut decoded_rx: Receiver<ExecutionPayload>,
    mut slot_bundle_tx: Sender<Vec<ExecutionPayload>>,
) -> JoinHandle<()> {
    spawn(async move {
        // Create a directory for the bundles, wipe it if it already exists.
        let path = format!("bundles_{}", day_bundle_id);
        let bundle_dir = std::path::Path::new(&path);
        if bundle_dir.exists() {
            std::fs::remove_dir_all(bundle_dir).unwrap();
        }
        std::fs::create_dir(bundle_dir).unwrap();

        let mut count = 0;
        let start = SystemTime::now();

        while let Some(payload) = decoded_rx.next().await {
            let payload_slot = payload.slot();

            // Create or open the slot bundle ndjson file.
            let slot_bundle_path = bundle_dir.join(format!("{}.ndjson", payload_slot));
            let mut slot_bundle_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(slot_bundle_path)
                .unwrap();

            // Write the payload to the slot bundle file.
            let payload = serde_json::to_vec(&payload).unwrap();
            slot_bundle_file.write_all(&payload).unwrap();
            slot_bundle_file.write_all(b"\n").unwrap();

            count += 1;

            // Every 1000 payloads report on the rate
            if count % 8000 == 0 {
                let elapsed = start.elapsed().unwrap().as_secs();
                print_rate(count, elapsed);
            }
        }

        info!(day_bundle_id, "full day bundle processed to slot bundles");

        // Iterate through all created files and send the slot bundles.
        let mut slot_bundle_paths = std::fs::read_dir(bundle_dir)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();
        slot_bundle_paths.sort();

        for slot_bundle_path in slot_bundle_paths {
            let mut slot_bundle_file = std::fs::File::open(slot_bundle_path).unwrap();
            let mut file_str = String::new();
            slot_bundle_file.read_to_string(&mut file_str).unwrap();
            let slot_bundle = file_str
                .lines()
                .map(|line| serde_json::from_str::<ExecutionPayload>(line).unwrap())
                .collect::<Vec<_>>();

            slot_bundle_tx.feed(slot_bundle).await.unwrap();
        }
        slot_bundle_tx.flush().await.unwrap();

        // Remove the bundle directory.
        std::fs::remove_dir_all(bundle_dir).unwrap();
    })
}

async fn store_bundle(
    ovh: &impl ObjectStore,
    payloads: Vec<ExecutionPayload>,
    payloads_migrated_counter: &AtomicUsize,
    start_time: SystemTime,
) -> anyhow::Result<()> {
    let interval_10_seconds = &Arc::new(Mutex::new(interval(Duration::from_secs(10))));

    let payloads_count = payloads.len();
    let slot = &payloads.first().unwrap().slot();

    debug!(slot = %slot, payloads_count, "storing slot bundle");

    let slot_date_time = slot.date_time();
    let year = slot_date_time.year();
    let month = slot_date_time.month();
    let day = slot_date_time.day();
    let hour = slot_date_time.hour();
    let minute = slot_date_time.minute();

    let path_string = format!(
        "old_formats/gcs_v3/{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}.ndjson.gz"
    );
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
        let bytes_gz = encoder.finish().unwrap();
        Bytes::from(bytes_gz)
    })
    .await
    .unwrap();

    let op = || async {
        ovh.put(&path, bytes_gz.clone()).await?;
        Ok(())
    };
    backoff::future::retry(ExponentialBackoff::default(), op).await?;

    let payloads_migrated_count =
        payloads_migrated_counter.fetch_add(payloads_count, std::sync::atomic::Ordering::Relaxed);

    // Check if it's time to report the migration rate
    if interval_10_seconds
        .lock()
        .unwrap()
        .tick()
        .now_or_never()
        .is_some()
    {
        let elapsed = SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_secs();

        print_rate(payloads_migrated_count, elapsed);
    }

    Ok::<_, anyhow::Error>(())
}

const CONCURRENT_PUT_LIMIT: usize = 8;

fn run_store_bundles(slot_bundle_rx: Receiver<Vec<ExecutionPayload>>) -> JoinHandle<()> {
    // Initialize OVH object store
    let ovh = object_stores::get_ovh_object_store().unwrap();
    debug!("initialized OVH Object Storage client");

    // Introduce a counter and a timer
    let payloads_migrated_counter = AtomicUsize::new(0);
    let start_time = SystemTime::now();

    spawn(async move {
        slot_bundle_rx
            .map(Ok)
            .try_for_each_concurrent(CONCURRENT_PUT_LIMIT, |payloads| {
                store_bundle(&ovh, payloads, &payloads_migrated_counter, start_time)
            })
            .await
            .unwrap();
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init();

    // Initialize GCS object store
    let gcs = object_stores::get_gcs_object_store()?;
    debug!("initialized Google Cloud Storage client");

    debug!("listing daily payload bundles in Google Cloud Storage");
    let day_bundle_meta_stream = gcs.list(None).await?;
    let mut day_bundle_metas = day_bundle_meta_stream.try_collect::<Vec<_>>().await?;
    debug!("found {} day bundles", day_bundle_metas.len());

    let progress = progress::read_progress()?;

    if let Some(last_file) = progress.as_ref() {
        info!(last_file = %last_file, "resuming migration");
        day_bundle_metas.retain(|file| file.location.to_string() > *last_file);
    } else {
        info!("starting migration from scratch");
    }

    if day_bundle_metas.is_empty() {
        info!("all files already migrated, shutting down");
        return Ok(());
    }

    // Sort the files by name to make sure we process them in order
    day_bundle_metas.sort_by(|a, b| a.location.to_string().cmp(&b.location.to_string()));

    for object_meta in &day_bundle_metas {
        info!(object = %object_meta.location, size_mib=object_meta.size / 1_000_000, "migrating bundle");

        // 2023-04-22.csv.gz -> 2023-04-22
        let bundle_id = object_meta
            .location
            .to_string()
            .split('.')
            .next()
            .unwrap()
            .to_string();

        let payload_day_stream = gcs.get(&object_meta.location).await?.into_stream();

        const DECODED_BUFFER_SIZE: usize = 128;
        let (payload_tx, payload_rx) = channel(DECODED_BUFFER_SIZE);

        const SLOT_BUNDLE_BUFFER_SIZE: usize = 6;
        let (slot_bundle_tx, slot_bundle_rx) = channel(SLOT_BUNDLE_BUFFER_SIZE);

        try_join!(
            run_decode_payloads(payload_day_stream, payload_tx),
            run_bundle_payloads(bundle_id, payload_rx, slot_bundle_tx),
            run_store_bundles(slot_bundle_rx),
        )?;

        // As we process concurrently on a sudden shut down, we may lose payloads we
        // processed before this one by skipping over them when we resume.
        progress::write_progress(object_meta)?;
    }

    Ok(())
}
