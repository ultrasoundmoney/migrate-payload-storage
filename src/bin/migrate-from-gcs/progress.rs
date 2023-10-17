use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

use object_store::ObjectMeta;
use tracing::{debug, info};

const PROGRESS_FILE_PATH: &str = "progress.json";

pub fn read_progress() -> anyhow::Result<Option<String>> {
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

pub fn write_progress(last_file: &ObjectMeta) -> anyhow::Result<()> {
    info!(last_file = %last_file.location, "writing progress");
    let mut file = File::create(Path::new(PROGRESS_FILE_PATH))?;
    let progress = format!("{}", last_file.location);
    file.write_all(progress.as_bytes())?;
    Ok(())
}
