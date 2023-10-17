use migrate_payload_archive::env::ENV_CONFIG;
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, ObjectStore, RetryConfig,
};
use tracing::info;

const GCS_BUCKET: &str = "execution-payload-archive";

pub fn get_gcs_object_store() -> anyhow::Result<impl ObjectStore> {
    // We can't read directly from the gcs_store so mimic what it does. No need to blow up if we fail.
    info!(gcs_bucket = GCS_BUCKET, "using GCS store");
    let gcs_store = GoogleCloudStorageBuilder::new()
        .with_service_account_path("./gcs_secret.json")
        .with_bucket_name(GCS_BUCKET)
        .with_retry(RetryConfig::default())
        .build()?;
    Ok(gcs_store)
}

pub fn get_ovh_object_store() -> anyhow::Result<impl ObjectStore> {
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
