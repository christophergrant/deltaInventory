use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use deltalake::{aws, Path};
use deltalake_core::errors::DeltaTableError;
use futures::StreamExt;
use log::debug;
use url::Url;
use aws_credential_types::provider::ProvideCredentials;
use aws_config::meta::region::RegionProviderChain;

#[derive(Serialize, Deserialize, Debug)]
struct TableStats {
    table_uri: String,
    min_version: i64,
    max_version: i64,
    total_tombstone_count: i64,
    total_tombstone_size: i64,
    total_unexpired_tombstone_count: i64,
    total_unexpired_tombstone_size: i64
}

#[derive(Serialize, Deserialize, Debug)]
struct TombstoneStats {
    total_tombstone_count: i64,
    total_tombstone_size: i64,
    unexpired_tombstone_count: i64,
    unexpired_tombstone_size: i64
}

#[derive(Serialize, Deserialize, Debug)]
struct SnapshotStats {
    version: i64,
    timestamp: i64,
    file_count: usize,
    app_transaction_versions: HashMap<String, i64>
}

#[derive(Serialize, Deserialize, Debug)]
struct TableConfig {
    error: String
}

#[derive(Serialize, Deserialize, Debug)]
struct FileStats {
    log_size: u64,
    log_file_count: u64,
    total_file_count: u64,
    total_size: u64,
    cdf_file_count: u64,
    cdf_size: u64
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableProfile {
    config: Option<TableConfig>,
    snapshot_stats: Option<SnapshotStats>,
    file_stats: Option<FileStats>,
    tombstone_stats: Option<TombstoneStats>,
}


enum SupportedScheme {
    LocalFile,
    S3,
}

impl SupportedScheme {
    async fn initialize(&self) -> Result<HashMap<String, String>, Box<dyn Error>> {
        match self {
            SupportedScheme::LocalFile => self.init_local_filesystem(),
            SupportedScheme::S3 => self.init_s3().await,
        }
    }

    fn init_local_filesystem(&self) -> Result<HashMap<String, String>, Box<dyn Error>> {
        Ok(HashMap::new())
    }

    async fn init_s3(&self) -> Result<HashMap<String, String>, Box<dyn Error>> {
        debug!("Initializing S3 storage...");
        let settings = get_storage_options().await;
        aws::register_handlers(None);
        settings
    }
}

fn determine_scheme(url: &str) -> Result<SupportedScheme, DeltaTableError> {
    let parsed_url = Url::parse(url);
    match parsed_url {
        Ok(url) => match url.scheme() {
            "s3" => Ok(SupportedScheme::S3),
            "file" | "" => Ok(SupportedScheme::LocalFile),
            _ => Err(DeltaTableError::InvalidTableLocation(String::from("Invalid scheme"))),
        },
        Err(_) => Err(DeltaTableError::Generic(String::from("Failed Parsing URL"))),
    }
}


pub async fn profile_table(table_uri: &str) -> Result<TableProfile, Box<dyn Error>> {

    // the idea with this pattern is to return some kind of error to calling functions that are not
    // directly accessing this Rust application. otherwise would not use.
    let scheme: SupportedScheme = determine_scheme(table_uri)?;

    let storage_options = scheme.initialize().await?;


    let table = deltalake::open_table_with_storage_options(table_uri, storage_options).await?;

    let snapshot = table.snapshot().unwrap();

    let snapshot_stats = SnapshotStats {
        version: snapshot.version(),
        timestamp: snapshot.version_timestamp(snapshot.version()).unwrap(),
        file_count: snapshot.files_count(),
        app_transaction_versions: snapshot.app_transaction_version().clone()
    };

    let tombstone_stats = TombstoneStats {
        total_tombstone_count: 0,
        total_tombstone_size: 0,
        unexpired_tombstone_count: 0,
        unexpired_tombstone_size: 0
    };

    let object_store = table.object_store();

    let mut list_stream = object_store.list(Some(&Path::from("/")));
    debug!("Listing all files");
    let mut total_file_count = 0;
    let mut log_file_count = 0;
    let mut cdf_file_count = 0;
    let mut total_size = 0u64;
    let mut log_size = 0u64;
    let mut cdf_size = 0u64;

    while let Some(meta) = list_stream.next().await.transpose()? {
        // Increment total files count
        total_file_count += 1;
        // Add the size of the current file to total size
        total_size += meta.size as u64;

        // Check if the file is inside the '_delta_log' directory
        if meta.location.prefix_matches(&Path::from("_delta_log/")) {
            // Increment delta log files count
            log_file_count += 1;
            // Add the size of the file to delta log size
            log_size += meta.size as u64;
        } else if meta.location.prefix_matches(&Path::from("_change_data/")){
            cdf_file_count += 1;
            // Add the size of the file to delta log size
            cdf_size += meta.size as u64;
        }
    }

    let file_stats = FileStats {
        log_size,
        log_file_count,
        total_file_count,
        total_size,
        cdf_file_count,
        cdf_size
    };

    Ok(TableProfile{
        config: None,
        snapshot_stats: Some(snapshot_stats),
        file_stats: Some(file_stats),
        tombstone_stats: Some(tombstone_stats),
    })
}

pub fn serde_profile(table_profile: Result<TableProfile, Box<dyn std::error::Error>>) {
    match table_profile {
        Ok(stats) => println!("{}", serde_json::to_string(&stats).unwrap()),
        Err(e) => eprintln!("{}", serde_json::to_string(&format!("Failed to access Delta table: {}", e)).unwrap()),
    }
}

// Separate async function to retrieve storage options
pub async fn get_storage_options() -> Result<HashMap<String, String>, Box<dyn Error>> {
    // Load AWS config from the environment asynchronously
    let config = aws_config::load_from_env().await;

    // Attempt to retrieve credentials, handling errors properly
    let credentials = config.credentials_provider()
        .ok_or("No credentials provider found")?
        .provide_credentials().await?;

    // Load region, handling errors properly
    let region_provider = RegionProviderChain::default_provider();
    let region = region_provider.region().await
        .ok_or("No region found")?.to_string();

    // Create a HashMap with the necessary information
    Ok(HashMap::from([
        (String::from("access_key_id"), credentials.access_key_id().to_string()),
        (String::from("secret_access_key"), credentials.secret_access_key().to_string()),
        (String::from("token"), credentials.session_token().unwrap_or_default().to_string()),
        (String::from("region"), region)
    ]))
}
