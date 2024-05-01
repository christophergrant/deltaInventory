use deltalake::{DeltaTableError, Path};
use deltalake::aws;
use async_std::task;
use futures::stream::StreamExt;
use colored::*;
use env_logger::Builder;
use serde::{Serialize, Deserialize};
use std::env;
use std::collections::HashMap;

mod s3;


#[derive(Serialize, Deserialize)]
struct TableStats {
    table_uri: String,
    min_version: i64,
    max_version: i64,
    total_tombstone_count: i64,
    total_tombstone_size: String,
    total_unexpired_tombstone_count: i64,
    total_unexpired_tombstone_size: String,
    error_count: i64,
}


fn human_readable_bytes(bytes: i64) -> String {
    let mut bytes = bytes as f64;
    let units = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut i = 0;
    while bytes >= 1024.0 && i < units.len() - 1 {
        bytes /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", bytes, units[i])
}

async fn load_and_display_snapshot(table_uri: &str) -> Result<TableStats, DeltaTableError> {

    let storage_options = s3::get_storage_options().await;


    aws::register_handlers(None);

    let error_count = 0;
    let table = deltalake::open_table_with_storage_options(table_uri, storage_options).await?;
    let snapshot = table.snapshot()?;
    let object_store = table.object_store();

    let mut min_version = i64::MAX;
    let mut max_version = i64::MIN;
    let mut list_stream = object_store.list(Some(&Path::from("_delta_log/")));

    while let Some(meta) = list_stream.next().await.transpose()? {
        if let Some(file_name) = meta.location.filename() {
            if let Some(version_str) = file_name.strip_suffix(".json") {
                if let Ok(version_num) = version_str.parse::<i64>() {
                    min_version = min_version.min(version_num);
                    max_version = max_version.max(version_num);
                }
            }
        }
    }

    if min_version == i64::MAX || max_version == i64::MIN {
        return Err(DeltaTableError::NotATable(table_uri.to_string()));
    }

    let all_tombstones = snapshot.all_tombstones(object_store.clone()).await?;
    let (total_tombstone_size, total_tombstone_count) = all_tombstones
        .map(|t| t.size.unwrap_or(0))
        .fold((0, 0), |(acc_size, acc_count), size| (acc_size + size, acc_count + 1));

    let unexpired_tombstones = snapshot.unexpired_tombstones(object_store.clone()).await?;
    let (total_unexpired_tombstone_size, total_unexpired_tombstone_count) = unexpired_tombstones
        .map(|t| t.size.unwrap_or(0))
        .fold((0, 0), |(acc_size, acc_count), size| (acc_size + size, acc_count + 1));

    Ok(TableStats {
    table_uri: table_uri.to_string(),
    min_version,
    max_version,
    total_tombstone_count,
    total_tombstone_size: human_readable_bytes(total_tombstone_size), // Assign to field
    total_unexpired_tombstone_count,
    total_unexpired_tombstone_size: human_readable_bytes(total_unexpired_tombstone_size), // Assign to field
    error_count,
    })

}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    // Check if the user has passed the required argument (the table path)
    if args.len() < 2 {
        eprintln!("Usage: {} <table_path>", args[0]);
        std::process::exit(1);
    }

    let table_uri = &args[1];

    task::block_on(async {
        match load_and_display_snapshot(table_uri).await {
            Ok(stats) => println!("{}", serde_json::to_string(&stats).unwrap()),
            Err(e) => eprintln!("{}", serde_json::to_string(&format!("Failed to access Delta table: {}", e)).unwrap()),
        }
    });
}

