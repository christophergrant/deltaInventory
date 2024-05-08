use delta_profiler;
use std::path::PathBuf;

#[tokio::test]
async fn basic_load() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("python/tables/golden-goose");
    let f = format!("{}{}", "file://", path.into_os_string().to_str().unwrap());
    let result = delta_profiler::profile_table(&f).await;
    println!(" {:#?}", result);
}
#[tokio::test]
async fn main() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("python/tables/golden-goose");
    let f = format!("{}{}", "file://", path.into_os_string().to_str().unwrap());
    delta_profiler::serde_profile(delta_profiler::profile_table(&f).await);
}
