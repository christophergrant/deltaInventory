use env_logger::Builder;
use std::env;
use std::io::Write;
use delta_profiler::{serde_profile, profile_table};

#[tokio::main]
pub async fn main() {

    let mut builder = Builder::from_default_env();
    builder.format(|buf, record| {
        writeln!(
            buf,
            "{} [{}] - {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level(),
            record.args()
        )
    });
    builder.init();

    let args: Vec<String> = env::args().collect();

    // Check if the user has passed the required argument (the table path)
    if args.len() < 2 {
        eprintln!("Usage: {} <table_path>", args[0]);
        std::process::exit(1);
    }

    let table_uri = &args[1];

    serde_profile(profile_table(table_uri).await)
}