use aws_credential_types::provider::ProvideCredentials;
use aws_config::meta::region::RegionProviderChain;
use std::collections::HashMap;

// Separate async function to retrieve storage options
pub async fn get_storage_options() -> HashMap<String, String> {
    let config_future = aws_config::load_from_env(); 
    let config = config_future.await; 
    let credentials = config.credentials_provider().unwrap().provide_credentials().await.unwrap();
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");

    HashMap::from([
        (String::from("access_key_id"), String::from(credentials.access_key_id())),
        (String::from("secret_access_key"), String::from(credentials.secret_access_key())),
        (String::from("token"), String::from(credentials.session_token().unwrap_or_default())),
        (String::from("region"), String::from(region_provider.region().await.unwrap().to_string()))
    ])
}

