use std::str::FromStr;

use dotenvy::dotenv;

#[derive(Debug, Clone)]
pub struct Config {
    pub link_type: LinkType,
    pub cache_watched: bool,
    pub port: u16,
    pub fork_url: String,
    pub max_watched_storage_slots: usize,
    pub max_watched_accounts: usize,
    pub executor: revm::primitives::Address,
    pub max_request_size: u64,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum LinkType {
    Reth,
    Geth,
}

impl std::str::FromStr for LinkType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Reth" => Ok(LinkType::Reth),
            "Geth" => Ok(LinkType::Geth),
            _ => Err("Invalid LinkType".to_string()),
        }
    }
}

pub fn load_config_from_env() -> Config {
    dotenv().ok();
    let link_type = std::env::var("RPC_TYPE")
        .unwrap_or("Reth".to_string())
        .parse::<LinkType>()
        .expect("RPC_TYPE must be a valid LinkType.");
    let cache_watched = std::env::var("CACHE_WATCHED")
        .unwrap_or("true".to_string())
        .parse::<bool>()
        .expect("CACHE_WATCHED must be a valid boolean.");
    let port = std::env::var("PORT")
        .unwrap_or("8080".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid u16.");
    let fork_url = std::env::var("FORK_URL").unwrap();
    let executor = std::env::var("EXECUTOR")
        .unwrap_or("0x0000000000000000000000000000000000000000".to_string());
    let executor =
        revm::primitives::Address::from_str(executor.as_str()).expect("Invalid executor address");
    let max_request_size = std::env::var("MAX_REQUEST_SIZE")
        .unwrap_or("1048576".to_string())
        .parse::<u64>()
        .expect("MAX_REQUEST_SIZE must be a valid u64");

    let max_watched_storage_slots = std::env::var("MAX_WATCHED_STORAGE_SLOTS")
        .unwrap_or("67108864".to_string())
        .parse::<usize>()
        .expect("MAX_WATCHED_STORAGE_SLOTS must be a valid usize");

    let max_watched_accounts = std::env::var("MAX_WATCHED_STORAGE_SLOTS")
        .unwrap_or("1048576".to_string())
        .parse::<usize>()
        .expect("MAX_WATCHED_STORAGE_SLOTS must be a valid usize");

    Config {
        link_type,
        cache_watched,
        fork_url,
        port,
        max_watched_storage_slots,
        max_watched_accounts,
        executor,
        max_request_size,
    }
}
