use dotenvy::dotenv;

#[derive(Debug, Clone)]
pub struct Config {
    pub link_type: LinkType,
    pub cache_watched: bool,
    pub port: u16,
    pub fork_url: String,
    pub ws_fork_url: String,
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
    let ws_fork_url = std::env::var("WS_FORK_URL").unwrap();
    let max_request_size = std::env::var("MAX_REQUEST_SIZE")
        .unwrap_or("1048576".to_string())
        .parse::<u64>()
        .expect("MAX_REQUEST_SIZE must be a valid u64");

    Config {
        link_type,
        cache_watched,
        fork_url,
        port,
        ws_fork_url,
        max_request_size,
    }
}
