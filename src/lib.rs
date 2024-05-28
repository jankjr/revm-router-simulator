use ethers::types as etrs;
use eyre::Context;
use revm::db::CacheDB;
use revm::{primitives::Address, Evm};
use serde::de::DeserializeOwned;
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;

use warp::{Filter, Rejection, Reply};

pub mod cannonical;
pub mod config;
use cannonical::{CannonicalFork, Forked};

use config::Config;
pub mod errors;

use simulation::{EthCallManyRequest, SimulateBundleRequest};

pub mod simulation;

pub const LOGGER_TARGET_MAIN: &str = "slot0";
pub const LOGGER_TARGET_SYNC: &str = "slot0::sync";
pub const LOGGER_TARGET_API: &str = "slot0::api";
pub const LOGGER_TARGET_SIMULATION: &str = "slot0::sim";

pub struct ApplicationState {
    pub cannonical: Arc<RwLock<CannonicalFork>>,
    pub erc20_abis: Arc<ethers::contract::BaseContract>,
}

impl ApplicationState {
    pub async fn create(
        config: Config,
        provider: ethers::providers::Provider<ethers::providers::Ws>,
        provider_trace: ethers::providers::Provider<ethers::providers::Http>,
        fork_block: etrs::Block<etrs::H256>,
        watched: Vec<(String, Vec<String>)>,
    ) -> eyre::Result<Self> {
        let out = ethers::contract::BaseContract::from(
            ethers::abi::parse_abi(&[
                "function approve(address spender, uint256 amount) external",
                "function transfer(address to, uint256 amount) external",
            ])
            .wrap_err("Failed to parse ERC20 abi")?,
        );

        
        let out = Self {
            cannonical: Arc::new(RwLock::new(CannonicalFork::new(
                provider,
                provider_trace,
                fork_block,
                config.clone()
            ))),
            erc20_abis: Arc::new(out),
        };

        if watched.is_empty() {
            log::info!(target: LOGGER_TARGET_SYNC, "No watched positions provided, skipping loading of watched positions");
            return Ok(out);
        }

        let initial_watched: Vec<(Address, Vec<revm::primitives::U256>)> = watched
            .into_iter()
            .filter_map(|(address_str, list_of_positions)| {
                let address = Address::from_str(address_str.as_str()).ok()?;
                let positions: Vec<revm::primitives::U256> = list_of_positions
                    .into_iter()
                    .filter_map(|pos| revm::primitives::U256::from_str(pos.as_str()).ok())
                    .collect();
                Some((address, positions))
            })
            .collect();

        log::info!(target: LOGGER_TARGET_SYNC, "Loading previously watched positions into cannonical fork");
        out.cannonical
            .write()
            .await
            .load_positions(initial_watched)
            .await
            .wrap_err("Failed to load initial watched positions")?;

        Ok(out)
    }
    pub async fn fork<'a>(&self) -> Evm<'a, (), CacheDB<Forked>> {
        // Forks the current state of the cannonical fork into into a new EVM instance with
        // it's own CacheDB that does the following:
        // All read that are not found in the fork, get loaded from the cannonical fork, which will either pull
        // it from the provider if the value is not present, or give it back a value it has in it's own cache

        // The cannonical fork will keep track of all previously read values from simulations
        // The cannonical depends on a RPC it can trust to provice accurate storage slot diffs between each block
        let out = revm::db::CacheDB::<Forked>::new(Forked {
            cannonical: self.cannonical.clone(),
        });

        let env = out.db.cannonical.read().await.block_env();

        let out = revm::Evm::builder()
            .with_block_env(env)
            .with_db(out)
            .with_spec_id(revm::primitives::SpecId::CANCUN)
            .build();

        out
    }
}

pub fn simulate_routes(
    config: Config,
    state: Arc<ApplicationState>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    simulate_bundle(config.clone(), state.clone()).or(call_many(config.clone(), state.clone()))
}

pub fn simulate_bundle(
    config: Config,
    state: Arc<ApplicationState>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("simulate")
        .and(warp::post())
        .and(json_body::<SimulateBundleRequest>(&config))
        .and(with_config(config))
        .and(with_state(state))
        .and_then(simulation::simulate_bundle)
}

pub fn call_many(
    config: Config,
    state: Arc<ApplicationState>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    warp::path!("rpc")
        .and(warp::post())
        .and(json_body::<EthCallManyRequest>(&config))
        .and(with_config(config))
        .and(with_state(state))
        .and_then(simulation::simulate_call_many)
}

fn with_state(
    state: Arc<ApplicationState>,
) -> impl Filter<Extract = (Arc<ApplicationState>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || state.clone())
}

fn json_body<T: DeserializeOwned + Send>(
    config: &Config,
) -> impl Filter<Extract = (T,), Error = Rejection> + Clone {
    warp::body::content_length_limit(config.max_request_size).and(warp::body::json())
}

fn with_config(
    config: Config,
) -> impl Filter<Extract = (Config,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || config.clone())
}
