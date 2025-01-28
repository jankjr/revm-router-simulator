use alloy::rpc::types::Block;
use alloy::transports::BoxTransport;
use eyre::Context;
use revm::db::CacheDB;
use revm::interpreter::{CallInputs, CallOutcome};
use revm::primitives::{Address, HashMap};
use revm::EvmContext;
use serde::de::DeserializeOwned;
use std::{str::FromStr, sync::Arc};

use warp::{Filter, Rejection, Reply};

pub mod cannonical;
pub mod config;
use cannonical::{CannonicalFork, Forked};

use config::Config;
pub mod errors;

use simulation::SimulateBundleRequest;

pub mod simulation;

pub const LOGGER_TARGET_MAIN: &str = "slot0";
pub const LOGGER_TARGET_SYNC: &str = "slot0::sync";
pub const LOGGER_TARGET_API: &str = "slot0::api";
pub const LOGGER_TARGET_SIMULATION: &str = "slot0::sim";

pub struct ApplicationState {
    pub cannonical: Arc<CannonicalFork>,
    pub config: Config,
}

pub struct LogTracer {
    executor: revm::primitives::Address,
    transfer_topic: revm::primitives::FixedBytes<32>,
    deltas: HashMap<
        revm::primitives::Address,
        HashMap<revm::primitives::Address, revm::primitives::I256>,
    >,
    calls: Vec<(revm::primitives::Address, revm::primitives::Bytes)>,
}
impl LogTracer {
    fn new(executor: revm::primitives::Address) -> Self {
        Self {
            executor,
            transfer_topic: revm::primitives::FixedBytes::<32>::from_str(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            )
            .unwrap(),
            deltas: HashMap::new(),
            calls: Vec::new(),
        }
    }
    fn add_delta(
        &mut self,
        token: revm::primitives::Address,
        owner: revm::primitives::Address,
        delta: revm::primitives::I256,
    ) {
        if !self.deltas.contains_key(&owner) {
            self.deltas.insert(owner, HashMap::new());
        }
        let balances = self.deltas.get_mut(&owner).unwrap();
        let previous = balances
            .get(&token)
            .unwrap_or(&revm::primitives::I256::ZERO);
        balances.insert(token, previous.wrapping_add(delta));
    }
}

impl revm::Inspector<&mut CacheDB<Forked>> for LogTracer {
    fn call(
        &mut self,
        _: &mut EvmContext<&mut CacheDB<Forked>>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        if !self.executor.is_zero() && inputs.target_address == self.executor {
            log::trace!(target: LOGGER_TARGET_SIMULATION, "CALL to={}, data={}", inputs.target_address, inputs.input.to_string());
            self.calls
                .push((inputs.target_address, inputs.input.clone()));
        }
        None
    }

    fn log(
        &mut self,
        _: &mut revm::interpreter::Interpreter,
        __: &mut EvmContext<&mut CacheDB<Forked>>,
        log: &revm::primitives::Log,
    ) {
        let topics = log.topics();

        if topics[0] != self.transfer_topic || topics.len() != 3 || log.data.data.len() < 32 {
            return;
        }

        let from = revm::primitives::Address::from_word(topics[1]);
        let to = revm::primitives::Address::from_word(topics[2]);
        let value =
            revm::primitives::I256::try_from_be_slice(&log.data.data.0[0..32]).unwrap_or_default();
        self.add_delta(log.address, from, -value);
        self.add_delta(log.address, to, value);
    }
}
impl ApplicationState {
    pub async fn create(
        config: Config,
        provider: alloy::providers::RootProvider<BoxTransport>,
        fork_block: Block,
        watched: Vec<(String, Vec<String>)>,
    ) -> eyre::Result<Self> {
        let out = Self {
            cannonical: Arc::new(CannonicalFork::new(provider, fork_block, config.clone())),
            config: config.clone(),
        };

        if watched.is_empty() {
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

        out.cannonical
            .load_positions(initial_watched)
            .await
            .wrap_err("Failed to load initial watched positions")?;

        Ok(out)
    }
    pub async fn fork_db<'a>(&self) -> CacheDB<Forked> {
        // Forks the current state of the cannonical fork into into a new EVM instance with
        // it's own CacheDB that does the following:
        // All read that are not found in the fork, get loaded from the cannonical fork, which will either pull
        // it from the provider if the value is not present, or give it back a value it has in it's own cache

        // The cannonical fork will keep track of all previously read values from simulations
        // The cannonical depends on a RPC it can trust to provice accurate storage slot diffs between each block
        let fork = self.cannonical.clone();
        let block_env = fork.block_env().await;
        let db = revm::db::CacheDB::<Forked>::new(Forked {
            cannonical: fork,
            env: block_env.clone(),
            seconds_per_block: revm::primitives::U256::from(12u64),
        });
        db
    }
}

pub fn simulate_routes(
    config: Config,
    state: Arc<ApplicationState>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    simulate_bundle(config.clone(), state.clone())
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
