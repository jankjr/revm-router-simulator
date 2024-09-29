use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use ethers::abi::{Address as EthrsAddress, Hash, Uint};

use ethers::types::{BigEndianHash, Bytes};
use ethers::utils::hex::ToHexExt;
use revm::db::{CacheDB, DbAccount};
use revm::primitives::{Address, Bytecode, TransactTo};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use warp::reply::Json;
use warp::Rejection;

use crate::{LogTracer, LOGGER_TARGET_SIMULATION};

use crate::errors::OverrideError;
use crate::{ApplicationState, Forked};

use super::config::Config;

#[derive(Debug, Default, Clone, Copy, Serialize, PartialEq)]
#[serde(transparent)]
pub struct PermissiveUint(pub Uint);

impl From<PermissiveUint> for Uint {
    fn from(value: PermissiveUint) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for PermissiveUint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Accept value in hex or decimal formats
        let value = String::deserialize(deserializer)?;
        let parsed = if value.starts_with("0x") {
            Uint::from_str(&value).map_err(serde::de::Error::custom)?
        } else {
            Uint::from_dec_str(&value).map_err(serde::de::Error::custom)?
        };
        Ok(Self(parsed))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRequest {
    pub chain_id: Option<u64>,

    pub from: revm::primitives::Address,
    pub to: revm::primitives::Address,
    pub data: Option<Bytes>,

    pub gas: Option<PermissiveUint>,
    pub value: Option<PermissiveUint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalSetup {
    pub owner: revm::primitives::Address,
    pub token: revm::primitives::Address,
    pub spender: revm::primitives::Address,
    pub value: PermissiveUint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveFunds {
    pub owner: revm::primitives::Address,
    pub spender: revm::primitives::Address,
    pub token: revm::primitives::Address,
    pub quantity: PermissiveUint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SimulateBundleRequest {
    pub setup_approvals: Option<Vec<ApprovalSetup>>,
    pub addresses: Option<Vec<revm::primitives::Address>>,
    pub move_funds: Option<Vec<MoveFunds>>,

    pub transactions: Vec<TransactionRequest>,
    pub state_override: Option<HashMap<EthrsAddress, StateOverride>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallManyBundle {
    pub transactions: Vec<TransactionRequest>,
    pub addresses: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateContext {
    pub block_number: Option<PermissiveUint>,
    pub transaction_index: Option<PermissiveUint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateOverride {
    pub balance: Option<PermissiveUint>,
    pub nonce: Option<PermissiveUint>,
    pub code: Option<Bytes>,
    #[serde(flatten)]
    pub state: Option<State>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum State {
    Full {
        state: HashMap<Hash, PermissiveUint>,
    },
    #[serde(rename_all = "camelCase")]
    Diff {
        state_diff: HashMap<Hash, PermissiveUint>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct StorageOverride {
    pub slots: HashMap<Hash, Uint>,
    pub diff: bool,
}

impl From<State> for StorageOverride {
    fn from(value: State) -> Self {
        let (slots, diff) = match value {
            State::Full { state } => (state, false),
            State::Diff { state_diff } => (state_diff, true),
        };

        StorageOverride {
            slots: slots
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            diff,
        }
    }
}

fn override_account(
    exec: &mut CacheDB<Forked>,
    address: Address,
    balance: Option<Uint>,
    nonce: Option<Uint>,
    code: Option<Bytes>,
    storage: Option<StorageOverride>,
) -> Result<(), OverrideError> {
    let account = exec
        .accounts
        .entry(revm::primitives::Address::from(address.0));

    let account = match account {
        Entry::Occupied(account) => account.into_mut(),
        Entry::Vacant(e) => e.insert(DbAccount::default()),
    };
    if let Some(balance) = balance {
        account.info.balance = revm::primitives::U256::from_limbs(balance.0);
    }
    if let Some(nonce) = nonce {
        account.info.nonce = nonce.as_u64();
    }
    if let Some(code) = code {
        account.info.code = Some(Bytecode::new_raw(code.to_vec().into()));
    }
    if let Some(storage) = storage {
        account
            .storage
            .extend(storage.slots.into_iter().map(|(key, value)| {
                (
                    revm::primitives::U256::from_limbs(key.into_uint().0),
                    revm::primitives::U256::from_limbs(value.0),
                )
            }));
    }

    Ok(())
}

fn commit_result_into_resp(
    tx: Result<revm::primitives::ExecutionResult, revm::primitives::EVMError<eyre::Report>>,
) -> SimulationResponse {
    let out = match tx {
        Ok(v) => {
            if !v.is_success() {
                log::error!(target: LOGGER_TARGET_SIMULATION, "Transaction failed {:?}", v);
            }
            match v {
                revm::primitives::ExecutionResult::Success {
                    reason: _,
                    gas_used,
                    gas_refunded: _,
                    logs,
                    output,
                } => {
                    let ouput = match output {
                        revm::primitives::Output::Call(data) => data,
                        revm::primitives::Output::Create(_, _) => {
                            revm::primitives::Bytes::default()
                        }
                    };

                    return SimulationResponse::Success {
                        value: ouput.encode_hex_with_prefix(),
                        gas_used: gas_used.into(),
                        logs: logs
                            .iter()
                            .map(|log| Log {
                                address: log.address.encode_hex_with_prefix(),
                                topics: log
                                    .topics()
                                    .iter()
                                    .map(|topic| topic.encode_hex_with_prefix())
                                    .collect(),
                                data: log.data.data.encode_hex_with_prefix(),
                            })
                            .collect(),
                    };
                }
                revm::primitives::ExecutionResult::Revert { gas_used, output } => {
                    SimulationResponse::Error {
                        value: output.encode_hex_with_prefix(),
                        gas_used: gas_used.into(),
                        message: "Reverted".to_string(),
                    }
                }
                revm::primitives::ExecutionResult::Halt { reason, gas_used } => match reason {
                    revm::primitives::HaltReason::OutOfGas(_) => SimulationResponse::Error {
                        value: "0x".to_string(),
                        gas_used: gas_used.into(),
                        message: "Out out gas".to_string(),
                    },
                    _ => SimulationResponse::Error {
                        value: "0x".to_string(),
                        gas_used: gas_used.into(),
                        message: "Unknown".to_string(),
                    },
                },
            }
        }
        Err(e) => SimulationResponse::Error {
            gas_used: 0u64.into(),
            value: "0x".to_string(),
            message: e.to_string(),
        },
    };
    out
}

pub async fn simulate_bundle(
    request: SimulateBundleRequest,
    _: Config,
    app_state: Arc<ApplicationState>,
) -> Result<Json, Rejection> {
    let handle: JoinHandle<eyre::Result<Vec<SimulationResponse>>> = tokio::spawn(async move {
        let abi = app_state.erc20_abis.clone();
        let mut response = Vec::<SimulationResponse>::with_capacity(request.transactions.len());

        let cannonical = app_state.cannonical.clone();
        let approvals = request.setup_approvals.unwrap_or_default();
        let movements = request.move_funds.unwrap_or_default();
        let transactions = request.transactions;

        let addreses_in_request = approvals
            .iter()
            .map(|x| vec![x.token.clone(), x.spender.clone(), x.owner.clone()])
            .flatten()
            .chain(
                movements
                    .iter()
                    .map(|x| vec![x.token.clone(), x.spender.clone(), x.owner.clone()])
                    .flatten(),
            )
            .chain(transactions.iter().map(|x| x.to.clone()))
            .chain(request.addresses.unwrap_or_default())
            .collect::<HashSet<revm::primitives::Address>>()
            .into_iter()
            .collect::<Vec<_>>();

        let addreses_to_preload: Vec<(Address, Vec<revm::primitives::U256>)> = addreses_in_request
            .into_iter()
            .map(|addr| {
                (
                    addr,
                    (0u64..10u64)
                        .map(|v| revm::primitives::U256::from(v))
                        .collect::<Vec<revm::primitives::U256>>(),
                )
            })
            .collect::<Vec<_>>();
        if addreses_to_preload.len() > 0 {
            let cannonical = app_state.cannonical.clone();
            if let Err(e) = cannonical.load_positions(addreses_to_preload).await {
                log::error!(target: LOGGER_TARGET_SIMULATION, "Failed to preload addresses {}", e);
            }
        }
        let block_env = cannonical.block_env().await;

        let mut db = app_state.fork_db().await;

        {
            let db = db.borrow_mut();
            let mut sim_fork = revm::Evm::builder()
                .with_block_env(block_env.clone())
                .modify_cfg_env(|f| {
                    f.memory_limit = 1024 * 1024 * 64;
                })
                .with_db(db)
                .with_spec_id(revm::primitives::SpecId::CANCUN)
                .build();

            log::info!(target: LOGGER_TARGET_SIMULATION, "Simulator started. Fork block {}", sim_fork.block().number);

            if let Some(state_override) = request.state_override {
                for (address, state_override) in state_override {
                    override_account(
                        sim_fork.db_mut(),
                        Address::from(address.0),
                        state_override.balance.map(Into::into),
                        state_override.nonce.map(Into::into),
                        state_override.code,
                        state_override.state.map(Into::into),
                    )
                    .unwrap();
                }
            }

            for approval in approvals.iter() {
                let encoded = abi
                    .encode(
                        "approve",
                        (
                            EthrsAddress::from_slice(approval.spender.0.as_slice()),
                            ethers::types::U256::from(approval.value),
                        ),
                    )
                    .unwrap();
                sim_fork = sim_fork
                    .modify()
                    .modify_tx_env(|env| {
                        env.caller = Address::from(approval.owner.0);
                        env.data = encoded.0.into();
                        env.value = revm::primitives::U256::from_limbs(Uint::from(0u64).0);
                        env.gas_limit = 100000u64;
                        env.gas_price = revm::primitives::U256::from(1);
                        env.transact_to = TransactTo::Call(Address::from(approval.token.0));
                    })
                    .build();

                let tx = sim_fork.transact_commit();
                response.push(commit_result_into_resp(tx));
            }

            for movement in movements {
                let encoded = abi
                    .encode(
                        "transfer",
                        (
                            EthrsAddress::from_slice(movement.spender.0.as_slice()),
                            ethers::types::U256::from(movement.quantity),
                        ),
                    )
                    .unwrap();

                sim_fork = sim_fork
                    .modify()
                    .modify_tx_env(|env| {
                        env.caller = Address::from(movement.owner.0);
                        env.data = encoded.0.into();
                        env.value = revm::primitives::U256::from(0u64);
                        env.gas_limit = 30000000u64;
                        env.gas_price = revm::primitives::U256::from(1);
                        env.transact_to = TransactTo::Call(Address::from(movement.token.0));
                    })
                    .build();

                let tx = sim_fork.transact_commit();
                response.push(commit_result_into_resp(tx));
            }
        }

        let mut sim_fork = revm::Evm::builder()
            .with_block_env(block_env.clone())
            .modify_cfg_env(|f| {
                f.memory_limit = 1024 * 1024 * 64;
            })
            .with_db(&mut db)
            .with_external_context(LogTracer::new(app_state.config.executor))
            .append_handler_register(revm::inspector_handle_register)
            .with_spec_id(revm::primitives::SpecId::CANCUN)
            .build();

        for tx in transactions {
            sim_fork = sim_fork
                .modify()
                .modify_tx_env(|env| {
                    env.caller = Address::from(tx.from.0);
                    env.data = tx.data.unwrap_or_default().0.into();
                    env.value = revm::primitives::U256::from_limbs(
                        Uint::from(tx.value.unwrap_or_default().0).0,
                    );
                    env.gas_limit = 25000000u64;
                    env.gas_price = revm::primitives::U256::from(1000000000u64);
                    env.transact_to = TransactTo::Call(Address::from(tx.to.0));
                })
                .build();

            let tx = sim_fork.transact_commit();

            if let Err(e) = &tx {
                log::error!(target: LOGGER_TARGET_SIMULATION, "Transaction failed {:?}", e);
            }
            response.push(commit_result_into_resp(tx));
        }
        log::debug!(target: LOGGER_TARGET_SIMULATION, "Simulation finished");
        Ok(response)
    });
    match handle.await {
        Ok(Ok(out)) => Ok(warp::reply::json(&out)),
        Ok(Err(e)) => {
            log::error!(target: LOGGER_TARGET_SIMULATION, "Unknown error {:?}", e);
            Err(warp::reject::reject())
        }
        Err(e) => {
            log::error!(target: LOGGER_TARGET_SIMULATION, "Join error {:?}", e);
            Err(warp::reject::reject())
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SimulationResponse {
    Error {
        gas_used: Uint,
        value: String,
        message: String,
    },
    Success {
        gas_used: Uint,
        value: String,
        logs: Vec<Log>,
    },
}