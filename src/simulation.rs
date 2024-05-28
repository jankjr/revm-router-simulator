use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use ethers::abi::{Address as EthrsAddress, Hash, Uint};

use ethers::types::{BigEndianHash, Bytes};
use ethers::utils::hex::ToHexExt;
use revm::db::{CacheDB, DbAccount};
use revm::primitives::alloy_primitives::U64;
use revm::primitives::{Address, Bytecode, TransactTo};
use serde::{Deserialize, Serialize};
use warp::reply::Json;
use warp::Rejection;

use crate::LOGGER_TARGET_SIMULATION;

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

    pub from: EthrsAddress,
    pub to: EthrsAddress,
    pub data: Option<Bytes>,

    pub gas: Option<PermissiveUint>,
    pub value: Option<PermissiveUint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalSetup {
    pub owner: EthrsAddress,
    pub token: EthrsAddress,
    pub spender: EthrsAddress,
    pub value: PermissiveUint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveFunds {
    pub owner: EthrsAddress,
    pub spender: EthrsAddress,
    pub token: EthrsAddress,
    pub quantity: PermissiveUint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SimulateBundleRequest {
    pub setup_approvals: Option<Vec<ApprovalSetup>>,
    pub move_funds: Option<Vec<MoveFunds>>,

    pub transactions: Vec<TransactionRequest>,
    pub state_override: Option<HashMap<EthrsAddress, StateOverride>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthCallManyRequest {
    pub method: String,
    pub jsonrpc: String,
    pub id: u64,
    pub params: EthCallManyParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallManyBundle {
    pub transactions: Vec<TransactionRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EthCallManyParams {
    #[serde(rename_all = "camelCase")]
    CallBundle(CallManyBundle),
    CallBundleWithStateContext(CallManyBundle, StateContext),
    CallBundleWithAllParams(
        CallManyBundle,
        StateContext,
        HashMap<EthrsAddress, StateOverride>,
    ),
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

pub async fn simulate_call_many(
    request: EthCallManyRequest,
    _: Config,
    app_state: Arc<ApplicationState>,
) -> Result<Json, Rejection> {
    let handle = tokio::spawn(async move {
        let (bundle, _, state_override) = match request.params {
            EthCallManyParams::CallBundle(bundle) => (bundle, None, None),
            EthCallManyParams::CallBundleWithStateContext(bundle, state_context) => {
                (bundle, Some(state_context), None)
            }
            EthCallManyParams::CallBundleWithAllParams(bundle, state_context, state_override) => {
                (bundle, Some(state_context), Some(state_override))
            }
        };
        let mut response = Vec::<SimulationResponse>::with_capacity(bundle.transactions.len());
        let mut sim_fork: revm::Evm<(), CacheDB<Forked>> = app_state.fork().await;
        if let Some(state_override) = state_override {
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

        for tx in bundle.transactions {
            sim_fork = sim_fork
                .modify()
                .modify_tx_env(|env| {
                    env.caller = Address::from(tx.from.0);
                    env.data = tx.data.unwrap_or_default().0.into();
                    env.value = revm::primitives::U256::from_limbs(
                        Uint::from(tx.value.unwrap_or_default().0).0,
                    );
                    env.gas_limit = 10000000u64;
                    env.gas_price = revm::primitives::U256::from(1u64);
                    env.transact_to = TransactTo::Call(Address::from(tx.to.0));
                })
                .build();

            let tx = sim_fork.transact_commit();
            response.push(commit_result_into_resp(tx));
        }

        response
    });

    let out = handle.await;
    match out {
        Ok(out) => Ok(warp::reply::json(&out)),
        Err(e) => {
            log::error!(target: LOGGER_TARGET_SIMULATION, "Join error {:?}", e);
            Err(warp::reject::reject())
        }
    }
}

fn commit_result_into_resp(
    tx: Result<revm::primitives::ExecutionResult, revm::primitives::EVMError<eyre::Report>>,
) -> SimulationResponse {
    match tx {
        Ok(v) => match v {
            revm::primitives::ExecutionResult::Success {
                reason: _,
                gas_used,
                gas_refunded: _,
                logs,
                output,
            } => {
                let ouput = match output {
                    revm::primitives::Output::Call(data) => data,
                    revm::primitives::Output::Create(_, _) => revm::primitives::Bytes::default(),
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
        },
        Err(e) => SimulationResponse::Error {
            gas_used: 0u64.into(),
            value: "0x".to_string(),
            message: e.to_string(),
        },
    }
}

pub async fn simulate_bundle(
    request: SimulateBundleRequest,
    _: Config,
    app_state: Arc<ApplicationState>,
) -> Result<Json, Rejection> {
    let handle = tokio::spawn(async move {
        let abi = app_state.erc20_abis.clone();
        let mut response = Vec::<SimulationResponse>::with_capacity(request.transactions.len());

        let mut sim_fork = app_state.fork().await;

        log::info!(target: LOGGER_TARGET_SIMULATION, "Simulator started. Fork block {}", sim_fork.block().number);

        let total_gas_in_block = U64::from(sim_fork.block().gas_limit);

        let total_transactions: u64 = {
            let mut total: usize = 0;
            let request = &request;
            let setup_approvals = &request.setup_approvals;
            let move_funds = &request.move_funds;
            let transactions = &request.transactions;
            if let Some(setup_approvals) = setup_approvals {
                total += setup_approvals.len();
            }
            if let Some(txes) = move_funds {
                total += txes.len()
            }
            total += transactions.len();
            total as u64
        };

        let mut total_gas_u64: u64 = total_gas_in_block.to();
        total_gas_u64 = total_gas_u64 - total_gas_u64 / 10;
        let gas_per_transaction = total_gas_u64 / total_transactions;

        let gas_pr_setup_tx = gas_per_transaction / 4;
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

        for approval in request.setup_approvals.unwrap_or_default().iter() {
            let encoded = abi
                .encode(
                    "approve",
                    (approval.spender, ethers::types::U256::from(approval.value)),
                )
                .unwrap();
            sim_fork = sim_fork
                .modify()
                .modify_tx_env(|env| {
                    env.caller = Address::from(approval.owner.0);
                    env.data = encoded.0.into();
                    env.value = revm::primitives::U256::from_limbs(Uint::from(0u64).0);
                    env.gas_limit = gas_pr_setup_tx;
                    env.gas_price = revm::primitives::U256::from(1);
                    env.transact_to = TransactTo::Call(Address::from(approval.token.0));
                })
                .build();

            let tx = sim_fork.transact_commit();
            log::debug!(target: LOGGER_TARGET_SIMULATION, "{:?}", tx);
            response.push(commit_result_into_resp(tx));
        }

        for movement in request.move_funds.unwrap_or_default() {
            let encoded = abi
                .encode(
                    "transfer",
                    (
                        movement.spender,
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
                    env.gas_limit = gas_pr_setup_tx;
                    env.gas_price = revm::primitives::U256::from(1);
                    env.transact_to = TransactTo::Call(Address::from(movement.token.0));
                })
                .build();

            let tx = sim_fork.transact_commit();
            response.push(commit_result_into_resp(tx));
        }

        for tx in request.transactions {
            sim_fork = sim_fork
                .modify()
                .modify_tx_env(|env| {
                    env.caller = Address::from(tx.from.0);
                    env.data = tx.data.unwrap_or_default().0.into();
                    env.value = revm::primitives::U256::from_limbs(
                        Uint::from(tx.value.unwrap_or_default().0).0,
                    );
                    env.gas_limit = gas_per_transaction;
                    env.gas_price = revm::primitives::U256::from(1u64);
                    env.transact_to = TransactTo::Call(Address::from(tx.to.0));
                })
                .build();

            let tx = sim_fork.transact_commit();
            response.push(commit_result_into_resp(tx));
        }
        response
    });

    let out = handle.await;
    match out {
        Ok(out) => Ok(warp::reply::json(&out)),
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
