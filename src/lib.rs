use futures::future::try_join_all;

use ethers::{
    providers::Middleware,
    types::BigEndianHash,
    utils::hex::{FromHex, ToHexExt},
};

use dashmap::DashMap;
use ethers::types as etrs;
use eyre::{Context, ContextCompat};
use revm::{
    db::{CacheDB, DatabaseRef},
    primitives::AccountInfo,
    Database,
};
use revm::{
    primitives::{alloy_primitives::aliases as prims, Address, StorageSlot, KECCAK_EMPTY},
    Evm,
};
use serde::de::DeserializeOwned;
use std::{
    borrow::BorrowMut,
    collections::{BTreeMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use tokio::{runtime::Handle, sync::RwLock};

use warp::{Filter, Rejection, Reply};

pub mod config;
use config::{Config, LinkType};
pub mod errors;

use simulation::{EthCallManyRequest, SimulateBundleRequest};

pub mod simulation;

#[derive(Debug, Clone)]
pub struct Forked {
    pub cannonical: Arc<RwLock<CannonicalFork>>,
}

pub const LOGGER_TARGET_MAIN: &str = "slot0";
pub const LOGGER_TARGET_SYNC: &str = "slot0::sync";
pub const LOGGER_TARGET_API: &str = "slot0::api";
pub const LOGGER_TARGET_SIMULATION: &str = "slot0::sim";

impl Forked {
    #[inline]
    fn block_on<F>(f: F) -> F::Output
    where
        F: core::future::Future + Send,
        F::Output: Send,
    {
        match Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                // This essentially equals to tokio::task::spawn_blocking because tokio doesn't
                // allow current_thread runtime to block_in_place
                tokio::runtime::RuntimeFlavor::CurrentThread => std::thread::scope(move |s| {
                    s.spawn(move || {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(f)
                    })
                    .join()
                    .unwrap()
                }),
                _ => tokio::task::block_in_place(move || handle.block_on(f)),
            },
            Err(_) => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f),
        }
    }
}

impl DatabaseRef for Forked {
    type Error = eyre::Error;

    fn basic_ref(
        &self,
        address: revm::primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        let f = async { self.cannonical.write().await.basic(address).await };
        Forked::block_on(f)
    }

    fn code_by_hash_ref(
        &self,
        code_hash: prims::B256,
    ) -> Result<revm::primitives::Bytecode, Self::Error> {
        Forked::block_on(async { self.cannonical.write().await.code_by_hash(code_hash).await })
    }

    fn storage_ref(
        &self,
        address: revm::primitives::Address,
        index: prims::U256,
    ) -> Result<prims::U256, Self::Error> {
        Forked::block_on(async { self.cannonical.write().await.storage(address, index).await })
    }

    fn block_hash_ref(&self, number: prims::U256) -> Result<prims::B256, Self::Error> {
        Forked::block_on(async { self.cannonical.write().await.block_hash(number).await })
    }
}

impl Database for Forked {
    type Error = eyre::Error;

    #[inline]
    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        <Self as DatabaseRef>::basic_ref(self, address)
    }

    #[inline]
    fn code_by_hash(
        &mut self,
        code_hash: prims::B256,
    ) -> Result<revm::primitives::Bytecode, Self::Error> {
        <Self as DatabaseRef>::code_by_hash_ref(self, code_hash)
    }

    #[inline]
    fn storage(
        &mut self,
        address: Address,
        index: prims::U256,
    ) -> Result<prims::U256, Self::Error> {
        <Self as DatabaseRef>::storage_ref(self, address, index)
    }

    #[inline]
    fn block_hash(&mut self, number: prims::U256) -> Result<prims::B256, Self::Error> {
        <Self as DatabaseRef>::block_hash_ref(self, number)
    }
}

async fn load_acc_info(
    provider: ethers::providers::Provider<ethers::providers::Ws>,
    address: Address,
) -> eyre::Result<(Address, AccountInfo)> {
    let account_state = tokio::spawn(async move {
        let ethers_address = etrs::Address::from(address.as_ref());
        tokio::try_join!(
            provider.get_balance(ethers_address, None),
            provider.get_transaction_count(ethers_address, None),
            provider.get_code(ethers_address, None)
        )
    })
    .await
    .wrap_err("Failed to fetch account info")
    .wrap_err("Failed to fetch account info")??;

    let (balance, nonce, code) = account_state;
    let (code, code_hash) = match code.is_empty() {
        false => {
            let code = revm::primitives::Bytecode::new_raw(code.0.into());
            if code.len() == 0 {
                (None, KECCAK_EMPTY)
            } else {
                (Some(code.clone()), code.hash_slow())
            }
        }
        true => (None, KECCAK_EMPTY),
    };
    return Ok((
        address,
        AccountInfo {
            balance: revm::primitives::U256::from_limbs(balance.0),
            nonce: nonce.as_u64(),
            code_hash: code_hash,
            code: code,
        },
    ));
}

async fn load_storage_slot(
    provider: ethers::providers::Provider<ethers::providers::Ws>,
    address: Address,
    index: prims::U256,
) -> eyre::Result<(Address, prims::U256, prims::U256)> {
    let ethers_address = etrs::Address::from(address.as_ref());
    let eindex = etrs::H256::from(index.to_be_bytes());
    let value = provider
        .get_storage_at(ethers_address, eindex, None)
        .await
        .wrap_err_with(|| "Failed to fetch storage slot")?;
    Ok((address, index, prims::U256::from_limbs(value.into_uint().0)))
}

#[derive(Debug)]
pub struct CannonicalFork {
    // Results fetched from the provider and maintained by apply_next_mainnet_block,
    // Contains the full state of the account & storage
    accounts: DashMap<Address, Arc<RwLock<AccountInfo>>>,
    link_type: LinkType,
    storage: DashMap<(Address, prims::U256), Arc<RwLock<StorageSlot>>>,

    watched_accounts: RwLock<DashMap<Address, HashSet<prims::U256>>>,
    pending_basic_reads: DashMap<Address, Arc<tokio::sync::RwLock<revm::primitives::AccountInfo>>>,
    pending_storage_reads: DashMap<(Address, prims::U256), Arc<RwLock<StorageSlot>>>,
    contracts: DashMap<prims::B256, Arc<RwLock<revm::primitives::Bytecode>>>,
    current_block: etrs::Block<etrs::H256>,
    provider: ethers::providers::Provider<ethers::providers::Ws>,
    provider_trace: ethers::providers::Provider<ethers::providers::Http>,

    total_number_of_watched_slots: u64,
    total_number_of_watched_accs: u64,
}

impl CannonicalFork {
    pub fn new(
        link_type: LinkType,
        provider: ethers::providers::Provider<ethers::providers::Ws>,
        provider_trace: ethers::providers::Provider<ethers::providers::Http>,
        fork_block: etrs::Block<etrs::H256>,
    ) -> Self {
        Self {
            link_type,
            accounts: DashMap::new(),
            storage: DashMap::new(),
            watched_accounts: RwLock::new(DashMap::new()),
            pending_basic_reads: DashMap::new(),
            pending_storage_reads: DashMap::new(),
            contracts: DashMap::new(),
            current_block: fork_block,
            provider,
            provider_trace,
            total_number_of_watched_slots: 0,
            total_number_of_watched_accs: 0,
        }
    }

    pub async fn export_watched(&self) -> Vec<(String, Vec<String>)> {
        let watched_accounts = self.watched_accounts.read().await;
        let mut out = Vec::new();
        for entry in watched_accounts.clone().into_iter() {
            let address = entry.0.encode_hex_with_prefix();
            out.push((address, entry.1.iter().map(|x| x.to_string()).collect()));
        }
        out
    }

    pub fn block_env(&self) -> revm::primitives::BlockEnv {
        revm::primitives::BlockEnv {
            number: prims::U256::from(self.current_block.number.unwrap().as_u64()),
            timestamp: prims::U256::from(self.current_block.timestamp.as_u64()),
            gas_limit: prims::U256::from(self.current_block.gas_limit.as_u64()),
            coinbase: revm::primitives::Address::from(self.current_block.author.unwrap().0),
            difficulty: prims::U256::from(self.current_block.difficulty.as_u128()),
            // basefee: prims::U256::from(self.current_block.base_fee_per_gas.unwrap().as_u64()),
            basefee: prims::U256::from(1),
            prevrandao: Some(prims::B256::from(self.current_block.mix_hash.unwrap().0)),
            ..Default::default()
        }
    }

    pub async fn apply_next_mainnet_block_geth(
        &mut self,
        block: etrs::Block<etrs::H256>,
        diffs: Vec<BTreeMap<etrs::H160, etrs::AccountState>>,
    ) -> eyre::Result<()> {
        self.current_block = block.clone();
        let mut number_of_updates = 0;
        for account_diffs in diffs {
            let watched_accounts_reader = self.watched_accounts.read().await;
            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);
                match watched_accounts_reader.get(&addr) {
                    Some(a) => a,
                    None => continue, // We don't care about this account
                };

                let info = match self.accounts.get_mut(&addr) {
                    Some(previous) => previous,
                    None => {
                        continue;
                    }
                };

                let mut info = info.write().await;

                info.balance = match v.balance {
                    Some(value) => revm::primitives::U256::from_limbs(value.0),
                    _ => info.balance,
                };
                info.nonce = match v.nonce {
                    Some(value) => value.as_u64(),
                    _ => info.nonce,
                };
                let new_code = match v.code {
                    Some(value) => Some(revm::primitives::Bytecode::new_raw(
                        revm::primitives::Bytes::from_hex(&value).wrap_err("Failed to hex")?,
                    )),
                    _ => None,
                };
                match new_code {
                    Some(code) => info.code = Some(code),
                    None => (),
                };

                number_of_updates += 1;

                let storage = match v.storage {
                    Some(value) => value,
                    _ => continue,
                };

                let acc_storage_writer = self.storage.borrow_mut();
                let watched_storage_slots = match watched_accounts_reader.get(&addr) {
                    Some(a) => a,
                    None => continue,
                };
                if watched_storage_slots.len() == 0 {
                    continue;
                }
                for (hpos, value) in storage {
                    let pos = revm::primitives::U256::from_limbs(hpos.into_uint().0);
                    if !watched_storage_slots.contains(&pos) {
                        continue;
                    }
                    number_of_updates += 1;
                    let value_to_insert = prims::U256::from_limbs(value.into_uint().0);
                    match acc_storage_writer.entry((addr, pos)) {
                        dashmap::mapref::entry::Entry::Occupied(a) => {
                            let mut prev = a.get().write().await;
                            prev.present_value = value_to_insert;
                        }
                        dashmap::mapref::entry::Entry::Vacant(accs) => {
                            accs.insert(Arc::new(RwLock::new(StorageSlot::new(value_to_insert))));
                        }
                    };
                }
            }
        }

        if number_of_updates != 0 {
            log::debug!(target: LOGGER_TARGET_SYNC, "Applied {} updates from mainnet", number_of_updates);
        }
        Ok(())
    }
    pub fn get_current_block(&self) -> eyre::Result<u64> {
        Ok(self
            .current_block
            .number
            .wrap_err("Current block has no number")?
            .as_u64())
    }

    pub async fn apply_next_block(&mut self, block: etrs::Block<etrs::H256>) -> eyre::Result<()> {
        let block_number = if let Some(number) = block.number {
            number
        } else {
            return Err(eyre::eyre!("Block has no number"));
        };

        log::debug!(target: LOGGER_TARGET_SYNC, "Applying block {:?}", block_number.as_u64());

        self.current_block = block.clone();

        if self.link_type == LinkType::Reth {
            log::debug!(target: LOGGER_TARGET_SYNC, "Using reth trace block to fetch diffs");
            let trace = self
                .provider_trace
                .trace_replay_block_transactions(
                    block_number.into(),
                    vec![ethers::types::TraceType::StateDiff],
                )
                .await
                .wrap_err(format!("Failed to fetch trace for {}", block_number))?;

            log::debug!("Applying reth diffs to state");
            self.apply_next_mainnet_reth_block(block, trace).await;
        } else {
            log::debug!(target: LOGGER_TARGET_SYNC, "Using geth debug trade to fetch diffs");
            let storage_changes = self
                .provider_trace
                .debug_trace_block_by_number(
                    Some(block_number.into()),
                    etrs::GethDebugTracingOptions {
                        disable_storage: Some(true),
                        disable_stack: Some(true),
                        enable_memory: Some(false),
                        enable_return_data: Some(false),
                        tracer: Some(ethers::types::GethDebugTracerType::BuiltInTracer(
                            etrs::GethDebugBuiltInTracerType::PreStateTracer,
                        )),
                        tracer_config: Some(etrs::GethDebugTracerConfig::BuiltInTracer(
                            ethers::types::GethDebugBuiltInTracerConfig::PreStateTracer(
                                etrs::PreStateConfig {
                                    diff_mode: Some(true),
                                },
                            ),
                        )),
                        timeout: None,
                    },
                )
                .await
                .wrap_err("Failed to fetch trace for {block_number}")?;
            log::debug!("Convering info list of diffs");

            let diffs = storage_changes
                .into_iter()
                .filter_map(|x| match x {
                    ethers::types::GethTrace::Known(etrs::GethTraceFrame::PreStateTracer(
                        ethers::types::PreStateFrame::Diff(etrs::DiffMode { pre: _, post }),
                    )) => Some(post),
                    _ => None,
                })
                .collect();

            log::debug!("Applying geth diffs to state");
            self.apply_next_mainnet_block_geth(block, diffs).await?;
        };
        log::debug!(target: LOGGER_TARGET_SYNC, "Block {:?} state syncced", block_number.as_u64());
        Ok(())
    }
    pub async fn apply_next_mainnet_reth_block(
        &mut self,
        block: etrs::Block<etrs::H256>,
        diff: Vec<ethers::types::BlockTrace>,
    ) {
        self.current_block = block.clone();
        let mut number_of_updates = 0;

        for trace in diff {
            let account_diffs = match trace.state_diff {
                None => continue,
                Some(d) => d.0,
            };

            let watched_accounts_reader = self.watched_accounts.read().await;

            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);
                match watched_accounts_reader.get(&addr) {
                    Some(a) => a,
                    None => continue, // We don't care about this account
                };

                let info = match self.accounts.get_mut(&addr) {
                    Some(previous) => previous,
                    None => {
                        continue;
                    }
                };

                let mut info = info.write().await;

                info.balance = match v.balance {
                    ethers::types::Diff::Born(value) => revm::primitives::U256::from_limbs(value.0),
                    ethers::types::Diff::Changed(value) => {
                        revm::primitives::U256::from_limbs(value.to.0)
                    }
                    _ => info.balance,
                };
                info.nonce = match v.nonce {
                    ethers::types::Diff::Born(value) => value.as_u64(),
                    ethers::types::Diff::Changed(value) => value.to.as_u64(),
                    _ => info.nonce,
                };
                let new_code = match v.code {
                    ethers::types::Diff::Born(value) => Some(revm::primitives::Bytecode::new_raw(
                        revm::primitives::Bytes(value.0),
                    )),
                    ethers::types::Diff::Changed(value) => Some(
                        revm::primitives::Bytecode::new_raw(revm::primitives::Bytes(value.to.0)),
                    ),
                    _ => None,
                };
                match new_code {
                    Some(code) => info.code = Some(code),
                    None => (),
                };

                number_of_updates += 1;

                if v.storage.is_empty() {
                    continue;
                }

                let acc_storage_writer = self.storage.borrow_mut();
                let watched_storage_slots = match watched_accounts_reader.get(&addr) {
                    Some(a) => a,
                    None => continue,
                };
                if watched_storage_slots.len() == 0 {
                    continue;
                }
                for (hpos, pos_val) in v.storage {
                    let pos = revm::primitives::U256::from_limbs(hpos.into_uint().0);
                    if !watched_storage_slots.contains(&pos) {
                        continue;
                    }
                    number_of_updates += 1;

                    let value = match pos_val {
                        ethers::types::Diff::Born(value) => value,
                        ethers::types::Diff::Changed(t) => t.to,
                        _ => continue,
                    };

                    let value_to_insert = prims::U256::from_limbs(value.into_uint().0);

                    match acc_storage_writer.entry((addr, pos)) {
                        dashmap::mapref::entry::Entry::Occupied(a) => {
                            let mut prev = a.get().write().await;
                            prev.present_value = value_to_insert;
                        }
                        dashmap::mapref::entry::Entry::Vacant(accs) => {
                            accs.insert(Arc::new(RwLock::new(StorageSlot::new(value_to_insert))));
                        }
                    };
                }
            }
        }
        if number_of_updates != 0 {
            log::debug!(target:LOGGER_TARGET_SYNC, "Applied {} updates from mainnet", number_of_updates);
        }
    }

    pub async fn get_total_watched(&self) -> (u64, u64) {
        let accs = self.watched_accounts.read().await;
        (
            accs.len() as u64,
            accs.iter().map(|x| x.len() as u64).sum::<u64>(),
        )
    }
    async fn load_positions(
        &mut self,
        positions: Vec<(Address, Vec<revm::primitives::U256>)>,
    ) -> eyre::Result<()> {
        log::debug!(target: LOGGER_TARGET_SYNC, "Loading cache");

        // load all basic info first
        let provider = self.provider.clone();
        let infos = try_join_all(
            positions
                .clone()
                .into_iter()
                .map(|(address, _)| load_acc_info(provider.clone(), address)),
        )
        .await
        .wrap_err("Failed to fetch basic info for all addresses")?;

        for (addr, info) in infos.iter() {
            self.accounts
                .insert(*addr, Arc::new(RwLock::new(info.clone())));
            self.total_number_of_watched_accs += 1;
            self.watched_accounts
                .write()
                .await
                .insert(*addr, HashSet::new());
        }
        log::debug!(target: LOGGER_TARGET_SYNC, "Loaded {} accounts from cache", infos.len());

        let addr_slot_pairs = positions
            .iter()
            .flat_map(|(addr, positions)| {
                positions
                    .into_iter()
                    .map(move |pos| (addr, pos))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        self.total_number_of_watched_slots += addr_slot_pairs.len() as u64;

        let provider = self.provider.clone();
        let storage_slots = try_join_all(
            addr_slot_pairs
                .into_iter()
                .map(|(addr, pos)| load_storage_slot(provider.clone(), *addr, *pos)),
        )
        .await
        .wrap_err("Failed to fetch storage slots for all addresses")?;

        log::debug!(target: LOGGER_TARGET_SYNC, "Loaded {} from cache", positions.len());

        for (addr, pos, value) in storage_slots {
            self.storage
                .insert((addr, pos), Arc::new(RwLock::new(StorageSlot::new(value))));
            self.watched_accounts
                .write()
                .await
                .entry(addr)
                .or_insert(HashSet::new())
                .insert(pos);
            self.total_number_of_watched_slots += 1;
        }
        log::info!(target: LOGGER_TARGET_SYNC, "Loaded {} previously watched accounts/positions from cache", positions.len());

        Ok(())
    }

    async fn fetch_basic_from_remote(
        &mut self,
        address: Address,
    ) -> eyre::Result<Arc<tokio::sync::RwLock<AccountInfo>>> {
        match self.pending_basic_reads.entry(address) {
            dashmap::mapref::entry::Entry::Occupied(a) => return Ok(a.get().clone()),
            dashmap::mapref::entry::Entry::Vacant(accs) => {
                let pending_account = Arc::new(RwLock::new(Default::default()));
                let mut pending_account_info = pending_account.write().await;
                accs.insert(pending_account.clone());

                let (_, info) = load_acc_info(self.provider.clone(), address.clone())
                    .await
                    .wrap_err_with(|| {
                        format!(
                            "Failed to fetch basic info for address {}",
                            address.encode_hex_with_prefix()
                        )
                    })?;
                match &info.code {
                    Some(code) => {
                        let code_lock = Arc::new(RwLock::new(code.clone()));
                        self.contracts.insert(info.code_hash, code_lock.clone());
                    }
                    None => (),
                }
                pending_account_info.balance = info.balance;
                pending_account_info.nonce = info.nonce;
                pending_account_info.code_hash = info.code_hash;
                pending_account_info.code = info.code;

                let out = self
                    .pending_basic_reads
                    .remove(&address)
                    .wrap_err("Failed to remove pending basic read")?;

                self.accounts.insert(address, pending_account.clone());

                // Now add it to list of vacant accounts
                match self.watched_accounts.write().await.entry(address) {
                    dashmap::mapref::entry::Entry::Occupied(_) => (),
                    dashmap::mapref::entry::Entry::Vacant(accs) => {
                        accs.insert(HashSet::new());
                        self.total_number_of_watched_accs += 1;
                        ()
                    }
                };
                return Ok(out.1.clone());
            }
        }
    }

    pub async fn basic(&mut self, address: Address) -> eyre::Result<Option<AccountInfo>> {
        match self.accounts.get_mut(&address) {
            Some(acc) => {
                let acc = acc.read().await;
                return Ok(Some(acc.clone()));
            }
            None => {}
        };
        let out = self.fetch_basic_from_remote(address).await?;
        let out = out.read().await;
        return Ok(Some(out.clone()));
    }
    pub async fn basic_ref(&self, address: Address) -> eyre::Result<Option<AccountInfo>> {
        Ok(match self.accounts.get(&address) {
            Some(acc) => Some(acc.read().await.clone()),
            None => match self.pending_basic_reads.get(&address) {
                Some(acc) => Some(acc.read().await.clone()),
                None => None,
            },
        })
    }
    pub async fn code_by_hash(
        &mut self,
        code_hash: prims::B256,
    ) -> eyre::Result<revm::primitives::Bytecode> {
        match self.contracts.get(&code_hash) {
            Some(acc) => {
                return Ok(acc.read().await.clone());
            }
            None => {
                return Ok(revm::primitives::Bytecode::new());
            }
        }
    }
    async fn fetch_storage(
        &mut self,
        address: Address,
        index: revm::primitives::ruint::Uint<256, 4>,
    ) -> eyre::Result<prims::U256> {
        let provider = self.provider.clone();
        match self.pending_storage_reads.entry((address, index)) {
            dashmap::mapref::entry::Entry::Occupied(accs) => {
                return Ok(accs.get().read().await.clone().present_value);
            }
            dashmap::mapref::entry::Entry::Vacant(accs) => {
                let storage_slot = Arc::new(RwLock::new(StorageSlot::default()));
                accs.insert(storage_slot.clone());
                {
                    // let mut storage_slot = accs.
                    let mut storage_slot = storage_slot.write().await;
                    let value = provider
                        .clone()
                        .get_storage_at(
                            etrs::Address::from(address.as_ref()),
                            etrs::H256::from(index.to_be_bytes()),
                            None,
                        )
                        .await?;
                    storage_slot.present_value = prims::U256::from_limbs(value.into_uint().0);
                }
                self.storage.insert((address, index), storage_slot.clone());
                match self
                    .watched_accounts
                    .borrow_mut()
                    .write()
                    .await
                    .entry(address)
                {
                    dashmap::mapref::entry::Entry::Vacant(a) => {
                        a.insert(HashSet::new());
                    }
                    dashmap::mapref::entry::Entry::Occupied(mut previous) => {
                        let prev = previous.get_mut();
                        prev.insert(index);
                    }
                };
                self.pending_storage_reads.remove(&(address, index));
                return Ok(storage_slot.clone().read().await.present_value);
            }
        }
    }

    pub async fn storage(
        &mut self,
        address: Address,
        index: revm::primitives::ruint::Uint<256, 4>,
    ) -> eyre::Result<prims::U256> {
        match self.storage.get(&(address, index)) {
            Some(acc) => {
                return Ok(acc.read().await.present_value);
            }
            None => {}
        };

        return self.fetch_storage(address, index).await;
    }
    pub async fn storage_ref(
        &self,
        address: Address,
        index: revm::primitives::ruint::Uint<256, 4>,
    ) -> eyre::Result<prims::U256> {
        Ok(match self.storage.get(&(address, index)) {
            Some(acc) => acc.clone().read().await.present_value,
            None => match self.pending_storage_reads.get(&(address, index)) {
                Some(acc) => acc.clone().read().await.present_value,
                None => prims::U256::default(),
            },
        })
    }
    pub async fn block_hash(&mut self, number: prims::U256) -> eyre::Result<prims::B256> {
        let num: u64 = number.to();
        let out = self
            .provider
            .get_block(num)
            .await
            .wrap_err_with(|| "Failed to fetch block hash")?
            .wrap_err_with(|| format!("Failed to fetch block hash for block {}", num))?;

        return Ok(prims::B256::from(out.hash.wrap_err("Invalid hash?")?.0));
    }
}

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
                config.link_type,
                provider,
                provider_trace,
                fork_block,
            ))),
            erc20_abis: Arc::new(out),
        };

        if watched.len() != 0 {
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
            .write()
            .await
            .load_positions(initial_watched)
            .await
            .wrap_err_with(|| "Failed to load initial watched positions")?;

        return Ok(out);
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
