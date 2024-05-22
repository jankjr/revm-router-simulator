use futures::future::try_join_all;

use ethers::{
    providers::Middleware,
    types::BigEndianHash,
    utils::hex::{FromHex, ToHexExt},
};

use dashmap::DashMap;
use ethers::types as etrs;
use eyre::{Context, ContextCompat};
use revm::primitives::{alloy_primitives::aliases as prims, Address, StorageSlot, KECCAK_EMPTY};
use revm::{db::DatabaseRef, primitives::AccountInfo, Database};
use std::{
    borrow::BorrowMut,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::{runtime::Handle, sync::RwLock};

use crate::{config::LinkType, LOGGER_TARGET_SYNC};

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

#[derive(Debug, Clone)]
pub struct Forked {
    pub cannonical: Arc<RwLock<CannonicalFork>>,
}
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

#[derive(Debug)]
pub struct CannonicalFork {
    // Results fetched from the provider and maintained by apply_next_mainnet_block,
    // Contains the full state of the account & storage
    accounts: DashMap<Address, Arc<RwLock<AccountInfo>>>,
    link_type: LinkType,
    storage: DashMap<(Address, prims::U256), Arc<RwLock<StorageSlot>>>,

    pending_basic_reads: DashMap<Address, Arc<tokio::sync::RwLock<revm::primitives::AccountInfo>>>,
    pending_storage_reads: DashMap<(Address, prims::U256), Arc<RwLock<StorageSlot>>>,
    contracts: DashMap<prims::B256, Arc<RwLock<revm::primitives::Bytecode>>>,
    current_block: etrs::Block<etrs::H256>,
    provider: ethers::providers::Provider<ethers::providers::Ws>,
    provider_trace: ethers::providers::Provider<ethers::providers::Http>,
    config: crate::config::Config,
}

impl CannonicalFork {
    pub fn new(
        provider: ethers::providers::Provider<ethers::providers::Ws>,
        provider_trace: ethers::providers::Provider<ethers::providers::Http>,
        fork_block: etrs::Block<etrs::H256>,
        config: crate::config::Config,
    ) -> Self {
        Self {
            config: config.clone(),
            link_type: config.link_type,
            accounts: DashMap::with_capacity(1024),
            storage: DashMap::with_capacity(1024 * 1024),
            pending_basic_reads: DashMap::new(),
            pending_storage_reads: DashMap::new(),
            contracts: DashMap::new(),
            current_block: fork_block,
            provider,
            provider_trace
        }
    }

    pub async fn collect_old(&mut self) {
        // If we're nearing the config.max_watched_account size limit
        let max_watched_accounts = self.config.max_watched_accounts;
        if self.accounts.len() > max_watched_accounts {
            // Delete the 10% of the oldest accounts
            let mut to_delete = self.accounts.len() / 10;
            log::info!(target: LOGGER_TARGET_SYNC, "Deleting {} accounts", to_delete);

            for r in self.accounts.iter() {
                self.accounts.remove(r.key());
                to_delete -= 1;
                if to_delete == 0 {
                    break;
                }
            }
        }

        let max_watched_storage_slots = self.config.max_watched_storage_slots;
        if self.storage.len() > max_watched_storage_slots {
            // Delete the 10% of the oldest storage slots
            let mut to_delete = self.storage.len() / 10;
            log::info!(target: LOGGER_TARGET_SYNC, "Deleting {} storage slots", to_delete);

            for r in self.storage.iter() {
                self.storage.remove(r.key());
                to_delete -= 1;
                if to_delete == 0 {
                    break;
                }
            }
        }


    }

    pub async fn export_watched(&self) -> Vec<(String, Vec<String>)> {
        let mut out: HashMap<Address, Vec<String>> = HashMap::new();
        for entry in self.accounts.iter() {
            let key = entry.key();
            out.insert(*key, vec![]);
        }
        for entry in self.storage.iter() {
            let key = entry.key();
            match out.entry(key.0) {
                std::collections::hash_map::Entry::Occupied(mut a) => {
                    a.get_mut().push(key.1.to_string());
                }
                std::collections::hash_map::Entry::Vacant(a) => {
                    a.insert(vec![key.1.to_string()]);
                }
            }
        }
        out.into_iter()
            .map(|(k, v)| (k.encode_hex_with_prefix(), v))
            .collect()
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

    async fn apply_next_geth_block(
        &mut self,
        block: etrs::Block<etrs::H256>,
        diffs: Vec<BTreeMap<etrs::H160, etrs::AccountState>>,
    ) -> eyre::Result<(u64, u64)> {
        self.current_block = block.clone();
        let mut number_of_account_updates = 0;
        let mut number_of_storage_updates = 0;
        for account_diffs in diffs {
            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);

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
                        revm::primitives::Bytes::from_hex(&value)
                            .wrap_err("Invalid hex served by geth instance?!")?,
                    )),
                    _ => None,
                };
                match new_code {
                    Some(code) => info.code = Some(code),
                    None => (),
                };

                number_of_account_updates += 1;

                let storage = match v.storage {
                    Some(value) => value,
                    _ => continue,
                };
                let acc_storage_writer: &mut DashMap<
                    (Address, revm::primitives::ruint::Uint<256, 4>),
                    Arc<RwLock<StorageSlot>>,
                > = self.storage.borrow_mut();
                for (hpos, value) in storage {
                    let pos = revm::primitives::U256::from_limbs(hpos.into_uint().0);
                    let key = (addr, pos);
                    if !acc_storage_writer.contains_key(&key) {
                        continue;
                    }
                    number_of_storage_updates += 1;
                    let value_to_insert = prims::U256::from_limbs(value.into_uint().0);
                    match acc_storage_writer.entry(key) {
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
        Ok((number_of_account_updates, number_of_storage_updates))
    }
    pub fn get_current_block(&self) -> eyre::Result<u64> {
        Ok(self
            .current_block
            .number
            .wrap_err("Current block has no number")?
            .as_u64())
    }

    async fn load_reth_trace_and_apply(
        &mut self,
        block_number: u64,
        block: etrs::Block<etrs::H256>,
    ) -> eyre::Result<(u64, u64)> {
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
        Ok(self.apply_next_reth_block(block, trace).await)
    }

    async fn load_geth_trace_and_apply(
        &mut self,
        block_number: u64,
        block: etrs::Block<etrs::H256>,
    ) -> eyre::Result<(u64, u64)> {
        log::debug!(target: LOGGER_TARGET_SYNC, "Loading geth trace for diffs");
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

        let diffs = storage_changes
            .into_iter()
            .filter_map(|x| match x {
                ethers::types::GethTrace::Known(etrs::GethTraceFrame::PreStateTracer(
                    ethers::types::PreStateFrame::Diff(etrs::DiffMode { pre: _, post }),
                )) => Some(post),
                _ => None,
            })
            .collect();

        log::debug!("Applying list of updates to state");
        self.apply_next_geth_block(block, diffs).await
    }

    pub async fn apply_next_block(&mut self, block: etrs::Block<etrs::H256>) -> eyre::Result<()> {
        let block_number = if let Some(number) = block.number {
            number
        } else {
            return Err(eyre::eyre!("Block has no number"));
        };

        log::debug!(target: LOGGER_TARGET_SYNC, "Applying block {:?}", block_number.as_u64());

        self.current_block = block.clone();

        let (account_updates, storage_updates) = if self.link_type == LinkType::Reth {
            self.load_reth_trace_and_apply(block_number.as_u64(), block.clone())
                .await?
        } else {
            self.load_geth_trace_and_apply(block_number.as_u64(), block.clone())
                .await?
        };

        if account_updates == 0 && storage_updates == 0 {
            log::debug!(
                target: LOGGER_TARGET_SYNC,
                "applied block {}: no updates",
                block_number.as_u64()
            );
            return Ok(());
        }

        log::debug!(
            target: LOGGER_TARGET_SYNC,
            "applied block {}: updated {} accounts, {} storage slots",
            block_number.as_u64(),
            account_updates,
            storage_updates
        );
        Ok(())
    }
    async fn apply_next_reth_block(
        &mut self,
        block: etrs::Block<etrs::H256>,
        diff: Vec<ethers::types::BlockTrace>,
    ) -> (u64, u64) {
        self.current_block = block.clone();
        let mut number_of_account_updates = 0;
        let mut number_of_storage_updates = 0;

        for trace in diff {
            let account_diffs = match trace.state_diff {
                None => continue,
                Some(d) => d.0,
            };

            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);

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

                number_of_account_updates += 1;

                if v.storage.is_empty() {
                    continue;
                }

                let acc_storage_writer = self.storage.borrow_mut();

                for (hpos, pos_val) in v.storage {
                    let pos = revm::primitives::U256::from_limbs(hpos.into_uint().0);
                    let key = (addr, pos);

                    match acc_storage_writer.entry(key) {
                        dashmap::mapref::entry::Entry::Occupied(a) => {
                            let value = match pos_val {
                                ethers::types::Diff::Born(value) => value,
                                ethers::types::Diff::Changed(t) => t.to,
                                _ => continue,
                            };
                            number_of_storage_updates += 1;

                            let value_to_insert = prims::U256::from_limbs(value.into_uint().0);

                            let mut prev = a.get().write().await;
                            prev.present_value = value_to_insert;
                        }
                        dashmap::mapref::entry::Entry::Vacant(_) => {
                            continue;
                        }
                    };
                }
            }
        }
        (number_of_account_updates, number_of_storage_updates)
    }

    pub async fn get_total_watched(&self) -> (u64, u64) {
        (self.accounts.len() as u64, self.storage.len() as u64)
    }
    pub(crate) async fn load_positions(
        &mut self,
        positions: Vec<(Address, Vec<revm::primitives::U256>)>,
    ) -> eyre::Result<()> {
        log::info!(target: LOGGER_TARGET_SYNC, "Loading cache");

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
        }
        log::info!(target: LOGGER_TARGET_SYNC, "Loaded {} accounts from cache", infos.len());

        let addr_slot_pairs = positions
            .iter()
            .flat_map(|(addr, positions)| {
                positions
                    .into_iter()
                    .map(move |pos| (addr, pos))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let provider = self.provider.clone();
        let storage_slots = try_join_all(
            addr_slot_pairs
                .into_iter()
                .map(|(addr, pos)| load_storage_slot(provider.clone(), *addr, *pos)),
        )
        .await
        .wrap_err("Failed to fetch storage slots for all addresses")?;

        log::info!(target: LOGGER_TARGET_SYNC, "Loaded {} from cache", positions.len());

        for (addr, pos, value) in storage_slots {
            self.storage
                .insert((addr, pos), Arc::new(RwLock::new(StorageSlot::new(value))));
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
