use futures::future::try_join_all;

use ethers::{
    providers::Middleware,
    types::BigEndianHash,
    utils::hex::{FromHex, ToHexExt},
};

use dashmap::Entry;
use dashmap::{try_result::TryResult, DashMap};
use ethers::types as etrs;
use eyre::{Context, ContextCompat};
use revm::primitives::{alloy_primitives::aliases as prims, Address, KECCAK_EMPTY};
use revm::{db::DatabaseRef, primitives::AccountInfo, Database};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::{runtime::Handle, sync::RwLock};

use crate::{config::LinkType, LOGGER_TARGET_SYNC};

async fn load_acc_info(
    provider: ethers::providers::Provider<ethers::providers::Ws>,
    address: Address,
) -> eyre::Result<(Address, AccountInfo)> {
    log::debug!(target: LOGGER_TARGET_SYNC, "Fetching account {}", address);
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
            if code.is_empty() {
                (None, KECCAK_EMPTY)
            } else {
                (Some(code.clone()), code.hash_slow())
            }
        }
        true => (None, KECCAK_EMPTY),
    };
    Ok((
        address,
        AccountInfo {
            balance: revm::primitives::U256::from_limbs(balance.0),
            nonce: nonce.as_u64(),
            code_hash,
            code,
        },
    ))
}

async fn load_storage_slot(
    provider: ethers::providers::Provider<ethers::providers::Ws>,
    address: Address,
    index: prims::U256,
) -> eyre::Result<(Address, prims::U256, prims::U256)> {
    log::debug!(target: LOGGER_TARGET_SYNC, "Fetching storage slot {} {}", address, index);
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
    pub cannonical: Arc<CannonicalFork>,
    pub env: revm::primitives::BlockEnv,
    pub seconds_per_block: revm::primitives::U256,
}
impl Forked {
    pub fn mine(&mut self, to_mine: u64) {
        let blocks = revm::primitives::U256::from(to_mine);
        self.env.number = self.env.number + blocks;
        self.env.timestamp += self.seconds_per_block * blocks;
        log::debug!(target: LOGGER_TARGET_SYNC, "Mined {} blocks, new block number {}", to_mine, self.env.number);
    }
    pub fn get_timestamp(&self) -> u64 {
        self.env.timestamp.to()
    }
    pub fn set_timestamp(&mut self, timestamp: u64) {
        log::debug!(target: LOGGER_TARGET_SYNC, "Setting timestamp to {}", timestamp);
        self.env.timestamp = revm::primitives::U256::from(timestamp);
    }
    pub fn get_block_number(&self) -> u64 {
        self.env.number.to()
    }

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
        let f = async { self.cannonical.basic(address).await };
        Forked::block_on(f)
    }

    fn code_by_hash_ref(
        &self,
        code_hash: prims::B256,
    ) -> Result<revm::primitives::Bytecode, Self::Error> {
        Forked::block_on(async { self.cannonical.code_by_hash(code_hash).await })
    }

    fn storage_ref(
        &self,
        address: revm::primitives::Address,
        index: prims::U256,
    ) -> Result<prims::U256, Self::Error> {
        Forked::block_on(async { self.cannonical.storage(address, index).await })
    }

    fn block_hash_ref(&self, number: u64) -> Result<prims::B256, Self::Error> {
        Forked::block_on(async { self.cannonical.block_hash(number).await })
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
    fn block_hash(&mut self, number: u64) -> Result<prims::B256, Self::Error> {
        <Self as DatabaseRef>::block_hash_ref(self, number)
    }
}

type SharedRef<T> = Arc<RwLock<T>>;
#[derive(Debug)]
pub struct CannonicalFork {
    contracts: DashMap<prims::B256, revm::primitives::Bytecode>,
    block_hashes: DashMap<u64, prims::B256>,
    storage: DashMap<Address, DashMap<prims::U256, prims::U256>>,
    accounts: DashMap<Address, AccountInfo>,
    pending_storage_reads: DashMap<(Address, prims::U256), prims::U256>,
    pending_basic_reads: DashMap<Address, revm::primitives::AccountInfo>,
    current_block: SharedRef<etrs::Block<etrs::H256>>,

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
            config,
            accounts: DashMap::new(),
            storage: DashMap::new(),
            pending_basic_reads: DashMap::new(), // Arc::new(RwLock::new(DashMap::new())),
            pending_storage_reads: DashMap::new(), //Arc::new(RwLock::new(DashMap::new())),
            contracts: DashMap::new(),
            current_block: Arc::new(RwLock::new(fork_block)),
            provider: provider.clone(),
            provider_trace: provider_trace.clone(),
            block_hashes: DashMap::new(),
        }
    }

    pub async fn block_env(&self) -> revm::primitives::BlockEnv {
        let block = self.current_block.read().await;
        revm::primitives::BlockEnv {
            number: prims::U256::from(block.number.unwrap().as_u64()),
            timestamp: prims::U256::from(block.timestamp.as_u64()),
            gas_limit: prims::U256::from(block.gas_limit.as_u64()),
            coinbase: revm::primitives::Address::from(block.author.unwrap().0),
            difficulty: prims::U256::from(block.difficulty.as_u128()),
            // basefee: prims::U256::from(block.base_fee_per_gas.unwrap().as_u64()),
            basefee: prims::U256::from(1),
            prevrandao: Some(prims::B256::from(block.mix_hash.unwrap().0)),
            ..Default::default()
        }
    }

    pub async fn collect_old(&self) {
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
            let addr = entry.key();
            for inner_iter in entry.value().iter() {
                let index = inner_iter.key();
                out.entry(*addr).and_modify(|col| {
                    col.push(index.to_string());
                });
            }
        }
        out.into_iter()
            .map(|(k, v)| (k.encode_hex_with_prefix(), v))
            .collect()
    }

    async fn apply_next_geth_block(
        &self,
        diffs: Vec<BTreeMap<etrs::H160, etrs::AccountState>>,
    ) -> eyre::Result<()> {
        for account_diffs in diffs {
            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);
                let code_update = v
                    .code
                    .map(|v| revm::primitives::Bytes::from_hex(&v))
                    .map(|v| v.map(|v| revm::primitives::Bytecode::new_raw(v)));

                self.accounts
                    .entry(addr)
                    .and_modify(|prev| {
                        prev.balance = match v.balance {
                            Some(value) => revm::primitives::U256::from_limbs(value.0),
                            _ => prev.balance,
                        };
                        prev.nonce = match v.nonce {
                            Some(value) => value.as_u64(),
                            _ => prev.nonce,
                        };
                        if let Some(Ok(code)) = code_update {
                            prev.code_hash = code.hash_slow();
                            prev.code = Some(code.clone());
                            self.contracts.insert(prev.code_hash, code);
                        }
                    })
                    .or_insert(AccountInfo::default());

                let storage = match v.storage {
                    Some(value) => value,
                    _ => continue,
                };

                for (hpos, value) in storage {
                    let pos = revm::primitives::U256::from_limbs(hpos.into_uint().0);
                    if !self.storage.contains_key(&addr) {
                        continue;
                    }
                    let value_to_insert = prims::U256::from_limbs(value.into_uint().0);
                    match self.storage.entry(addr) {
                        Entry::Occupied(mut a) => {
                            a.get_mut().insert(pos, value_to_insert);
                        }
                        Entry::Vacant(acc_storage) => {
                            let table = acc_storage.insert(DashMap::new());
                            table.insert(pos, value_to_insert);
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub async fn get_current_block(&self) -> eyre::Result<u64> {
        Ok(self
            .current_block
            .read()
            .await
            .number
            .wrap_err("Current block has no number")?
            .as_u64())
    }

    async fn load_reth_trace_and_apply(&self, block_number: u64) -> eyre::Result<()> {
        let trace = self
            .provider_trace
            .trace_replay_block_transactions(
                block_number.into(),
                vec![ethers::types::TraceType::StateDiff],
            )
            .await
            .wrap_err(format!("Failed to fetch trace for {}", block_number))?;

        Ok(self.apply_next_reth_block(trace).await)
    }

    async fn load_geth_trace_and_apply(&self, block_number: u64) -> eyre::Result<()> {
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

        self.apply_next_geth_block(diffs).await
    }

    pub async fn apply_next_block(&self, block: etrs::Block<etrs::H256>) -> eyre::Result<()> {
        let block_number = if let Some(number) = block.number {
            number
        } else {
            return Err(eyre::eyre!("Block has no number"));
        };

        *self.current_block.write().await = block.clone();

        if self.config.link_type == LinkType::Reth {
            self.load_reth_trace_and_apply(block_number.as_u64())
                .await?
        } else {
            self.load_geth_trace_and_apply(block_number.as_u64())
                .await?
        };
        Ok(())
    }
    async fn apply_next_reth_block(&self, diff: Vec<ethers::types::BlockTrace>) {
        for trace in diff {
            let account_diffs = match trace.state_diff {
                None => continue,
                Some(d) => d.0,
            };

            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);
                if self.accounts.contains_key(&addr) {
                    self.accounts.entry(addr).and_modify(|info| {
                        let code_update = match v.code {
                            ethers::types::Diff::Born(value) => {
                                Some(revm::primitives::Bytecode::new_raw(
                                    revm::primitives::Bytes(value.0),
                                ))
                            }
                            ethers::types::Diff::Changed(value) => {
                                Some(revm::primitives::Bytecode::new_raw(
                                    revm::primitives::Bytes(value.to.0),
                                ))
                            }
                            _ => None,
                        };

                        info.balance = match v.balance {
                            ethers::types::Diff::Born(value) => {
                                revm::primitives::U256::from_limbs(value.0)
                            }
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
                        if let Some(code) = code_update {
                            info.code_hash = code.hash_slow();
                            info.code = Some(code.clone());
                            self.contracts.insert(info.code_hash, code);
                        }
                    });
                }
                if v.storage.is_empty() {
                    continue;
                }

                for (hpos, pos_val) in v.storage {
                    let value_to_insert = match pos_val {
                        ethers::types::Diff::Born(value) => value,
                        ethers::types::Diff::Changed(t) => t.to,
                        _ => continue,
                    };
                    let pos = revm::primitives::U256::from_limbs(hpos.into_uint().0);
                    if !self.storage.contains_key(&addr) {
                        continue;
                    }
                    let value_to_insert = prims::U256::from_limbs(value_to_insert.into_uint().0);

                    match self.storage.entry(addr) {
                        Entry::Occupied(mut a) => {
                            a.get_mut().insert(pos, value_to_insert);
                        }
                        Entry::Vacant(acc_storage) => {
                            let table = acc_storage.insert(DashMap::new());
                            table.insert(pos, value_to_insert);
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn load_positions(
        &self,
        positions: Vec<(Address, Vec<revm::primitives::U256>)>,
    ) -> eyre::Result<()> {
        let positions = positions
            .iter()
            .filter(|addr| self.accounts.try_get(&addr.0).is_absent())
            .collect::<Vec<_>>();
        if positions.len() == 0 {
            return Ok(());
        }
        let provider = self.provider.clone();
        let infos = try_join_all(
            positions
                .clone()
                .into_iter()
                .map(|(address, _)| load_acc_info(provider.clone(), *address)),
        )
        .await
        .wrap_err("Failed to fetch basic info for all addresses")?;

        for (addr, info) in infos.iter() {
            self.accounts.insert(*addr, info.clone());
        }

        let addr_slot_pairs = positions
            .iter()
            .zip(infos)
            .filter(|(_, info)| info.1.code.is_some())
            .flat_map(|((addr, positions), _)| {
                positions
                    .iter()
                    .filter(|pos| {
                        match self.storage.try_get(addr) {
                            TryResult::Present(acc) => {
                                acc.try_get(&pos).is_absent()
                            }
                            TryResult::Absent => true,
                            TryResult::Locked => false
                        }
                    })
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

        for (addr, pos, value) in storage_slots {
            match self.storage.entry(addr) {
                Entry::Occupied(mut a) => {
                    a.get_mut().insert(pos, value);
                }
                Entry::Vacant(accs) => {
                    let table = accs.insert(DashMap::new());
                    table.insert(pos, value);
                }
            }
        }

        Ok(())
    }

    async fn fetch_basic_from_remote(&self, address: Address) -> eyre::Result<AccountInfo> {
        match self.pending_basic_reads.entry(address) {
            Entry::Occupied(read) => {
                return Ok(read.get().clone());
            }
            Entry::Vacant(accs) => {
                let mut pending_account_info = accs.insert(Default::default());

                let (_, info) = load_acc_info(self.provider.clone(), address)
                    .await
                    .wrap_err_with(|| {
                        format!(
                            "Failed to fetch basic info for address {}",
                            address.encode_hex_with_prefix()
                        )
                    })?;

                let info = info.clone();
                pending_account_info.balance = info.balance;
                pending_account_info.nonce = info.nonce;
                pending_account_info.code_hash = info.code_hash;
                pending_account_info.code = info.code;

                self.accounts.insert(address, pending_account_info.clone());
                return Ok(pending_account_info.clone());
            }
        };
    }

    pub async fn basic(&self, address: Address) -> eyre::Result<Option<AccountInfo>> {
        if let TryResult::Present(acc) = self.accounts.try_get(&address) {
            return Ok(Some(acc.clone()));
        }
        Ok(Some(self.fetch_basic_from_remote(address).await?))
    }
    pub async fn code_by_hash(
        &self,
        code_hash: prims::B256,
    ) -> eyre::Result<revm::primitives::Bytecode> {
        match self.contracts.get(&code_hash) {
            Some(acc) => Ok(acc.clone()),
            None => Ok(revm::primitives::Bytecode::new()),
        }
    }

    async fn fetch_storage(
        &self,
        address: Address,
        index: revm::primitives::ruint::Uint<256, 4>,
    ) -> eyre::Result<prims::U256> {
        match self.pending_storage_reads.entry((address, index)) {
            Entry::Vacant(accs) => {
                let data = {
                    let data = load_storage_slot(self.provider.clone(), address, index)
                        .await
                        .unwrap()
                        .2;
                    accs.insert(data);
                    data.clone()
                };
                match self.storage.entry(address) {
                    Entry::Occupied(mut a) => {
                        a.get_mut().insert(index, data);
                    }
                    Entry::Vacant(acc_storage) => {
                        let table = acc_storage.insert(DashMap::new());
                        table.insert(index, data);
                    }
                }
                return Ok(data);
            }
            Entry::Occupied(account_table) => {
                return Ok(account_table.get().clone());
            }
        }
    }

    pub async fn storage(
        &self,
        address: Address,
        index: revm::primitives::ruint::Uint<256, 4>,
    ) -> eyre::Result<prims::U256> {
        if let TryResult::Present(acc) = self.storage.try_get(&address) {
            if let TryResult::Present(acc) = acc.try_get(&index) {
                return Ok(acc.clone());
            }
        }
        Ok(self.fetch_storage(address, index).await?)
    }
    pub async fn block_hash(&self, num: u64) -> eyre::Result<prims::B256> {
        match self.block_hashes.entry(num) {
            dashmap::Entry::Occupied(out) => Ok(out.get().clone()),
            dashmap::Entry::Vacant(e) => {
                log::debug!(target: LOGGER_TARGET_SYNC, "Fetching block hash {}", num);
                let out = self
                    .provider
                    .get_block(num)
                    .await
                    .wrap_err_with(|| "Failed to fetch block hash")?
                    .wrap_err_with(|| format!("Failed to fetch block hash for block {}", num))?;
                let out = prims::B256::from(out.hash.wrap_err("Invalid hash?")?.0);
                e.insert_entry(out);
                Ok(out)
            }
        }
    }

    pub async fn get_total_watched(&self) -> (u64, u64) {
        let slots: usize = self.storage.iter().map(|v| v.len()).sum();
        (self.accounts.len() as u64, slots as u64)
    }
}
