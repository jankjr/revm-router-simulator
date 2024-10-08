use alloy::{
    eips::BlockId, hex::{FromHex, ToHexExt}, pubsub::PubSubFrontend, rpc::types::{
        trace::{
            common::TraceResult,
            geth::{
                AccountState, DiffMode, GethDebugBuiltInTracerType, GethDebugTracerConfig,
                GethDebugTracerType, GethDebugTracingOptions, GethDefaultTracingOptions, GethTrace,
                PreStateFrame,
            },
            parity::{Delta, TraceResultsWithTransactionHash, TraceType},
        },
        Block,
    }, sol, transports::http::{Client, Http}
};
use alloy_provider::{
    ext::{DebugApi, TraceApi},
    Provider,
};
use futures::future::try_join_all;

use dashmap::{try_result::TryResult, DashMap, Entry};
use eyre::{Context, ContextCompat};
use revm::primitives::{alloy_primitives::aliases as prims, Address, KECCAK_EMPTY};
use revm::{db::DatabaseRef, primitives::AccountInfo, Database};
use std::{collections::BTreeMap, sync::Arc};
use tokio::{runtime::Handle, sync::RwLock};

use crate::{config::LinkType, LOGGER_TARGET_SYNC};


async fn load_acc_info(
    provider: alloy::providers::RootProvider<PubSubFrontend>,
    address: Address,
) -> eyre::Result<(Address, AccountInfo)> {
    log::trace!(target: LOGGER_TARGET_SYNC, "Fetching account {}", address);
    let account_state = tokio::spawn(async move {
        tokio::try_join!(
            provider.get_balance(address),
            provider.get_transaction_count(address),
            provider.get_code_at(address)
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
            balance: balance,
            nonce: nonce,
            code_hash,
            code,
        },
    ))
}

async fn load_storage_slot(
    provider: alloy::providers::RootProvider<PubSubFrontend>,
    address: Address,
    index: prims::U256,
) -> eyre::Result<(Address, prims::U256, prims::U256)> {
    log::trace!(target: LOGGER_TARGET_SYNC, "Fetching storage {} {}", &address, &index);
    let value = provider
        .get_storage_at(address, index)
        .await
        .wrap_err_with(|| "Failed to fetch storage slot")?;
    Ok((address, index, value))
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
        Forked::block_on(async { self.cannonical.clone().basic(address).await })
    }

    fn code_by_hash_ref(
        &self,
        code_hash: prims::B256,
    ) -> Result<revm::primitives::Bytecode, Self::Error> {
        Forked::block_on(async { self.cannonical.clone().code_by_hash(code_hash).await })
    }

    fn storage_ref(
        &self,
        address: revm::primitives::Address,
        index: prims::U256,
    ) -> Result<prims::U256, Self::Error> {
        Forked::block_on(async { self.cannonical.clone().storage(address, index).await })
    }

    fn block_hash_ref(&self, number: u64) -> Result<prims::B256, Self::Error> {
        Forked::block_on(async { self.cannonical.clone().block_hash(number).await })
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

#[derive(Debug, Clone)]
pub struct CannonicalFork {
    // Results fetched from the provider and maintained by apply_next_mainnet_block,
    // Contains the full state of the account & storage
    link_type: LinkType,

    contracts: DashMap<prims::B256, revm::primitives::Bytecode>,
    block_hashes: DashMap<u64, prims::B256>,
    storage: DashMap<Address, DashMap<prims::U256, prims::U256>>,
    accounts: DashMap<Address, AccountInfo>,
    pending_storage_reads: DashMap<(Address, prims::U256), prims::U256>,
    pending_basic_reads: DashMap<Address, revm::primitives::AccountInfo>,

    current_block: Arc<RwLock<Block>>,
    provider: alloy::providers::RootProvider<PubSubFrontend>,
    provider_trace: alloy::providers::RootProvider<Http<Client>>,
}

impl CannonicalFork {
    pub fn new(
        provider: alloy::providers::RootProvider<PubSubFrontend>,
        provider_trace: alloy::providers::RootProvider<Http<Client>>,
        fork_block: Block,
        config: crate::config::Config,
    ) -> Self {
        Self {
            link_type: config.link_type,
            accounts: DashMap::with_capacity(1024 * 8),
            storage: DashMap::with_capacity(1024 * 64),
            pending_basic_reads: DashMap::with_capacity(1024 * 8),
            pending_storage_reads: DashMap::with_capacity(1024 * 64),
            contracts: DashMap::with_capacity(1024 * 8),
            block_hashes: DashMap::with_capacity(1024 * 8),
            current_block: Arc::new(RwLock::new(fork_block)),
            provider,
            provider_trace,
        }
    }

    pub async fn block_env(&self) -> revm::primitives::BlockEnv {
        let block = self.current_block.read().await;
        revm::primitives::BlockEnv {
            number: prims::U256::from(block.header.number),
            timestamp: prims::U256::from(block.header.timestamp),
            gas_limit: prims::U256::from(block.header.gas_limit),
            coinbase: block.header.miner,
            difficulty: block.header.difficulty,
            basefee: prims::U256::from(1),
            ..Default::default()
        }
    }

    async fn apply_next_geth_block(
        &self,
        diffs: Vec<BTreeMap<Address, AccountState>>,
    ) -> eyre::Result<()> {
        for account_diffs in diffs {
            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);
                if !self.accounts.contains_key(&addr) {
                    continue;
                }
                let code_update = v
                    .code
                    .map(|v| revm::primitives::Bytes::from_hex(&v))
                    .map(|v| v.map(|v| revm::primitives::Bytecode::new_raw(v)));

                match self.accounts.entry(addr) {
                    Entry::Occupied(mut prev) => {
                        let prev = prev.get_mut();
                        prev.balance = match v.balance {
                            Some(value) => value,
                            _ => prev.balance,
                        };
                        prev.nonce = match v.nonce {
                            Some(value) => value,
                            _ => prev.nonce,
                        };
                        if let Some(Ok(code)) = code_update {
                            prev.code_hash = code.hash_slow();
                            prev.code = Some(code.clone());
                            self.contracts.insert(prev.code_hash, code);
                        }
                    }
                    _ => {
                        continue;
                    }
                };

                if !self.storage.contains_key(&addr) {
                    continue;
                }

                for (hpos, value) in v.storage {
                    let pos: revm::primitives::U256 = hpos.into();
                    let value_to_insert: revm::primitives::U256 = value.into();
                    match self.storage.try_get(&addr) {
                        TryResult::Present(acc) => match acc.entry(pos) {
                            Entry::Occupied(acs) => {
                                acs.replace_entry(value_to_insert);
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
    pub async fn get_current_block(&self) -> eyre::Result<u64> {
        Ok(self.current_block.read().await.header.number)
    }

    async fn load_reth_trace_and_apply(&self, block_number: u64) -> eyre::Result<()> {
        let trace = self
            .provider_trace
            .trace_replay_block_transactions(block_number.into(), &[TraceType::StateDiff])
            .await
            .wrap_err(format!("Failed to fetch trace for {}", block_number))?;

        Ok(self.apply_next_reth_block(trace).await)
    }

    async fn load_geth_trace_and_apply(&self, block_number: u64) -> eyre::Result<()> {
        // log::debug!(target: LOGGER_TARGET_SYNC, "Loading geth trace for diffs");
        let prestate_config = alloy::rpc::types::trace::geth::PreStateConfig {
            diff_mode: Some(true),
        };

        let storage_changes = self
            .provider_trace
            .debug_trace_block_by_number(
                alloy::eips::BlockNumberOrTag::Number(block_number),
                GethDebugTracingOptions {
                    config: GethDefaultTracingOptions {
                        disable_storage: Some(true),
                        disable_stack: Some(true),
                        enable_memory: Some(false),
                        enable_return_data: Some(false),
                        ..Default::default()
                    },
                    tracer: Some(GethDebugTracerType::BuiltInTracer(
                        GethDebugBuiltInTracerType::PreStateTracer,
                    )),
                    tracer_config: GethDebugTracerConfig::from(prestate_config),
                    timeout: None,
                },
            )
            .await
            .wrap_err("Failed to fetch trace for {block_number}")?;

        let diffs = storage_changes
            .into_iter()
            .filter_map(|x| match x {
                TraceResult::Success {
                    result:
                        GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode { pre: _, post })),
                    tx_hash: _,
                } => Some(post),
                _ => None,
            })
            .collect();

        self.apply_next_geth_block(diffs).await
    }

    pub async fn apply_next_block(&self, block: Block) -> eyre::Result<()> {
        let block_number = block.header.number;
        {
            *self.current_block.write().await = block.clone();
        }

        if self.link_type == LinkType::Reth {
            self.load_reth_trace_and_apply(block_number).await?
        } else {
            self.load_geth_trace_and_apply(block_number).await?
        };
        Ok(())
    }
    async fn apply_next_reth_block(&self, diff: Vec<TraceResultsWithTransactionHash>) {
        for trace in diff {
            let account_diffs = match trace.full_trace.state_diff {
                None => continue,
                Some(d) => d.0,
            };

            for (k, v) in account_diffs {
                let addr = revm::primitives::Address::from(k.0);
                if !self.accounts.contains_key(&addr) {
                    continue;
                }
                match self.accounts.entry(addr) {
                    Entry::Occupied(mut a) => {
                        let code_update = match v.code {
                            Delta::Added(value) => Some(revm::primitives::Bytecode::new_raw(value)),
                            Delta::Changed(value) => {
                                Some(revm::primitives::Bytecode::new_raw(value.to))
                            }
                            _ => None,
                        };
                        let code_update = code_update.map(|code| (code.hash_slow(), code));

                        let info: &mut AccountInfo = a.get_mut();
                        info.balance = match v.balance {
                            Delta::Added(value) => value,
                            Delta::Changed(value) => value.to,
                            _ => info.balance,
                        };
                        info.nonce = match v.nonce {
                            Delta::Added(value) => value.to(),
                            Delta::Changed(value) => value.to.to(),
                            _ => info.nonce,
                        };
                        if let Some((hash, code)) = code_update.clone() {
                            info.code_hash = hash;
                            info.code = Some(code.clone());
                            self.contracts.insert(hash, code);
                        }
                    }
                    _ => {
                        continue;
                    }
                };

                if v.storage.is_empty() && !self.storage.contains_key(&addr) {
                    continue;
                }
                match self.storage.entry(addr) {
                    Entry::Occupied(a) => {
                        for (pos, pos_val) in v.storage {
                            let value_to_insert = match pos_val {
                                Delta::Added(value) => value,
                                Delta::Changed(t) => t.to,
                                _ => continue,
                            };
                            let table = a.get();
                            match table.entry(pos.into()) {
                                Entry::Occupied(a) => {
                                    a.replace_entry(value_to_insert.into());
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {
                        continue;
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
            .filter(|addr| !self.accounts.contains_key(&addr.0))
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
            if let Some(code) = info.code.clone() {
                self.contracts.insert(info.code_hash, code);
            }
            self.accounts.insert(*addr, info.clone());
        }

        let addr_slot_pairs = positions
            .iter()
            .zip(infos)
            .filter(|(_, info)| info.1.code.is_some())
            .flat_map(|((addr, positions), _)| {
                positions
                    .iter()
                    .filter(|pos| match self.storage.try_get(addr) {
                        TryResult::Present(acc) => !acc.contains_key(&pos),
                        _ => true,
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
                if let Some(code) = info.code.clone() {
                    self.contracts.insert(info.code_hash, code);
                }
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

    async fn basic(&self, address: Address) -> eyre::Result<Option<AccountInfo>> {
        if let TryResult::Present(acc) = self.accounts.try_get(&address) {
            return Ok(Some(acc.clone()));
        }
        Ok(Some(self.fetch_basic_from_remote(address).await?))
    }
    async fn code_by_hash(
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

    async fn storage(
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
    async fn block_hash(&self, num: u64) -> eyre::Result<prims::B256> {
        match self.block_hashes.entry(num) {
            dashmap::Entry::Occupied(out) => Ok(out.get().clone()),
            dashmap::Entry::Vacant(e) => {
                log::debug!(target: LOGGER_TARGET_SYNC, "Fetching block hash {}", num);
                let out = self
                    .provider
                    .get_block(
                        BlockId::Number(alloy::eips::BlockNumberOrTag::Number(num)),
                        alloy::rpc::types::BlockTransactionsKind::Hashes,
                    )
                    .await
                    .wrap_err_with(|| "Failed to fetch block hash")?
                    .wrap_err_with(|| format!("Failed to fetch block hash for block {}", num))?;
                let out = out.header.hash;
                e.insert_entry(out);
                Ok(out)
            }
        }
    }
}
