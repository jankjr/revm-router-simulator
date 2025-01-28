use alloy::eips::BlockId;
use alloy::rpc::types::Block;

use alloy::transports::BoxTransport;
use alloy_provider::Provider;
use eyre::{Context, ContextCompat, Ok};
use slot0_simulator::config::load_config_from_env;
use slot0_simulator::simulate_routes;
use tokio::task::JoinHandle;

use std::env;
use std::sync::Arc;

use slot0_simulator::{LOGGER_TARGET_API, LOGGER_TARGET_MAIN, LOGGER_TARGET_SYNC};

use slot0_simulator::{errors::handle_rejection, ApplicationState};
use warp::Filter;

async fn on_block(
    provider: alloy::providers::RootProvider<BoxTransport>,
    app_state: Arc<ApplicationState>,
    block: Block,
) -> eyre::Result<()> {
    let latest_block = block.header.number;

    let mut current_syncced_block = {
        let reader = app_state.cannonical.clone();

        reader
            .get_current_block()
            .await
            .wrap_err("Fork block has no block number?")?
    };

    let delta = latest_block - current_syncced_block;

    if delta == 0 {
        return Ok(());
    }

    if delta > 1 {
        log::info!(target: LOGGER_TARGET_SYNC, "We are behind by {delta} blocks, catching up first");
        while current_syncced_block < latest_block - 1 {
            let block_number: u64 = current_syncced_block + 1;
            log::info!(target: LOGGER_TARGET_SYNC, "Fetching {block_number}");
            let block = provider
                .get_block(
                    BlockId::Number(alloy::eips::BlockNumberOrTag::Number(block_number)),
                    alloy::rpc::types::BlockTransactionsKind::Hashes,
                )
                .await
                .wrap_err(format!("Failed to fetch block {block_number} from RPC"))?
                .wrap_err("block does not exist")?;

            app_state.cannonical.apply_next_block(block).await?;
            current_syncced_block = block_number;
        }
    }

    log::debug!(target: LOGGER_TARGET_SYNC, "Applying latest block {latest_block}");
    app_state.cannonical.apply_next_block(block).await
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> eyre::Result<(), eyre::Report> {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "slot0::api");
    }
    log::set_max_level(log::LevelFilter::Debug);

    pretty_env_logger::init();

    let config = load_config_from_env();

    let provider = if config.fork_url.starts_with("ws") {
        alloy::providers::ProviderBuilder::new()
            .on_ws(alloy::providers::WsConnect::new(config.fork_url.as_str()))
            .await?
            .boxed()
    } else {
        alloy::providers::ProviderBuilder::new()
            .on_http(config.fork_url.as_str().parse()?)
            .boxed()
    };

    let fork_block = provider
        .get_block(
            BlockId::Number(alloy::eips::BlockNumberOrTag::Latest),
            alloy::rpc::types::BlockTransactionsKind::Hashes
        )
        .await.expect("Failed to fetch latest block from RPC")
              .expect("RPC returns None for latest block number, something is not with the RPC provider or chain");

    let base_app_state: Arc<ApplicationState> = Arc::new(
        ApplicationState::create(config.clone(), provider.clone(), fork_block, vec![])
            .await
            .wrap_err("Failed to create application state")?,
    );

    let api_config = config.clone();
    let app_state = base_app_state.clone();

    log::info!(target: LOGGER_TARGET_API, "Starting server");

    let handle_http_server: JoinHandle<eyre::Result<()>> = tokio::spawn(async move {
        let port = api_config.port;
        let api_base = warp::path("api").and(warp::path("v1"));
        api_base.boxed();
        let routes = api_base
            .and(simulate_routes(api_config, app_state))
            .recover(handle_rejection)
            .with(warp::log(LOGGER_TARGET_API));

        log::info!(
            target: LOGGER_TARGET_API,
            "Server running on port {port}"
        );
        warp::serve(routes).run(([127, 0, 0, 1], port)).await;

        Ok(())
    });

    log::debug!(target: LOGGER_TARGET_MAIN, "Starting sync loop");
    let app_state = base_app_state.clone();

    let provider = provider.clone();
    let handle_sync_loop: JoinHandle<eyre::Result<()>> = tokio::spawn(async move {
        log::debug!(target: LOGGER_TARGET_SYNC, "Starting sync loop");
        let provider = provider.clone();

        let mut stream = provider
            .subscribe_blocks()
            .await
            .wrap_err("Failed to subscribe to block stream")
            .wrap_err("Failed to subscribe to block stream")?;

        while let Result::Ok(block) = stream.recv().await {
            let provider = provider.clone();
            let result = on_block(provider.clone().boxed(), app_state.clone(), block).await;

            match result {
                eyre::Result::Ok(()) => {}
                eyre::Result::Err(err) => {
                    log::error!(target: LOGGER_TARGET_SYNC, "Error in sync loop: {}", err);
                }
            }
        }
        Ok(())
    });

    log::debug!(target: LOGGER_TARGET_MAIN, "All services started");
    let out = tokio::join!(handle_sync_loop, handle_http_server);

    log::debug!(target: LOGGER_TARGET_MAIN, "All tasks shut down {:?}", out);
    Ok(())
}
