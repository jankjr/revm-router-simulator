use ethers::providers::{Http, Middleware};
use eyre::{Context, ContextCompat, Ok};
use futures::AsyncWriteExt;
use slot0_simulator::config::{load_config_from_env, Config};
use slot0_simulator::simulate_routes;
use tokio::task::JoinHandle;

use std::sync::Arc;
use std::{env, fs};

use slot0_simulator::{LOGGER_TARGET_API, LOGGER_TARGET_MAIN, LOGGER_TARGET_SYNC};

use ethers::providers::{Provider, StreamExt};
use slot0_simulator::{errors::handle_rejection, ApplicationState};
use warp::Filter;
async fn save_watched(watched: Vec<(String, Vec<String>)>, cache_file: String) -> eyre::Result<()> {
    let mut file = async_fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(cache_file.clone())
        .await?;

    let blob = serde_json::to_string_pretty(&watched)
        .wrap_err("Failed to serialize watched addresses and storage to JSON")?;
    file.write(blob.as_bytes())
        .await
        .wrap_err("Failed to write watched addresses and storage to cache file")?;

    return Ok(());
}
fn load_watched_cache(cache_file: &String) -> eyre::Result<Vec<(String, Vec<String>)>> {
    let mut watched = Vec::<(String, Vec<String>)>::new();

    log::info!("Will cached watched positions to file: {}", cache_file);
    // Cache file is ./.[CHAIN_ID]_watched.json
    // If file exists, read it and populate the watched addressess

    if let eyre::Result::Ok(file) = std::fs::File::open(cache_file.clone()) {
        log::info!(target: LOGGER_TARGET_SYNC, "Reading previously watched addresses and storage from cache file");
        let watched_cache: Vec<(String, Vec<String>)> = serde_json::from_reader(file)
            .wrap_err("Caching enabled, but provided cache file could not be read")?;
        let mut total_accs = 0;
        let mut total_slots = 0;
        for (key, value) in watched_cache {
            total_accs += 1;
            total_slots += value.len();
            watched.push((key, value));
        }
        log::info!(target: LOGGER_TARGET_SYNC, "Will load {} accounts and {} slots from cache file", total_accs, total_slots);
    } else {
        match std::fs::write(cache_file.clone(), "[]") {
            eyre::Result::Ok(_) => {
                log::debug!(
                    target: LOGGER_TARGET_SYNC,
                    "Successfully created cache file for watched addresses"
                )
            }
            Err(e) => {
                log::error!(
                    target: LOGGER_TARGET_SYNC,
                    "Failed to create cache file for watched addresses: {}", e
                );
            }
        };
    }
    Ok(watched)
}

async fn on_block(
    provider: Provider<ethers::providers::Http>,
    app_state: Arc<ApplicationState>,
    block: ethers::types::Block<ethers::types::H256>,
) -> eyre::Result<()> {
    let latest_block = block
        .number
        .wrap_err("Failed to read block from block update")?
        .as_u64();
    log::debug!(target: LOGGER_TARGET_SYNC, "Got block {:?}", latest_block);
    let mut current_syncced_block = {
        let reader = app_state.cannonical.read().await;
        let out = reader.get_current_block().wrap_err("No block?")?;
        out
    };

    if current_syncced_block >= latest_block {
        log::debug!(target: LOGGER_TARGET_SYNC, "We're up to date, current block: {current_syncced_block}, latest block: {latest_block}");
        return Ok(());
    }

    let delta = latest_block - current_syncced_block;

    if delta == 1 {
        log::debug!(target: LOGGER_TARGET_SYNC, "We're at block {}, latest cannonical block {}", current_syncced_block, latest_block);
        app_state
            .cannonical
            .write()
            .await
            .apply_next_block(block)
            .await?;
    } else if delta > 1 {
        log::info!(target: LOGGER_TARGET_SYNC, "We're {} blocks behind cannonical {}", delta, latest_block);

        while current_syncced_block <= latest_block {
            let block_number: u64 = current_syncced_block + 1;
            log::debug!(target: LOGGER_TARGET_SYNC, "Fetching block {:?}", block_number);
            let block = provider
                .get_block(block_number)
                .await?
                .wrap_err(format!("Failed to fetch block {block_number} from RPC"))
                .wrap_err(format!("Failed to fetch block {block_number} from RPC"))?;

            log::debug!(target: LOGGER_TARGET_SYNC, "Applying block {:?}", block_number);
            app_state
                .cannonical
                .write()
                .await
                .apply_next_block(block)
                .await?;
            current_syncced_block = block_number;
        }
    }

    if current_syncced_block % 24 == 0 {
        log::info!(target: LOGGER_TARGET_MAIN, "Cannonical block: {current_syncced_block}");
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> eyre::Result<(), eyre::Report> {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "slot0::api");
    }
    log::set_max_level(log::LevelFilter::Debug);

    pretty_env_logger::init();

    let config = load_config_from_env();

    let fork_url = config.fork_url.clone();
    let fork_url_ws = config.ws_fork_url.clone();

    let provider = Provider::<Http>::try_from(fork_url).wrap_err("Failed to create provider")?;
    let provider_ws = Provider::connect(fork_url_ws.clone())
        .await
        .wrap_err("We failed to establish websocket connection with provided FORK_URL_WS ")?;

    let fork_block = provider_ws
        .get_block(ethers::types::BlockId::Number(
            ethers::types::BlockNumber::Latest,
        ))
        .await.expect("Failed to fetch latest block from RPC")
              .expect("RPC returns None for latest block number, something is not with the RPC provider or chain");

    let chain_id = provider
        .get_chainid()
        .await
        .wrap_err("Failed to fetch chain id from RPC provider")?;

    let cache_watched_positions = config.cache_watched;
    let cache_file = format!("./.{}_watched.json", chain_id);

    let watched = if cache_watched_positions == true {
        load_watched_cache(&cache_file)?
    } else {
        Vec::new()
    };

    let base_app_state: Arc<ApplicationState> = Arc::new(
        ApplicationState::create(
            config.clone(),
            provider_ws.clone(),
            provider.clone(),
            fork_block,
            watched,
        )
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
            .with(warp::log(&LOGGER_TARGET_API));

        log::info!(
            target: LOGGER_TARGET_API,
            "Starting server on port {port}"
        );
        warp::serve(routes).run(([127, 0, 0, 1], port)).await;

        Ok(())
    });

    let app_state = base_app_state.clone();
    let api_config = config.clone();
    let save_watched_cache_handle = tokio::spawn(async move {
        if !api_config.cache_watched {
            log::info!(target: LOGGER_TARGET_SYNC, "Watched cache disabled, not starting save loop");
            return Ok(());
        }

        let mut total_watched = { app_state.cannonical.read().await.get_total_watched().await };
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
            let reader = app_state.cannonical.read().await;
            let total_watched_current = reader.get_total_watched().await;
            let diff = (
                total_watched_current.0 - total_watched.0,
                total_watched_current.1 - total_watched.1,
            );
            if diff.0 + diff.1 == 0 {
                continue;
            }

            let data_to_save = reader.export_watched().await;
            match save_watched(data_to_save, cache_file.clone()).await {
                eyre::Result::Ok(()) => {
                    log::info!(target: LOGGER_TARGET_SYNC, "Saved new {} accounts and {} slots to watched cache", diff.0, diff.1);
                }
                eyre::Result::Err(err) => {
                    log::error!(target: LOGGER_TARGET_SYNC, "Error saving watched cache: {:?}", err);
                }
            };
            total_watched = total_watched_current;
        }
    });

    // This task will receive updates from the handle-sync-loop task whenever there are any changes to the watched set
    // It does this with a channl that just needs to contain a single pendin item that should eventually be consumed by the
    // task

    log::debug!(target: LOGGER_TARGET_MAIN, "Starting sync loop");
    let app_state = base_app_state.clone();

    let provider = provider.clone();
    let api_config = config.clone();
    let handle_sync_loop: JoinHandle<eyre::Result<()>> = tokio::spawn(async move {
        log::debug!(target: LOGGER_TARGET_SYNC, "Starting sync loop");
        let ws_provider = Provider::connect(fork_url_ws.clone())
            .await
            .wrap_err("Failed to connect to websocket provider")
            .wrap_err("Failed to connect to websocket provider")?;

        let mut stream = ws_provider
            .subscribe_blocks()
            .await
            .wrap_err("Failed to subscribe to block stream")
            .wrap_err("Failed to subscribe to block stream")?;

        log::debug!(target: LOGGER_TARGET_SYNC, "Stream initialized. Loading current state");
        let mut total_watched = { app_state.cannonical.read().await.get_total_watched().await };
        while let Some(block) = stream.next().await {
            let provider = provider.clone();
            let result = on_block(provider.clone(), app_state.clone(), block).await;

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
    let out = tokio::join!(
        handle_sync_loop,
        handle_http_server,
        save_watched_cache_handle
    );

    log::debug!(target: LOGGER_TARGET_MAIN, "All tasks shut down {:?}", out);
    Ok(())
}
