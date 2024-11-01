use ethers_core::types::H256;
use log::{error, info};
use pangea_client::Client;
use pangea_client::{
    futures::StreamExt, provider::FuelProvider, query::Bound, requests::fuel::GetSparkOrderRequest,
    ClientBuilder, Format, WsProvider,
};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use crate::config::env::ev;
use crate::error::Error;
use crate::indexer::order_event_handler::handle_order_event;
use crate::indexer::order_event_handler::PangeaOrderEvent;
use crate::storage::order_book::OrderBook;

pub async fn initialize_pangea_indexer(
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    order_book: Arc<OrderBook>,
) -> Result<(), Error> {
    let ws_task_pangea = tokio::spawn(async move {
        if let Err(e) = start_pangea_indexer(order_book).await {
            eprintln!("Pangea error: {}", e);
        }
    });

    tasks.push(ws_task_pangea);
    Ok(())
}

async fn start_pangea_indexer(order_book: Arc<OrderBook>) -> Result<(), Error> {
    let client = create_pangea_client().await?;

    let contract_start_block: i64 = ev("CONTRACT_START_BLOCK")?.parse()?;
    let contract_h256 = H256::from_str(&ev("CONTRACT_ID")?)?;

    let mut last_processed_block =
        fetch_historical_data(&client, &order_book, contract_start_block, contract_h256).await?;

    if last_processed_block == 0 {
        last_processed_block = contract_start_block;
    }

    info!("Switching to listening for new orders (deltas)");

    listen_for_new_deltas(&client, &order_book, last_processed_block, contract_h256).await
}

async fn create_pangea_client() -> Result<Client<WsProvider>, Error> {
    let username = ev("PANGEA_USERNAME")?;
    let password = ev("PANGEA_PASSWORD")?;
    let url = ev("PANGEA_URL")?;

    let client = ClientBuilder::default()
        .endpoint(&url)
        .credential(username, password)
        .build::<WsProvider>()
        .await?;

    info!("Pangea ws client created and connected.");
    Ok(client)
}

async fn fetch_historical_data(
    client: &Client<WsProvider>,
    order_book: &Arc<OrderBook>,
    contract_start_block: i64,
    contract_h256: H256,
) -> Result<i64, Error> {
    let request_all = GetSparkOrderRequest {
        from_block: Bound::Exact(contract_start_block),
        to_block: Bound::Latest,
        market_id__in: HashSet::from([contract_h256]),
        ..Default::default()
    };

    let stream_all = client
        .get_fuel_spark_orders_by_format(request_all, Format::JsonStream, false)
        .await
        .expect("Failed to get fuel spark orders");

    pangea_client::futures::pin_mut!(stream_all);

    info!("Starting to load all historical orders...");
    let mut last_processed_block = 0;

    while let Some(data) = stream_all.next().await {
        match data {
            Ok(data) => {
                let data = String::from_utf8(data)?;
                let order: PangeaOrderEvent = serde_json::from_str(&data)?;
                last_processed_block = order.block_number;
                handle_order_event(order_book.clone(), order).await;
            }
            Err(e) => {
                error!("Error in the stream of historical orders: {e}");
                break;
            }
        }
    }

    Ok(last_processed_block)
}

async fn listen_for_new_deltas(
    client: &Client<WsProvider>,
    order_book: &Arc<OrderBook>,
    mut last_processed_block: i64,
    contract_h256: H256,
) -> Result<(), Error> {
    loop {
        let request_deltas = GetSparkOrderRequest {
            from_block: Bound::Exact(last_processed_block + 1),
            to_block: Bound::Subscribe,
            market_id__in: HashSet::from([contract_h256]),
            ..Default::default()
        };

        let stream_deltas = client
            .get_fuel_spark_orders_by_format(request_deltas, Format::JsonStream, true)
            .await
            .expect("Failed to get fuel spark deltas");

        pangea_client::futures::pin_mut!(stream_deltas);

        while let Some(data) = stream_deltas.next().await {
            match data {
                Ok(data) => {
                    let data = String::from_utf8(data)?;
                    let order: PangeaOrderEvent = serde_json::from_str(&data)?;
                    last_processed_block = order.block_number;
                    handle_order_event(order_book.clone(), order).await;
                }
                Err(e) => {
                    error!("Error in the stream of new orders (deltas): {e}");
                    break;
                }
            }
        }

        info!("Reconnecting to listen for new deltas...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
