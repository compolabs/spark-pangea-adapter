use config::env::ev;
use error::Error;
use futures_util::future::FutureExt;
use futures_util::future::{join_all, select};
use indexer::pangea::initialize_pangea_indexer;
use std::sync::Arc;
use storage::order_book::OrderBook;
use tokio::signal;
use web::server::rocket;

pub mod config;
pub mod error;
pub mod indexer;
pub mod storage;
pub mod web;

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv::dotenv().ok();
    env_logger::init();

    let order_book = Arc::new(OrderBook::new());
    let mut tasks = vec![];

    initialize_pangea_indexer(&mut tasks, Arc::clone(&order_book)).await?;
    let port = ev("SERVER_PORT")?.parse()?;
    let rocket_task = tokio::spawn(run_rocket_server(port, Arc::clone(&order_book)));
    tasks.push(rocket_task);

    let ctrl_c_task = tokio::spawn(async {
        signal::ctrl_c().await.expect("failed to listen for event");
        println!("Ctrl+C received!");
    });
    tasks.push(ctrl_c_task);

    let shutdown_signal = signal::ctrl_c().map(|_| {
        println!("Shutting down gracefully...");
    });

    select(join_all(tasks).boxed(), shutdown_signal.boxed()).await;

    println!("Application is shutting down.");
    Ok(())
}

async fn run_rocket_server(port: u16, order_book: Arc<OrderBook>) {
    let rocket = rocket(port, order_book);
    let _ = rocket.launch().await;
}
