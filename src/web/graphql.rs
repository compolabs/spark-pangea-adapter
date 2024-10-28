use crate::indexer::spot_order::OrderType;
use crate::storage::order_book::OrderBook;
use async_graphql::{Context, Object, SimpleObject, Subscription};
use async_stream::stream;
use futures_util::stream::BoxStream;
use std::sync::Arc;
use tokio::time::{self, Duration};

#[derive(SimpleObject, Clone)]
pub struct Order {
    id: String,
    user: String,
    asset: String,
    amount: String,
    price: String,
    timestamp: u64,
    order_type: String,
    status: Option<String>,
}

#[derive(SimpleObject, Clone)]
pub struct TradeOrderEvent {
    id: String,
    trade_price: String,
    trade_size: String,
    timestamp: u64,
}

pub struct Query;

#[Object]
impl Query {
    pub async fn buy_orders(&self, ctx: &Context<'_>) -> Vec<Order> {
        let order_book = ctx.data::<Arc<OrderBook>>().unwrap();
        let buy_orders = order_book.get_orders_in_range(0, u128::MAX, OrderType::Buy);
        buy_orders
            .into_iter()
            .map(|order| Order {
                id: order.id,
                user: order.user,
                asset: order.asset,
                amount: order.amount.to_string(),
                price: order.price.to_string(),
                timestamp: order.timestamp,
                order_type: "Buy".to_string(),
                status: order.status.map(|s| format!("{:?}", s)),
            })
            .collect()
    }

    pub async fn sell_orders(&self, ctx: &Context<'_>) -> Vec<Order> {
        let order_book = ctx.data::<Arc<OrderBook>>().unwrap();
        let sell_orders = order_book.get_orders_in_range(0, u128::MAX, OrderType::Sell);
        sell_orders
            .into_iter()
            .map(|order| Order {
                id: order.id,
                user: order.user,
                asset: order.asset,
                amount: order.amount.to_string(),
                price: order.price.to_string(),
                timestamp: order.timestamp,
                order_type: "Sell".to_string(),
                status: order.status.map(|s| format!("{:?}", s)),
            })
            .collect()
    }

    pub async fn spread(&self, ctx: &Context<'_>) -> Option<String> {
        let order_book = ctx.data::<Arc<OrderBook>>().unwrap();
        let buy_orders = order_book.get_orders_in_range(0, u128::MAX, OrderType::Buy);
        let sell_orders = order_book.get_orders_in_range(0, u128::MAX, OrderType::Sell);

        let max_buy_price = buy_orders.iter().map(|o| o.price).max();
        let min_sell_price = sell_orders.iter().map(|o| o.price).min();

        if let (Some(max_buy), Some(min_sell)) = (max_buy_price, min_sell_price) {
            Some((min_sell as i128 - max_buy as i128).to_string())
        } else {
            None
        }
    }

    pub async fn all_orders(&self, ctx: &Context<'_>, limit: Option<i32>, offset: Option<i32>) -> Vec<Order> {
        let order_book = ctx.data::<Arc<OrderBook>>().unwrap();
        let mut all_orders = vec![];

        let buy_orders = order_book.get_orders_in_range(0, u128::MAX, OrderType::Buy);
        let sell_orders = order_book.get_orders_in_range(0, u128::MAX, OrderType::Sell);

        all_orders.extend(buy_orders.into_iter().map(|order| Order {
            id: order.id.clone(),
            user: order.user.clone(),
            asset: order.asset.clone(),
            amount: order.amount.to_string(),
            price: order.price.to_string(),
            timestamp: order.timestamp,
            order_type: "Buy".to_string(),
            status: order.status.map(|s| format!("{:?}", s)),
        }));

        all_orders.extend(sell_orders.into_iter().map(|order| Order {
            id: order.id.clone(),
            user: order.user.clone(),
            asset: order.asset.clone(),
            amount: order.amount.to_string(),
            price: order.price.to_string(),
            timestamp: order.timestamp,
            order_type: "Sell".to_string(),
            status: order.status.map(|s| format!("{:?}", s)),
        }));

        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.unwrap_or(all_orders.len() as i32) as usize;
        all_orders.into_iter().skip(offset).take(limit).collect()
    }

    pub async fn trade_events(&self, ctx: &Context<'_>, limit: Option<i32>, offset: Option<i32>) -> Vec<TradeOrderEvent> {
        let order_book = ctx.data::<Arc<OrderBook>>().unwrap();

        let events = order_book.get_trade_events();
        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.unwrap_or(events.len() as i32) as usize;
        events.into_iter().skip(offset).take(limit).collect()
    }
}

pub struct Subscription;

#[Subscription]
impl Subscription {
    async fn active_orders(
        &self,
        ctx: &Context<'_>,
        order_type: String,
    ) -> BoxStream<'static, Vec<Order>> {
        let order_book = ctx.data::<Arc<OrderBook>>().unwrap().clone();  // Клонируем Arc<OrderBook>, чтобы он был 'static

        Box::pin(stream! {
            loop {
                let orders = match order_type.as_str() {
                    "Buy" => order_book.get_orders_in_range(0, u128::MAX, OrderType::Buy),
                    "Sell" => order_book.get_orders_in_range(0, u128::MAX, OrderType::Sell),
                    _ => vec![],
                };

                yield orders.into_iter().map(|order| Order {
                    id: order.id.clone(),
                    user: order.user.clone(),
                    asset: order.asset.clone(),
                    amount: order.amount.to_string(),
                    price: order.price.to_string(),
                    timestamp: order.timestamp,
                    order_type: order_type.clone(),
                    status: order.status.map(|s| format!("{:?}", s)),
                }).collect();

                time::sleep(Duration::from_secs(1)).await;
            }
        })
    }

    async fn trade_events(
        &self,
        ctx: &Context<'_>,
    ) -> BoxStream<'static, Vec<TradeOrderEvent>> {
        let order_book = ctx.data::<Arc<OrderBook>>().unwrap().clone();  // Клонируем Arc<OrderBook>

        Box::pin(stream! {
            loop {
                let events = order_book.get_trade_events();

                yield events.clone();

                time::sleep(Duration::from_secs(1)).await;
            }
        })
    }
}
