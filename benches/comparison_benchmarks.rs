use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polyfill_rs::types::{BookUpdate, OrderSummary};
use polyfill_rs::{OrderArgs, OrderBookImpl, Side};
use rust_decimal::Decimal;
use std::str::FromStr;

fn benchmark_create_order_eip712(c: &mut Criterion) {
    c.bench_function("create_order_eip712_signature", |b| {
        b.iter(|| {
            let order_args = OrderArgs::new(
                "test_token_id",
                Decimal::from_str("0.75").unwrap(),
                Decimal::from_str("100.0").unwrap(),
                Side::BUY,
            );
            black_box(order_args)
        })
    });
}

fn benchmark_json_parsing(c: &mut Criterion) {
    let sample_json = r#"{"data":[{"condition_id":"test","question":"Test Question","description":"Test Description","end_date_iso":"2024-01-01T00:00:00Z","game_start_time":"2024-01-01T00:00:00Z","image":"","icon":"","active":true,"closed":false,"archived":false,"accepting_orders":true,"minimum_order_size":"1.0","minimum_tick_size":"0.01","market_slug":"test","seconds_delay":0,"fpmm":"0x123","rewards":{"min_size":"1.0","max_spread":"0.1"},"tokens":[{"token_id":"123","outcome":"Yes","price":"0.5","winner":false}]}]}"#;

    c.bench_function("json_parsing_markets", |b| {
        b.iter(|| {
            let result: Result<serde_json::Value, _> = serde_json::from_str(sample_json);
            black_box(result)
        })
    });
}

fn benchmark_order_book_operations(c: &mut Criterion) {
    c.bench_function("order_book_updates", |b| {
        b.iter(|| {
            let mut book = OrderBookImpl::new("test_token".to_string(), 100);

            for i in 1u64..=100 {
                let bids: Vec<OrderSummary> = (0..50)
                    .map(|j| OrderSummary {
                        price: Decimal::from_str(&format!("0.{:04}", 5000 + j + (i % 20))).unwrap(),
                        size: Decimal::from_str("100.0").unwrap(),
                    })
                    .collect();
                let asks: Vec<OrderSummary> = (0..50)
                    .map(|j| OrderSummary {
                        price: Decimal::from_str(&format!("0.{:04}", 6000 + j + (i % 20))).unwrap(),
                        size: Decimal::from_str("100.0").unwrap(),
                    })
                    .collect();
                let update = BookUpdate {
                    asset_id: "test_token".to_string(),
                    market: "0xabc".to_string(),
                    timestamp: i,
                    bids,
                    asks,
                    hash: None,
                };
                let _ = book.apply_book_update(&update);
            }

            black_box(book)
        })
    });
}

fn benchmark_fast_operations(c: &mut Criterion) {
    let mut book = OrderBookImpl::new("test_token".to_string(), 100);

    let bids: Vec<OrderSummary> = (0..25)
        .map(|i| OrderSummary {
            price: Decimal::from_str(&format!("0.{:04}", 5000 + i)).unwrap(),
            size: Decimal::from_str("100.0").unwrap(),
        })
        .collect();
    let asks: Vec<OrderSummary> = (0..25)
        .map(|i| OrderSummary {
            price: Decimal::from_str(&format!("0.{:04}", 6000 + i)).unwrap(),
            size: Decimal::from_str("100.0").unwrap(),
        })
        .collect();
    book.apply_book_update(&BookUpdate {
        asset_id: "test_token".to_string(),
        market: "0xabc".to_string(),
        timestamp: 1,
        bids,
        asks,
        hash: None,
    })
    .unwrap();

    c.bench_function("fast_spread_mid_calculations", |b| {
        b.iter(|| {
            let spread = book.spread_fast();
            let mid = book.mid_price_fast();
            black_box((spread, mid))
        })
    });
}

criterion_group!(
    benches,
    benchmark_create_order_eip712,
    benchmark_json_parsing,
    benchmark_order_book_operations,
    benchmark_fast_operations
);
criterion_main!(benches);
