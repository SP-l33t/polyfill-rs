//! Benchmark for fill processing
//!
//! This benchmark measures the performance of trade execution and
//! fill processing operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polyfill_rs::{
    book::OrderBook,
    fill::{FillEngine, FillProcessor},
    types::{BookUpdate, FillEvent, MarketOrderRequest, OrderSummary, Side},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn make_book_update(
    token_id: &str,
    timestamp: u64,
    bids: Vec<OrderSummary>,
    asks: Vec<OrderSummary>,
) -> BookUpdate {
    BookUpdate {
        asset_id: token_id.to_string(),
        market: "0xabc".to_string(),
        timestamp,
        bids,
        asks,
        hash: None,
    }
}

fn bench_fill_engine_creation(c: &mut Criterion) {
    c.bench_function("fill_engine_creation", |b| {
        b.iter(|| {
            let _engine = FillEngine::new(black_box(dec!(1)), black_box(dec!(5)), black_box(10));
        });
    });
}

fn bench_market_order_execution(c: &mut Criterion) {
    let mut engine = FillEngine::new(dec!(1), dec!(5), 10);
    let mut book = OrderBook::new("test_token".to_string(), 100);

    let bids: Vec<OrderSummary> = (1..=10)
        .map(|i| OrderSummary {
            price: Decimal::from(50 + i) / Decimal::from(100),
            size: dec!(100),
        })
        .collect();
    let asks: Vec<OrderSummary> = (1..=10)
        .map(|i| OrderSummary {
            price: Decimal::from(60 + i) / Decimal::from(100),
            size: dec!(100),
        })
        .collect();
    book.apply_book_update(&make_book_update("test_token", 1, bids, asks))
        .unwrap();

    c.bench_function("market_order_execution", |b| {
        b.iter(|| {
            let request = MarketOrderRequest {
                token_id: "test_token".to_string(),
                side: black_box(Side::BUY),
                amount: black_box(dec!(50)),
                slippage_tolerance: Some(dec!(1.0)),
                client_id: Some("bench_order".to_string()),
            };
            let _result = engine.execute_market_order(&request, &book);
        });
    });
}

fn bench_fill_processor(c: &mut Criterion) {
    let mut processor = FillProcessor::new(1000);

    c.bench_function("fill_processor", |b| {
        b.iter(|| {
            let fill = FillEvent {
                id: "fill_1".to_string(),
                order_id: "order_1".to_string(),
                token_id: "test_token".to_string(),
                side: black_box(Side::BUY),
                price: black_box(dec!(0.5)),
                size: black_box(dec!(100)),
                timestamp: chrono::Utc::now(),
                maker_address: alloy_primitives::Address::ZERO,
                taker_address: alloy_primitives::Address::ZERO,
                fee: black_box(dec!(0.1)),
            };
            processor.process_fill(fill).unwrap();
        });
    });
}

fn bench_market_impact_calculation(c: &mut Criterion) {
    let mut book = OrderBook::new("test_token".to_string(), 100);

    let bids: Vec<OrderSummary> = (1..=15)
        .map(|i| OrderSummary {
            price: Decimal::from(50 + i) / Decimal::from(100),
            size: Decimal::from(100 + i * 10),
        })
        .collect();
    let asks: Vec<OrderSummary> = (1..=15)
        .map(|i| OrderSummary {
            price: Decimal::from(65 + i) / Decimal::from(100),
            size: Decimal::from(100 + i * 10),
        })
        .collect();
    book.apply_book_update(&make_book_update("test_token", 1, bids, asks))
        .unwrap();

    c.bench_function("market_impact_calculation", |b| {
        b.iter(|| {
            let _impact = book.calculate_market_impact(Side::BUY, dec!(50));
            let _impact = book.calculate_market_impact(Side::SELL, dec!(50));
        });
    });
}

fn bench_high_frequency_fills(c: &mut Criterion) {
    c.bench_function("high_frequency_fills", |b| {
        b.iter(|| {
            let mut engine = FillEngine::new(dec!(1), dec!(2), 5);
            let mut book = OrderBook::new("test_token".to_string(), 100);

            for i in 1u64..=100 {
                let bids: Vec<OrderSummary> = (0..20)
                    .map(|j| OrderSummary {
                        price: Decimal::from(5000 + j + (i % 10)) / Decimal::from(10000),
                        size: Decimal::from(10 + (i % 90)),
                    })
                    .collect();
                let asks: Vec<OrderSummary> = (0..20)
                    .map(|j| OrderSummary {
                        price: Decimal::from(6000 + j + (i % 10)) / Decimal::from(10000),
                        size: Decimal::from(10 + (i % 90)),
                    })
                    .collect();
                book.apply_book_update(&make_book_update("test_token", i, bids, asks))
                    .unwrap();

                if i % 5 == 0 {
                    let request = MarketOrderRequest {
                        token_id: "test_token".to_string(),
                        side: if i % 2 == 0 { Side::BUY } else { Side::SELL },
                        amount: dec!(10),
                        slippage_tolerance: Some(dec!(1.0)),
                        client_id: Some(format!("order_{}", i)),
                    };
                    let _result = engine.execute_market_order(&request, &book);
                }
            }
        });
    });
}

fn bench_fill_statistics(c: &mut Criterion) {
    let mut engine = FillEngine::new(dec!(1), dec!(5), 10);

    for i in 1u64..=100 {
        let side = if i % 2 == 0 { Side::BUY } else { Side::SELL };
        let request = MarketOrderRequest {
            token_id: "test_token".to_string(),
            side,
            amount: dec!(10),
            slippage_tolerance: Some(dec!(1.0)),
            client_id: Some(format!("order_{}", i)),
        };

        let mut book = OrderBook::new("test_token".to_string(), 100);
        book.apply_book_update(&make_book_update(
            "test_token",
            i,
            vec![OrderSummary {
                price: dec!(0.5),
                size: dec!(100),
            }],
            vec![OrderSummary {
                price: dec!(0.55),
                size: dec!(100),
            }],
        ))
        .unwrap();

        let _result = engine.execute_market_order(&request, &book);
    }

    c.bench_function("fill_statistics", |b| {
        b.iter(|| {
            let _stats = engine.get_stats();
        });
    });
}

criterion_group!(
    benches,
    bench_fill_engine_creation,
    bench_market_order_execution,
    bench_fill_processor,
    bench_market_impact_calculation,
    bench_high_frequency_fills,
    bench_fill_statistics,
);
criterion_main!(benches);
