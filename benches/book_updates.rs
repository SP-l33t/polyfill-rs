use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polyfill_rs::{
    book::OrderBook,
    types::{BookUpdate, OrderSummary, Side},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

fn make_book_update(token_id: &str, timestamp: u64, bids: Vec<OrderSummary>, asks: Vec<OrderSummary>) -> BookUpdate {
    BookUpdate {
        asset_id: token_id.to_string(),
        market: "0xabc".to_string(),
        timestamp,
        bids,
        asks,
        hash: None,
    }
}

fn make_levels(base_price: u64, count: u64, size: Decimal) -> Vec<OrderSummary> {
    (0..count)
        .map(|i| OrderSummary {
            price: Decimal::from(base_price + i) / Decimal::from(10000),
            size,
        })
        .collect()
}

fn bench_book_creation(c: &mut Criterion) {
    c.bench_function("book_creation", |b| {
        b.iter(|| {
            let _book = OrderBook::new(black_box("test_token".to_string()), black_box(100));
        });
    });
}

fn bench_snapshot_application(c: &mut Criterion) {
    let mut book = OrderBook::new("test_token".to_string(), 100);

    // Warm up with a realistic snapshot
    book.apply_book_update(&make_book_update(
        "test_token", 1,
        make_levels(5000, 20, dec!(100)),
        make_levels(6000, 20, dec!(100)),
    )).unwrap();

    c.bench_function("snapshot_application_20_levels", |b| {
        let mut ts = 2u64;
        b.iter(|| {
            ts += 1;
            let update = make_book_update(
                "test_token",
                ts,
                make_levels(5000 + (ts % 10), 20, dec!(100)),
                make_levels(6000 + (ts % 10), 20, dec!(100)),
            );
            book.apply_book_update(&update).unwrap();
        });
    });
}

fn bench_best_price_lookup(c: &mut Criterion) {
    let mut book = OrderBook::new("test_token".to_string(), 100);

    book.apply_book_update(&make_book_update(
        "test_token", 1,
        make_levels(5000, 10, dec!(100)),
        make_levels(6000, 10, dec!(100)),
    )).unwrap();

    c.bench_function("best_price_lookup", |b| {
        b.iter(|| {
            let _bid = book.best_bid();
            let _ask = book.best_ask();
            let _spread = book.spread();
            let _mid = book.mid_price();
        });
    });
}

fn bench_book_snapshot(c: &mut Criterion) {
    let mut book = OrderBook::new("test_token".to_string(), 100);

    book.apply_book_update(&make_book_update(
        "test_token", 1,
        make_levels(5000, 25, dec!(100)),
        make_levels(7500, 25, dec!(100)),
    )).unwrap();

    c.bench_function("book_snapshot", |b| {
        b.iter(|| {
            let _snapshot = book.snapshot();
        });
    });
}

fn bench_market_impact_calculation(c: &mut Criterion) {
    let mut book = OrderBook::new("test_token".to_string(), 100);

    book.apply_book_update(&make_book_update(
        "test_token", 1,
        make_levels(5000, 15, dec!(100)),
        make_levels(6500, 15, dec!(100)),
    )).unwrap();

    c.bench_function("market_impact_calculation", |b| {
        b.iter(|| {
            let _impact = book.calculate_market_impact(Side::BUY, dec!(50));
        });
    });
}

fn bench_max_depth_cutoff(c: &mut Criterion) {
    // Main perf win: max_depth=20, but snapshot has 200 levels per side.
    // We stop parsing after 20, skipping 180 levels of work.
    let mut book = OrderBook::new("test_token".to_string(), 20);

    // Warm up
    book.apply_book_update(&make_book_update(
        "test_token", 1,
        make_levels(5000, 200, dec!(100)),
        make_levels(7000, 200, dec!(100)),
    )).unwrap();

    c.bench_function("snapshot_200_levels_depth_20", |b| {
        let mut ts = 2u64;
        b.iter(|| {
            ts += 1;
            let update = make_book_update(
                "test_token",
                ts,
                make_levels(5000 + (ts % 10), 200, dec!(100)),
                make_levels(7000 + (ts % 10), 200, dec!(100)),
            );
            book.apply_book_update(&update).unwrap();
        });
    });

    // Compare: same 200 levels but max_depth=200 (no cutoff)
    let mut book_full = OrderBook::new("test_token".to_string(), 200);
    book_full.apply_book_update(&make_book_update(
        "test_token", 1,
        make_levels(5000, 200, dec!(100)),
        make_levels(7000, 200, dec!(100)),
    )).unwrap();

    c.bench_function("snapshot_200_levels_depth_200", |b| {
        let mut ts = 2u64;
        b.iter(|| {
            ts += 1;
            let update = make_book_update(
                "test_token",
                ts,
                make_levels(5000 + (ts % 10), 200, dec!(100)),
                make_levels(7000 + (ts % 10), 200, dec!(100)),
            );
            book_full.apply_book_update(&update).unwrap();
        });
    });
}

fn bench_high_frequency_updates(c: &mut Criterion) {
    c.bench_function("high_frequency_snapshots_50_levels", |b| {
        b.iter(|| {
            let mut book = OrderBook::new("test_token".to_string(), 50);

            for i in 1u64..=100 {
                let update = make_book_update(
                    "test_token",
                    i,
                    make_levels(5000 + (i % 20), 50, Decimal::from(10 + (i % 90))),
                    make_levels(7000 + (i % 20), 50, Decimal::from(10 + (i % 90))),
                );
                book.apply_book_update(&update).unwrap();

                if i % 10 == 0 {
                    let _bid = book.best_bid();
                    let _ask = book.best_ask();
                }
            }
        });
    });
}

fn bench_concurrent_access(c: &mut Criterion) {
    use polyfill_rs::book::OrderBookManager;

    c.bench_function("concurrent_access", |b| {
        b.iter(|| {
            let manager = Arc::new(OrderBookManager::new(100));
            manager.get_or_create_book("test_token").unwrap();

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut tasks = Vec::new();

                for i in 1u64..=10 {
                    let mgr = manager.clone();
                    tasks.push(tokio::spawn(async move {
                        let update = BookUpdate {
                            asset_id: "test_token".to_string(),
                            market: "0xabc".to_string(),
                            timestamp: i,
                            bids: (0..20).map(|j| OrderSummary {
                                price: Decimal::from(5000 + j + i) / Decimal::from(10000),
                                size: dec!(100),
                            }).collect(),
                            asks: (0..20).map(|j| OrderSummary {
                                price: Decimal::from(6000 + j + i) / Decimal::from(10000),
                                size: dec!(100),
                            }).collect(),
                            hash: None,
                        };
                        let _ = mgr.apply_book_update(&update);
                    }));
                }

                for _ in 0..20 {
                    let mgr = manager.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = mgr.get_book("test_token");
                    }));
                }

                for task in tasks {
                    let _ = task.await;
                }
            });
        });
    });
}

criterion_group!(
    benches,
    bench_book_creation,
    bench_snapshot_application,
    bench_best_price_lookup,
    bench_book_snapshot,
    bench_market_impact_calculation,
    bench_max_depth_cutoff,
    bench_high_frequency_updates,
    bench_concurrent_access,
);
criterion_main!(benches);
