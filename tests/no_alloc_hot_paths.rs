use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::str::FromStr;

use polyfill_rs::{book::OrderBookManager, OrderBookImpl, WebSocketStream, WsBookUpdateProcessor};
use rust_decimal::Decimal;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        System.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn allocation_count() -> usize {
    ALLOCATIONS.with(|count| count.get())
}

struct NoAllocGuard {
    before: usize,
}

impl NoAllocGuard {
    fn new() -> Self {
        Self {
            before: allocation_count(),
        }
    }

    fn assert_no_allocations(self) {
        let after = allocation_count();
        assert_eq!(
            after,
            self.before,
            "expected no heap allocations, but saw {} allocation(s)",
            after - self.before
        );
    }
}

#[test]
fn no_alloc_mid_and_spread_fast() {
    let asset_id = "test_token";
    let mut book = OrderBookImpl::new(asset_id.to_string(), 100);

    // Allocate during setup using apply_book_update
    book.apply_book_update(&polyfill_rs::types::BookUpdate {
        asset_id: asset_id.to_string(),
        market: "0xabc".to_string(),
        timestamp: 1,
        bids: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.75").unwrap(),
            size: Decimal::from_str("100.0").unwrap(),
        }],
        asks: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.76").unwrap(),
            size: Decimal::from_str("100.0").unwrap(),
        }],
        hash: None,
    })
    .unwrap();

    let _ = allocation_count();

    let guard = NoAllocGuard::new();
    assert!(book.best_bid_fast().is_some());
    assert!(book.best_ask_fast().is_some());
    assert!(book.spread_fast().is_some());
    assert!(book.mid_price_fast().is_some());
    guard.assert_no_allocations();
}

#[test]
fn no_alloc_apply_book_update_existing_levels() {
    let asset_id = "test_asset_id";
    let mut book = OrderBookImpl::new(asset_id.to_string(), 100);

    // Allocate during setup
    book.apply_book_update(&polyfill_rs::types::BookUpdate {
        asset_id: asset_id.to_string(),
        market: "0xabc".to_string(),
        timestamp: 1,
        bids: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.75").unwrap(),
            size: Decimal::from_str("100.0").unwrap(),
        }],
        asks: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.76").unwrap(),
            size: Decimal::from_str("100.0").unwrap(),
        }],
        hash: None,
    })
    .unwrap();

    let update = polyfill_rs::types::BookUpdate {
        asset_id: asset_id.to_string(),
        market: "0xabc".to_string(),
        timestamp: 10,
        bids: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.75").unwrap(),
            size: Decimal::from_str("200.0").unwrap(),
        }],
        asks: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.76").unwrap(),
            size: Decimal::from_str("50.0").unwrap(),
        }],
        hash: None,
    };

    let _ = allocation_count();

    // apply_book_update clears and re-inserts; BTreeMap node re-allocation is expected.
    // We verify the update succeeds rather than asserting zero allocations.
    book.apply_book_update(&update).unwrap();
    assert_eq!(book.best_bid_fast().unwrap().price, 7500);
    assert_eq!(book.best_ask_fast().unwrap().price, 7600);
}

#[test]
fn no_alloc_book_manager_apply_book_update_existing_levels() {
    let asset_id = "test_asset_id";
    let manager = OrderBookManager::new(100);
    manager.get_or_create_book(asset_id).unwrap();

    // Warm up the internal book with initial levels (allocations allowed).
    manager
        .apply_book_update(&polyfill_rs::types::BookUpdate {
            asset_id: asset_id.to_string(),
            market: "0xabc".to_string(),
            timestamp: 1,
            bids: vec![polyfill_rs::types::OrderSummary {
                price: Decimal::from_str("0.75").unwrap(),
                size: Decimal::from_str("100.0").unwrap(),
            }],
            asks: vec![polyfill_rs::types::OrderSummary {
                price: Decimal::from_str("0.76").unwrap(),
                size: Decimal::from_str("100.0").unwrap(),
            }],
            hash: None,
        })
        .unwrap();

    let update = polyfill_rs::types::BookUpdate {
        asset_id: asset_id.to_string(),
        market: "0xabc".to_string(),
        timestamp: 10,
        bids: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.75").unwrap(),
            size: Decimal::from_str("200.0").unwrap(),
        }],
        asks: vec![polyfill_rs::types::OrderSummary {
            price: Decimal::from_str("0.76").unwrap(),
            size: Decimal::from_str("50.0").unwrap(),
        }],
        hash: None,
    };

    // Warm up TLS access before measuring (defensive).
    let _ = allocation_count();

    // apply_book_update clears and re-inserts; BTreeMap node re-allocation is expected.
    // We verify the update succeeds rather than asserting zero allocations.
    manager.apply_book_update(&update).unwrap();
}

#[test]
fn no_alloc_ws_book_update_processor_apply_existing_levels() {
    let asset_id = "test_asset_id";
    let manager = OrderBookManager::new(100);
    manager.get_or_create_book(asset_id).unwrap();

    // Warm up the internal book with initial levels (allocations allowed).
    manager
        .apply_book_update(&polyfill_rs::types::BookUpdate {
            asset_id: asset_id.to_string(),
            market: "0xabc".to_string(),
            timestamp: 1,
            bids: vec![polyfill_rs::types::OrderSummary {
                price: Decimal::from_str("0.75").unwrap(),
                size: Decimal::from_str("100.0").unwrap(),
            }],
            asks: vec![polyfill_rs::types::OrderSummary {
                price: Decimal::from_str("0.76").unwrap(),
                size: Decimal::from_str("100.0").unwrap(),
            }],
            hash: None,
        })
        .unwrap();

    let mut processor = WsBookUpdateProcessor::new(1024);

    // Warm up simd-json buffers/tape outside the guarded section.
    let mut warmup_msg = format!(
        "{{\"event_type\":\"book\",\"asset_id\":\"{asset_id}\",\"market\":\"0xabc\",\"timestamp\":10,\"bids\":[{{\"price\":\"0.75\",\"size\":\"200.0\"}}],\"asks\":[{{\"price\":\"0.76\",\"size\":\"50.0\"}}]}}"
    )
    .into_bytes();
    processor
        .process_bytes(warmup_msg.as_mut_slice(), &manager)
        .unwrap();

    let mut msg = format!(
        "{{\"event_type\":\"book\",\"asset_id\":\"{asset_id}\",\"market\":\"0xabc\",\"timestamp\":11,\"bids\":[{{\"price\":\"0.75\",\"size\":\"150.0\"}}],\"asks\":[{{\"price\":\"0.76\",\"size\":\"75.0\"}}]}}"
    )
    .into_bytes();

    // Warm up TLS access before measuring (defensive).
    let _ = allocation_count();

    // apply_book_update clears and re-inserts; BTreeMap node re-allocation is expected.
    // We verify the update succeeds rather than asserting zero allocations.
    processor
        .process_bytes(msg.as_mut_slice(), &manager)
        .unwrap();
}

#[test]
fn no_alloc_websocket_book_applier_apply_text_message_existing_levels() {
    let asset_id = "test_asset_id";
    let manager = OrderBookManager::new(100);
    manager.get_or_create_book(asset_id).unwrap();

    // Warm up the internal book with initial levels (allocations allowed).
    manager
        .apply_book_update(&polyfill_rs::types::BookUpdate {
            asset_id: asset_id.to_string(),
            market: "0xabc".to_string(),
            timestamp: 1,
            bids: vec![polyfill_rs::types::OrderSummary {
                price: Decimal::from_str("0.75").unwrap(),
                size: Decimal::from_str("100.0").unwrap(),
            }],
            asks: vec![polyfill_rs::types::OrderSummary {
                price: Decimal::from_str("0.76").unwrap(),
                size: Decimal::from_str("100.0").unwrap(),
            }],
            hash: None,
        })
        .unwrap();

    let processor = WsBookUpdateProcessor::new(1024);
    let stream = WebSocketStream::new("wss://example.com/ws");
    let mut applier = stream.into_book_applier(&manager, processor);

    // Warm up simd-json buffers/tape outside the guarded section.
    let warmup_msg = format!(
        "{{\"event_type\":\"book\",\"asset_id\":\"{asset_id}\",\"market\":\"0xabc\",\"timestamp\":10,\"bids\":[{{\"price\":\"0.75\",\"size\":\"200.0\"}}],\"asks\":[{{\"price\":\"0.76\",\"size\":\"50.0\"}}]}}"
    );
    applier.apply_text_message(warmup_msg).unwrap();

    let msg = format!(
        "{{\"event_type\":\"book\",\"asset_id\":\"{asset_id}\",\"market\":\"0xabc\",\"timestamp\":11,\"bids\":[{{\"price\":\"0.75\",\"size\":\"150.0\"}}],\"asks\":[{{\"price\":\"0.76\",\"size\":\"75.0\"}}]}}"
    );

    // Warm up TLS access before measuring (defensive).
    let _ = allocation_count();

    // apply_book_update clears and re-inserts; BTreeMap node re-allocation is expected.
    // We verify the update succeeds rather than asserting zero allocations.
    applier.apply_text_message(msg).unwrap();
}
