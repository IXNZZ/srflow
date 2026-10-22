//! T02 外部可见性负例：自动推导桥类型未从 crate 根导出。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_neg_extern_auto_bridge.rs \
//!   -o /tmp/v22_t02_neg_extern_auto_bridge.rmeta
//! ```
//!
//! 预期主诊断：E0603（模块 `core` 私有）／E0432（名字不存在），落在 use 行。

#![allow(unused_imports)]

use srflow::core::engine::node::AutoCallable;

fn main() {}
