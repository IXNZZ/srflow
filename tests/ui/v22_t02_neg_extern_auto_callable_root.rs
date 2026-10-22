//! T02 外部可见性负例：自动推导桥 trait 未从 crate 根导出（业务可达路径）。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t02_neg_extern_auto_callable_root.rs \
//!   -o /tmp/v22_t02_neg_extern_auto_callable_root.rmeta
//! ```
//!
//! 预期主诊断：E0432（根未导出 `AutoCallable`），落在 use 行。

#![allow(unused_imports)]

use srflow::AutoCallable;

fn main() {}
