//! T03 外部可见性负例：自动推导桥类型未从 crate 根导出，且私有父模块路径不可达。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_auto_bridge.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_auto_bridge.rmeta
//! ```
//!
//! 预期主诊断：E0603（模块 `core` 私有）／E0432（名字不存在），落在 use 行。
//!
//! T03 迁移：`AutoCallable` 本身已由 crate 根 `#[doc(hidden)]` 导出（其正例见
//! `v22_t03_from_t02_neg_extern_auto_callable_root.rs`），但 `srflow::core` 仍是 crate 私有模块，
//! 因此“借私有父模块路径取支持桥”这条路径继续被拒绝：本夹具的调用点、断言与诊断阶段不变。

#![allow(unused_imports)]

use srflow::core::engine::node::AutoCallable;

fn main() {}
