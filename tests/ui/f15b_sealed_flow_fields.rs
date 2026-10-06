//! F15 负例：完成态 Flow 的字段私有，不能从原始 Definition 直接构造未验证 Flow。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/f15b_sealed_flow_fields.rs -o /tmp/f15b.rmeta
//! ```
//!
//! 预期：`definition`／`marker` 为私有字段（E0451）。正例见 `core::v21_06_tests::f01_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::Definition;
use core::flow::Flow;
use core::signature::Unit;
use std::sync::Arc;

fn main() {
    let _ = Flow::<(u32,), Unit> {
        definition: Arc::new(Definition::new()),
        marker: std::marker::PhantomData,
    };
}
