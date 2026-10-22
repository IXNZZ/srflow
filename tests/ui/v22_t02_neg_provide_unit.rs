//! T02 负例：返回 unit 的 Factory 不属于契约（`T: Data` 排除 `()`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_neg_provide_unit.rs -o /tmp/v22_t02_neg_provide_unit.rmeta
//! ```
//!
//! 预期主诊断：E0277（`()` 不满足 `Data`），落在 `provide` 调用行。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::flow::Flow;

fn wire<'n>(flow: &mut Flow<'n>) {
    // 期望主诊断：E0277——unit 不是业务 Data。
    flow.provide(|| ());
}

fn main() {}
