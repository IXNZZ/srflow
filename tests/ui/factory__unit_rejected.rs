//! T03 负例：返回 unit 的 Factory 不属于契约（`T: Data` 排除 `()`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/factory__unit_rejected.rs -o /tmp/v22_t03_from_t02_neg_provide_unit.rmeta
//! ```
//!
//! 预期主诊断：E0277（`()` 不满足 `Data`），落在 `provide` 调用行。
//!
//! T03 迁移：`provide` 已是公开方法，`T: Data` 约束不变；调用点、断言与诊断阶段不变，
//! 原“unit Factory 被拒绝”的目的完整保留。该锚点行同时被
//! `v22_t03_from_t02_witness_unrelated_e0277.rs` 用作严格判定见证的声明锚点。
#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::flow::Flow;

fn wire<'n>(flow: &mut Flow<'n>) {
    // 期望主诊断：E0277——unit 不是业务 Data。
    flow.provide(|| ());
}

fn main() {}
