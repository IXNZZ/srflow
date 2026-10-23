//! T03 负例：直接传入 owned 值不是 Factory（`provide(value)` 已取消）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t03_from_t02_neg_provide_value.rs -o /tmp/v22_t03_from_t02_neg_provide_value.rmeta
//! ```
//!
//! 预期主诊断：E0277（`Amount` 不是 `Fn() -> T`），落在 `provide` 调用行。
//!
//! T03 迁移：`provide` 已是公开方法，只接受 `Fn() -> T`；调用点、断言与诊断阶段不变，
//! 原“编排期 owned 值注入旁路不开放”的目的完整保留。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::flow::Flow;

struct Amount(u32);
impl Data for Amount {}

fn wire<'n>(flow: &mut Flow<'n>) {
    // 期望主诊断：E0277——编排期 owned 值注入旁路不开放。
    flow.provide(Amount(1));
}

fn main() {}
