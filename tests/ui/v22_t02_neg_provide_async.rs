//! T02 负例：异步 Factory 不属于同步 Factory 契约。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_neg_provide_async.rs -o /tmp/v22_t02_neg_provide_async.rmeta
//! ```
//!
//! 预期主诊断：E0277（闭包返回 `impl Future`，不满足 `T: Data`），落在 `provide` 调用行。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::flow::Flow;

struct Amount(u32);
impl Data for Amount {}

fn wire<'n>(flow: &mut Flow<'n>) {
    // 期望主诊断：E0277——Future 不是 Data；异步生产应使用零输入 Node。
    flow.provide(|| async { Amount(1) });
}

fn main() {}
