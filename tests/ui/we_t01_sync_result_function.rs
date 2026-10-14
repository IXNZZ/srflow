//! WE-T01 A05 负例 3：同步 `Result` 函数被当作 Node 传给 `then`。
//!
//! 意图：Node 协议要求异步签名（`AsyncFn`）。普通同步 `fn(Query<&A>) -> Result<O, BodyError>`
//! 不得替代异步协议登记为 Node。
//!
//! 预期主诊断：E0277（函数项未实现 `AsyncFn(Query<&Amount>) -> Result<Amount, BodyError>`），
//! 落在本文件第 34 行 `flow.then(sync_double, amount)`（2026-10-11 实测）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/we_t01_sync_result_function.rs -o /tmp/we_t01_sync_result_function.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::build_definition;
use crate::core::engine::error::BodyError;
use crate::core::engine::shape::Query;

struct Amount(u32);

impl Data for Amount {}

/// 同步函数：签名形状正确，但不是异步协议。
fn sync_double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

fn main() {
    let _ = build_definition::<Amount, _, _>(|flow, amount| {
        // 期望主诊断：E0277——同步函数项不实现 AsyncFn。
        flow.then(sync_double, amount)
    });
}
