//! WE-T01 A05 负例 3：同步 `Result` 函数被当作 Node 传给接线入口。
//!
//! 意图：Node 协议要求异步签名（`AsyncFn`）。普通同步 `fn(Query<&A>) -> Result<O, BodyError>`
//! 不得替代异步协议登记为 Node。
//!
//! 预期主诊断：E0277（函数项未实现 `AsyncFn(Query<&Amount>) -> Result<Amount, BodyError>`），
//! 落在本文件 `main` 内的显式接线调用行。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_sync_result_function.rs -o /tmp/v22_t03_from_we_t01_sync_result_function.rmeta
//! ```
//!
//! T03 迁移：旧两参 `then(sync_double, amount)` 改为显式取输出的 `then_with_output`；
//! Root 闭包固定返回 `()`，Out 泛型取原尾值形状的 owned `()`。同步函数项在这一行即被拒绝，
//! 因此不声明 Root 输出。拒绝原因与目的不变。

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
    let _ = build_definition::<Amount, (), _>(|flow, amount| {
        // 期望主诊断：E0277——同步函数项不实现 AsyncFn。
        flow.then_with_output(sync_double, amount);
    });
}
