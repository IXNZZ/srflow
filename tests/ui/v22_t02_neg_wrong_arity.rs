//! T02 负例：显式连接 arity 不符（单槽函数给 `()` 输入）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_neg_wrong_arity.rs -o /tmp/v22_t02_neg_wrong_arity.rmeta
//! ```
//!
//! 预期主诊断：E0271（`<() as InputSpec>::Borrowed` 与函数声明的 `&Amount` 不匹配），落在 `then_with` 行。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::flow::Flow;
use crate::core::engine::shape::Query;

struct Amount(u32);
impl Data for Amount {}

async fn add_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

fn wire<'n>(flow: &mut Flow<'n>) {
    // 期望主诊断：E0271——单槽函数不接受零位置输入。
    let _ = flow.then_with(&add_one, ());
}

fn main() {}
