//! T02 负例：显式连接类型不符（`add_one` 声明 `Query<&Amount>`，这里给 `Ref<Doubled>`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_neg_wrong_type.rs -o /tmp/v22_t02_neg_wrong_type.rmeta
//! ```
//!
//! 预期主诊断：E0271（`<Doubled as InputSpec>::Borrowed` 与函数声明的 `&Amount` 不匹配），落在 `then_with` 行。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::flow::Flow;
use crate::core::engine::shape::{Query, Ref};

struct Amount(u32);
impl Data for Amount {}

struct Doubled(u32);
impl Data for Doubled {}

async fn add_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

fn wire<'n>(flow: &mut Flow<'n>, doubled: Ref<Doubled>) {
    // 期望主诊断：E0271——`Doubled` 不是该函数的声明输入。
    let _ = flow.then_with(&add_one, doubled);
}

fn main() {}
