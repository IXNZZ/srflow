//! T02 内部装配编译正例：同一 crate 内部可见性下六个 T02 内部入口全部可用。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_ok_internal.rs -o /tmp/v22_t02_ok_internal.rmeta
//! ```
//!
//! 预期：编译成功。装配为 `#[path]` 引入正式 `src/core`，不带 `--cfg test`、不带 `--extern`，
//! 因此可见性与 crate 内部一致；业务 Data 手写 `impl Data`（本装配没有 derive 根别名）。

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::flow::Flow;
use crate::core::engine::node::Node;
use crate::core::engine::shape::{Query, Ref};

struct Amount(u32);
impl Data for Amount {}

async fn add_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

struct Bump;

impl Node for Bump {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Amount>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 10))
    }
}

/// 自动／显式 × 普通／取输出，加两种 Provide 形式：六个内部入口都可见且类型正确。
fn wire<'n>(flow: &mut Flow<'n>, amount: Ref<Amount>) {
    flow.then_auto(&add_one)
        .then_auto(Bump)
        .then_with(&add_one, amount);
    let _ = flow.then_auto_output(&add_one);
    let _ = flow.then_with_output(&add_one, amount);
    flow.provide(|| Amount(1)).provide(|| Amount(2));
    let _ = flow.provide_output(|| Amount(3));
}

fn main() {}
