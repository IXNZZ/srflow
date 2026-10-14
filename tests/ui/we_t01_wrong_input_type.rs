//! WE-T01 A05 负例 1：结构体 Node 声明 `Input = Amount`，却收到 `Ref<Other>`。
//!
//! 意图：`Flow::then` 的接线由类型系统连接（`Callable` 的 `A: RefShape<Spec = N::Input>`），
//! 错误输入类型必须在编译期被拒绝，不得靠运行期检查。
//!
//! 预期主诊断：E0271（`<Ref<Other> as RefShape>::Spec == Amount` 无法满足），
//! 落在本文件第 45 行 `flow.then(AddOne, other)`（2026-10-11 实测）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/we_t01_wrong_input_type.rs -o /tmp/we_t01_wrong_input_type.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;
use crate::core::engine::definition::build_definition;
use crate::core::engine::error::BodyError;
use crate::core::engine::node::Node;
use crate::core::engine::shape::Query;

struct Amount(u32);

impl Data for Amount {}

struct Other(u32);

impl Data for Other {}

struct AddOne;

impl Node for AddOne {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

fn main() {
    let _ = build_definition::<Other, _, _>(|flow, other| {
        // 期望主诊断：E0271——AddOne 声明 Input = Amount，这里给的是 Ref<Other>。
        flow.then(AddOne, other)
    });
}
