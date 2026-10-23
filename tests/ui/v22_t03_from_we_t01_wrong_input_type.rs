//! WE-T01 A05 负例 1：结构体 Node 声明 `Input = Amount`，却收到 `Ref<Other>`。
//!
//! 意图：`Flow::then_with_output` 的接线由类型系统连接（`Callable` 的
//! `A: RefShape<Spec = N::Input>`），错误输入类型必须在编译期被拒绝，不得靠运行期检查。
//!
//! 预期主诊断：E0271（`<Ref<Other> as RefShape>::Spec == Amount` 无法满足），
//! 落在本文件 `main` 内的显式接线调用行（紧随下方注释）。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_wrong_input_type.rs -o /tmp/v22_t03_from_we_t01_wrong_input_type.rmeta
//! ```
//!
//! T03 迁移：旧两参 `then(AddOne, other)` 改为显式取输出的 `then_with_output`；
//! Root 闭包固定返回 `()`，Out 泛型写准确 owned `Amount`，接线结果仍交给 `output_with`
//! 声明一次。拒绝阶段与目的不变（接线调用本身即拒绝点）。

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
    let _ = build_definition::<Other, Amount, _>(|flow, other| {
        // 期望主诊断：E0271——AddOne 声明 Input = Amount，这里给的是 Ref<Other>。
        let refs = flow.then_with_output(AddOne, other);
        flow.output_with(refs);
    });
}
