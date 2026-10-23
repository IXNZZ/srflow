//! T03 迁移自 `tests/ui/we_t02_wrong_input_type.rs`（WE-T02 B22 负例）：Node 声明输入类型与
//! 实际 Ref 输入类型不匹配。
//!
//! 结构体 Node 的 `type Input = Amount`，却收到 `Ref<Other>`；类型系统在连接处要求
//! `RefShape::Spec == N::Input`，形状不匹配即拒绝。
//!
//! T03 迁移点：旧两参 `then(&node, other)` 更名并取输出为 `then_with_output(&node, other)`——
//! 传入的仍是同一个 `Ref<Other>`，拒绝点仍是这次连接本身。owned Out 由
//! `execute::<_, Amount, _>` 显式给出（该 Node 声明的输出类型）；Root 闭包固定返回 `()`，
//! 因此连接结果先绑定局部 `refs` 再交给 `flow.output_with(refs)`（求值一次，顺序不变）。
//!
//! 期望主诊断：E0271（`<Ref<Other> as RefShape>::Spec == Amount` 不成立），落在本夹具连接行。

use srflow::{BodyError, Data, Node, Query, Runtime};

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Other(u32);

struct AddOne;
impl Node for AddOne {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

fn main() {
    let runtime = Runtime::new();
    let node = AddOne;
    let _ = runtime.execute::<_, Amount, _>(
        |flow, other| {
            // 期望主诊断：E0271——Ref<Other> 不是 AddOne 声明的 Amount 输入。
            let refs = flow.then_with_output(&node, other);
            flow.output_with(refs);
        },
        Other(21),
    );
}
