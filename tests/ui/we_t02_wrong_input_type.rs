//! WE-T02 B22 负例：Node 声明输入类型与实际 Ref 输入类型不匹配。
//!
//! 结构体 Node 的 `type Input = Amount`，却收到 `Ref<Other>`；类型系统在
//! `then` 处要求 `RefShape::Spec == N::Input`，形状不匹配即拒绝。
//!
//! 期望主诊断：E0271（`<Ref<Other> as RefShape>::Spec == Amount` 不成立）。

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
    let _ = runtime.execute(
        |flow, other| {
            // 期望主诊断：E0271——Ref<Other> 不是 AddOne 声明的 Amount 输入。
            flow.then(&node, other)
        },
        Other(21),
    );
}
