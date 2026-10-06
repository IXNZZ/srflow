//! J 负例：双输入形状不能用单输入接线（pack／Signature 形态不匹配）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/j03_loop_arity_mismatch.rs -o /tmp/j03.rmeta
//! ```
//! 预期：E0271 `Retry2<…> as LoopShape>::I == (u32,)` 不成立。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::flow::FlowBuilder;
use core::loop_orchestrator::{LoopBuilder, LoopControl, LoopDecision, Retry2};
use core::node::NodeCall2;
use core::ref_id::RefId;
use core::signature::{Data, NodeFut, NodeSig, NodeSig as _};
use core::builder::TypedCallBuilder;

#[derive(Debug, PartialEq, Eq)]
struct Draft(u32);

impl LoopControl for Draft {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

struct Rules(u32);

struct Body;

impl NodeCall2<u32, Rules, Data<Draft>> for Body {
    fn call<'a>(&'a self, first: &'a u32, _second: &'a Rules) -> NodeFut<'a, Draft> {
        Box::pin(async move { Ok(Draft(*first)) })
    }
}

fn main() {
    let mut retry: LoopBuilder<Retry2<u32, Rules, Draft>> = LoopBuilder::start().expect("retry");
    retry
        .then_body::<_, NodeSig<(u32, Rules), Data<Draft>>>(Body)
        .expect("body");
    let orchestrator = retry.finish().expect("finish");
    let (mut parent, input) = FlowBuilder::<(u32,)>::start().expect("parent");
    // 单输入接线接到双输入形状：预期编译失败。
    let _ = parent.then::<_, core::signature::OrchSig<u32, Data<Draft>>, _>(orchestrator, input);
}
