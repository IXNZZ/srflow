//! J 负例：未完成的 `LoopBuilder` 不具备 Orchestrator 协议，不能作为 child 接入。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/j05_loop_unfinished_builder_as_child.rs -o /tmp/j05.rmeta
//! ```
//! 预期：E0277／E0599：`LoopBuilder<…>` 不满足 `OrchCall`／`BuildSite`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::flow::FlowBuilder;
use core::loop_orchestrator::{Iter1, LoopBuilder, LoopControl, LoopDecision};
use core::signature::{Data, OrchSig};

#[derive(Debug, PartialEq, Eq)]
struct State(u32);

impl LoopControl for State {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

fn main() {
    let (mut parent, input) = FlowBuilder::<(State,)>::start().expect("parent");
    let unfinished: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("loop");
    let _ = parent.then::<_, OrchSig<State, Data<State>>, _>(unfinished, input);
}
