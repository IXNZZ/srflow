//! L23 负例：未完成的 child Builder 不能接入整合链。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/l03_unfinished_child_integration.rs -o /tmp/l03.rmeta
//! ```
//! 预期：E0277——构建态 Builder 不是完成态 Orchestrator。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::data_ref::DataRef;
use core::flow::FlowBuilder;
use core::signature::{Data, OrchSig};

struct State(u32);
struct ItemResult(u32);

fn main() {
    let (mut root, states) = FlowBuilder::<(Vec<State>,)>::start().expect("root");
    let (child, _state) = FlowBuilder::<(State,)>::start().expect("child");
    // 期望：E0277——未完成 Builder 不能作为 child。
    let _ = root.then::<_, OrchSig<Vec<State>, Data<Vec<ItemResult>>>, _>(child, states);
}
