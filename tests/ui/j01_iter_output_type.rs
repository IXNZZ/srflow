//! J 负例：Iter 的 body 必须产生 `Data<S>`（`O ≠ S` 在编译期拒绝）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/j01_iter_output_type.rs -o /tmp/j01.rmeta
//! ```
//! 预期：E0271 `<SyncFnSig<(State,), Data<u32>> as Wiring>::BuildOutput == DataRef<State>`
//! 不成立（Iter 的 `Value = S`）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::loop_orchestrator::{Iter1, LoopBuilder, LoopControl, LoopDecision};
use core::signature::{Data, SyncFnSig};

struct State(u32, LoopDecision);

impl LoopControl for State {
    fn loop_decision(&self) -> LoopDecision {
        self.1
    }
}

fn body(state: &State) -> Result<u32, BodyError> {
    Ok(state.0)
}

fn main() {
    let mut iter: LoopBuilder<Iter1<State>> = LoopBuilder::start().expect("iter");
    let _ = iter.then_body::<_, SyncFnSig<(State,), Data<u32>>>(
        body as fn(&State) -> Result<u32, BodyError>,
    );
}
