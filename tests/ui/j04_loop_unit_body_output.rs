//! J 负例：unit／`Data<()>` body 在 Loop 形态下不可表达（构建期拒绝）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/j04_loop_unit_body_output.rs -o /tmp/j04.rmeta
//! ```
//! 预期：E0271 `SyncFnSig<(u32,), Data<()>>` 的 `BuildOutput == DataRef<Draft>` 不成立
//! （`(): LoopControl` 也不成立）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::loop_orchestrator::{LoopBuilder, LoopControl, LoopDecision, Retry1};
use core::signature::{Data, SyncFnSig};

struct Draft(u32);

impl LoopControl for Draft {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

fn body(_input: &u32) -> Result<(), BodyError> {
    Ok(())
}

fn main() {
    let mut retry: LoopBuilder<Retry1<u32, Draft>> = LoopBuilder::start().expect("retry");
    let _ = retry.then_body::<_, SyncFnSig<(u32,), Data<()>>>(
        body as fn(&u32) -> Result<(), BodyError>,
    );
}
