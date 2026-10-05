//! J 负例：包装 body 的输入类型必须与 Loop 形状的包装输入一致。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/j06_loop_wrong_body_input_type.rs -o /tmp/j06.rmeta
//! ```
//! 预期：E0271／E0631：`SyncFnSig<(u32,), …>` 与 `(String,)` 接线不匹配。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::loop_orchestrator::{LoopBuilder, LoopControl, LoopDecision, Retry1};
use core::signature::{Data, SyncFnSig};

#[derive(Debug, PartialEq, Eq)]
struct Draft(u32);

impl LoopControl for Draft {
    fn loop_decision(&self) -> LoopDecision {
        LoopDecision::Finish
    }
}

fn body(input: &u32) -> Result<Draft, BodyError> {
    Ok(Draft(*input))
}

fn main() {
    // 形状声明输入为 `u32`，却用 `(String,)` 接线。
    let mut retry: LoopBuilder<Retry1<String, Draft>> = LoopBuilder::start().expect("retry");
    let _ = retry.then_body::<_, SyncFnSig<(u32,), Data<Draft>>>(
        body as fn(&u32) -> Result<Draft, BodyError>,
    );
}
