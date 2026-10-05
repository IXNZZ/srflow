//! J 负例：body 输出类型必须表达二值控制（`LoopControl`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/j02_loop_body_needs_control.rs -o /tmp/j02.rmeta
//! ```
//! 预期：E0277 `NoControl: LoopControl` 不成立（reader 是形状约束的一部分）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::loop_orchestrator::{LoopBuilder, Retry1};
use core::signature::{Data, SyncFnSig};

struct Input(u32);
struct NoControl(u32);

fn body(input: &Input) -> Result<NoControl, BodyError> {
    Ok(NoControl(input.0))
}

fn main() {
    let mut retry: LoopBuilder<Retry1<Input, NoControl>> = LoopBuilder::start().expect("retry");
    let _ = retry.then_body::<_, SyncFnSig<(Input,), Data<NoControl>>>(
        body as fn(&Input) -> Result<NoControl, BodyError>,
    );
}
