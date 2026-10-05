//! K04／K23 负例：构建态 `FlowBuilder` 不能作为 Root 执行。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/k01_unfinished_builder_is_not_a_root.rs -o /tmp/k01.rmeta
//! ```
//! 预期：E0277——`FlowBuilder<(Left,)>` 不是 `OrchCall<_, _>`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::flow::FlowBuilder;
use core::runtime::Runtime;
use core::signature::Unit;

struct Left(u32);

fn main() {
    let (builder, _handle): (FlowBuilder<(Left,)>, _) = FlowBuilder::start().expect("start");
    // 期望：E0277——未完成 Builder 不能作为 Root。
    let _ = Runtime::execute::<_, _, Unit>(&builder, (Left(1),));
}
