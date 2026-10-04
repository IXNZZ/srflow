//! F15 负例：未完成 Builder 与完成态封装的边界。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/f15_unfinished_builder_and_sealed_completion.rs \
//!   -o /tmp/f15.rmeta
//! ```
//!
//! 预期：
//! - 未完成 Builder 传入 `then`（E0277：不满足 `BuildSite`／无 Orchestrator 协议）；
//! - 完成态追加 Step 或重新完成（E0599：`Flow` 没有 `then`／`finish`）；
//! 直接构造完成态结构体字段由 `f15b_sealed_flow_fields.rs` 单独覆盖（隐私检查需独立编译单元）。
//! 正例见 `core::v21_06_tests::f01_*`、`f13_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{Definition, TypedCallBuilder};
use core::context::BodyError;
use core::data_ref::DataRef;
use core::flow::{Flow, FlowBuilder};
use core::signature::Data;

fn inc(a: &u32) -> Result<u64, BodyError> {
    Ok(u64::from(*a))
}

fn main() {
    let mut parent = Definition::new();
    let number = parent.declare_input::<u32>("n").expect("position");
    let (builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    // 负例 1：未完成 Builder 接入 then。
    let _ = parent.then(builder, number.clone());

    let (mut builder, _) = FlowBuilder::<(u32,)>::start().expect("builder");
    let widened: DataRef<u64> = builder.then(inc, input.clone()).expect("step");
    let flow: Flow<(u32,), Data<u64>> = builder.finish(widened).expect("finish");
    // 负例 2：完成态追加 Step。
    let _ = flow.then(inc, input.clone());
    // 负例 3：完成态重新完成。
    let _ = flow.finish(());
}
