//! F14 负例：完整 Flow 的接线与完成选择必须在 typed 边界被拒绝。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/f14_flow_wiring_and_finish_type_negatives.rs \
//!   -o /tmp/f14.rmeta
//! ```
//!
//! 预期四处独立主诊断：
//! - 用错 DataRef 类型接线完整 child Flow；
//! - 单输入 Flow 给了两个位置（参数数量不符）；
//! - `finish` 选择类型与标注的输出分类不符；
//! - 一位业务 tuple Data 当作两个位置使用。
//! 正例见 `core::v21_06_tests::f02_*`、`f04_*`、`f13_*`、`f16_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::data_ref::DataRef;
use core::flow::{Flow, FlowBuilder};
use core::signature::Data;

fn inc(a: &u32) -> Result<u64, BodyError> {
    Ok(u64::from(*a))
}

fn main() {
    let (child, child_input) = {
        let (mut builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
        let widened: DataRef<u64> = builder.then(inc, input.clone()).expect("step");
        (builder.finish(widened).expect("finish"), input)
    };
    let _ = child_input;

    let mut parent = core::builder::Definition::new();
    let text = parent.declare_input::<String>("t").expect("position");
    // 负例 1：child 声明 u32 输入，接线给 String 位置。
    let _ = parent.then(child.clone(), text.clone());

    let number = parent.declare_input::<u32>("n").expect("position");
    // 负例 2：单输入 Flow 给了两个位置。
    let _ = parent.then(child.clone(), (number.clone(), number.clone()));

    // 负例 3：标注的输出分类与 finish 选择不符。
    let (builder, input) = FlowBuilder::<(u32,)>::start().expect("builder");
    let narrow: DataRef<u32> = input;
    let _: Flow<(u32,), Data<u64>> = builder.finish(narrow).expect("finish");

    // 负例 4：一位业务 tuple Data 不能当作两个位置。
    let (mut tuple_builder, tuple_input) = FlowBuilder::<((u32, u64),)>::start().expect("builder");
    let text: DataRef<String> = tuple_builder.then(tuple_echo, tuple_input).expect("step");
    let _ = tuple_builder.finish::<Data<String>, _>((text.clone(), text.clone()));
}

fn tuple_echo(a: &(u32, u64)) -> Result<String, BodyError> {
    Ok(format!("{}/{}", a.0, a.1))
}
