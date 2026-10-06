//! H25 负例：本任务每个 Each 只支持零／一个 shared，额外输入没有实现。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_extra_input_arity.rs \
//!   -o /tmp/h25_arity.rmeta
//! ```
//!
//! 预期错误在 `FlowBuilder::<(Vec<u32>, u32, u32)>::start()` 行（E0277：三输入的
//! `FlowInputs` 没有实现）。本任务每个 Each 只接受零／一个 shared，因此"再加一个 shared"
//! 在接线层就已经无法表达：父 Flow 无法声明三个输入位置。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::each::{EachBuilder, EachShared};
use core::flow::FlowBuilder;
use core::signature::{Data, OrchSig, SyncFnSig};

fn body(item: &u32, shared: &u32) -> Result<u32, BodyError> {
    Ok(*item + *shared)
}

fn main() {
    let (mut parent, (collection, shared, extra)) =
        FlowBuilder::<(Vec<u32>, u32, u32)>::start().expect("parent");
    let mut builder = EachBuilder::<EachShared<u32, u32>, u32>::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(u32, u32), Data<u32>>>(
            body as fn(&u32, &u32) -> Result<u32, BodyError>,
        )
        .expect("body");
    let each = builder.finish().expect("finish");
    // 第二个 shared 超出本任务范围：没有 OrchCall／接线实现。
    let _ = parent.then::<_, OrchSig<(Vec<u32>, u32, u32), Data<Vec<u32>>>, _>(
        each,
        (collection, shared, extra),
    );
}
