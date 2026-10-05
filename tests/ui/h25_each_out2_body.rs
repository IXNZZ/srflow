//! H25 负例：Each body 不能是两个独立输出（Out2），必须收集单份 owned `O`。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_out2_body.rs -o /tmp/h25_out2.rmeta
//! ```
//!
//! 预期错误在 `then_body` 调用行（E0271：`<OrchSig<u32, Out2<u32, u32>> as Wiring>::BuildOutput == DataRef<u32>` 不成立）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::each::{EachBuilder, EachOnly};
use core::flow::FlowBuilder;
use core::signature::{Data, OrchSig, Out2, SyncFnSig};

fn body(item: &u32) -> Result<u32, BodyError> {
    Ok(*item)
}

fn first(item: &u32) -> Result<u32, BodyError> {
    Ok(*item)
}

fn second(item: &u32) -> Result<u32, BodyError> {
    Ok(*item + 1)
}

fn main() {
    let (mut outer, input) = FlowBuilder::<(u32,)>::start().expect("outer");
    let one = outer
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(first as fn(&u32) -> Result<u32, BodyError>, input.clone())
        .expect("one");
    let two = outer
        .then::<_, SyncFnSig<(u32,), Data<u32>>, _>(second as fn(&u32) -> Result<u32, BodyError>, input)
        .expect("two");
    let body_flow = outer.finish::<Out2<u32, u32>, _>((one, two)).expect("finish");
    let mut builder = EachBuilder::<EachOnly<u32>, u32>::start().expect("each");
    let _ = body;
    builder
        .then_body::<_, OrchSig<u32, Out2<u32, u32>>>(body_flow)
        .expect("body");
}
