//! L23 负例：Each 元素连接错误在类型层拒绝。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/l02_wrong_each_element.rs -o /tmp/l02.rmeta
//! ```
//! 预期：E0277／E0271——body 包装输入必须是 `(State,)`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::data_ref::DataRef;
use core::each::{EachBuilder, EachOnly};
use core::flow::{Flow, FlowBuilder};
use core::signature::{Data, OrchSig, SyncFnSig};

struct State(u32);
struct Other(u32);
struct ItemResult(u32);

fn main() {
    // body 以 `Other` 为输入，而 Each 的元素是 `State`。
    let (mut body, other) = FlowBuilder::<(Other,)>::start().expect("body");
    let out: DataRef<ItemResult> = body
        .then::<_, SyncFnSig<(Other,), Data<ItemResult>>, _>(
            (|other: &Other| Ok(ItemResult(other.0))) as fn(&Other) -> Result<ItemResult, BodyError>,
            other,
        )
        .expect("result step");
    let body: Flow<(Other,), Data<ItemResult>> = body.finish::<Data<ItemResult>, _>(out).expect("finish");
    let mut each: EachBuilder<EachOnly<State>, ItemResult> = EachBuilder::start().expect("each");
    // 期望：E0277／E0271——元素类型不符。
    let _ = each.then_body::<_, OrchSig<Other, Data<ItemResult>>>(body);
}
