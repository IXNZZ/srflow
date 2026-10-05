//! H25 负例：构建态 Each 不实现 Orchestrator 协议，不能作为父 Flow 的 child。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_unfinished_builder_as_child.rs \
//!   -o /tmp/h25_unfinished_child.rmeta
//! ```
//!
//! 预期错误在 `then` 调用行（E0277：`EachBuilder<EachOnly<u32>, u32>: OrchCall<(Vec<u32>,), Data<Vec<u32>>>` 不成立）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::each::{EachBuilder, EachOnly};
use core::flow::FlowBuilder;
use core::signature::{Data, OrchSig};

fn main() {
    let (mut parent, collection) = FlowBuilder::<(Vec<u32>,)>::start().expect("parent");
    let unfinished = EachBuilder::<EachOnly<u32>, u32>::start().expect("each");
    let _ = parent.then::<_, OrchSig<Vec<u32>, Data<Vec<u32>>>, _>(unfinished, collection);
}
