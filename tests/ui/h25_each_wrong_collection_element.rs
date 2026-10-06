//! H25 负例：Each 的集合输入元素类型不符（`EachOnly<u32>` 接线到 `Vec<u64>`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_wrong_collection_element.rs \
//!   -o /tmp/h25_collection.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `then` 调用行
//! （E0271：`<EachOnly<u32> as EachShape>::I == (Vec<u64>,)` 不成立）。
//! 合法对照见 `core::v21_08_tests::h01_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::{Definition, TypedCallBuilder};
use core::context::BodyError;
use core::data_ref::DataRef;
use core::each::{EachBuilder, EachOnly};
use core::flow::FlowBuilder;
use core::signature::{Data, OrchSig, SyncFnSig};

fn body(item: &u32) -> Result<u32, BodyError> {
    Ok(*item)
}

fn main() {
    let (mut parent, collection) = FlowBuilder::<(Vec<u64>,)>::start().expect("parent");
    let mut builder = EachBuilder::<EachOnly<u32>, u32>::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(u32,), Data<u32>>>(body as fn(&u32) -> Result<u32, BodyError>)
        .expect("body");
    let each = builder.finish().expect("finish");
    // 集合元素类型错：Each 期望 `Vec<u32>`，这里给 `Vec<u64>`。
    let _ = parent.then::<_, OrchSig<Vec<u64>, Data<Vec<u32>>>, _>(each, collection);
    let _: Option<DataRef<Vec<u32>>> = None;
    let _ = Definition::new();
}
