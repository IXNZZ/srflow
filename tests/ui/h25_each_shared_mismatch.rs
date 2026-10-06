//! H25 负例：Each 的 shared 输入类型不符（`EachShared<u32, u32>` 接线到 `(Vec<u32>, u64)`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_shared_mismatch.rs \
//!   -o /tmp/h25_shared.rmeta
//! ```
//!
//! 预期错误在 `then` 调用行（E0271：`<EachShared<u32, u32> as EachShape>::I == (Vec<u32>, u64)` 不成立）。

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
    let (mut parent, (collection, shared)) = FlowBuilder::<(Vec<u32>, u64)>::start().expect("parent");
    let mut builder = EachBuilder::<EachShared<u32, u32>, u32>::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(u32, u32), Data<u32>>>(
            body as fn(&u32, &u32) -> Result<u32, BodyError>,
        )
        .expect("body");
    let each = builder.finish().expect("finish");
    // shared 类型错：Each 期望 `u32`，这里给 `u64`。
    let _ = parent.then::<_, OrchSig<(Vec<u32>, u64), Data<Vec<u32>>>, _>(each, (collection, shared));
}
