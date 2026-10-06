//! H25 负例：Each body 的输入类型与集合元素不符（`&u64` 对 `EachOnly<u32>`）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_wrong_body_input.rs \
//!   -o /tmp/h25_body_input.rmeta
//! ```
//!
//! 预期错误在 `then_body` 调用行（E0277：`fn(&u64) -> …` 不满足 `BuildSite<_, DataRef<u32>>`）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::each::{EachBuilder, EachOnly};
use core::signature::{Data, SyncFnSig};

fn wrong(item: &u64) -> Result<u32, BodyError> {
    Ok(*item as u32)
}

fn main() {
    let mut builder = EachBuilder::<EachOnly<u32>, u32>::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(u64,), Data<u32>>>(wrong as fn(&u64) -> Result<u32, BodyError>)
        .expect("body");
}
