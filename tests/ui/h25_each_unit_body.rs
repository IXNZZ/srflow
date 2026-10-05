//! H25 负例：Each body 的输出不能是 `()`（Unit 不是业务 Data）。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_unit_body.rs -o /tmp/h25_unit.rmeta
//! ```
//!
//! 预期错误在 `then_body` 调用行（E0271：`<SyncFnSig<(u32,), Unit> as Wiring>::BuildOutput == DataRef<u32>` 不成立）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::each::{EachBuilder, EachOnly};
use core::signature::{SyncFnSig, Unit};

fn unit_body(_item: &u32) -> Result<(), BodyError> {
    Ok(())
}

fn main() {
    let mut builder = EachBuilder::<EachOnly<u32>, u32>::start().expect("each");
    builder
        .then_body::<_, SyncFnSig<(u32,), Unit>>(unit_body as fn(&u32) -> Result<(), BodyError>)
        .expect("body");
}
