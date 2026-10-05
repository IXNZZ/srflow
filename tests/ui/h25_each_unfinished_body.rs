//! H25 负例：未完成的 `FlowBuilder` 不能作为 Each body 登记。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/h25_each_unfinished_body.rs \
//!   -o /tmp/h25_unfinished_body.rmeta
//! ```
//!
//! 预期错误在 `then_body` 调用行（E0277：`FlowBuilder<(u32,)>: BuildSite<_, DataRef<u32>>` 不成立）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::each::{EachBuilder, EachOnly};
use core::flow::FlowBuilder;

fn main() {
    let (unfinished, _input) = FlowBuilder::<(u32,)>::start().expect("unfinished");
    let mut builder = EachBuilder::<EachOnly<u32>, u32>::start().expect("each");
    builder.then_body(unfinished).expect("body");
}
