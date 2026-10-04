//! G09 负例：构建态 `MatchBuilder` 不是完成态，不能作为 child 组合或执行。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g09_match_builder_is_not_composable.rs \
//!   -o /tmp/g09.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `then` 调用行
//! （E0277：`&mut MatchBuilder<...>` 不满足 `BuildSite<_, _>`）。正例见
//! `core::v21_07_tests::m01_*`（只有 `finish` 之后的 Match 才能接线）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::flow::FlowBuilder;
use core::match_orchestrator::MatchBuilder;
use core::signature::Data;

struct Seed(u32);

#[derive(PartialEq, Eq)]
struct Route(u8);

fn main() {
    let (mut parent, seed) = FlowBuilder::<(Seed,)>::start().expect("parent");
    let mut builder = MatchBuilder::<Route, Seed, Data<u32>>::start().expect("match");
    let _ = parent.then(&mut builder, seed);
}
