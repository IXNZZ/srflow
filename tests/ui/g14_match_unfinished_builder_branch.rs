//! G14 负例：未完成的 `FlowBuilder` 不能作为 Match 的 branch 登记。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g14_match_unfinished_builder_branch.rs \
//!   -o /tmp/g14.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `branch` 调用行（E0277：
//! 构建态不满足 `BuildSite`）。完整 Flow 作为 branch 的合法对照见
//! `core::v21_07_tests::m03_*` 与 `m23_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::flow::FlowBuilder;
use core::match_orchestrator::MatchBuilder;
use core::signature::Data;

struct Seed(u32);

#[derive(PartialEq, Eq)]
struct Route(u8);

fn main() {
    let (unfinished, _seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let mut builder = MatchBuilder::<Route, Seed, Data<u32>>::start().expect("match");
    let _ = builder.branch(Route(0), unfinished);
}
