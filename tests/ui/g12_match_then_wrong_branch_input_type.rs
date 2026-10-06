//! G12 负例：Match 作为 child 接线时，第二个业务输入的类型与 `Match<R, A, K>` 的 `A` 不符。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g12_match_then_wrong_branch_input_type.rs \
//!   -o /tmp/g12.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `then` 调用行
//! （E0308：需要 `(DataRef<Route>, DataRef<Seed>)`，第二个位置给了 `DataRef<Route>`）。
//! 合法对照见 `core::v21_07_tests::m03_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::data_ref::DataRef;
use core::match_orchestrator::MatchBuilder;
use core::signature::Data;

struct Seed(u32);

#[derive(PartialEq, Eq)]
struct Route(u8);

fn ok(seed: &Seed) -> Result<u32, BodyError> {
    Ok(seed.0)
}

fn main() {
    let (mut parent, seed) = core::flow::FlowBuilder::<(Seed,)>::start().expect("parent");
    let mut builder = MatchBuilder::<Route, Seed, Data<u32>>::start().expect("match");
    builder.branch(Route(0), ok).expect("branch");
    let matched = builder.finish().expect("finish");
    let mut route_definition = core::builder::Definition::new();
    let route = route_definition.declare_input::<Route>("route").expect("route");
    // 第二个输入类型错：Match 需要 `DataRef<Seed>`，这里给 `DataRef<Route>`。
    let _ = parent.then(matched, (route.clone(), route));
    let _ = seed;
    let _: Option<DataRef<u32>> = None;
}
