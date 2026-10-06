//! G15 负例：Match 作为 child 接线时，第一个输入（路由 `R`）的类型与 `Match<R, A, K>` 不符。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g15_match_then_wrong_route_type.rs \
//!   -o /tmp/g15.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `then` 调用行（E0308：
//! 需要 `(DataRef<Route>, DataRef<Seed>)`，这里给了 `(DataRef<u8>, DataRef<Seed>)`）。
//! 合法对照见 `core::v21_07_tests::m03_*`（路由与业务输入类型都正确）。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
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
    // 路由位置类型错：Match 需要 `DataRef<Route>`，这里给 `DataRef<u8>`。
    let wrong_route = route_definition.declare_input::<u8>("route").expect("route");
    let _ = parent.then(matched, (wrong_route, seed));
}
