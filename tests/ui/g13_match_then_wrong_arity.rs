//! G13 负例：Match 作为 child 接线时参数数量与 `Match<R, A, K>` 的 `(R, A)` 不符。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g13_match_then_wrong_arity.rs \
//!   -o /tmp/g13.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `then` 调用行
//! （E0308：需要 `(DataRef<Route>, DataRef<Seed>)`，这里只给一个位置）。

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
    // 只给一个位置：Match 需要 (路由, 业务输入) 两个。
    let _ = parent.then(matched, seed);
}
