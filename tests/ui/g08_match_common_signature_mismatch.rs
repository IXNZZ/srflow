//! G08 负例：branch 的完成输出分类与 Match 的共同 `K` 不符，必须编译失败。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g08_match_common_signature_mismatch.rs \
//!   -o /tmp/g08.rmeta
//! ```
//!
//! 装配真实 `src/core/mod.rs`，不使用 `--cfg test`；预期错误在 `branch` 调用行
//! （E0277：`(DataRef<u32>, DataRef<u32>)` 不满足 `FlowOutput<Data<u32>>`）。
//! 正例见 `core::v21_07_tests::m03_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::builder::TypedCallBuilder;
use core::context::BodyError;
use core::data_ref::DataRef;
use core::flow::FlowBuilder;
use core::match_orchestrator::MatchBuilder;
use core::signature::{Data, Out2};

struct Seed(u32);

#[derive(PartialEq, Eq)]
struct Route(u8);

fn inc(seed: &Seed) -> Result<u32, BodyError> {
    Ok(seed.0)
}

fn main() {
    let (mut child, seed) = FlowBuilder::<(Seed,)>::start().expect("builder");
    let first: DataRef<u32> = child.then(inc, seed.clone()).expect("a");
    let second: DataRef<u32> = child.then(inc, seed).expect("b");
    let pair: core::flow::Flow<(Seed,), Out2<u32, u32>> = child.finish((first, second)).expect("finish");
    let mut builder = MatchBuilder::<Route, Seed, Data<u32>>::start().expect("match");
    let _ = builder.branch(Route(0), pair);
}
