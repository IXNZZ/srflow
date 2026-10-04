//! G07 负例：branch 的输入类型与 Match 的业务输入 `A` 不符，必须在真实 typed 边界编译失败。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/g07_match_branch_wrong_input_type.rs \
//!   -o /tmp/g07.rmeta
//! ```
//!
//! 夹具按真实 `src/core/mod.rs` 装配，不使用 `--cfg test`；预期错误在 `branch` 调用行
//! （E0277：`fn(&String)` 不满足 `BuildSite<_, DataRef<Seed>>`）。正例见
//! `core::v21_07_tests::m02_*`。

#[path = "../../src/core/mod.rs"]
mod core;

use core::context::BodyError;
use core::match_orchestrator::MatchBuilder;
use core::signature::Data;

struct Seed(u32);

#[derive(PartialEq, Eq)]
struct Route(u8);

fn wants_text(text: &String) -> Result<u32, BodyError> {
    Ok(text.len() as u32)
}

fn main() {
    let mut builder = MatchBuilder::<Route, Seed, Data<u32>>::start().expect("match");
    let _ = builder.branch(Route(0), wants_text);
}
