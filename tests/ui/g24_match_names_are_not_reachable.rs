//! G24 边界负例（外部视角）：业务侧不能取得内部 Match 类型或其构建态。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/g24_match_names_are_not_reachable.rs -o /tmp/g24.rmeta
//! ```
//!
//! 预期失败：`core` 模块对外私有（E0603），因此 `core::match_orchestrator`、
//! `MatchBuilder` 与 `Match` 都不可解析（E0432／E0433）。正例见
//! `core::v21_07_tests::m01_*` 等 crate 内样本。

use srflow::core::match_orchestrator::{Match, MatchBuilder};

fn main() {
    let _ = std::mem::size_of::<MatchBuilder<u8, u8, ()>>();
    let _ = std::mem::size_of::<Match<u8, u8, ()>>();
}
