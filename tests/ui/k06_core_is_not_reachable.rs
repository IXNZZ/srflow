//! K24 负例（外部视角）：外部无法取得 Runtime／提取入口。
//!
//! 本文件必须针对已构建的 `srflow` rlib 编译（不使用 `#[path]` 装配），否则 core 项反而可见。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/k06_core_is_not_reachable.rs -o /tmp/k06.rmeta
//! ```
//! 预期失败：E0603（`core` 与 `Runtime` 对外私有）。

use srflow::core::runtime::Runtime;

fn main() {
    let _ = std::mem::size_of::<Runtime>();
}
