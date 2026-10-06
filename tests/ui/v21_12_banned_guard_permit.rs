//! V21-12 外部禁名负例（Invocation guard 与 collector permit）：本文件必须针对已构建的 `srflow` rlib 编译。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_banned_guard_permit.rs -o /tmp/v21_12_banned_guard_permit.rmeta
//! ```
//! 预期失败：E0432／E0603——crate 根没有该名字，`core` 模块不可达。
//!
//! 该名字由 crate 内部持有；业务侧不得导入、构造或命名。

use srflow::InvocationGuard;
use srflow::core as _;

fn main() {}
