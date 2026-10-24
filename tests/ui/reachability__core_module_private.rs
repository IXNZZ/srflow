//! A04 边界负例（外部视角）：业务侧不能取得 core 内部类型或通用存储入口。
//!
//! 与其它 fixture 不同，本文件必须针对已构建的 `srflow` rlib 编译，而不是用
//! `#[path]` 装配内部源码；否则 core 项反而可见，得不到预期失败。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/reachability__core_module_private.rs -o /tmp/a04_core_is_not_reachable.rmeta
//! ```
//!
//! 预期失败：`core` 模块对外私有（E0603），且其中类型不可解析（E0432）。
//! V22-T03 迁移：源 `tests/ui/a04_core_is_not_reachable.rs`。T03 未在 crate 根新增该旧名、`mod core` 仍私有，
//! 因此「根不可达」判据逐字承接：主诊断 E0432。
use srflow::core::DataId;

fn main() {
    let _ = std::mem::size_of::<DataId>();
}