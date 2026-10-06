//! H25 负例（外部视角）：Each 及其 cap／collector 内部能力对外不可访问。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/h25_each_names_are_not_reachable.rs -o /tmp/h25_reach.rmeta
//! ```
//!
//! 预期失败：`core` 模块对外私有（E0603），`each` 模块与其类型不可解析（E0432/E0433）。

use srflow::core::each::{Each, EachBuilder};

fn main() {
    let _ = std::mem::size_of::<Option<Each<(), ()>>>();
    let _ = std::mem::size_of::<Option<EachBuilder<(), ()>>>();
}
