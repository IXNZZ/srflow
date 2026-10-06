//! E24 负例（外部视角）：业务侧不能取得 `DataRef`／`CallSite` 等内部接线类型。
//!
//! 与其它 fixture 不同，本文件必须针对已构建的 `srflow` rlib 编译，而不是用
//! `#[path]` 装配内部源码；否则 core 项反而可见，得不到预期失败。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/e24_internal_wiring_names_are_not_reachable.rs -o /tmp/e24.rmeta
//! ```
//!
//! 预期失败：`core::data_ref`／`core::builder` 对外不可见（E0432），其中类型不可解析
//! （E0603）。

use srflow::core::builder::CallSite;
use srflow::core::data_ref::DataRef;

fn main() {
    let _ = std::mem::size_of::<Option<CallSite>>();
    let _ = std::mem::size_of::<Option<DataRef<u32>>>();
}
