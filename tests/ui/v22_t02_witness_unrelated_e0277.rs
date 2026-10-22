//! 判定能力见证：完全不调用 SRFlow 的无关 E0277，不能冒充“unit Factory 被拒”。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t02_witness_unrelated_e0277.rs \
//!   -o /tmp/v22_t02_witness_unrelated_e0277.rmeta
//! ```
//!
//! 本文件不含任何 SRFlow 调用；`require_copy::<String>()` 触发 E0277。
//! 见证清单仍以 `unit Factory` 的锚点/符号要求判定，因此严格匹配必须判 FAIL
//! （仅比较错误码的旧判定会误判 PASS）。

fn require_copy<T: Copy>() {}

fn main() {
    require_copy::<String>();
}
