//! 判定能力见证：完全不调用 SRFlow 的无关 E0277，不能冒充“unit Factory 被拒”。
//!
//! ```sh
//! rustc --edition=2024 --emit=metadata tests/ui/v22_t03_from_t02_witness_unrelated_e0277.rs \
//!   -o /tmp/v22_t03_from_t02_witness_unrelated_e0277.rmeta
//! ```
//!
//! 本文件不含任何 SRFlow 调用；`require_copy::<String>()` 触发 E0277。
//! 见证清单仍以 T03 目标正例 `unit Factory`（见 `v22_t03_from_t02_neg_provide_unit.rs`）的
//! 锚点行 `flow.provide(|| ());` 与符号 `Data` 判定，因此严格匹配必须判 FAIL
//! （仅比较错误码的宽松判定会误判 PASS）。
//!
//! T03 迁移：见证目的（宽松判定误通过、严格判定拒绝）不变；被冒充的目标仍是同一
//! “unit Factory 编译拒绝”正例，仅该正例文件改名为 `v22_t03_from_t02_neg_provide_unit.rs`。

fn require_copy<T: Copy>() {}

fn main() {
    require_copy::<String>();
}
