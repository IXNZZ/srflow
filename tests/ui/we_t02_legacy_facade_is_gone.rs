//! WE-T02 B23（B 阶段按任务书 §7.1／X29 反转）：旧门面命名空间已完全退出。
//!
//! 历史：原夹具名 `we_t02_legacy_flow_builder_reachable.rs`，是 A 阶段之前的**过渡期正例**
//! （证明旧 `FlowBuilder` 仅在历史命名空间 `srflow::legacy` 可达、不残留于 crate 根）。
//! B 阶段退出 `pub mod legacy` 后，本夹具的判据反转为：**该命名空间不存在**，
//! 因此必须编译失败。这是 X29「旧公开门面退出」的直接证据，与
//! `we_t02_root_flow_builder_absent.rs`（根不可达）互为正反。
//!
//! 期望主诊断：E0432（`unresolved import`：`srflow` 中不存在 `legacy`）。

use srflow::legacy::FlowBuilder;

fn main() {
    let _ = std::mem::size_of::<FlowBuilder<()>>();
}
