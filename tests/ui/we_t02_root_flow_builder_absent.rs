//! WE-T02 B22 负例：迁移后旧名 `FlowBuilder` 不残留于 crate 根。
//!
//! 新契约占用 crate 根的正式名称；旧公开门面已移至 `srflow::legacy`
//! （见 `we_t02_legacy_flow_builder_reachable.rs`）。根路径下不再可达。
//!
//! 期望主诊断：E0432（`srflow::FlowBuilder` 无法解析的导入）。

use srflow::FlowBuilder;

fn main() {
    let _ = std::mem::size_of::<FlowBuilder<()>>();
}
