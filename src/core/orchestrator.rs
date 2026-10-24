//! Engine 只读观察面：真实调用建立的 child Scope 属于哪一类调用。
//!
//! B 阶段清理后本模块只保留 [`ScopeRole`]。它**不是**任何执行协议的一部分：没有为
//! Match／Branch 增加 `InvocationKind`，分支边界仍是
//! [`InvocationKind::Boundary`](super::context::InvocationKind::Boundary)。本标签只用于
//! `cfg(test)` 只读记录真实创建点（见 `test_support::boundary_creation_snapshot` 与
//! `engine/boundary.rs` 的 `#[cfg(test)]` 记录调用），说明该 child Scope 是由哪一类
//! 调用建立的。新主链实际产生 `Flow`／`Each`／`Item` 三类；旧 Match／Loop 控制器的
//! 标签随其协议退出后不再由生产路径构造，仅作为观察枚举的完整取值保留。

#[cfg(test)]
#[path = "../../tests/internal/core/orchestrator_observe.rs"]
pub(crate) mod observe;
