//! Engine 只读观察面：真实调用建立的 child Scope 属于哪一类调用。
//!
//! B 阶段清理后本模块只保留 [`ScopeRole`]。它**不是**任何执行协议的一部分：没有为
//! Match／Branch 增加 `InvocationKind`，分支边界仍是
//! [`InvocationKind::Boundary`](super::context::InvocationKind::Boundary)。本标签只用于
//! `cfg(test)` 只读记录真实创建点（见 `test_support::boundary_creation_snapshot` 与
//! `engine/boundary.rs` 的 `#[cfg(test)]` 记录调用），说明该 child Scope 是由哪一类
//! 调用建立的。新主链实际产生 `Flow`／`Each`／`Item` 三类；旧 Match／Loop 控制器的
//! 标签随其协议退出后不再由生产路径构造，仅作为观察枚举的完整取值保留。

/// 调用语义中的 Scope 角色（只读诊断用标签，不是新的身份种类）。
///
/// 仅由 `cfg(test)` 只读观察路径消费（`test_support` 与 `engine/boundary.rs` 的
/// `#[cfg(test)]` 记录块），因此整项在非 test 构建下不存在。
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeRole {
    /// Flow／SubFlow 调用建立的 child Scope。
    Flow,
    /// Match 调用建立的 child Scope（MatchScope）。
    #[allow(dead_code)] // 旧 Match 协议退出后不再由生产路径构造，仅保留观察取值
    Match,
    /// Match 内部被选 branch 的包装调用建立的 child Scope（BranchScope）。
    #[allow(dead_code)] // 旧 Match 协议退出后不再由生产路径构造，仅保留观察取值
    Branch,
    /// Each 调用建立的 child Scope（EachScope）。
    Each,
    /// Each 内部每个 item 建立的 child Scope（ItemScope）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    Item,
    /// Loop 调用建立的 child Scope（LoopScope）。
    #[allow(dead_code)] // 旧 Loop 协议退出后不再由生产路径构造，仅保留观察取值
    Loop,
    /// Loop 内部每轮建立的 child Scope（RoundScope）。
    #[allow(dead_code)] // 旧 Loop 协议退出后不再由生产路径构造，仅保留观察取值
    Round,
}
