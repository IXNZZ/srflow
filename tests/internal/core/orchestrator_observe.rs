//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。

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
