//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::context::ExecutionContext;
use crate::core::identity::ScopeId;
use crate::core::ref_id::RefId;
use std::cell::Cell;

/// cfg(test)：安装一次 Root 退出清理故障（业务 Failure／Control 收口的清理诊断样本）。
#[cfg(test)]
pub(crate) fn install_root_cleanup_fault() {
    install_root_cleanup_faults(1);
}

/// cfg(test)：安装 N 次 Root 退出清理故障（后续故障不得覆盖首清理诊断的样本）。
#[cfg(test)]
pub(crate) fn install_root_cleanup_faults(count: usize) {
    ROOT_CLEANUP_FAULT.with(|fault| fault.set(count));
}

#[cfg(test)]
pub(super) fn take_root_cleanup_fault() -> usize {
    ROOT_CLEANUP_FAULT.with(|fault| fault.replace(0))
}

/// cfg(test)：安装一次 Root 输入登记失败前提（旧 `k05` 的 `PrebindInput` 承接）。
///
/// 安装后，登记第 `index`（0 基）个声明输入前，先把该声明位置预先绑定到已成功接管的
/// 第一个输入；随后**未修改**的 `register_owned_erased` 命中位置占用检查并返回
/// `RefAlreadyBound`。这只是破坏前提：自然业务不会产生该状态——静态输入形状的每个声明
/// 位置互不相同且初始未绑定，登记入口的占用检查只防守重复登记。
#[cfg(test)]
pub(crate) fn install_root_input_prebind_fault(index: usize) {
    ROOT_INPUT_PREBIND.with(|fault| fault.set(Some(index)));
}

#[cfg(test)]
pub(super) fn take_root_input_prebind_fault() -> Option<usize> {
    ROOT_INPUT_PREBIND.with(|fault| fault.take())
}

/// cfg(test) 故障注入：把第 `index` 个声明输入位置预绑定到已接管的首个输入。
pub(super) fn inject_root_input_prebind_fault_if(
    context: &mut ExecutionContext,
    prebind_fault: Option<usize>,
    index: usize,
    root: &ScopeId,
    position: &RefId,
    seed: Option<&crate::core::identity::DataId>,
) {
    if prebind_fault == Some(index)
        && let Some(seed) = seed
    {
        context.inject_scope_target_probe(
            root,
            position,
            crate::core::scope::RefTarget::Data(seed.clone()),
        );
    }
}

/// cfg(test) 故障注入：按已安装次数销毁前 N 个 Root 输入（Root 退出清理诊断样本）。
pub(super) fn inject_root_cleanup_faults(
    context: &mut ExecutionContext,
    ids: &[crate::core::identity::DataId],
) {
    let faults = take_root_cleanup_fault();
    for id in ids.iter().take(faults) {
        context.destroy_probe(id);
    }
}

#[cfg(test)]
thread_local! {
    /// cfg(test)：登记 owned 输入后销毁前 N 个 entry，使 Root 退出清理真实失败。
    static ROOT_CLEANUP_FAULT: Cell<usize> = const { Cell::new(0) };
    /// cfg(test)：登记第 N 个 Root 输入前，先把该声明位置预先绑定（构造 `RefAlreadyBound` 前提）。
    static ROOT_INPUT_PREBIND: Cell<Option<usize>> = const { Cell::new(None) };
}
