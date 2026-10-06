//! 一次 Root Execution 的拥有者与驱动。
//!
//! `RootExecution` 是内部 Root 设施：它创建身份根与 [`ExecutionContext`]，并作为本次
//! Execution 的唯一拥有者。nested 调用只接收 `&mut ExecutionContext`，没有重建 Root
//! 或替换 Container 的入口，因此一次 Execution 始终只有一个身份根、一个协调组件与
//! 唯一 DataContainer。
//!
//! V21-10 起本模块同时交付 Application 侧唯一的生产 Root 入口 [`Runtime::execute`]：
//! 它登记 owned Root 输入、经 `orchestrator.rs` 的受控装配入口运行完成态 Orchestrator，
//! 再按 [`RootOutputs`] 的声明形状整组提取 owned 输出。`RootExecution`／`run_root` 仍是
//! 该路径内部的新 Execution 构造与 frame／失败清理机制；`test_support` 的空输出驱动只
//! 保留测试观察职责。

use std::any::Any;

#[cfg(test)]
use super::context::ExecutionTermination;
use super::context::{
    BodyError, CleanupDiagnostic, ExecutionContext, InvocationKind, TerminationKind,
};
use super::identity::{ExecutionIdentity, ScopeId};
use super::internal_error::ScopeError;
use super::orchestrator::{OrchCall, PackFromPorts};
use super::root_signature::{RootInputs, RootOutputs, check_root_output_signature};
use super::signature::{BuildError, DeclaredPort, InputTypes, OutKind};

/// 本次 Root Execution 的拥有者。
///
/// 丢弃它（例如丢弃持有它的 Root Future）即销毁 Context 与其中唯一 DataContainer；
/// 丢弃前若仍有未完成调用，由对应 guard 的取消清理负责。
pub(crate) struct RootExecution {
    context: ExecutionContext,
}

impl RootExecution {
    /// 创建一次 Root Execution：新身份根 + 新 Context（含唯一协调组件与 Container）。
    pub(crate) fn start() -> Self {
        Self {
            context: ExecutionContext::new(ExecutionIdentity::new()),
        }
    }

    /// 当前 Context 的只读视图。
    pub(crate) fn context(&self) -> &ExecutionContext {
        &self.context
    }

    /// 当前 Context 的可变视图（供 Root 驱动使用）。
    pub(crate) fn context_mut(&mut self) -> &mut ExecutionContext {
        &mut self.context
    }
}
/// Root 驱动的收口结果：拥有执行体错误、首次终止原因、Root 关闭诊断与首个清理诊断。
///
/// 这是内部驱动的结果记录，不是公开 Execution Error API。原执行失败与清理失败都在这里
/// 保留，互不覆盖，也不会在收口时被丢弃。
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct RootExit {
    body_error: Option<BodyError>,
    termination: Option<ExecutionTermination>,
    close_error: Option<ScopeError>,
    cleanup_failure: Option<CleanupDiagnostic>,
    #[cfg(test)]
    cleanup_events: usize,
}

#[cfg(test)]
impl RootExit {
    /// 执行体的原始失败（含说明与可选 Scope 诊断）。
    pub(crate) fn body_error(&self) -> Option<&BodyError> {
        self.body_error.as_ref()
    }

    /// 首次终止类别（如有）。
    pub(crate) fn terminated(&self) -> Option<TerminationKind> {
        self.termination
            .as_ref()
            .map(|termination| termination.kind())
    }

    /// 首次终止原因说明（如有），例如执行体失败的原文。
    pub(crate) fn termination_note(&self) -> Option<&'static str> {
        self.termination
            .as_ref()
            .map(|termination| termination.note())
    }

    /// 首次终止相关的 Scope（如有）。
    pub(crate) fn termination_scope(&self) -> Option<&ScopeId> {
        self.termination
            .as_ref()
            .and_then(|termination| termination.scope())
    }

    /// RootScope 正常收口的诊断（如有）。
    pub(crate) fn close_error(&self) -> Option<&ScopeError> {
        self.close_error.as_ref()
    }

    /// 首次失败／取消保存的原始 Scope 诊断（如有）。
    pub(crate) fn termination_scope_error(&self) -> Option<&ScopeError> {
        self.termination
            .as_ref()
            .and_then(|termination| termination.scope_error())
    }

    /// 首个清理失败的故障 Scope（如有）。
    pub(crate) fn cleanup_scope(&self) -> Option<&ScopeId> {
        self.cleanup_failure
            .as_ref()
            .map(|diagnostic| diagnostic.scope())
    }

    /// 首个清理失败的诊断（如有）。
    pub(crate) fn cleanup_error(&self) -> Option<&ScopeError> {
        self.cleanup_failure
            .as_ref()
            .map(|diagnostic| diagnostic.error())
    }

    /// guard 在退出中执行清理的次数（仅测试观测）。
    #[cfg(test)]
    pub(crate) fn cleanup_events(&self) -> usize {
        self.cleanup_events
    }
}

/// Root 驱动：拥有 Context 的 async 执行体。
///
/// 进入 Root frame 后运行 `body`；成功返回且本次执行未被终止时，沿已验收的 finalization
/// 路径以**空声明输出与空输出位置**关闭 RootScope。执行体失败、收口失败或执行已被取消时
/// 都走受控失败退出：由 guard 清理 RootScope 与未完成责任，并保留首次原因与清理诊断。
///
/// `config` 是 Root 执行体自带的非 Data 上下文参数（V21-10 的 Root Orchestrator 与
/// 定义），驱动原样透传，不解释也不存储。V21-05 起 `config` 可以是借用（例如
/// `&Definition`）：驱动不要求 `'static`，也不把借用存进任何长期结构。
#[cfg(test)]
pub(crate) async fn run_root<X, F>(mut execution: RootExecution, config: X, body: F) -> RootExit
where
    F: for<'a> AsyncFnOnce(&'a mut ExecutionContext, X) -> Result<(), BodyError>,
{
    let root_scope = execution.context.root_scope();
    let mut close_error: Option<ScopeError> = None;

    let body_error = {
        let mut guard = execution
            .context
            .enter(InvocationKind::Root, &root_scope, true)
            .expect("a fresh execution context accepts its root frame");

        let body_result = body(&mut guard, config).await;
        match &body_result {
            Ok(()) => {
                if guard.is_terminated() {
                    // 执行体返回 Ok，但本次执行已被取消或失败：不能正常收口，
                    // 必须走受控失败退出，由 guard 清理 RootScope 与未完成责任。
                    guard.failed("root closed by a terminated execution");
                } else {
                    // 成功收口：Root frame 仍在，RootScope 属于当前调用的可见范围。
                    match guard.finalize(&root_scope, &[], &mut Vec::new()) {
                        Ok(()) => guard.complete(),
                        Err(error) => {
                            close_error = Some(error);
                            guard.failed("root finalization failed");
                        }
                    }
                }
            }
            Err(error) => guard.failed(error.note()),
        }
        body_result.err()
    };

    // 只取诊断副本：Context 自身的终止状态保持不可恢复。
    let termination = execution.context.termination_report();
    let cleanup_failure = execution.context.cleanup_report();
    RootExit {
        body_error,
        termination,
        close_error,
        cleanup_failure,
        #[cfg(test)]
        cleanup_events: execution.context.cleanup_events(),
    }
}

/// Root 执行错误的阶段：区分构建／装配、body、提取预检与关闭，不把可预期拒绝混成一种。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootErrorStage {
    /// 输入／输出 Signature 预检：真实 Definition 与 `I`／`K` 不一致。
    Signature,
    /// Root 输入登记（装配）失败：业务体不执行。
    Assembly,
    /// Root body（含其 child 调用）执行失败。
    Body,
    /// Root 提取预检拒绝：任何 take 之前整体拒绝。
    Preflight,
    /// Root 提取后的关闭失败（清理未完成；不覆盖同一次执行中更早的失败）。
    Close,
}

/// Root 执行错误：保留原始类别、说明与完整身份，并独立保存清理诊断。
///
/// 这不是公开错误 API（V21-12 收口）。每个阶段保留自己的原始诊断：Signature 阶段保留
/// [`BuildError`]，装配／body／预检／关闭阶段保留真实 [`ScopeError`]（含完整 `ScopeId`／
/// `RefId`／`DataId`），body 错误另保留 [`BodyError`] 的说明；清理失败单独记录，不覆盖首次
/// 失败。外围通用文本不会替换这些字段。
#[derive(Debug)]
pub(crate) struct RootError {
    stage: RootErrorStage,
    note: &'static str,
    build: Option<BuildError>,
    scope: Option<ScopeError>,
    termination: Option<TerminationKind>,
    termination_scope: Option<ScopeId>,
    termination_note: Option<&'static str>,
    close: Option<ScopeError>,
    cleanup: Option<CleanupDiagnostic>,
    registered_inputs: usize,
}

impl RootError {
    fn signature(build: BuildError) -> Self {
        Self {
            stage: RootErrorStage::Signature,
            note: "root signature rejected",
            build: Some(build),
            scope: None,
            termination: None,
            termination_scope: None,
            termination_note: None,
            close: None,
            cleanup: None,
            registered_inputs: 0,
        }
    }

    fn assembly(scope: ScopeError, registered_inputs: usize) -> Self {
        Self {
            stage: RootErrorStage::Assembly,
            note: "root input registration failed",
            build: None,
            scope: Some(scope),
            termination: None,
            termination_scope: None,
            termination_note: None,
            close: None,
            cleanup: None,
            registered_inputs,
        }
    }

    /// 本错误的阶段。
    pub(crate) fn stage(&self) -> RootErrorStage {
        self.stage
    }

    /// 稳定的说明文本。
    pub(crate) fn note(&self) -> &'static str {
        self.note
    }

    /// Signature 阶段的原始构建诊断。
    pub(crate) fn build_error(&self) -> Option<&BuildError> {
        self.build.as_ref()
    }

    /// 装配／body／预检／关闭阶段的原始 Scope 诊断（保留完整身份）。
    pub(crate) fn scope_error(&self) -> Option<&ScopeError> {
        self.scope.as_ref()
    }

    /// 首次终止类别（body／取消）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn termination(&self) -> Option<TerminationKind> {
        self.termination
    }

    /// 首次终止的定位 Scope（最深实际调用位置）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn termination_scope(&self) -> Option<&ScopeId> {
        self.termination_scope.as_ref()
    }

    /// 首次终止时**实际保存**的说明（从真实 `termination_report` 回读）。
    pub(crate) fn termination_note(&self) -> Option<&'static str> {
        self.termination_note
    }

    /// 关闭阶段的诊断（如有）。
    pub(crate) fn close_error(&self) -> Option<&ScopeError> {
        self.close.as_ref()
    }

    /// 首次清理失败的诊断（如有），与上面的阶段诊断互不覆盖。
    pub(crate) fn cleanup_failure(&self) -> Option<&CleanupDiagnostic> {
        self.cleanup.as_ref()
    }

    /// 装配失败时已成功登记的输入数（业务体未执行）。
    pub(crate) fn registered_inputs(&self) -> usize {
        self.registered_inputs
    }
}

/// Application 侧唯一的非 test Root 执行入口。
///
/// 无状态：不缓存上一次的 Context／输入／结果／终止状态。每次调用都创建新的
/// `ExecutionIdentity`、新 Context（含唯一 Container）与新 RootScope；同一个完成态定义
/// 可以执行多次，每次独立身份空间。Root 对象只被不可变借用（`&O`），Future 拥有本次
/// `RootExecution`；未被 poll 时尚未开始执行，输入随 Future 一同销毁。
#[derive(Debug)]
pub(crate) struct Runtime;

impl Runtime {
    /// 执行一个完成态 Root Orchestrator，成功时把声明输出全部移交 Application。
    ///
    /// 支持范围见 [`RootInputs`]／[`RootOutputs`]：单／双非空 owned 输入与
    /// `Unit`／`Data<O>`／`Out2<O1,O2>` 输出。顺序为"Signature 预检 → 登记输入 →
    /// 运行 body → 冻结／整组预检 → 同步 take 并解除 Root 责任 → 关闭 Root"；
    /// 任何一步失败都不返回部分 owned 输出。
    pub(crate) async fn execute<O, I, K>(
        root: &O,
        input: I,
    ) -> Result<<K as RootOutputs<K>>::Owned, RootError>
    where
        O: OrchCall<I, K>,
        I: 'static + InputTypes + RootInputs<I>,
        K: OutKind + RootOutputs<K>,
    {
        let definition = root.definition();

        // 1) Signature 预检：真实 Definition 的输入端口与 I／pack、输出端口与 K 的
        //    数量／类型必须一致；失败在创建 Execution 与登记输入之前结束。
        <O::Pack as PackFromPorts>::from_ports(definition.inputs())
            .map_err(RootError::signature)?;
        #[allow(unused_mut)] // 非 test 构建没有口径故障注入，端口列表不会被改写
        let mut ports: Vec<DeclaredPort> = definition.output_ports().to_vec();
        #[cfg(test)]
        apply_root_ports_fault(&mut ports);
        check_root_output_signature::<K>(&ports).map_err(RootError::signature)?;

        // 2) 新 Execution 与 Root 输入登记：只走正式 register_owned；失败不执行业务体，
        //    已登记的值随本次 Execution 清理，尚未登记的输入随本地集合各析构一次。
        let mut execution = RootExecution::start();
        let root_scope = execution.context().root_scope();
        let mut pending = input.into_values().into_iter();
        let mut registered_inputs = 0usize;
        for (index, port) in definition.inputs().iter().enumerate() {
            #[cfg(not(test))]
            let _ = index;
            let Some((type_name, value)) = pending.next() else {
                break;
            };
            #[cfg(test)]
            apply_root_input_fault_at(execution.context_mut(), &root_scope, index, port.position());
            match execution.context_mut().register_owned_erased(
                &root_scope,
                port.position(),
                type_name,
                value,
            ) {
                Ok(_) => registered_inputs += 1,
                Err(scope) => return Err(RootError::assembly(scope, registered_inputs)),
            }
        }

        // 3) Root frame 内运行 body，再冻结／预检／提交／关闭。
        let values =
            run_root_owned::<O, I, K>(execution, root, &ports, definition.inputs()).await?;

        // 4) 按 K 的声明形状组装 owned 结果（类型已由预检证明）。
        Ok(<K as RootOutputs<K>>::assemble(values))
    }
}

/// Root owned 驱动：进入 Root frame、经受控装配入口运行 Root Orchestrator、整组提取并关闭。
///
/// 失败点与预检覆盖（§4.4）：冻结与整组预检在任何 take 之前完成；提交段只做
/// `DataContainer::take_verified` 与 `RootScope.owned` 移除，两者都在同一不可观察边界内，
/// 且身份与存活已由预检证明，因此没有可恢复分支。关闭阶段复用已验收的整组清理：其
/// 潜在失败点（存储破坏、collector／state 登记不一致）已由预检的 `cleanup_targets`
/// 整组覆盖，失败会作为清理诊断保存，不覆盖首次失败，也不重放已移交的值。
async fn run_root_owned<O, I, K>(
    mut execution: RootExecution,
    root: &O,
    ports: &[DeclaredPort],
    #[allow(unused_variables)] // 端口故障注入只在 test 构建消费 inputs
    inputs: &[DeclaredPort],
) -> Result<Vec<Box<dyn Any>>, RootError>
where
    O: OrchCall<I, K>,
    I: 'static + InputTypes,
    K: OutKind,
{
    let root_scope = execution.context.root_scope();
    let mut error: Option<RootError> = None;
    let mut values: Vec<Box<dyn Any>> = Vec::new();

    {
        let mut guard = execution
            .context
            .enter(InvocationKind::Root, &root_scope, true)
            .expect("a fresh execution context accepts its root frame");

        let body_result = super::orchestrator::run_root_call(&mut guard, root, &root_scope).await;
        #[cfg(test)]
        if body_result.is_ok() && !guard.is_terminated() {
            apply_root_post_body_fault(&mut guard, &root_scope, ports, inputs);
        }
        match body_result {
            Ok(()) if !guard.is_terminated() => {
                match guard.prepare_root_extraction(&root_scope, ports) {
                    Ok(plan) => {
                        let expected = plan.take_count();
                        #[cfg(test)]
                        let taken_ids = plan.taken_ids();
                        let (owned, close_targets) =
                            guard.commit_root_extraction(&root_scope, plan);
                        debug_assert_eq!(
                            owned.len(),
                            expected,
                            "commit moves every selected value"
                        );
                        #[cfg(test)]
                        record_root_snapshot(
                            &guard,
                            &root_scope,
                            RootSnapshotPhase::AfterCommit,
                            expected,
                            &taken_ids,
                        );
                        match guard.close_root(&root_scope, close_targets) {
                            Ok(()) => {
                                #[cfg(test)]
                                record_root_snapshot(
                                    &guard,
                                    &root_scope,
                                    RootSnapshotPhase::AfterClose,
                                    expected,
                                    &taken_ids,
                                );
                                values = owned;
                                guard.complete();
                            }
                            Err(close_error) => {
                                // 已移交的值不再属于 Container：不返回给 Application，
                                // 也不由后续清理重复销毁；关闭失败作为该阶段诊断保存。
                                drop(owned);
                                error = Some(RootError {
                                    stage: RootErrorStage::Close,
                                    note: "root close after extraction failed",
                                    build: None,
                                    scope: None,
                                    termination: Some(TerminationKind::BodyError),
                                    termination_scope: None,
                                    termination_note: None,
                                    close: Some(close_error),
                                    cleanup: None,
                                    registered_inputs: 0,
                                });
                                guard.failed("root close after extraction failed");
                            }
                        }
                    }
                    Err(scope) => {
                        #[cfg(test)]
                        record_root_snapshot(
                            &guard,
                            &root_scope,
                            RootSnapshotPhase::PreflightRejected,
                            0,
                            &[],
                        );
                        let primary = BodyError::from(scope.clone());
                        error = Some(RootError {
                            stage: RootErrorStage::Preflight,
                            note: "root output preflight rejected",
                            build: None,
                            scope: Some(scope),
                            termination: Some(TerminationKind::BodyError),
                            termination_scope: None,
                            termination_note: None,
                            close: None,
                            cleanup: None,
                            registered_inputs: 0,
                        });
                        guard.failed_with(&primary);
                    }
                }
            }
            Ok(()) => {
                error = Some(RootError {
                    stage: RootErrorStage::Body,
                    note: "root closed by a terminated execution",
                    build: None,
                    scope: None,
                    termination: Some(TerminationKind::BodyError),
                    termination_scope: None,
                    termination_note: None,
                    close: None,
                    cleanup: None,
                    registered_inputs: 0,
                });
                guard.failed("root closed by a terminated execution");
            }
            Err(body_error) => {
                error = Some(RootError {
                    stage: RootErrorStage::Body,
                    note: body_error.note(),
                    build: None,
                    scope: body_error.scope_error().cloned(),
                    termination: Some(TerminationKind::BodyError),
                    termination_note: None,
                    termination_scope: None,
                    close: None,
                    cleanup: None,
                    registered_inputs: 0,
                });
                guard.failed(error.as_ref().expect("just set").note());
            }
        }
    }

    match error {
        Some(mut error) => {
            #[cfg(test)]
            record_root_snapshot(
                &execution.context,
                &root_scope,
                RootSnapshotPhase::AfterFailureCleanup,
                0,
                &[],
            );
            #[cfg(test)]
            if super::test_support::take_post_terminate_probe() {
                // L19：终止（含失败清理）之后，业务入口与普通 commit 必须被拒绝；
                // 受控清理入口仍可用（返回真实清理诊断，而不是 Terminated）。
                let fresh = super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
                    .allocate()
                    .expect("probe position");
                let business = execution
                    .context
                    .register_owned::<u8>(&root_scope, &fresh, 7u8);
                super::test_support::record(&format!("post-fail-business:{business:?}"));
                let commit = execution
                    .context
                    .finalize(&root_scope, &[], &mut Vec::new())
                    .map_err(|error| format!("{error:?}"));
                super::test_support::record(&format!("post-fail-commit:{commit:?}"));
                let cleanup = execution.context.abort(&root_scope);
                super::test_support::record(&format!("post-fail-cleanup:{cleanup:?}"));
            }
            // 首次失败与清理诊断分别保存，互不覆盖。
            error.cleanup = execution.context.cleanup_report();
            if let Some(termination) = execution.context.termination_report() {
                if error.termination.is_none() {
                    error.termination = Some(termination.kind());
                }
                if error.termination_scope.is_none() {
                    error.termination_scope = termination.scope().cloned();
                }
                if error.termination_note.is_none() {
                    error.termination_note = Some(termination.note());
                }
            }
            Err(error)
        }
        None => Ok(values),
    }
}

#[cfg(test)]
use super::test_support::{RootFault, RootSnapshotPhase};

/// 应用一次口径故障（端口列表类）：只改变本次 Root 的声明端口元数据。
#[cfg(test)]
fn apply_root_ports_fault(ports: &mut Vec<DeclaredPort>) {
    match super::test_support::take_root_ports_fault() {
        Some(RootFault::ReplacePorts(replacement)) => *ports = replacement,
        Some(RootFault::DuplicatePort(index)) => {
            // 保持与真实声明相同的数量与类型：只把下一个位置替换成同一声明位置的副本，
            // 使数量／类型型的 Signature 预检仍然通过，由提取预检发现重复。
            if let (Some(port), true) = (ports.get(index).cloned(), ports.len() > index + 1) {
                ports[index + 1] = port;
            }
        }
        Some(RootFault::AppendPort(port)) => ports.push(port),
        _ => {}
    }
}

/// 应用输入装配故障：在真实登记之前，于第 `index` 个声明输入位置上预占一个值。
#[cfg(test)]
fn apply_root_input_fault_at(
    ctx: &mut ExecutionContext,
    root: &ScopeId,
    index: usize,
    position: &super::ref_id::RefId,
) {
    if super::test_support::take_root_input_fault_at(index).is_some() {
        ctx.register_owned_erased(root, position, std::any::type_name::<u8>(), Box::new(0u8))
            .expect("the fixture occupies a fresh input position");
    }
}

/// 应用 body 之后、预检之前的故障：只改前置元数据，不改判断路径。
#[cfg(test)]
fn apply_root_post_body_fault(
    ctx: &mut ExecutionContext,
    root: &ScopeId,
    ports: &[DeclaredPort],
    inputs: &[DeclaredPort],
) {
    let Some(fault) = super::test_support::take_root_post_body_fault() else {
        return;
    };
    let owned = ctx
        .snapshot_targets_probe(root)
        .map(|(_, owned)| owned)
        .unwrap_or_default();
    match fault {
        RootFault::DestroyRootOwned(index) => {
            if let Some(id) = owned.get(index) {
                ctx.destroy_probe(id);
            }
        }
        RootFault::RelocateOwnedToClosedScope(index) => {
            let Some(id) = owned.get(index) else {
                return;
            };
            let closed = super::test_support::closed_scope_snapshot();
            let target = closed.iter().find(|scope| *scope != root);
            if let Some(scope) = target {
                let _ = ctx.relocate_ownership_probe(id, scope);
            }
        }
        RootFault::InjectOutputTarget { index, target } => {
            if let Some(port) = ports.get(index) {
                ctx.inject_scope_target_probe(root, port.position(), target);
            }
        }
        RootFault::RetargetOutputFromInput { index, input } => {
            let (Some(port), Some(input_position)) = (ports.get(index), inputs.get(input)) else {
                return;
            };
            let target = ctx
                .snapshot_targets_probe(root)
                .map(|(refs, _)| {
                    refs.into_iter()
                        .find(|(position, _)| position == input_position.position())
                        .map(|(_, target)| target)
                })
                .ok()
                .flatten();
            if let Some(target) = target {
                let target = match target {
                    super::scope::TargetSnapshot::Data(id) => super::scope::RefTarget::Data(id),
                    _ => return,
                };
                ctx.inject_scope_target_probe(root, port.position(), target);
            }
        }
        _ => {}
    }
}

/// 记录一个 Root 提取观察点（严格路径：失败只登记错误、不落默认值快照）。
#[cfg(test)]
fn record_root_snapshot(
    ctx: &ExecutionContext,
    root: &ScopeId,
    phase: RootSnapshotPhase,
    planned_takes: usize,
    taken: &[super::identity::DataId],
) {
    super::test_support::record_strict_root_snapshot(ctx, root, phase, planned_takes, taken);
}
