//! 一次 Execution 的内部调用设施：`ExecutionContext`、Invocation frame 与退出 guard。
//!
//! 本模块把已验收的 [`ScopeCoordinator`] 接入一次 Execution：
//!
//! - **单 Context**：`ExecutionContext` 只持有一个 `ScopeCoordinator`（其中含唯一
//!   `DataContainer`），不再在外层放第二份存储；创建入口只出现在 `runtime` 的 Root
//!   设施里，nested 调用一律接收 `&mut ExecutionContext`。
//! - **Invocation ≠ Scope**：`InvocationFrame` 只记录 parent、调用类别、使用的 Scope
//!   与是否承担其退出责任；frame 身份是当前调用栈下标，不新增 InvocationId 序列，
//!   也不保留执行后的调用树。
//! - **借用与 mutation 分离**：guard 持有 `&mut Context`；叶子调用从它做共享重借用，
//!   `resolve` 得到的 `&T` 随调用语句／块结束而结束，之后才允许 `&mut` 登记、提交或
//!   关闭 Scope。业务叶子只接收 `&T`，不接触 Context。
//! - **三类退出**：正常完成（`complete()`：解除责任，之后 Drop 不再清理）、执行错误
//!   （`failed()`：Drop 清理本人负责的 Scope 并记录错误退出）、未标记而 Drop（取消：
//!   清理并记录取消）。清理失败另行保存，不覆盖首次终止原因。
//!
//! 本阶段采用单线程、顺序、允许非 `Send` Future／Data 的内部路径；不附加 `Send`／
//! `Sync`，也不引入 executor 依赖。

use std::any::Any;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::identity::{CollectorId, DataId, ExecutionIdentity, ScopeId};
use super::internal_error::ScopeError;
use super::ref_id::RefId;
use super::scope::{
    ControlStateId, ExportSlot, ImportSlot, ScopeCoordinator, ScopeState, StateImportSlot,
};
/// 首次终止类别：执行失败或取消。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) enum TerminationKind {
    /// 执行体返回失败（含其子调用的失败传播）。
    BodyError,
    /// 未完成 Future 本体被丢弃。
    Cancelled,
}
/// 首次终止原因及最小定位。
///
/// 首次原因一旦记录就不再被后续失败或取消覆盖；后续清理故障另存于
/// [`CleanupDiagnostic`]。
#[derive(Debug, Clone)]
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) struct ExecutionTermination {
    kind: TerminationKind,
    #[allow(dead_code)]
    scope: Option<ScopeId>,
    note: &'static str,
    scope_error: Option<ScopeError>,
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl ExecutionTermination {
    /// 终止类别。
    #[allow(dead_code)] // 非 test 构建下尚无可达的生产入口；由 V21-04 验收样本或 V21-05 正式路径驱动
    pub(crate) fn kind(&self) -> TerminationKind {
        self.kind
    }

    /// 相关 Scope（如有）。
    pub(crate) fn scope(&self) -> Option<&ScopeId> {
        self.scope.as_ref()
    }

    /// 说明文本。
    pub(crate) fn note(&self) -> &'static str {
        self.note
    }

    /// 首次失败时的原始 Scope／执行诊断（如有），不是通用字符串。
    pub(crate) fn scope_error(&self) -> Option<&ScopeError> {
        self.scope_error.as_ref()
    }
}
/// 清理失败诊断：故障 Scope 与错误。
///
/// 与终止原因并存，彼此不覆盖；有故障的 Scope 不会被标成 Closed。
#[derive(Debug, Clone)]
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) struct CleanupDiagnostic {
    #[allow(dead_code)]
    scope: ScopeId,
    error: ScopeError,
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl CleanupDiagnostic {
    /// 清理失败的 Scope。
    #[allow(dead_code)] // 非 test 构建下尚无可达的生产入口；由 V21-04 验收样本或 V21-05 正式路径驱动
    pub(crate) fn scope(&self) -> &ScopeId {
        &self.scope
    }

    /// 清理失败的诊断。
    pub(crate) fn error(&self) -> &ScopeError {
        &self.error
    }
}
/// Invocation 类别。
///
/// Root 使用 RootScope；Boundary 建立并承担一个直接 child Scope 的退出责任；Leaf
/// 沿用 caller 的 Scope 且不承担其销毁责任。Item／Round 等作用域由所属编排调用建立，
/// 作为该边界的 descendant 一并清理，不单独占用调用类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) enum InvocationKind {
    /// 本次 Root Execution 的调用。
    Root,
    /// 建立生命周期边界的调用。
    Boundary,
    /// 沿用 caller Scope 的轻量叶子调用。
    Leaf,
}
/// 一个调用 frame 的元数据。
#[derive(Debug)]
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) struct InvocationFrame {
    parent: Option<usize>,
    kind: InvocationKind,
    scope: Option<ScopeId>,
    owns_scope: bool,
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl InvocationFrame {
    /// 本 frame 的调用类别。
    #[allow(dead_code)] // 非 test 构建下尚无可达的生产入口；由 V21-04 验收样本或 V21-05 正式路径驱动
    pub(crate) fn kind(&self) -> InvocationKind {
        self.kind
    }

    /// 本 frame 使用的 Scope。
    pub(crate) fn scope(&self) -> Option<&ScopeId> {
        self.scope.as_ref()
    }

    /// 是否承担该 Scope 的退出责任。
    pub(crate) fn owns_scope(&self) -> bool {
        self.owns_scope
    }
}
/// guard 的退出处置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitDisposition {
    /// 尚未标记：Drop 视为取消。
    Pending,
    /// 正常完成：Drop 不再清理。
    Completed,
    /// 执行错误：Drop 清理并记录错误退出。
    Failed,
}

/// 一次 Execution 的内部调用与数据设施。
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) struct ExecutionContext {
    coordinator: ScopeCoordinator,
    frames: Vec<InvocationFrame>,
    termination: Option<ExecutionTermination>,
    cleanup_failure: Option<CleanupDiagnostic>,
    #[cfg(test)]
    cleanup_events: usize,
}

/// 测试观测：Context 最终析构事件。
#[cfg(test)]
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl Drop for ExecutionContext {
    fn drop(&mut self) {
        creation_counts::record_event("context-drop");
    }
}

#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl ExecutionContext {
    /// 以给定身份根建立 Context 与内部协调组件（唯一 DataContainer）。
    ///
    /// 只由 `runtime` 的 Root 设施在创建一次 Root Execution 时调用。
    #[allow(dead_code)] // 非 test 构建下尚无可达的生产入口；由 V21-04 验收样本或 V21-05 正式路径驱动
    pub(crate) fn new(execution: Arc<ExecutionIdentity>) -> Self {
        #[cfg(test)]
        creation_counts::count_context();
        Self {
            coordinator: ScopeCoordinator::new(execution),
            frames: Vec::new(),
            termination: None,
            cleanup_failure: None,
            #[cfg(test)]
            cleanup_events: 0,
        }
    }

    /// 本次 Execution 的 RootScope 身份。
    pub(crate) fn root_scope(&self) -> ScopeId {
        self.coordinator.root()
    }

    /// 当前 frame 使用的 Scope（无 frame 时为空）。
    pub(crate) fn current_scope(&self) -> Option<ScopeId> {
        self.frames.last().and_then(|frame| frame.scope.clone())
    }

    /// 当前调用栈深度。
    pub(crate) fn frame_depth(&self) -> usize {
        self.frames.len()
    }

    /// 当前 frame 的只读元数据。
    pub(crate) fn current_frame(&self) -> Option<&InvocationFrame> {
        self.frames.last()
    }

    /// 是否已经终止（首次执行失败或取消）。
    pub(crate) fn is_terminated(&self) -> bool {
        self.termination.is_some()
    }

    /// 首次终止原因。
    pub(crate) fn termination(&self) -> Option<&ExecutionTermination> {
        self.termination.as_ref()
    }

    /// 首个清理失败诊断。
    pub(crate) fn cleanup_failure(&self) -> Option<&CleanupDiagnostic> {
        self.cleanup_failure.as_ref()
    }

    /// 首次终止原因的副本，供 Root 收口写入 [`RootExit`](super::runtime::RootExit)。
    ///
    /// 只返回副本：终止状态本身不可被活跃调用或诊断移交清除。
    pub(crate) fn termination_report(&self) -> Option<ExecutionTermination> {
        self.termination.clone()
    }

    /// 首个清理失败诊断的副本，同上。
    pub(crate) fn cleanup_report(&self) -> Option<CleanupDiagnostic> {
        self.cleanup_failure.clone()
    }

    /// 测试观测：guard 在 Drop 中执行清理的次数。
    #[cfg(test)]
    pub(crate) fn cleanup_events(&self) -> usize {
        self.cleanup_events
    }

    /// 当前调用可见的 Scope 集合：当前 frame 的 Scope 本身，或它在本调用中建立的后代。
    ///
    /// 这条检查落实两侧隔离：child 调用不能通过 Context 直接读写 caller／ancestor 的本地
    /// 位置或责任，即使它拿到了对方的 ScopeId。Root frame 之前没有 frame，此时只允许
    /// RootScope，用于 Application 输入交接的 Root 初始化边界。
    fn require_call_scope(&self, scope: &ScopeId) -> Result<(), ScopeError> {
        let Some(current) = self.current_scope() else {
            return if *scope == self.coordinator.root() {
                Ok(())
            } else {
                Err(ScopeError::OutsideInvocation {
                    scope: scope.clone(),
                    current: None,
                })
            };
        };
        if *scope == current {
            return Ok(());
        }
        let mut cursor = Some(scope.clone());
        while let Some(candidate) = cursor {
            if candidate == current {
                return Ok(());
            }
            cursor = self.coordinator.parent_of(&candidate)?;
        }
        Err(ScopeError::OutsideInvocation {
            scope: scope.clone(),
            current: Some(current),
        })
    }

    /// 普通业务与正常提交入口的终止检查。
    ///
    /// 在产生任何副作用之前调用；`abort`、元数据读取与纯观察不经过它。
    fn require_running(&self) -> Result<(), ScopeError> {
        match &self.termination {
            Some(termination) => Err(ScopeError::Terminated {
                kind: termination.kind,
            }),
            None => Ok(()),
        }
    }

    /// 记录首次终止原因；后续失败或取消不覆盖首次原因（驱动内部与收口使用）。
    ///
    /// 终止状态一旦记录就不可恢复：诊断移交只返回副本，不清空该状态（§4.8 明确
    /// “不自动重置为可继续执行”）。
    pub(crate) fn terminate(
        &mut self,
        kind: TerminationKind,
        scope: Option<ScopeId>,
        note: &'static str,
        scope_error: Option<ScopeError>,
    ) {
        if self.termination.is_none() {
            self.termination = Some(ExecutionTermination {
                kind,
                scope,
                note,
                scope_error,
            });
        }
    }

    /// 记录首个清理失败；后续清理故障不覆盖首个诊断。
    pub(crate) fn record_cleanup_failure(&mut self, scope: ScopeId, error: ScopeError) {
        if self.cleanup_failure.is_none() {
            self.cleanup_failure = Some(CleanupDiagnostic { scope, error });
        }
    }

    /// 进入一次调用：校验 frame 与 Scope 的关系后返回 guard。
    ///
    /// - `Root`：调用栈为空且 `scope` 是本 Execution 的 RootScope。
    /// - `Boundary`：`scope` 必须是当前 frame Scope 的直接 child，且 `owns_scope` 为真。
    /// - `Leaf`：`scope` 必须等于当前 frame 的 Scope，且 `owns_scope` 为假。
    pub(crate) fn enter(
        &mut self,
        kind: InvocationKind,
        scope: &ScopeId,
        owns_scope: bool,
    ) -> Result<InvocationGuard<'_>, ScopeError> {
        self.require_running()?;
        // 被关闭或正在收口的 Scope 不能再进入调用：身份 tombstone 与 Finalizing 都拒绝。
        match self.coordinator.state(scope)? {
            ScopeState::Active => {}
            ScopeState::Finalizing => {
                return Err(ScopeError::ScopeNotActive {
                    scope: scope.clone(),
                    state: ScopeState::Finalizing,
                });
            }
            ScopeState::Closed => {
                return Err(ScopeError::ScopeClosed {
                    scope: scope.clone(),
                });
            }
        }
        let parent_index = if self.frames.is_empty() {
            None
        } else {
            Some(self.frames.len() - 1)
        };
        let current = self.current_scope();
        match kind {
            InvocationKind::Root => {
                if parent_index.is_some() || *scope != self.coordinator.root() {
                    return Err(ScopeError::Invariant {
                        violated: "a root invocation must be the first frame on the RootScope",
                    });
                }
                if !owns_scope {
                    return Err(ScopeError::Invariant {
                        violated: "a root invocation must own the RootScope",
                    });
                }
            }
            InvocationKind::Boundary => {
                let parent = current.ok_or(ScopeError::Invariant {
                    violated: "a boundary invocation requires an active caller frame",
                })?;
                if self.coordinator.parent_of(scope)?.as_ref() != Some(&parent) {
                    return Err(ScopeError::NotDirectParent {
                        child: scope.clone(),
                        caller: parent,
                    });
                }
                if !owns_scope {
                    return Err(ScopeError::Invariant {
                        violated: "a boundary invocation must own the scope it creates",
                    });
                }
            }
            InvocationKind::Leaf => {
                if current.as_ref() != Some(scope) {
                    return Err(ScopeError::Invariant {
                        violated: "a leaf invocation must reuse the caller scope",
                    });
                }
                if owns_scope {
                    return Err(ScopeError::Invariant {
                        violated: "a leaf invocation must not own the caller scope",
                    });
                }
            }
        }

        let frame_index = self.frames.len();
        self.frames.push(InvocationFrame {
            parent: parent_index,
            kind,
            scope: Some(scope.clone()),
            owns_scope,
        });
        Ok(InvocationGuard {
            context: self,
            frame_index,
            responsible: if owns_scope {
                vec![scope.clone()]
            } else {
                Vec::new()
            },
            exit: ExitDisposition::Pending,
        })
    }

    // ---- 受控数据与 Scope 入口（终止与调用可见范围检查后委托协调组件） ----

    /// 业务读取：只读取当前调用可见 Scope 的本地位置。
    ///
    /// 返回的 `&T` 绑定本 Context 的借用；借用未结束时无法可变借用 Context。
    pub(crate) fn resolve<T: Any>(
        &self,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<&T, ScopeError> {
        self.require_running()?;
        self.require_call_scope(scope)?;
        self.coordinator.resolve::<T>(scope, position)
    }

    /// 输出位置预检：终止／可见范围检查后，校验该位置尚未被绑定。
    ///
    /// 叶子 site 在业务体运行前调用；失败时业务体不执行，诊断保留真实 `RefAlreadyBound`。
    pub(crate) fn precheck_output(
        &self,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(scope)?;
        self.coordinator.precheck_output_position(scope, position)
    }

    /// 位置校验：不借用业务值，只校验 Scope 访问关系与容器归属 → 存活 → 类型。
    ///
    /// Orchestrator 输入 pack 的擦除后校验入口；它不建立长期借用，也不跳过访问关系
    /// 检查，与 `resolve` 使用同一检查顺序。
    pub(crate) fn validate_position(
        &self,
        scope: &ScopeId,
        position: &RefId,
        expected: std::any::TypeId,
        expected_name: &'static str,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(scope)?;
        self.coordinator
            .validate_position(scope, position, expected, expected_name)
    }

    /// 从有效 Active parent 建立 child Scope。
    pub(crate) fn create_child(&mut self, parent: &ScopeId) -> Result<ScopeId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(parent)?;
        self.coordinator.create_child(parent)
    }

    /// 登记一个新产生的 owned Data，并绑定本地输出位置。
    pub(crate) fn register_owned<T: Any>(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        value: T,
    ) -> Result<DataId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(scope)?;
        self.coordinator.register_owned(scope, position, value)
    }

    /// 整组 Import（本地引用来源）。
    pub(crate) fn import_batch(
        &mut self,
        child: &ScopeId,
        caller: &ScopeId,
        inputs: &[ImportSlot],
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(child)?;
        self.require_call_scope(caller)?;
        self.coordinator.import_batch(child, caller, inputs)
    }

    /// 整组 Import（本地引用与状态来源混合）。
    pub(crate) fn import_batch_with_states(
        &mut self,
        child: &ScopeId,
        caller: &ScopeId,
        local: &[ImportSlot],
        from_state: &[StateImportSlot],
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(child)?;
        self.require_call_scope(caller)?;
        self.coordinator
            .import_batch_with_states(child, caller, local, from_state)
    }

    /// 在一个有效 Active 控制器 Scope 下建立空 collector。
    pub(crate) fn begin_collector<O: Any>(
        &mut self,
        owner: &ScopeId,
    ) -> Result<CollectorId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(owner)?;
        self.coordinator.begin_collector::<O>(owner)
    }

    /// 直接 Consume：把 ItemScope 的完整 owned 输出移入其直接 parent 的 collector。
    pub(crate) fn consume_item(
        &mut self,
        item: &ScopeId,
        selected: &RefId,
        collector: &CollectorId,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(item)?;
        // collector 的责任方（Item 的直接 parent）同样必须可见。
        self.require_call_scope(&self.coordinator.collector_owner(collector)?)?;
        self.coordinator.consume_item(item, selected, collector)
    }

    /// Promote：把来源 child 的选定结果保留到父控制器的控制状态。
    pub(crate) fn promote(
        &mut self,
        source: &ScopeId,
        selected: &RefId,
        state: &ControlStateId,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(source)?;
        // 控制提交是两端操作：接收 state 的控制器也必须属于当前调用可见范围，
        // 否则会出现"提交已发生、调用随后失败"的越权。
        self.require_call_scope(&self.coordinator.state_owner(state)?)?;
        self.coordinator.promote(source, selected, state)
    }

    /// 完成 collector：建构值成为新的普通 `Vec<O>` Data 并绑定控制器本地输出位置。
    pub(crate) fn finish_collector(
        &mut self,
        controller: &ScopeId,
        collector: &CollectorId,
        position: &RefId,
    ) -> Result<DataId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(controller)?;
        self.coordinator
            .finish_collector(controller, collector, position)
    }

    /// 以本地引用初始化控制状态位置。
    pub(crate) fn register_state<T: Any>(
        &mut self,
        controller: &ScopeId,
        from_local: &RefId,
    ) -> Result<ControlStateId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(controller)?;
        self.coordinator.register_state::<T>(controller, from_local)
    }

    /// 登记尚未初始化的控制状态位置。
    #[allow(dead_code)] // 非 test 构建下尚无可达的生产入口；由 V21-04 验收样本或 V21-05 正式路径驱动
    pub(crate) fn register_uninitialized_state<T: Any>(
        &mut self,
        controller: &ScopeId,
    ) -> Result<ControlStateId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(controller)?;
        self.coordinator
            .register_uninitialized_state::<T>(controller)
    }

    /// 把控制状态的当前 target 一次性绑定到最终声明的本地输出位置。
    pub(crate) fn bind_state_output(
        &mut self,
        controller: &ScopeId,
        state: &ControlStateId,
        position: &RefId,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(controller)?;
        self.coordinator
            .bind_state_output(controller, state, position)
    }

    /// 受控回收：销毁满足条件的 pending 旧状态。
    pub(crate) fn recycle_pending(&mut self, controller: &ScopeId) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(controller)?;
        self.coordinator.recycle_pending(controller)
    }

    /// 正常退出：冻结、整组输出预检与提交、失效引用、清理并关闭。
    pub(crate) fn finalize(
        &mut self,
        scope: &ScopeId,
        declared: &[RefId],
        outputs: &mut Vec<ExportSlot>,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(scope)?;
        self.coordinator.finalize(scope, declared, outputs)
    }

    /// 边界进入前的可失败预检：终止状态、caller 可见范围与 caller 的 Active 状态。
    ///
    /// 与 `create_child`／`enter` 分离，使"建立 child 之后才失败"只可能来自内部不变量。
    fn require_boundary_entry(&self, caller: &ScopeId) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(caller)?;
        match self.coordinator.state(caller)? {
            ScopeState::Active => Ok(()),
            ScopeState::Finalizing => Err(ScopeError::ScopeNotActive {
                scope: caller.clone(),
                state: ScopeState::Finalizing,
            }),
            ScopeState::Closed => Err(ScopeError::ScopeClosed {
                scope: caller.clone(),
            }),
        }
    }

    /// 受控失败清理：从最深 descendant 开始清理整棵子树。
    ///
    /// 终止后仍允许，用于收拢既有责任；它是清理入口，不构成继续业务执行的旁路。
    pub(crate) fn abort(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
        // 受控失败清理同样限于本人负责的调用范围：child 不能借清理入口关闭 caller／
        // ancestor 的 Scope。guard 自身的失败退出直接使用协调组件，不经过本入口。
        self.require_call_scope(scope)?;
        self.coordinator.abort(scope)
    }

    /// 只读元数据：Scope 当前状态。
    pub(crate) fn state(&self, scope: &ScopeId) -> Result<ScopeState, ScopeError> {
        self.coordinator.state(scope)
    }

    /// 只读元数据：Scope 的直接 parent。
    pub(crate) fn parent_of(&self, scope: &ScopeId) -> Result<Option<ScopeId>, ScopeError> {
        self.coordinator.parent_of(scope)
    }

    /// 测试观测：本 Execution 身份根的稳定地址。
    #[cfg(test)]
    pub(crate) fn identity_probe(&self) -> *const () {
        self.coordinator.identity_probe()
    }

    /// 测试观测：内部协调组件的稳定地址。
    #[cfg(test)]
    pub(crate) fn coordinator_probe(&self) -> *const () {
        self.coordinator.coordinator_probe()
    }

    /// 测试观测：只读快照某 Scope 的本地引用与责任集合（终止后仍允许，不授予读取权）。
    #[cfg(test)]
    pub(crate) fn snapshot_probe(
        &self,
        scope: &ScopeId,
    ) -> Result<super::scope::ScopeSnapshot, ScopeError> {
        self.coordinator.snapshot_probe(scope)
    }

    /// 测试观测：某 DataId 是否仍在本 Execution 内存活（只读诊断，不授予读取权）。
    #[cfg(test)]
    pub(crate) fn alive_probe(&self, id: &super::identity::DataId) -> bool {
        self.coordinator.alive_probe(id)
    }

    /// 测试观测：某 DataId 当前的责任 Scope（只读诊断，不授予读取权）。
    #[cfg(test)]
    pub(crate) fn owner_probe(&self, id: &super::identity::DataId) -> Result<ScopeId, ScopeError> {
        self.coordinator.owner_probe(id)
    }

    /// 测试观测：唯一 DataContainer 的稳定地址。
    #[cfg(test)]
    pub(crate) fn container_probe(&self) -> *const () {
        self.coordinator.container_probe()
    }
}

/// 调用 guard：持有对当前 Context 的可变借用与本人负责退出的 Scope。
///
/// `Deref`／`DerefMut` 直接把 Context 暴露给同一调用链；guard 不持有业务值、输入
/// Future 本体或第二份 Container。
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) struct InvocationGuard<'ctx> {
    context: &'ctx mut ExecutionContext,
    frame_index: usize,
    responsible: Vec<ScopeId>,
    exit: ExitDisposition,
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl Deref for InvocationGuard<'_> {
    type Target = ExecutionContext;

    fn deref(&self) -> &ExecutionContext {
        self.context
    }
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl DerefMut for InvocationGuard<'_> {
    fn deref_mut(&mut self) -> &mut ExecutionContext {
        self.context
    }
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl InvocationGuard<'_> {
    /// 本 guard 对应 frame 的只读元数据。
    #[allow(dead_code)] // 非 test 构建下尚无可达的生产入口；由 V21-04 验收样本或 V21-05 正式路径驱动
    pub(crate) fn frame(&self) -> &InvocationFrame {
        &self.context.frames[self.frame_index]
    }

    /// 本人仍负责退出的 Scope 列表。
    pub(crate) fn responsible_scopes(&self) -> &[ScopeId] {
        &self.responsible
    }

    /// 纳入一个由本调用建立、尚未关闭的 Scope。
    ///
    /// 只接受当前调用可见范围（本 frame 的 Scope 或它建立的 descendant）：传入
    /// caller／ancestor 的 ScopeId 会被拒绝，不能借责任接管让祖先被本 guard 关闭。
    #[allow(dead_code)] // 非 test 构建下尚无可达的生产入口；由 V21-04 验收样本或 V21-05 正式路径驱动
    pub(crate) fn take_responsibility(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
        self.context.require_call_scope(scope)?;
        if !self.responsible.iter().any(|known| known == scope) {
            self.responsible.push(scope.clone());
        }
        Ok(())
    }

    /// 测试观测：立即触发本 guard 的 Drop 校验（等价于在当前位置丢弃 guard）。
    #[cfg(test)]
    pub(crate) fn exit_for_probe(self) {}

    /// 解除对某个已正常关闭（或已由 Promote／Consume 关闭）来源的责任。
    pub(crate) fn release_responsibility(&mut self, scope: &ScopeId) {
        self.responsible.retain(|known| known != scope);
    }

    /// 标记正常完成：解除所有责任，之后 Drop 不再清理。
    pub(crate) fn complete(mut self) {
        self.responsible.clear();
        self.exit = ExitDisposition::Completed;
    }

    /// 标记执行错误退出：Drop 时清理本人仍负责的 Scope，并记录错误退出。
    pub(crate) fn failed(self, note: &'static str) {
        self.failed_with(&BodyError::new(note));
    }

    /// 以实际错误标记执行错误退出：说明与可选 Scope 诊断都保留。
    ///
    /// 定位取**本次调用 frame 使用的 Scope**，而不是责任集合中的任一项；原始执行／
    /// Scope 错误随终止原因保存，之后即使外围返回 Ok 也不会丢失。
    pub(crate) fn failed_with(self, error: &BodyError) {
        let mut this = self;
        this.exit = ExitDisposition::Failed;
        let scope = this.context.frames[this.frame_index]
            .scope
            .clone()
            .or_else(|| this.responsible.first().cloned());
        this.context.terminate(
            TerminationKind::BodyError,
            scope,
            error.note(),
            error.scope_error().cloned(),
        );
    }
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl Drop for InvocationGuard<'_> {
    fn drop(&mut self) {
        // 先校验 frame 关系：只有"本 guard 的 frame 正是栈顶、且与责任集合一致"时才
        // 结束 frame。异常关系记录为内部不变量诊断并保留现场，不静默 truncate 掉
        // caller 或更深的 frame。
        let relation = self.frame_relation();
        if let Err(violated) = relation {
            self.context.terminate(
                TerminationKind::BodyError,
                self.context
                    .frames
                    .get(self.frame_index)
                    .and_then(|frame| frame.scope().cloned()),
                violated,
                Some(ScopeError::Invariant { violated }),
            );
            #[cfg(test)]
            creation_counts::record_event("frame-relation-violated");
            return;
        }

        // 按三类处置：正常完成只退出 frame；错误／取消先同步清理并记录诊断，最后才
        // 退出 frame（与清理顺序一致，且每一步都有事件可观测）。
        if self.exit != ExitDisposition::Completed {
            let cancelled = self.exit == ExitDisposition::Pending;
            let frame_scope = self.context.frames[self.frame_index].scope().cloned();
            let scopes = std::mem::take(&mut self.responsible);
            #[cfg(test)]
            {
                let label = scopes
                    .first()
                    .map(|scope| scope.seq().to_string())
                    .unwrap_or_else(|| String::from("none"));
                creation_counts::record_event(&format!("cleanup-start:{label}"));
            }
            let mut failure: Option<(ScopeId, ScopeError)> = None;
            for scope in &scopes {
                if let Err(error) = self.context.coordinator.abort(scope)
                    && failure.is_none()
                {
                    failure = Some((scope.clone(), error));
                }
                #[cfg(test)]
                creation_counts::record_event(&format!("guard-cleanup:{}", scope.seq()));
            }
            #[cfg(test)]
            {
                self.context.cleanup_events += 1;
                creation_counts::count_guard_cleanup();
                let label = scopes
                    .first()
                    .map(|scope| scope.seq().to_string())
                    .unwrap_or_else(|| String::from("none"));
                creation_counts::record_event(&format!("cleanup-end:{label}"));
            }
            if cancelled {
                // 与执行错误一致：定位取本次调用 frame 使用的 Scope，而不是从责任
                // 集合推导（共享 Scope 的 leaf 责任集合为空）。
                let scope = frame_scope.clone();
                self.context.terminate(
                    TerminationKind::Cancelled,
                    scope,
                    "pending future dropped",
                    None,
                );
            }
            if let Some((scope, error)) = failure {
                self.context.record_cleanup_failure(scope, error);
            }
        }

        #[cfg(test)]
        {
            let kind = match self.context.frames[self.frame_index].kind() {
                InvocationKind::Root => "root",
                InvocationKind::Boundary => "boundary",
                InvocationKind::Leaf => "leaf",
            };
            let scope = self.context.frames[self.frame_index].scope().cloned();
            self.context.frames.truncate(self.frame_index);
            match &scope {
                Some(scope) => {
                    creation_counts::record_event(&format!("frame-exit:{kind}:{}", scope.seq()))
                }
                None => creation_counts::record_event(&format!("frame-exit:{kind}")),
            }
        }
        #[cfg(not(test))]
        self.context.frames.truncate(self.frame_index);
    }
}

#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl InvocationGuard<'_> {
    /// frame 关系校验：本 guard 的 frame 必须是栈顶，且与责任集合、退出处置一致。
    fn frame_relation(&self) -> Result<(), &'static str> {
        let Some(frame) = self.context.frames.get(self.frame_index) else {
            return Err("invocation guard lost its frame");
        };
        if self.context.frames.len() != self.frame_index + 1 {
            return Err(
                "an invocation guard must be dropped while its frame is the top of the stack",
            );
        }
        // parent 字段、调用类别与 Scope 关系（身份／关系校验，不要求 Scope 仍 Active：
        // 正常 finalize 后已 Closed 的 Scope 仍可合法退出）。
        let caller_scope = if self.frame_index == 0 {
            None
        } else {
            self.context.frames[self.frame_index - 1].scope().cloned()
        };
        match frame.kind {
            InvocationKind::Root => {
                if self.frame_index != 0 || frame.parent.is_some() {
                    return Err("a root invocation frame must sit at the bottom without a parent");
                }
                if frame.scope.as_ref() != Some(&self.context.coordinator.root()) {
                    return Err("a root invocation frame must use the actual RootScope");
                }
                if !frame.owns_scope {
                    return Err("a root invocation frame must own the RootScope");
                }
            }
            InvocationKind::Boundary => {
                if self.frame_index == 0 || frame.parent != Some(self.frame_index - 1) {
                    return Err(
                        "a nested invocation frame must point at its immediate caller frame",
                    );
                }
                if !frame.owns_scope {
                    return Err("a boundary invocation must own the scope it creates");
                }
                let Some(scope) = frame.scope.as_ref() else {
                    return Err("a boundary invocation must use its own scope");
                };
                let direct_child = match self.context.coordinator.parent_of(scope) {
                    Ok(parent) => parent.as_ref() == caller_scope.as_ref(),
                    Err(_) => false,
                };
                if !direct_child {
                    return Err(
                        "a boundary invocation scope must be a direct child of the caller scope",
                    );
                }
            }
            InvocationKind::Leaf => {
                if self.frame_index == 0 || frame.parent != Some(self.frame_index - 1) {
                    return Err(
                        "a nested invocation frame must point at its immediate caller frame",
                    );
                }
                if frame.owns_scope {
                    return Err("a leaf invocation must not own the caller scope");
                }
                if frame.scope.as_ref() != caller_scope.as_ref() {
                    return Err("a leaf invocation must reuse the caller scope");
                }
            }
        }
        if frame.owns_scope() {
            // 责任可能已在正常完成／已关闭来源时被解除；此时集合为空是合法的。
            if let Some(first) = self.responsible.first()
                && Some(first) != frame.scope()
            {
                return Err("an owning invocation guard must hold its own scope first");
            }
        } else if !self.responsible.is_empty() {
            return Err("a non-owning leaf guard must not hold scope responsibilities");
        }
        Ok(())
    }
}

/// 执行体失败标记：说明文本与可选的 Scope 诊断。
///
/// 内部调用驱动的失败通道，不是公开 Execution Error API。
#[derive(Debug)]
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) struct BodyError {
    note: &'static str,
    scope: Option<ScopeError>,
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl BodyError {
    /// 以说明文本构造。
    pub(crate) fn new(note: &'static str) -> Self {
        Self { note, scope: None }
    }

    /// 说明文本。
    pub(crate) fn note(&self) -> &'static str {
        self.note
    }

    /// 内的 Scope 诊断（如有）。
    pub(crate) fn scope_error(&self) -> Option<&ScopeError> {
        self.scope.as_ref()
    }
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl From<super::signature::BuildError> for BodyError {
    fn from(source: super::signature::BuildError) -> Self {
        Self::new(source.note())
    }
}
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl From<ScopeError> for BodyError {
    fn from(source: ScopeError) -> Self {
        Self {
            note: "scope operation failed",
            scope: Some(source),
        }
    }
}
/// 叶子调用驱动：沿用 caller Scope，只把 `&T` 交给业务形状的 async fn。
///
/// 顺序固定为：进入 Leaf frame → 共享重借用 Context 并 `resolve` 输入 → await 叶子
/// （输入借用随该语句结束）→ 以 `&mut` 登记 owned 输出 → 正常完成。
///
/// `config` 是执行体自带的非 Data 上下文参数（对应 V21-05 起 Node／Orchestrator 的定义
/// 与配置）：驱动原样透传，不解释、不存储，也不构成业务 Data 通道。
#[allow(dead_code)] // V21-04 的验收样本直接驱动；V21-05 的正式 adapter 采用借用式叶子骨架与自建编排边界，不复用按值 config 形态
pub(crate) async fn invoke_leaf<T, X, O, F>(
    context: &mut ExecutionContext,
    scope: &ScopeId,
    input: &RefId,
    output: &RefId,
    config: X,
    leaf: F,
) -> Result<DataId, BodyError>
where
    T: Any,
    X: 'static,
    O: Any,
    F: for<'a> AsyncFnOnce(&'a T, X) -> O,
{
    let mut guard = context.enter(InvocationKind::Leaf, scope, false)?;
    match leaf_step(&mut guard, scope, input, output, config, leaf).await {
        Ok(id) => {
            guard.complete();
            Ok(id)
        }
        Err(error) => {
            // 叶子只沿用 caller 的 Scope，不承担其退出责任；失败仍要显式标记为执行错误，
            // 不能让它退化成"未标记 = 取消"。
            guard.failed_with(&error);
            Err(error)
        }
    }
}
/// 叶子调用的可失败步骤：调用方保证 guard 在失败时标记为执行错误退出。
#[allow(dead_code)] // V21-04 的验收样本直接驱动；V21-05 的正式 adapter 采用借用式叶子骨架与自建编排边界，不复用按值 config 形态
async fn leaf_step<T, X, O, F>(
    guard: &mut InvocationGuard<'_>,
    scope: &ScopeId,
    input: &RefId,
    output: &RefId,
    config: X,
    leaf: F,
) -> Result<DataId, BodyError>
where
    T: Any,
    X: 'static,
    O: Any,
    F: for<'a> AsyncFnOnce(&'a T, X) -> O,
{
    let produced = {
        // 共享重借用：输入 Future 与其 `&T` 限定在本块内结束。
        let shared: &ExecutionContext = guard;
        let borrowed = shared.resolve::<T>(scope, input)?;
        leaf(borrowed, config).await
    };
    Ok(guard.register_owned(scope, output, produced)?)
}
/// 可失败叶子调用驱动：叶子以 `Result<O, BodyError>` 表达执行错误。
///
/// 成功时与 [`invoke_leaf`] 相同地登记 owned 输出；失败时叶子 guard 只标记执行错误并
/// 传播，**不清理 caller 的 Scope**——沿用 caller Scope 的叶子不承担其退出责任。
#[allow(dead_code)] // V21-04 的验收样本直接驱动；V21-05 的正式 adapter 采用借用式叶子骨架与自建编排边界，不复用按值 config 形态
pub(crate) async fn invoke_fallible_leaf<T, X, O, F>(
    context: &mut ExecutionContext,
    scope: &ScopeId,
    input: &RefId,
    output: &RefId,
    config: X,
    leaf: F,
) -> Result<DataId, BodyError>
where
    T: Any,
    X: 'static,
    O: Any,
    F: for<'a> AsyncFnOnce(&'a T, X) -> Result<O, BodyError>,
{
    let mut guard = context.enter(InvocationKind::Leaf, scope, false)?;
    match fallible_leaf_step(&mut guard, scope, input, output, config, leaf).await {
        Ok(id) => {
            guard.complete();
            Ok(id)
        }
        Err(error) => {
            guard.failed_with(&error);
            Err(error)
        }
    }
}
/// 可失败叶子的步骤：失败不触碰 caller Scope 的绑定与责任。
#[allow(dead_code)] // V21-04 的验收样本直接驱动；V21-05 的正式 adapter 采用借用式叶子骨架与自建编排边界，不复用按值 config 形态
async fn fallible_leaf_step<T, X, O, F>(
    guard: &mut InvocationGuard<'_>,
    scope: &ScopeId,
    input: &RefId,
    output: &RefId,
    config: X,
    leaf: F,
) -> Result<DataId, BodyError>
where
    T: Any,
    X: 'static,
    O: Any,
    F: for<'a> AsyncFnOnce(&'a T, X) -> Result<O, BodyError>,
{
    let produced = {
        let shared: &ExecutionContext = guard;
        let borrowed = shared.resolve::<T>(scope, input)?;
        leaf(borrowed, config).await?
    };
    Ok(guard.register_owned(scope, output, produced)?)
}
/// 无输出叶子调用驱动：`()` 输出不登记业务 Data，也不产生 `DataId`。
///
/// 顺序与 [`invoke_leaf`] 相同，但成功后不调用 `register_owned`（`()` 不是业务 Data）。
#[allow(dead_code)] // V21-04 的验收样本直接驱动；V21-05 的正式 adapter 采用借用式叶子骨架与自建编排边界，不复用按值 config 形态
pub(crate) async fn invoke_leaf_unit<T, X, F>(
    context: &mut ExecutionContext,
    scope: &ScopeId,
    input: &RefId,
    config: X,
    leaf: F,
) -> Result<(), BodyError>
where
    T: Any,
    X: 'static,
    F: for<'a> AsyncFnOnce(&'a T, X) -> (),
{
    let guard = context.enter(InvocationKind::Leaf, scope, false)?;
    let outcome = {
        let shared: &ExecutionContext = &guard;
        let borrowed = shared.resolve::<T>(scope, input);
        match borrowed {
            Ok(borrowed) => {
                leaf(borrowed, config).await;
                Ok(())
            }
            Err(error) => Err(error),
        }
    };
    match outcome {
        Ok(()) => {
            guard.complete();
            Ok(())
        }
        Err(error) => {
            let body_error = BodyError::from(error);
            guard.failed_with(&body_error);
            Err(body_error)
        }
    }
}
/// 边界调用驱动：建立 child Scope、导入输入、运行 body、由边界整组 Export 并关闭。
///
/// body 使用同一个 Context；它可以通过 `create_child` 建立 Item／Round 等 descendant
/// 作用域，这些作用域由本边界的 Scope 退出清理覆盖。body 失败时 guard 走执行错误
/// 退出：清理本边界仍负责的 Scope 后把失败交给调用方。`config` 与叶子驱动同样原样透传。
#[allow(dead_code)] // V21-04 的验收样本直接驱动；V21-05 的正式 adapter 采用借用式叶子骨架与自建编排边界，不复用按值 config 形态
pub(crate) async fn invoke_boundary<X, F>(
    context: &mut ExecutionContext,
    imports: &[ImportSlot],
    declared: &[RefId],
    outputs: &mut Vec<ExportSlot>,
    config: X,
    body: F,
) -> Result<ScopeId, BodyError>
where
    X: 'static,
    F: for<'a> AsyncFnOnce(&'a mut ExecutionContext, &'a ScopeId, X) -> Result<(), BodyError>,
{
    let caller = context.current_scope().ok_or(ScopeError::Invariant {
        violated: "a boundary invocation requires an active caller frame",
    })?;
    // 先做全部可失败检查（终止、可见范围、caller Active），再建立 child：
    // 这样"child 已建立但无法进入 guard"不可能出现在可返回路径上。
    context.require_boundary_entry(&caller)?;
    let child = context.create_child(&caller)?;
    // Import 在 guard 生效前完成显式输入装配；失败时立刻显式清理刚建立的 child，
    // 不留孤立的、不受 guard 保护的 Scope，也不留下部分输入绑定。
    if let Err(error) = context.import_batch(&child, &caller, imports) {
        // 建立阶段失败属于首次执行失败：记录终止与定位，并保存原始诊断；清理失败
        // 另行记录（`record_cleanup_failure`），不覆盖原错误。
        context.terminate(
            TerminationKind::BodyError,
            Some(child.clone()),
            "boundary input assembly failed",
            Some(error.clone()),
        );
        if let Err(cleanup_error) = context.abort(&child) {
            context.record_cleanup_failure(child.clone(), cleanup_error);
        }
        return Err(error.into());
    }
    // 预检已保证这条进入是不可失败的内部不变量：此后 body／finalize 的失败都在 guard
    // 生效后发生，由受控退出清理 child 与未完成责任。
    let mut guard = context
        .enter(InvocationKind::Boundary, &child, true)
        .expect("boundary entry after its pre-checks cannot fail");
    match boundary_step(&mut guard, &child, declared, outputs, config, body).await {
        Ok(()) => {
            guard.complete();
            Ok(child)
        }
        Err(error) => {
            // 边界自身与尚未关闭的 descendant 由 guard 的执行错误退出清理。
            guard.failed_with(&error);
            Err(error)
        }
    }
}
/// 边界调用的可失败步骤：运行 body、整组 Export 并解除责任。
#[allow(dead_code)] // V21-04 的验收样本直接驱动；V21-05 的正式 adapter 采用借用式叶子骨架与自建编排边界，不复用按值 config 形态
async fn boundary_step<X, F>(
    guard: &mut InvocationGuard<'_>,
    child: &ScopeId,
    declared: &[RefId],
    outputs: &mut Vec<ExportSlot>,
    config: X,
    body: F,
) -> Result<(), BodyError>
where
    X: 'static,
    F: for<'a> AsyncFnOnce(&'a mut ExecutionContext, &'a ScopeId, X) -> Result<(), BodyError>,
{
    body(&mut *guard, child, config).await?;
    guard.finalize(child, declared, outputs)?;
    guard.release_responsibility(child);
    Ok(())
}

/// 测试观测：Context／协调组件／Container 的创建计数。
///
/// 仅用于证明"一次 Execution 只创建一份"；不是生产 Trace，也不能作为跨执行的持久
/// 身份。
#[cfg(test)]
pub(crate) mod creation_counts {
    use std::cell::Cell;

    thread_local! {
        static CONTEXTS: Cell<usize> = const { Cell::new(0) };
        static COORDINATORS: Cell<usize> = const { Cell::new(0) };
        static CONTAINERS: Cell<usize> = const { Cell::new(0) };
        static GUARD_CLEANUPS: Cell<usize> = const { Cell::new(0) };
    }

    thread_local! {
        static EVENTS: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    /// 记录一次由 guard 退出触发的清理（错误或取消）。
    pub(crate) fn count_guard_cleanup() {
        GUARD_CLEANUPS.with(|count| count.set(count.get() + 1));
    }

    /// guard 退出清理的累计次数。
    pub(crate) fn guard_cleanups() -> usize {
        GUARD_CLEANUPS.with(Cell::get)
    }

    pub(crate) fn count_context() {
        CONTEXTS.with(|count| count.set(count.get() + 1));
    }

    /// 在 `ScopeCoordinator::new` 的实际创建处记录协调组件。
    pub(crate) fn count_coordinator() {
        COORDINATORS.with(|count| count.set(count.get() + 1));
    }

    /// 在 `DataContainer::with_identity` 的实际创建处记录 Container。
    pub(crate) fn count_container() {
        CONTAINERS.with(|count| count.set(count.get() + 1));
    }

    /// 事件日志：按发生顺序记录析构／清理事件，用于验证先后关系。
    pub(crate) fn record_event(event: &str) {
        EVENTS.with(|events| events.borrow_mut().push(event.to_string()));
    }

    /// 取走当前线程的事件序列。
    pub(crate) fn take_events() -> Vec<String> {
        EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
    }

    /// 当前线程的计数快照。
    ///
    /// 计数是**线程局部**的：每个测试在自己的线程上驱动同一条顺序执行路径，因此
    /// 并发测试不会互相干扰；它不是跨执行的持久身份。
    pub(crate) fn snapshot() -> (usize, usize, usize) {
        (
            CONTEXTS.with(Cell::get),
            COORDINATORS.with(Cell::get),
            CONTAINERS.with(Cell::get),
        )
    }
}
#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context as TaskContext, Poll, Waker};

    use super::creation_counts;
    use super::{
        BodyError, ExecutionContext, InvocationFrame, InvocationGuard, InvocationKind,
        TerminationKind, invoke_boundary, invoke_fallible_leaf, invoke_leaf, invoke_leaf_unit,
    };
    use crate::core::internal_error::ScopeError;
    use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};
    use crate::core::runtime::{RootExecution, run_root};
    use crate::core::scope::{ExportSlot, ImportSlot, ScopeState, StateImportSlot};

    /// 非 Clone 业务值，带 Drop 观测。
    struct Tracked {
        num: u32,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            #[cfg(test)]
            creation_counts::record_event(&format!("value:{}", self.num));
        }
    }

    /// 记录"内层 Future 析构"事件的探针：随 async 状态机一起析构。
    struct Probe(&'static str);

    impl Drop for Probe {
        fn drop(&mut self) {
            creation_counts::record_event(self.0);
        }
    }

    /// 可控挂起点：关闭时返回真实 Pending，`release` 后下一个 poll 返回 Ready。
    ///
    /// 不使用 `wake_by_ref` 自旋；测试通过手动 poll 驱动，无需 executor。
    #[derive(Clone)]
    struct Gate {
        open: Rc<Cell<bool>>,
    }

    impl Gate {
        fn closed() -> Self {
            Self {
                open: Rc::new(Cell::new(false)),
            }
        }

        fn release(&self) {
            self.open.set(true);
        }

        async fn wait(&self) {
            std::future::poll_fn(|_| {
                if self.open.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await
        }
    }

    /// 手动推进直到 Ready；每次 Pending 后调用 `on_pending`（用于释放挂起点）。
    fn drive_to_ready<F: Future>(future: F, mut on_pending: impl FnMut(usize)) -> F::Output {
        let mut future = Box::pin(future);
        let mut pending = 0usize;
        loop {
            let waker = Waker::noop();
            let mut cx = TaskContext::from_waker(waker);
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => {
                    pending += 1;
                    assert!(pending < 64, "future is not making progress");
                    on_pending(pending);
                }
            }
        }
    }

    /// 推进到第 `stops` 次 Pending 后停下，返回仍持有 Future 本体的 Box。
    ///
    /// 丢弃该 Box 才是"丢弃 Future 本体"；只丢一个 `Pin<&mut F>` 或引用不算。
    fn advance_to_pending<F: Future>(future: F, stops: usize) -> Pin<Box<F>> {
        let mut boxed = Box::pin(future);
        for stop in 1..=stops {
            let waker = Waker::noop();
            let mut cx = TaskContext::from_waker(waker);
            match boxed.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(_) => panic!("future completed before pending stop {stop}"),
            }
        }
        boxed
    }

    /// 诊断观察：经协调组件读取业务值，不经 Context 业务入口。
    ///
    /// 终止后的观察只能用这一条（§4.8 允许纯诊断观察，业务入口已拒绝）。
    fn observe_num(
        ctx: &ExecutionContext,
        scope: &crate::core::identity::ScopeId,
        position: &RefId,
    ) -> Result<u32, ScopeError> {
        Ok(ctx.coordinator.resolve::<Tracked>(scope, position)?.num)
    }

    /// 诊断观察：一个 DataId 的唯一责任 Scope。
    fn observe_owner(
        ctx: &ExecutionContext,
        id: &crate::core::identity::DataId,
    ) -> Result<crate::core::identity::ScopeId, ScopeError> {
        ctx.coordinator.owner_probe(id)
    }

    /// 以给定 Drop 观测句柄构造业务值。
    fn tracked_num(num: u32, drops: &Arc<AtomicUsize>) -> Tracked {
        Tracked {
            num,
            drops: Arc::clone(drops),
        }
    }

    struct Fixture {
        execution: RootExecution,
        ids: RefIdAllocator,
        drops: Arc<AtomicUsize>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                execution: RootExecution::start(),
                ids: RefIdAllocator::new(RefIdSource::new()),
                drops: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn context_mut(&mut self) -> &mut ExecutionContext {
            self.execution.context_mut()
        }

        fn take_execution(self) -> RootExecution {
            self.execution
        }

        fn ref_id(&self) -> RefId {
            self.ids.allocate().unwrap()
        }

        fn tracked(&self, num: u32) -> Tracked {
            Tracked {
                num,
                drops: Arc::clone(&self.drops),
            }
        }

        fn drops(&self) -> usize {
            self.drops.load(Ordering::SeqCst)
        }

        fn root(&self) -> crate::core::identity::ScopeId {
            self.execution.context().root_scope()
        }

        /// Root 输入夹具：经真实 `register_owned` 登记 Application 交接的 owned 输入。
        fn root_input(&mut self, value: Tracked) -> (RefId, crate::core::identity::DataId) {
            let position = self.ref_id();
            let root = self.root();
            let id = self
                .execution
                .context_mut()
                .register_owned(&root, &position, value)
                .unwrap();
            (position, id)
        }

        fn spare_value(&self, num: u32) -> Tracked {
            self.tracked(num)
        }
    }

    // ---- 叶子／编排执行体（只接收 &T 或 Context，不接收 caller 输出位置） ----

    struct LeafConfig {
        gate: Gate,
    }

    async fn leaf_double(input: &Tracked, config: LeafConfig) -> Tracked {
        config.gate.wait().await;
        Tracked {
            num: input.num * 2,
            drops: Arc::clone(&input.drops),
        }
    }

    #[test]
    fn d01_root_creation_is_unique_and_shared() {
        let before = creation_counts::snapshot();
        let mut first = RootExecution::start();
        let second = RootExecution::start();
        let after = creation_counts::snapshot();
        assert_eq!(
            (after.0 - before.0, after.1 - before.1, after.2 - before.2),
            (2, 2, 2),
            "each root execution creates exactly one context, coordinator and container"
        );

        // 两个同时存活的 Root：身份与三项地址互不相同。
        let identity_first = first.context().identity_probe();
        let coordinator_first = first.context().coordinator_probe();
        let container_first = first.context().container_probe();
        assert_ne!(identity_first, second.context().identity_probe());
        assert_ne!(coordinator_first, second.context().coordinator_probe());
        assert_ne!(container_first, second.context().container_probe());

        // 同一 Execution 内 child／grandchild 共享身份与存储，且不新建任何一份。
        let root = first.context().root_scope();
        {
            let guard = first
                .context_mut()
                .enter(InvocationKind::Root, &root, true)
                .unwrap();
            let mut guard = guard;
            let child = guard.create_child(&root).unwrap();
            let grandchild = guard.create_child(&child).unwrap();
            assert!(grandchild != child);
            guard.complete();
        }
        assert_eq!(
            creation_counts::snapshot(),
            after,
            "nested scopes create no new pieces"
        );
        assert_eq!(identity_first, first.context().identity_probe());
        assert_eq!(coordinator_first, first.context().coordinator_probe());
        assert_eq!(container_first, first.context().container_probe());
    }

    #[test]
    fn d02_root_input_fixture_and_nested_separation() {
        struct Config {
            input: RefId,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            assert_eq!(
                ctx.frame_depth(),
                1,
                "nested entry uses the current context only"
            );
            assert_eq!(ctx.current_frame().unwrap().kind(), InvocationKind::Root);
            let root = ctx.root_scope();
            assert_eq!(ctx.resolve::<Tracked>(&root, &config.input)?.num, 7);
            let child = ctx.create_child(&root)?;
            assert_eq!(ctx.parent_of(&child)?.as_ref(), Some(&root));
            ctx.abort(&child)?;
            Ok(())
        }

        let mut f = Fixture::new();
        let (input_pos, input_id) = f.root_input(f.tracked(7));
        let execution = f.take_execution();
        let exit = drive_to_ready(
            run_root(execution, Config { input: input_pos }, body),
            |_| {},
        );

        assert!(exit.terminated().is_none());
        assert!(
            exit.close_error().is_none(),
            "empty-output root finalization closes cleanly"
        );
        assert_eq!(
            exit.cleanup_events(),
            0,
            "a normally completed root closes through finalization, not through guard cleanup"
        );
        let _ = input_id;
    }

    #[test]
    fn d03_invocation_is_not_scope() {
        struct Config {
            root: crate::core::identity::ScopeId,
            input: RefId,
            leaf_output: RefId,
            gate: Gate,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            assert_eq!(ctx.frame_depth(), 1);
            // 叶子沿用 RootScope：不新增 Scope，也不承担 RootScope 的销毁责任。
            invoke_leaf(
                ctx,
                &root,
                &config.input,
                &config.leaf_output,
                LeafConfig {
                    gate: config.gate.clone(),
                },
                leaf_double,
            )
            .await?;
            assert_eq!(ctx.frame_depth(), 1, "leaf frame restored to its caller");
            assert_eq!(ctx.current_scope().as_ref(), Some(&root));
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input_pos, input_id) = f.root_input(f.tracked(2));
        let leaf_output = f.ref_id();
        let gate = Gate::closed();
        let execution = f.take_execution();
        let exit = drive_to_ready(
            run_root(
                execution,
                Config {
                    root: root.clone(),
                    input: input_pos,
                    leaf_output: leaf_output.clone(),
                    gate: gate.clone(),
                },
                body,
            ),
            |pending| {
                if pending >= 1 {
                    gate.release();
                }
            },
        );
        assert!(exit.terminated().is_none());
        let _ = (input_id, leaf_output);
    }

    #[test]
    fn d04_frame_stack_restores_and_orders() {
        struct ChildConfig {
            root: crate::core::identity::ScopeId,
            child_input: RefId,
            leaf_output: RefId,
            leaf_output_again: RefId,
            declared_output: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
        }
        struct Config {
            root: crate::core::identity::ScopeId,
            input: RefId,
            child_input: RefId,
            leaf_output: RefId,
            leaf_output_again: RefId,
            boundary_output: RefId,
            root_export: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
        }

        async fn child_body(
            ctx: &mut ExecutionContext,
            child: &crate::core::identity::ScopeId,
            config: ChildConfig,
        ) -> Result<(), BodyError> {
            assert_eq!(
                ctx.frame_depth(),
                2,
                "boundary frame sits on the root frame"
            );
            assert_eq!(
                ctx.current_frame().unwrap().kind(),
                InvocationKind::Boundary
            );
            assert_eq!(ctx.current_scope().as_ref(), Some(child));

            // 边界内两次顺序叶子调用：每次结束后都回到本边界 frame。
            invoke_leaf(
                ctx,
                child,
                &config.child_input,
                &config.leaf_output,
                LeafConfig {
                    gate: config.gate.clone(),
                },
                leaf_double,
            )
            .await?;
            assert_eq!(ctx.frame_depth(), 2);
            // 顺序第二次调用：输出位置必须另取，单赋值不可重绑。
            invoke_leaf(
                ctx,
                child,
                &config.child_input,
                &config.leaf_output_again,
                LeafConfig {
                    gate: config.gate.clone(),
                },
                leaf_double,
            )
            .await?;
            assert_eq!(ctx.frame_depth(), 2);

            // 编排执行体可建立 Item／Round 等 descendant 作用域，由本边界退出覆盖。
            let extra = ctx.create_child(child)?;
            assert_eq!(ctx.parent_of(&extra)?.as_ref(), Some(child));
            ctx.abort(&extra)?;

            let value = {
                let gate = config.gate.clone();
                gate.wait().await;
                tracked_num(3, &config.drops)
            };
            ctx.register_owned(child, &config.declared_output, value)?;
            let _ = &config.root;
            Ok(())
        }

        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            assert_eq!(ctx.frame_depth(), 1);
            assert_eq!(ctx.current_frame().unwrap().kind(), InvocationKind::Root);

            let mut outputs = vec![ExportSlot::new::<Tracked>(
                &config.boundary_output,
                &config.root_export,
            )];
            let declared = vec![config.boundary_output.clone()];
            let imports = [ImportSlot::new::<Tracked>(
                &config.input,
                &config.child_input,
            )];
            invoke_boundary(
                ctx,
                &imports,
                &declared,
                &mut outputs,
                ChildConfig {
                    root: root.clone(),
                    child_input: config.child_input.clone(),
                    leaf_output: config.leaf_output.clone(),
                    leaf_output_again: config.leaf_output_again.clone(),
                    declared_output: config.boundary_output.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                },
                child_body,
            )
            .await?;

            assert_eq!(
                ctx.frame_depth(),
                1,
                "caller frame restored after the boundary"
            );
            assert_eq!(ctx.current_scope().as_ref(), Some(&root));
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input_pos, _) = f.root_input(f.tracked(2));
        let child_input = f.ref_id();
        let leaf_output = f.ref_id();
        let leaf_output_again = f.ref_id();
        let boundary_output = f.ref_id();
        let root_export = f.ref_id();
        let gate = Gate::closed();
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(
            run_root(
                execution,
                Config {
                    root,
                    input: input_pos,
                    child_input,
                    leaf_output,
                    leaf_output_again,
                    boundary_output,
                    root_export,
                    gate: gate.clone(),
                    drops,
                },
                body,
            ),
            |pending| {
                if pending >= 1 {
                    gate.release();
                }
            },
        );
        assert!(
            exit.terminated().is_none(),
            "unexpected termination: {:?}",
            exit.terminated()
        );
        assert!(exit.close_error().is_none());
    }

    #[test]
    fn d23_guard_and_frame_hold_metadata_only() {
        fn frame_shape(frame: &InvocationFrame) {
            let InvocationFrame {
                parent,
                kind,
                scope,
                owns_scope,
            } = frame;
            let _: &Option<usize> = parent;
            let _: &InvocationKind = kind;
            let _: &Option<crate::core::identity::ScopeId> = scope;
            let _: &bool = owns_scope;
        }
        fn guard_shape(guard: &InvocationGuard<'_>) {
            let InvocationGuard {
                context,
                frame_index,
                responsible,
                exit,
            } = guard;
            let _: &&mut ExecutionContext = context;
            let _: &usize = frame_index;
            let _: &Vec<crate::core::identity::ScopeId> = responsible;
            let _: &super::ExitDisposition = exit;
        }

        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let context = execution.context_mut();
        let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
        frame_shape(root_guard.frame());
        guard_shape(&root_guard);
        assert_eq!(root_guard.responsible_scopes(), std::slice::from_ref(&root));

        // child 边界 guard 承担 child Scope；叶子 guard 不承担任何 Scope。
        let child = root_guard.create_child(&root).unwrap();
        {
            let boundary = root_guard
                .enter(InvocationKind::Boundary, &child, true)
                .unwrap();
            frame_shape(boundary.frame());
            assert!(boundary.frame().owns_scope());
            assert_eq!(boundary.responsible_scopes(), std::slice::from_ref(&child));
            boundary.complete();
        }
        {
            let leaf = root_guard
                .enter(InvocationKind::Leaf, &root, false)
                .unwrap();
            assert!(!leaf.frame().owns_scope());
            assert!(leaf.responsible_scopes().is_empty());
            leaf.complete();
        }
        root_guard.complete();
    }

    /// 另一类型，用于多输出边界与类型不符。
    struct Other(u32);

    #[test]
    fn d05_explicit_input_isolation_and_rejections() {
        struct Config {
            root: crate::core::identity::ScopeId,
            root_local: RefId,
            child_local: RefId,
            state_local: RefId,
            mixed_local: RefId,
            mixed_state: RefId,
            not_imported: RefId,
            illegal_position: RefId,
            foreign_scope: crate::core::identity::ScopeId,
            sibling_position: RefId,
            root_id: crate::core::identity::DataId,
            drops: Arc<AtomicUsize>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            // sibling-owned 值：由本调用建立（Root 的直接 child）并登记。
            let sibling = ctx.create_child(&root)?;
            let sibling_owned = ctx.register_owned(
                &sibling,
                &config.sibling_position,
                tracked_num(99, &config.drops),
            )?;
            // 导入不复制业务值、不改变 owner。
            assert_eq!(observe_owner(ctx, &config.root_id)?, root);
            assert_eq!(observe_owner(ctx, &sibling_owned)?, sibling);

            // 显式导入后才可解析；未导入位置不搜索 ancestor.refs。
            let s1 = ctx.create_child(&root)?;
            ctx.import_batch(
                &s1,
                &root,
                &[ImportSlot::new::<Tracked>(
                    &config.root_local,
                    &config.child_local,
                )],
            )?;
            assert_eq!(ctx.resolve::<Tracked>(&s1, &config.child_local)?.num, 11);
            assert!(matches!(
                ctx.resolve::<Tracked>(&s1, &config.not_imported),
                Err(ScopeError::RefNotBound { .. })
            ));
            // foreign 与 Closed 在返回借用前拒绝。
            assert!(matches!(
                ctx.resolve::<Tracked>(&config.foreign_scope, &config.child_local),
                Err(ScopeError::ForeignExecution { .. })
            ));
            let closed = ctx.create_child(&root)?;
            ctx.abort(&closed)?;
            assert!(matches!(
                ctx.resolve::<Tracked>(&closed, &config.child_local),
                Err(ScopeError::ScopeClosed { .. })
            ));
            // 非法 owner（sibling-owned）：单有 target 存活不授予读取权。
            let s2 = ctx.create_child(&root)?;
            ctx.coordinator.inject_target_probe(
                &s2,
                &config.illegal_position,
                crate::core::scope::RefTarget::Data(sibling_owned.clone()),
            );
            assert!(matches!(
                ctx.resolve::<Tracked>(&s2, &config.illegal_position),
                Err(ScopeError::IllegalOwner { .. })
            ));

            // 混合 local + 状态来源的整组导入。
            let controller = ctx.create_child(&root)?;
            ctx.import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(
                    &config.root_local,
                    &config.state_local,
                )],
            )?;
            let state = ctx.register_state::<Tracked>(&controller, &config.state_local)?;
            let target = ctx.create_child(&controller)?;
            ctx.import_batch_with_states(
                &target,
                &controller,
                &[ImportSlot::new::<Tracked>(
                    &config.state_local,
                    &config.mixed_local,
                )],
                &[StateImportSlot::new::<Tracked>(&state, &config.mixed_state)],
            )?;
            assert_eq!(
                ctx.resolve::<Tracked>(&target, &config.mixed_local)?.num,
                11
            );
            assert_eq!(
                ctx.resolve::<Tracked>(&target, &config.mixed_state)?.num,
                11
            );

            assert_eq!(
                config.drops.load(Ordering::SeqCst),
                0,
                "imports neither clone nor drop input data"
            );
            // 本调用建立的 Scope 必须由自己关闭，否则 Root 收口会被活跃 descendant 拒绝。
            ctx.abort(&target)?;
            ctx.abort(&controller)?;
            ctx.abort(&s1)?;
            ctx.abort(&s2)?;
            ctx.abort(&sibling)?;
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (root_local, root_id) = f.root_input(f.tracked(11));
        let child_local = f.ref_id();
        let state_local = f.ref_id();
        let mixed_local = f.ref_id();
        let mixed_state = f.ref_id();
        let not_imported = f.ref_id();
        let illegal_position = f.ref_id();
        // 另一次 Execution 的 ScopeId。
        let foreign_execution = RootExecution::start();
        let foreign_scope = foreign_execution.context().root_scope();

        let config = Config {
            root: root.clone(),
            root_local,
            child_local,
            state_local,
            mixed_local,
            mixed_state,
            not_imported,
            illegal_position,
            foreign_scope,
            sibling_position: f.ref_id(),
            root_id,
            drops: Arc::clone(&f.drops),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
        assert!(exit.close_error().is_none());
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "the sibling value and the root input each drop exactly once at scope exit"
        );
    }

    #[test]
    fn d06_leaf_await_registers_output_once_and_unit_leaves_allocate_nothing() {
        let _ = Other(0);
        async fn unit_leaf(_input: &Tracked, gate: Gate) {
            gate.wait().await;
        }
        async fn body(ctx: &mut ExecutionContext, config: D06Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let id = invoke_leaf(
                ctx,
                &root,
                &config.input,
                &config.output,
                LeafConfig {
                    gate: config.gate.clone(),
                },
                leaf_double,
            )
            .await?;
            assert_eq!(ctx.resolve::<Tracked>(&root, &config.output)?.num, 4);
            assert_eq!(ctx.frame_depth(), 1, "leaf frame restored");
            // 无输出叶子：不登记 Data，也不产生新的 DataId。
            invoke_leaf_unit(ctx, &root, &config.input, config.gate.clone(), unit_leaf).await?;
            let probe = ctx.register_owned(&root, &config.probe, tracked_num(1, &config.drops))?;
            assert_eq!(probe.seq(), id.seq() + 1, "unit output allocated no DataId");
            // 输入保留原 owner；输出恰好登记一次。
            assert_eq!(observe_owner(ctx, &config.input_id)?, root);
            assert_eq!(observe_num(ctx, &root, &config.output)?, 4);
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input_pos, input_id) = f.root_input(f.tracked(2));
        let output = f.ref_id();
        let probe_pos = f.ref_id();
        let gate = Gate::closed();
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let config = D06Config {
            root: root.clone(),
            input: input_pos,
            input_id: input_id.clone(),
            output: output.clone(),
            probe: probe_pos,
            gate: gate.clone(),
            drops,
        };
        let exit = drive_to_ready(run_root(execution, config, body), |pending| {
            if pending >= 1 {
                gate.release();
            }
        });
        assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
        assert!(exit.close_error().is_none());
        let _ = (&input_id, &root);
    }

    struct D06Config {
        root: crate::core::identity::ScopeId,
        input: RefId,
        input_id: crate::core::identity::DataId,
        output: RefId,
        probe: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
    }

    /// 手动推进到 Ready，同时保留 Future 本体（用于"Ready 后丢弃"对照）。
    fn poll_to_ready_keeping<F: Future>(
        future: F,
        mut on_pending: impl FnMut(usize),
    ) -> (Pin<Box<F>>, F::Output) {
        let mut boxed = Box::pin(future);
        let mut pending = 0usize;
        loop {
            let waker = Waker::noop();
            let mut cx = TaskContext::from_waker(waker);
            match boxed.as_mut().poll(&mut cx) {
                Poll::Ready(output) => return (boxed, output),
                Poll::Pending => {
                    pending += 1;
                    assert!(pending < 64, "future is not making progress");
                    on_pending(pending);
                }
            }
        }
    }

    struct D07Config {
        root: crate::core::identity::ScopeId,
        first: RefId,
        second: RefId,
        export_a: RefId,
        export_b: RefId,
        export_a2: RefId,
        export_b2: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    #[test]
    fn d07_boundary_exports_two_outputs_atomically() {
        struct ChildConfig {
            first: RefId,
            second: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn child_body(
            ctx: &mut ExecutionContext,
            child: &crate::core::identity::ScopeId,
            config: ChildConfig,
        ) -> Result<(), BodyError> {
            config.slot.set(Some(child.clone()));
            let value = {
                let gate = config.gate.clone();
                gate.wait().await;
                tracked_num(1, &config.drops)
            };
            ctx.register_owned(child, &config.first, value)?;
            ctx.register_owned(child, &config.second, Other(2))?;
            Ok(())
        }
        async fn body(ctx: &mut ExecutionContext, config: D07Config) -> Result<(), BodyError> {
            let root = config.root.clone();

            // 正常：两项声明输出由调用边界整组提交。
            let mut outputs = vec![
                ExportSlot::new::<Tracked>(&config.first, &config.export_a),
                ExportSlot::new::<Other>(&config.second, &config.export_b),
            ];
            let declared = vec![config.first.clone(), config.second.clone()];
            invoke_boundary(
                ctx,
                &[],
                &declared,
                &mut outputs,
                ChildConfig {
                    first: config.first.clone(),
                    second: config.second.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                child_body,
            )
            .await?;
            assert_eq!(observe_num(ctx, &root, &config.export_a)?, 1);
            assert_eq!(ctx.resolve::<Other>(&root, &config.export_b)?.0, 2);

            // 失败：后项类型不符 → 无部分输出，child 临时值恰一次清理。
            let mut outputs_bad = vec![
                ExportSlot::new::<Tracked>(&config.first, &config.export_a2),
                ExportSlot::new::<Tracked>(&config.second, &config.export_b2),
            ];
            let slot = Rc::new(Cell::new(None));
            let failure = invoke_boundary(
                ctx,
                &[],
                &declared,
                &mut outputs_bad,
                ChildConfig {
                    first: config.first.clone(),
                    second: config.second.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&slot),
                },
                child_body,
            )
            .await
            .unwrap_err();
            assert!(
                failure.scope_error().is_some(),
                "second output type mismatch"
            );
            let faulty = slot.take().expect("child scope recorded");
            assert_eq!(
                ctx.state(&faulty)?,
                ScopeState::Closed,
                "failed boundary closes its scope"
            );
            assert_eq!(
                config.drops.load(Ordering::SeqCst),
                1,
                "only the failed boundary's own temporary is dropped so far"
            );
            assert!(ctx.parent_of(&root)?.is_none());
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (_input_pos, _) = f.root_input(f.tracked(5));
        let gate = Gate::closed();
        let config = D07Config {
            root: root.clone(),
            first: f.ref_id(),
            second: f.ref_id(),
            export_a: f.ref_id(),
            export_b: f.ref_id(),
            export_a2: f.ref_id(),
            export_b2: f.ref_id(),
            gate: gate.clone(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |pending| {
            if pending >= 1 {
                gate.release();
            }
        });
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert!(
            exit.cleanup_events() >= 1,
            "failing boundary guard cleaned its scope"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            3,
            "failed temporary + the value exported before the failure + the root input, each once"
        );
    }

    struct ControllerConfig {
        rules: RefId,
        rules_id: crate::core::identity::DataId,
        item_out: RefId,
        round_out: RefId,
        expose_a: RefId,
        expose_b: RefId,
        vec_out: RefId,
        drops: Arc<AtomicUsize>,
    }

    async fn controller_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ControllerConfig,
    ) -> Result<(), BodyError> {
        // 控制器输入仍是 root 负责的 imported 值。
        let collector = ctx.begin_collector::<Tracked>(child)?;

        // D09：ItemScope（控制器的直接 child）直接 Consume。
        let item = ctx.create_child(child)?;
        ctx.register_owned(&item, &config.item_out, tracked_num(7, &config.drops))?;
        ctx.consume_item(&item, &config.item_out, &collector)?;
        assert_eq!(ctx.state(&item)?, ScopeState::Closed);

        // imported 输出拒绝：ItemScope 重新暴露 imported Rules 没有可转移责任。
        let item2 = ctx.create_child(child)?;
        ctx.import_batch(
            &item2,
            child,
            &[ImportSlot::new::<Tracked>(&config.rules, &config.item_out)],
        )?;
        assert!(matches!(
            ctx.consume_item(&item2, &config.item_out, &collector),
            Err(ScopeError::IllegalOwner { .. })
        ));
        assert_eq!(
            ctx.coordinator.refs_len_probe(child)?,
            1,
            "no per-item parent ref is bound on the controller"
        );

        // D08：两轮 Promote；第二轮原样保留同一 DataId。
        let state = ctx.register_state::<Tracked>(child, &config.rules)?;
        let round = ctx.create_child(child)?;
        ctx.register_owned(&round, &config.round_out, tracked_num(8, &config.drops))?;
        ctx.promote(&round, &config.round_out, &state)?;
        assert_eq!(ctx.state(&round)?, ScopeState::Closed);

        let round2 = ctx.create_child(child)?;
        ctx.import_batch_with_states(
            &round2,
            child,
            &[],
            &[
                StateImportSlot::new::<Tracked>(&state, &config.expose_a),
                StateImportSlot::new::<Tracked>(&state, &config.expose_b),
            ],
        )?;
        ctx.promote(&round2, &config.expose_b, &state)?;
        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            0,
            "same-DataId promotion destroys nothing"
        );
        ctx.recycle_pending(child)?;
        assert_eq!(
            observe_owner(ctx, &config.rules_id)?,
            ctx.coordinator.owner_probe(&config.rules_id)?,
            "imported initial value keeps its owner"
        );
        assert_eq!(observe_num(ctx, child, &config.rules)?, 11);

        // 完成 collector：新的普通 Vec 绑定控制器最终输出位置。
        ctx.finish_collector(child, &collector, &config.vec_out)?;
        assert_eq!(
            ctx.resolve::<Vec<Tracked>>(child, &config.vec_out)?.len(),
            1
        );
        Ok(())
    }

    #[test]
    fn d08_promote_and_d09_consume_through_context() {
        struct Config {
            root: crate::core::identity::ScopeId,
            rules: RefId,
            rules_id: crate::core::identity::DataId,
            controller_rules: RefId,
            controller_rules_id: crate::core::identity::DataId,
            item_out: RefId,
            round_out: RefId,
            expose_a: RefId,
            expose_b: RefId,
            vec_out: RefId,
            root_export: RefId,
            drops: Arc<AtomicUsize>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs = vec![ExportSlot::new::<Vec<Tracked>>(
                &config.vec_out,
                &config.root_export,
            )];
            let declared = vec![config.vec_out.clone()];
            let imports = [ImportSlot::new::<Tracked>(
                &config.rules,
                &config.controller_rules,
            )];
            let child = invoke_boundary(
                ctx,
                &imports,
                &declared,
                &mut outputs,
                ControllerConfig {
                    rules: config.controller_rules.clone(),
                    rules_id: config.controller_rules_id.clone(),
                    item_out: config.item_out.clone(),
                    round_out: config.round_out.clone(),
                    expose_a: config.expose_a.clone(),
                    expose_b: config.expose_b.clone(),
                    vec_out: config.vec_out.clone(),
                    drops: Arc::clone(&config.drops),
                },
                controller_body,
            )
            .await?;
            assert_eq!(
                ctx.state(&child)?,
                ScopeState::Closed,
                "exported boundary closed"
            );
            assert_eq!(observe_owner(ctx, &config.rules_id)?, root);
            assert_eq!(
                ctx.resolve::<Vec<Tracked>>(&root, &config.root_export)?
                    .len(),
                1
            );
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (rules, rules_id) = f.root_input(f.tracked(11));
        let controller_rules = f.ref_id();
        let item_out = f.ref_id();
        let round_out = f.ref_id();
        let expose_a = f.ref_id();
        let expose_b = f.ref_id();
        let vec_out = f.ref_id();
        let root_export = f.ref_id();
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let config = Config {
            root: root.clone(),
            rules: rules.clone(),
            rules_id: rules_id.clone(),
            controller_rules,
            controller_rules_id: rules_id.clone(),
            item_out,
            round_out,
            expose_a,
            expose_b,
            vec_out,
            root_export,
            drops: Arc::clone(&drops),
        };
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
        assert!(exit.close_error().is_none());
        // Rules 由 root 负责，未被子边界回收；controller 的自有值随后各一次析构。
        assert_eq!(
            drops.load(Ordering::SeqCst),
            3,
            "controller state + collected element (exported then dropped with the root) + root input"
        );
        let _ = (root, rules);
    }

    struct ErrorConfig {
        input: RefId,
        temp: RefId,
        leaf_out: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn failing_leaf(_input: &Tracked, gate: Gate) -> Result<Tracked, BodyError> {
        gate.wait().await;
        Err(BodyError::new("leaf body failed"))
    }

    async fn error_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ErrorConfig,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(3, &config.drops))?;

        let result = invoke_fallible_leaf(
            ctx,
            child,
            &config.input,
            &config.leaf_out,
            config.gate.clone(),
            failing_leaf,
        )
        .await;
        assert!(result.is_err());
        // D11：沿用 caller Scope 的叶子 guard 不清理 caller 的绑定与责任。
        assert_eq!(
            ctx.state(child)?,
            ScopeState::Active,
            "leaf guard must not clean the caller scope"
        );
        assert_eq!(
            observe_num(ctx, child, &config.temp)?,
            3,
            "caller-owned temporary is untouched by the leaf failure"
        );
        Err(BodyError::new("boundary propagates the leaf failure"))
    }

    #[test]
    fn d10_error_exit_and_d11_leaf_guard_keeps_caller_scope() {
        async fn body(ctx: &mut ExecutionContext, config: D10Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs: Vec<ExportSlot> = Vec::new();
            let failure = invoke_boundary(
                ctx,
                &[ImportSlot::new::<Tracked>(
                    &config.input,
                    &config.child_input,
                )],
                &[],
                &mut outputs,
                ErrorConfig {
                    input: config.child_input.clone(),
                    temp: config.temp.clone(),
                    leaf_out: config.leaf_out.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                error_child_body,
            )
            .await;
            let failure = failure.expect_err("boundary must fail");
            assert_eq!(failure.note(), "boundary propagates the leaf failure");

            let child = config.slot.take().expect("child scope recorded");
            assert_eq!(
                ctx.state(&child)?,
                ScopeState::Closed,
                "error exit clean closed the child"
            );
            assert_eq!(
                config.drops.load(Ordering::SeqCst),
                1,
                "the child's own temporary drops exactly once"
            );
            // ancestor 保留且可读（纯诊断观察，业务入口已终止）。
            assert_eq!(observe_num(ctx, &root, &config.input)?, 3);
            assert_eq!(observe_owner(ctx, &config.input_id)?, root);
            assert_eq!(
                ctx.termination().unwrap().kind(),
                TerminationKind::BodyError
            );
            assert!(
                ctx.cleanup_failure().is_none(),
                "this failure cleaned successfully"
            );
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input, input_id) = f.root_input(f.tracked(3));
        let broken_pos = f.ref_id();
        let config = D10Config {
            root: root.clone(),
            input,
            input_id: input_id.clone(),
            child_input: f.ref_id(),
            temp: f.ref_id(),
            leaf_out: f.ref_id(),
            broken: broken_pos,
            probe: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let gate = config.gate.clone();
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |pending| {
            if pending >= 1 {
                gate.release();
            }
        });
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert!(exit.cleanup_events() >= 1);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "child temporary + root input, each exactly once"
        );
        let _ = (root, input_id);
    }

    struct D10Config {
        root: crate::core::identity::ScopeId,
        input: RefId,
        input_id: crate::core::identity::DataId,
        child_input: RefId,
        temp: RefId,
        leaf_out: RefId,
        broken: RefId,
        probe: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    #[test]
    fn d12_error_with_cleanup_failure_keeps_both_diagnostics() {
        async fn child_body(
            ctx: &mut ExecutionContext,
            child: &crate::core::identity::ScopeId,
            config: ErrorConfig,
        ) -> Result<(), BodyError> {
            config.slot.set(Some(child.clone()));
            ctx.register_owned(child, &config.temp, tracked_num(1, &config.drops))?;
            let broken =
                ctx.register_owned(child, &config.leaf_out, tracked_num(2, &config.drops))?;
            // test-only 故障：销毁 entry，责任记录保留 → guard 清理前提失败。
            ctx.coordinator.destroy_probe(&broken);
            Err(BodyError::new(
                "body fails with a broken cleanup precondition",
            ))
        }
        async fn body(ctx: &mut ExecutionContext, config: D10Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs: Vec<ExportSlot> = Vec::new();
            let failure = invoke_boundary(
                ctx,
                &[ImportSlot::new::<Tracked>(
                    &config.input,
                    &config.child_input,
                )],
                &[],
                &mut outputs,
                ErrorConfig {
                    input: config.child_input.clone(),
                    temp: config.temp.clone(),
                    leaf_out: config.broken.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                child_body,
            )
            .await
            .expect_err("boundary must fail");
            let _ = failure;

            // 两类诊断并存且互不覆盖。
            assert_eq!(
                ctx.termination().unwrap().kind(),
                TerminationKind::BodyError
            );
            let cleanup = ctx
                .cleanup_failure()
                .expect("cleanup failure is recorded separately");
            let child = config.slot.take().expect("child scope recorded");
            assert_eq!(cleanup.scope(), &child);
            assert_ne!(
                ctx.state(&child)?,
                ScopeState::Closed,
                "a scope with a failed cleanup is not disguised as closed"
            );
            // 终止后普通业务与提交入口拒绝；受控失败清理仍允许。
            assert!(matches!(
                ctx.register_owned(&root, &config.probe, tracked_num(9, &config.drops)),
                Err(ScopeError::Terminated {
                    kind: TerminationKind::BodyError
                })
            ));
            assert!(matches!(
                ctx.resolve::<Tracked>(&root, &config.input),
                Err(ScopeError::Terminated { .. })
            ));
            assert!(matches!(
                ctx.finalize(&root, &[], &mut Vec::new()),
                Err(ScopeError::Terminated { .. })
            ));
            assert!(
                ctx.abort(&child).is_err(),
                "broken cleanup still reports its diagnostic"
            );
            assert_eq!(
                observe_num(ctx, &root, &config.input)?,
                3,
                "ancestor preserved"
            );
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input, input_id) = f.root_input(f.tracked(3));
        let broken_pos = f.ref_id();
        let config = D10Config {
            root: root.clone(),
            input,
            input_id,
            child_input: f.ref_id(),
            temp: f.ref_id(),
            leaf_out: f.ref_id(),
            broken: broken_pos,
            probe: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            4,
            "destroyed entry + surviving temporary + root input + the armed probe value"
        );
        let _ = root;
    }

    #[test]
    fn d13_unpolled_and_ready_drops_do_not_recidivate() {
        // (a) 未 poll 就丢弃：body 不启动，不建立 Scope，也不清理。
        let started = Rc::new(Cell::new(false));
        let execution = RootExecution::start();
        {
            let future = run_root(execution, Rc::clone(&started), unreached_body);
            drop(future);
        }
        assert!(!started.get(), "an unpolled future never runs its body");

        // (b) 正常 Ready 后丢弃本体：不再重复清理。
        let mut f = Fixture::new();
        let root = f.root();
        let (input, _) = f.root_input(f.tracked(4));
        let config = D13Config {
            root: root.clone(),
            input,
            output: f.ref_id(),
            gate: Gate::closed(),
        };
        let gate = config.gate.clone();
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let (boxed, exit) =
            poll_to_ready_keeping(run_root(execution, config, ok_body), |pending| {
                if pending >= 1 {
                    gate.release();
                }
            });
        assert!(exit.terminated().is_none());
        assert_eq!(
            exit.cleanup_events(),
            0,
            "successful root closes by finalization"
        );
        drop(boxed);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "input + leaf output drop exactly once at context teardown"
        );

        // (c) 错误 Ready 后丢弃本体：不改写成取消，也不重复清理。
        let mut f = Fixture::new();
        let root = f.root();
        let (input, _) = f.root_input(f.tracked(6));
        let config = D13Config {
            root: root.clone(),
            input,
            output: f.ref_id(),
            gate: Gate::closed(),
        };
        let gate = config.gate.clone();
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let (boxed, exit) =
            poll_to_ready_keeping(run_root(execution, config, fail_body), |pending| {
                if pending >= 1 {
                    gate.release();
                }
            });
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        let cleanup_events = exit.cleanup_events();
        assert_eq!(
            cleanup_events, 2,
            "leaf guard exit plus the root guard's controlled failure exit"
        );
        drop(boxed);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "only the root input remains; no second cleanup and no double drop"
        );
    }

    struct D13Config {
        root: crate::core::identity::ScopeId,
        input: RefId,
        output: RefId,
        gate: Gate,
    }

    async fn unreached_body(
        _ctx: &mut ExecutionContext,
        started: Rc<Cell<bool>>,
    ) -> Result<(), BodyError> {
        started.set(true);
        Ok(())
    }

    async fn ok_body(ctx: &mut ExecutionContext, config: D13Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        invoke_leaf(
            ctx,
            &root,
            &config.input,
            &config.output,
            LeafConfig {
                gate: config.gate.clone(),
            },
            leaf_double,
        )
        .await?;
        Ok(())
    }

    async fn fail_body(ctx: &mut ExecutionContext, config: D13Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let _ = invoke_fallible_leaf(
            ctx,
            &root,
            &config.input,
            &config.output,
            config.gate.clone(),
            failing_leaf,
        )
        .await;
        Ok(())
    }

    struct CancelConfig {
        root: crate::core::identity::ScopeId,
        input: RefId,
        child_input: RefId,
        temp: RefId,
        child_output: RefId,
        root_export: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn cancel_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ChildCancelConfig,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(5, &config.drops))?;
        config.gate.wait().await;
        Ok(())
    }

    struct ChildCancelConfig {
        temp: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    struct CounterLeaf {
        runs: Rc<Cell<usize>>,
    }

    async fn counting_unit_leaf(_input: &Tracked, config: CounterLeaf) {
        config.runs.set(config.runs.get() + 1);
    }

    struct CounterChild {
        runs: Rc<Cell<usize>>,
    }

    async fn counting_child(
        _ctx: &mut ExecutionContext,
        _child: &crate::core::identity::ScopeId,
        config: CounterChild,
    ) -> Result<(), BodyError> {
        config.runs.set(config.runs.get() + 1);
        Ok(())
    }

    #[test]
    fn d14_cancel_after_scope_creation_cleans_child_only() {
        async fn body(ctx: &mut ExecutionContext, config: CancelConfig) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs = vec![ExportSlot::new::<Tracked>(
                &config.child_output,
                &config.root_export,
            )];
            let declared = vec![config.child_output.clone()];
            {
                let imports = [ImportSlot::new::<Tracked>(
                    &config.input,
                    &config.child_input,
                )];
                let future = invoke_boundary(
                    ctx,
                    &imports,
                    &declared,
                    &mut outputs,
                    ChildCancelConfig {
                        temp: config.temp.clone(),
                        gate: config.gate.clone(),
                        drops: Arc::clone(&config.drops),
                        slot: Rc::clone(&config.slot),
                    },
                    cancel_child_body,
                );
                // 推到真实 Pending 后丢弃持有本体的 Box。
                let boxed = advance_to_pending(future, 1);
                drop(boxed);
            }

            let child = config.slot.take().expect("child scope recorded");
            assert_eq!(
                ctx.state(&child)?,
                ScopeState::Closed,
                "cancelled boundary closed its scope"
            );
            assert!(ctx.is_terminated());
            assert_eq!(
                ctx.termination().unwrap().kind(),
                TerminationKind::Cancelled
            );
            // R13：诊断上报只返回副本，终止状态不可被活跃调用清除。
            assert!(ctx.termination_report().is_some());
            assert!(
                ctx.is_terminated(),
                "reporting diagnostics must not clear the termination state"
            );
            assert_eq!(
                config.drops.load(Ordering::SeqCst),
                1,
                "guard cleanup dropped the child's own temporary once"
            );
            assert_eq!(
                observe_num(ctx, &root, &config.input)?,
                3,
                "ancestor survives"
            );
            assert_eq!(ctx.frame_depth(), 1, "child frame removed by the guard");
            assert_eq!(
                ctx.coordinator.refs_len_probe(&root)?,
                1,
                "no output binding was committed"
            );

            // D20：取消后的调用边界。
            assert!(matches!(
                ctx.register_owned(&root, &config.child_output, tracked_num(9, &config.drops)),
                Err(ScopeError::Terminated {
                    kind: TerminationKind::Cancelled
                })
            ));
            assert!(matches!(
                ctx.resolve::<Tracked>(&root, &config.input),
                Err(ScopeError::Terminated { .. })
            ));
            assert!(matches!(
                ctx.finalize(&root, &[], &mut Vec::new()),
                Err(ScopeError::Terminated { .. })
            ));
            assert!(
                ctx.abort(&child).is_ok(),
                "controlled failure cleanup is still allowed"
            );
            assert_eq!(ctx.frame_depth(), 1, "no new invocation was created");

            // D20：真正发起叶子／边界调用，执行体计数必须为 0，且不新增 frame／Scope。
            let leaf_runs = Rc::new(Cell::new(0));
            let leaf_result = invoke_leaf_unit(
                ctx,
                &root,
                &config.input,
                CounterLeaf {
                    runs: Rc::clone(&leaf_runs),
                },
                counting_unit_leaf,
            )
            .await;
            assert!(matches!(
                leaf_result.as_ref().unwrap_err().scope_error(),
                Some(ScopeError::Terminated { .. })
            ));
            assert_eq!(
                leaf_runs.get(),
                0,
                "no business body ran after cancellation"
            );

            let child_runs = Rc::new(Cell::new(0));
            let mut outputs: Vec<ExportSlot> = Vec::new();
            let boundary_result = invoke_boundary(
                ctx,
                &[],
                &[],
                &mut outputs,
                CounterChild {
                    runs: Rc::clone(&child_runs),
                },
                counting_child,
            )
            .await;
            assert!(matches!(
                boundary_result.as_ref().unwrap_err().scope_error(),
                Some(ScopeError::Terminated { .. })
            ));
            assert_eq!(
                child_runs.get(),
                0,
                "no boundary body ran after cancellation"
            );
            assert_eq!(ctx.frame_depth(), 1);
            assert_eq!(
                ctx.coordinator.refs_len_probe(&root)?,
                1,
                "no new binding was committed"
            );
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input, _) = f.root_input(f.tracked(3));
        let config = CancelConfig {
            root: root.clone(),
            input,
            child_input: f.ref_id(),
            temp: f.ref_id(),
            child_output: f.ref_id(),
            root_export: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        assert!(exit.cleanup_events() >= 1);
    }

    #[test]
    fn d15_cancel_while_input_borrow_is_pending() {
        struct LeafCfg;
        struct Config {
            root: crate::core::identity::ScopeId,
            input: RefId,
            child_input: RefId,
            temp: RefId,
            leaf_out: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn holding_leaf(_input: &Tracked, gate: Gate) -> Tracked {
            gate.wait().await;
            Tracked {
                num: 0,
                drops: Arc::new(AtomicUsize::new(0)),
            }
        }
        async fn child_body(
            ctx: &mut ExecutionContext,
            child: &crate::core::identity::ScopeId,
            config: CancellingChild,
        ) -> Result<(), BodyError> {
            config.slot.set(Some(child.clone()));
            ctx.register_owned(child, &config.temp, tracked_num(4, &config.drops))?;
            // 输入借用跨真实 Pending：叶子持 &Tracked 等待挂起点。
            let _ = invoke_leaf(
                ctx,
                child,
                &config.child_input,
                &config.leaf_out,
                config.gate.clone(),
                holding_leaf,
            )
            .await;
            Ok(())
        }
        struct CancellingChild {
            child_input: RefId,
            temp: RefId,
            leaf_out: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs: Vec<ExportSlot> = Vec::new();
            {
                let imports = [ImportSlot::new::<Tracked>(
                    &config.input,
                    &config.child_input,
                )];
                let future = invoke_boundary(
                    ctx,
                    &imports,
                    &[],
                    &mut outputs,
                    CancellingChild {
                        child_input: config.child_input.clone(),
                        temp: config.temp.clone(),
                        leaf_out: config.leaf_out.clone(),
                        gate: config.gate.clone(),
                        drops: Arc::clone(&config.drops),
                        slot: Rc::clone(&config.slot),
                    },
                    child_body,
                );
                let boxed = advance_to_pending(future, 1);
                drop(boxed);
            }

            let child = config.slot.take().expect("child scope recorded");
            // 借用已随内层 Future 析构结束：随后还能可变借用 Context（清理入口）。
            assert!(
                ctx.abort(&child).is_ok(),
                "the input borrow ended before cleanup"
            );
            assert_eq!(ctx.state(&child)?, ScopeState::Closed);
            assert_eq!(
                config.drops.load(Ordering::SeqCst),
                1,
                "the child's own temporary drops exactly once"
            );
            assert_eq!(
                observe_num(ctx, &root, &config.input)?,
                3,
                "ancestor borrow source survives"
            );
            assert_eq!(ctx.frame_depth(), 1);
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input, _) = f.root_input(f.tracked(3));
        let config = Config {
            root: root.clone(),
            input,
            child_input: f.ref_id(),
            temp: f.ref_id(),
            leaf_out: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        let _ = LeafCfg;
    }

    #[test]
    fn d16_multilevel_drop_cleans_every_level() {
        struct InnerCfg {
            temp: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn inner_body(
            ctx: &mut ExecutionContext,
            inner: &crate::core::identity::ScopeId,
            config: InnerCfg,
        ) -> Result<(), BodyError> {
            config.slot.set(Some(inner.clone()));
            ctx.register_owned(inner, &config.temp, tracked_num(1, &config.drops))?;
            config.gate.wait().await;
            Ok(())
        }
        struct OuterCfg {
            inner: InnerCfg,
            outer_temp: RefId,
            outer_slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
            drops: Arc<AtomicUsize>,
        }
        async fn outer_body(
            ctx: &mut ExecutionContext,
            outer: &crate::core::identity::ScopeId,
            config: OuterCfg,
        ) -> Result<(), BodyError> {
            config.outer_slot.set(Some(outer.clone()));
            ctx.register_owned(outer, &config.outer_temp, tracked_num(2, &config.drops))?;
            let mut inner_outputs: Vec<ExportSlot> = Vec::new();
            let _ = invoke_boundary(ctx, &[], &[], &mut inner_outputs, config.inner, inner_body)
                .await?;
            Ok(())
        }
        struct Config {
            inner: InnerCfg,
            outer_temp: RefId,
            outer_slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
            drops: Arc<AtomicUsize>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let mut outputs: Vec<ExportSlot> = Vec::new();
            let inner_slot_recorded = Rc::clone(&config.inner.slot);
            {
                let outer_slot = Rc::clone(&config.outer_slot);
                let drops = Arc::clone(&config.drops);
                let inner_slot = Rc::clone(&config.inner.slot);
                let inner = config.inner;
                let outer_temp = config.outer_temp;
                let future = invoke_boundary(
                    ctx,
                    &[],
                    &[],
                    &mut outputs,
                    OuterCfg {
                        inner,
                        outer_temp,
                        outer_slot,
                        drops,
                    },
                    outer_body,
                );
                let boxed = advance_to_pending(future, 1);
                drop(boxed);
                let _ = inner_slot;
            }

            assert_eq!(ctx.frame_depth(), 1, "all nested frames removed");
            let outer = config.outer_slot.take().expect("outer recorded");
            let inner = inner_slot_recorded.take().expect("inner recorded");
            assert_eq!(ctx.state(&outer)?, ScopeState::Closed);
            assert_eq!(ctx.state(&inner)?, ScopeState::Closed, "no detached child");
            assert_eq!(
                config.drops.load(Ordering::SeqCst),
                2,
                "each level's own temporary dropped exactly once"
            );
            assert_eq!(
                ctx.termination().unwrap().kind(),
                TerminationKind::Cancelled
            );
            Ok(())
        }

        let f = Fixture::new();
        let root = f.root();
        let _ = root;
        let config = Config {
            inner: InnerCfg {
                temp: f.ref_id(),
                gate: Gate::closed(),
                drops: Arc::clone(&f.drops),
                slot: Rc::new(Cell::new(None)),
            },
            outer_temp: f.ref_id(),
            outer_slot: Rc::new(Cell::new(None)),
            drops: Arc::clone(&f.drops),
        };
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
    }

    #[test]
    fn d17_cancel_with_partial_results_and_promoted_state() {
        struct ChildCfg {
            rules: RefId,
            item_out: RefId,
            round_out: RefId,
            round_out_again: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn child_body(
            ctx: &mut ExecutionContext,
            child: &crate::core::identity::ScopeId,
            config: ChildCfg,
        ) -> Result<(), BodyError> {
            config.slot.set(Some(child.clone()));
            let collector = ctx.begin_collector::<Tracked>(child)?;
            let item = ctx.create_child(child)?;
            ctx.register_owned(&item, &config.item_out, tracked_num(7, &config.drops))?;
            ctx.consume_item(&item, &config.item_out, &collector)?;
            let state = ctx.register_state::<Tracked>(child, &config.rules)?;
            let round = ctx.create_child(child)?;
            ctx.register_owned(&round, &config.round_out, tracked_num(8, &config.drops))?;
            ctx.promote(&round, &config.round_out, &state)?;
            // 第二次替换：上一轮 controller-owned 状态成为 pending 回收责任。
            let round2 = ctx.create_child(child)?;
            ctx.register_owned(
                &round2,
                &config.round_out_again,
                tracked_num(10, &config.drops),
            )?;
            ctx.promote(&round2, &config.round_out_again, &state)?;
            config.gate.wait().await;
            Ok(())
        }
        struct Config {
            root: crate::core::identity::ScopeId,
            rules: RefId,
            child_rules: RefId,
            rules_id: crate::core::identity::DataId,
            item_out: RefId,
            round_out: RefId,
            round_out_again: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs: Vec<ExportSlot> = Vec::new();
            {
                let imports = [ImportSlot::new::<Tracked>(
                    &config.rules,
                    &config.child_rules,
                )];
                let future = invoke_boundary(
                    ctx,
                    &imports,
                    &[],
                    &mut outputs,
                    ChildCfg {
                        rules: config.child_rules.clone(),
                        item_out: config.item_out.clone(),
                        round_out: config.round_out.clone(),
                        round_out_again: config.round_out_again.clone(),
                        gate: config.gate.clone(),
                        drops: Arc::clone(&config.drops),
                        slot: Rc::clone(&config.slot),
                    },
                    child_body,
                );
                let boxed = advance_to_pending(future, 1);
                drop(boxed);
            }
            let child = config.slot.take().expect("child recorded");
            assert_eq!(ctx.state(&child)?, ScopeState::Closed);
            assert_eq!(
                config.drops.load(Ordering::SeqCst),
                3,
                "collector partial result, pending state and current state each drop once"
            );
            // imported 初值仍由 root 负责，且没有正常集合输出。
            assert_eq!(observe_num(ctx, &root, &config.rules)?, 11);
            assert_eq!(observe_owner(ctx, &config.rules_id)?, root);
            assert_eq!(
                ctx.coordinator.refs_len_probe(&root)?,
                1,
                "no Vec output was bound"
            );
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (rules, rules_id) = f.root_input(f.tracked(11));
        let config = Config {
            root: root.clone(),
            rules: rules.clone(),
            child_rules: f.ref_id(),
            rules_id: rules_id.clone(),
            item_out: f.ref_id(),
            round_out: f.ref_id(),
            round_out_again: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            4,
            "collector partial + pending state + current state + root input, each once"
        );
        let _ = rules;
    }

    #[test]
    fn d18_root_future_owner_drop_is_a_controlled_exit() {
        struct Config {
            temp: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn child_body(
            ctx: &mut ExecutionContext,
            child: &crate::core::identity::ScopeId,
            config: ChildCancelConfig,
        ) -> Result<(), BodyError> {
            config.slot.set(Some(child.clone()));
            ctx.register_owned(child, &config.temp, tracked_num(1, &config.drops))?;
            config.gate.wait().await;
            Ok(())
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = ctx.root_scope();
            let mut outputs: Vec<ExportSlot> = Vec::new();
            let _ = invoke_boundary(
                ctx,
                &[],
                &[],
                &mut outputs,
                ChildCancelConfig {
                    temp: config.temp.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                child_body,
            )
            .await;
            let _ = root;
            Ok(())
        }

        let before_cleanups = creation_counts::guard_cleanups();
        let mut f = Fixture::new();
        let (input, input_id) = f.root_input(f.tracked(42));
        let config = Config {
            temp: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let other = Fixture::new();
        let execution = f.take_execution();
        let boxed = advance_to_pending(run_root(execution, config, body), 1);
        // 丢弃拥有 Context 的 Root Future 本体。
        drop(boxed);

        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "child temporary (guard cleanup) + root input (context teardown), each once"
        );
        assert!(
            creation_counts::guard_cleanups() > before_cleanups,
            "the cancel path ran through guard cleanup, not only natural container drop"
        );
        assert_eq!(other.drops(), 0, "another execution is unaffected");
        let _ = (input, input_id);
    }

    #[test]
    fn d19_drop_cleanup_failure_stays_observable() {
        struct Config {
            root: crate::core::identity::ScopeId,
            input: RefId,
            child_input: RefId,
            temp: RefId,
            other_collector: RefId,
            other_value: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn child_body(
            ctx: &mut ExecutionContext,
            child: &crate::core::identity::ScopeId,
            config: ChildCancelConfig2,
        ) -> Result<(), BodyError> {
            config.slot.set(Some(child.clone()));
            let broken =
                ctx.register_owned(child, &config.broken, tracked_num(1, &config.drops))?;
            ctx.register_owned(child, &config.temp, tracked_num(2, &config.drops))?;
            ctx.coordinator.destroy_probe(&broken);
            config.gate.wait().await;
            Ok(())
        }
        struct ChildCancelConfig2 {
            broken: RefId,
            temp: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs: Vec<ExportSlot> = Vec::new();
            {
                let imports = [ImportSlot::new::<Tracked>(
                    &config.input,
                    &config.child_input,
                )];
                let future = invoke_boundary(
                    ctx,
                    &imports,
                    &[],
                    &mut outputs,
                    ChildCancelConfig2 {
                        broken: config.other_value.clone(),
                        temp: config.temp.clone(),
                        gate: config.gate.clone(),
                        drops: Arc::clone(&config.drops),
                        slot: Rc::clone(&config.slot),
                    },
                    child_body,
                );
                let boxed = advance_to_pending(future, 1);
                drop(boxed);
            }

            let child = config.slot.take().expect("child recorded");
            assert_eq!(
                ctx.termination().unwrap().kind(),
                TerminationKind::Cancelled
            );
            let cleanup = ctx
                .cleanup_failure()
                .expect("cancellation cleanup failure recorded");
            assert_eq!(cleanup.scope(), &child);
            assert_ne!(
                ctx.state(&child)?,
                ScopeState::Closed,
                "broken scope is not disguised"
            );
            // ancestor 与另一份 collector 内容不受影响。
            assert_eq!(observe_num(ctx, &root, &config.input)?, 3);
            assert_eq!(
                ctx.coordinator
                    .resolve::<Tracked>(&root, &config.other_collector)?
                    .num,
                5
            );
            assert!(matches!(
                ctx.register_owned(
                    &root,
                    &config.other_collector,
                    tracked_num(9, &config.drops)
                ),
                Err(ScopeError::Terminated { .. })
            ));
            let _ = config.other_value;
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (input, _) = f.root_input(f.tracked(3));
        let other_pos = f.ref_id();
        let other_value = f.spare_value(5);
        f.context_mut()
            .register_owned(&root, &other_pos, other_value)
            .unwrap();
        let config = Config {
            root: root.clone(),
            input,
            child_input: f.ref_id(),
            temp: f.ref_id(),
            other_collector: other_pos.clone(),
            other_value: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        assert!(
            drops.load(Ordering::SeqCst) >= 4,
            "destroyed entry, surviving temporary, root input and sibling value all accounted"
        );
        let _ = root;
    }

    // ---- R7～R11：复审反例的永久回归 ----

    struct R7Config {
        root: crate::core::identity::ScopeId,
        root_input: RefId,
        unbound_source: RefId,
        child_input: RefId,
        runs: Rc<Cell<usize>>,
        drops: Arc<AtomicUsize>,
    }

    struct R7ChildConfig {
        runs: Rc<Cell<usize>>,
    }

    async fn r7_child_body(
        _ctx: &mut ExecutionContext,
        _child: &crate::core::identity::ScopeId,
        config: R7ChildConfig,
    ) -> Result<(), BodyError> {
        config.runs.set(config.runs.get() + 1);
        Ok(())
    }

    async fn r7_body(ctx: &mut ExecutionContext, config: R7Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let refs_before = ctx.coordinator.refs_len_probe(&root)?;
        let imports = [ImportSlot::new::<Tracked>(
            &config.unbound_source,
            &config.child_input,
        )];
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let error = invoke_boundary(
            ctx,
            &imports,
            &[],
            &mut outputs,
            R7ChildConfig {
                runs: Rc::clone(&config.runs),
            },
            r7_child_body,
        )
        .await
        .expect_err("caller position is not bound");
        assert!(
            matches!(error.scope_error(), Some(ScopeError::RefNotBound { .. })),
            "the original import diagnostic is returned: {error:?}"
        );

        // 首次执行失败记录：定位与原始诊断都保存，不被清理结果覆盖。
        assert!(
            ctx.is_terminated(),
            "establishment failure terminates the execution"
        );
        let termination = ctx.termination().expect("termination recorded");
        assert_eq!(termination.kind(), TerminationKind::BodyError);
        assert_eq!(termination.note(), "boundary input assembly failed");
        assert!(
            matches!(
                termination.scope_error(),
                Some(ScopeError::RefNotBound { .. })
            ),
            "the first failure keeps the original Scope error"
        );
        assert!(
            ctx.cleanup_failure().is_none(),
            "this path has no ownable child content, so cleanup succeeds"
        );

        // 无部分 Import；child body 未运行；随后新业务与正常 commit 全部被拒绝。
        assert_eq!(ctx.coordinator.refs_len_probe(&root)?, refs_before);
        assert_eq!(config.runs.get(), 0, "the child body never ran");
        assert!(matches!(
            ctx.register_owned(&root, &config.child_input, tracked_num(1, &config.drops)),
            Err(ScopeError::Terminated { .. })
        ));
        assert!(matches!(
            ctx.resolve::<Tracked>(&root, &config.root_input),
            Err(ScopeError::Terminated { .. })
        ));
        assert!(matches!(
            ctx.finalize(&root, &[], &mut Vec::new()),
            Err(ScopeError::Terminated { .. })
        ));
        assert_eq!(
            ctx.frame_depth(),
            1,
            "no boundary frame survived the failure"
        );
        Ok(())
    }

    #[test]
    fn r7_import_failure_records_first_failure_and_blocks_new_work() {
        let mut f = Fixture::new();
        let root = f.root();
        let (input, _) = f.root_input(f.tracked(1));
        let config = R7Config {
            root: root.clone(),
            root_input: input.clone(),
            unbound_source: f.ref_id(),
            child_input: f.ref_id(),
            runs: Rc::new(Cell::new(0)),
            drops: Arc::clone(&f.drops),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, r7_body), |_| {});
        assert_eq!(
            exit.terminated(),
            Some(TerminationKind::BodyError),
            "the root goes through the controlled failure exit"
        );
        assert!(
            exit.close_error().is_none(),
            "no normal close was attempted"
        );
        assert_eq!(
            exit.termination_note(),
            Some("boundary input assembly failed"),
            "the first failure note survives the root close"
        );
        assert!(
            exit.cleanup_events() >= 1,
            "the root guard cleaned its subtree"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "root input plus the value handed to the rejected registration, each exactly once"
        );
        let _ = (input, root);
    }

    struct R7TypeConfig {
        root: crate::core::identity::ScopeId,
        source: RefId,
        first_target: RefId,
        second_target: RefId,
        runs: Rc<Cell<usize>>,
        drops: Arc<AtomicUsize>,
    }

    async fn r7_type_body(
        ctx: &mut ExecutionContext,
        config: R7TypeConfig,
    ) -> Result<(), BodyError> {
        let root = config.root.clone();
        let imports = [
            ImportSlot::new::<Tracked>(&config.source, &config.first_target),
            // 第二项声明 `Other`，实际是 `Tracked`：整组必须一起失败。
            ImportSlot::new::<Other>(&config.source, &config.second_target),
        ];
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let error = invoke_boundary(
            ctx,
            &imports,
            &[],
            &mut outputs,
            R7ChildConfig {
                runs: Rc::clone(&config.runs),
            },
            r7_child_body,
        )
        .await
        .expect_err("the second slot declares the wrong type");
        assert!(matches!(
            error.scope_error(),
            Some(ScopeError::TypeMismatch { .. })
        ));
        assert_eq!(
            ctx.coordinator.refs_len_probe(&root)?,
            1,
            "no partial import binding is committed (group is all-or-nothing)"
        );
        assert_eq!(config.runs.get(), 0);
        assert_eq!(ctx.frame_depth(), 1);
        assert!(ctx.is_terminated());
        assert!(matches!(
            ctx.register_owned(&root, &config.second_target, tracked_num(1, &config.drops)),
            Err(ScopeError::Terminated { .. })
        ));
        Ok(())
    }

    #[test]
    fn r7_second_slot_type_failure_leaves_no_partial_bindings() {
        let mut f = Fixture::new();
        let root = f.root();
        let (source, _) = f.root_input(f.tracked(1));
        let config = R7TypeConfig {
            root: root.clone(),
            source,
            first_target: f.ref_id(),
            second_target: f.ref_id(),
            runs: Rc::new(Cell::new(0)),
            drops: Arc::clone(&f.drops),
        };
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, r7_type_body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert!(exit.cleanup_events() >= 1);
        let _ = root;
    }

    struct R8Config {
        root: crate::core::identity::ScopeId,
        root_pos: RefId,
        root_id: crate::core::identity::DataId,
        child_pos: RefId,
        forbidden_write: RefId,
    }

    async fn r8_body(ctx: &mut ExecutionContext, config: R8Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        // caller（Root）的位置属于当前调用自己，可读。
        assert_eq!(
            *ctx.coordinator.resolve::<u32>(&root, &config.root_pos)?,
            41u32
        );
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let imports = [ImportSlot::new::<u32>(&config.root_pos, &config.child_pos)];
        invoke_boundary(
            ctx,
            &imports,
            &[],
            &mut outputs,
            R8ChildConfig {
                root: root.clone(),
                root_pos: config.root_pos.clone(),
                child_pos: config.child_pos.clone(),
                forbidden_write: config.forbidden_write.clone(),
            },
            r8_child_body,
        )
        .await
        .expect("the boundary itself succeeds; the child's attempts are what must fail");
        // 越权尝试没有留下任何绑定：Root 只保留最初的输入位置。
        assert_eq!(ctx.coordinator.refs_len_probe(&root)?, 1);
        let _ = &config.root_id;
        Ok(())
    }

    struct R8ChildConfig {
        root: crate::core::identity::ScopeId,
        root_pos: RefId,
        child_pos: RefId,
        forbidden_write: RefId,
    }

    async fn r8_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: R8ChildConfig,
    ) -> Result<(), BodyError> {
        assert_eq!(ctx.current_scope().as_ref(), Some(child));
        // 读 caller／ancestor 的本地位置被拒绝。
        assert!(matches!(
            ctx.resolve::<u32>(&config.root, &config.root_pos),
            Err(ScopeError::OutsideInvocation { .. })
        ));
        // 向 caller 直接登记输出同样被拒绝。
        assert!(matches!(
            ctx.register_owned(&config.root, &config.forbidden_write, 42u32),
            Err(ScopeError::OutsideInvocation { .. })
        ));
        // child 自己的（已导入）位置仍然可用。
        assert_eq!(*ctx.resolve::<u32>(child, &config.child_pos)?, 41u32);
        Ok(())
    }

    #[test]
    fn r8_child_cannot_touch_caller_scope_through_context() {
        let mut f = Fixture::new();
        let root = f.root();
        let root_pos = f.ref_id();
        let root_id = f
            .context_mut()
            .register_owned(&root, &root_pos, 41u32)
            .unwrap();
        // child 自己的输入位置（边界导入）
        let child_pos = f.ref_id();
        let forbidden_write = f.ref_id();
        let execution = f.take_execution();
        let config = R8Config {
            root: root.clone(),
            root_pos: root_pos.clone(),
            root_id: root_id.clone(),
            child_pos,
            forbidden_write,
        };
        let exit = drive_to_ready(run_root(execution, config, r8_body), |_| {});
        assert!(
            exit.terminated().is_none(),
            "terminated={:?} body={:?} scope={:?}",
            exit.terminated(),
            exit.body_error().map(|error| error.note()),
            exit.body_error()
                .and_then(|error| error.scope_error().map(|scope| scope.to_string()))
        );
        assert!(exit.close_error().is_none(), "{:?}", exit.close_error());
    }

    struct R9Config {
        root: crate::core::identity::ScopeId,
        live_child: RefId,
        temp: RefId,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn r9_body(ctx: &mut ExecutionContext, config: R9Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        // body 建立一个直接 child 与自有临时值后直接返回 Ok，未退出 child。
        let child = ctx.create_child(&root)?;
        config.slot.set(Some(child.clone()));
        ctx.register_owned(&child, &config.temp, tracked_num(1, &config.drops))?;
        let _ = config.live_child;
        Ok(())
    }

    #[test]
    fn r9_root_close_failure_enters_controlled_exit() {
        let mut f = Fixture::new();
        let root = f.root();
        let (_, _) = f.root_input(f.tracked(1));
        let config = R9Config {
            root: root.clone(),
            live_child: f.ref_id(),
            temp: f.ref_id(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, r9_body), |_| {});

        assert_eq!(
            exit.terminated(),
            Some(TerminationKind::BodyError),
            "a failed root close is a controlled failure exit, not a quiet close_error"
        );
        assert!(
            matches!(
                exit.close_error(),
                Some(ScopeError::ActiveDescendants { .. })
            ),
            "the close diagnostic is preserved: {:?}",
            exit.close_error()
        );
        assert_eq!(exit.cleanup_scope(), None, "cleanup itself succeeded");
        assert!(
            exit.cleanup_events() >= 1,
            "the guard cleaned the root subtree"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "the live child's temporary and the root input each drop exactly once"
        );
    }

    struct SwallowConfig {
        temp: RefId,
        drops: Arc<AtomicUsize>,
        gate: Gate,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn swallow_body(
        ctx: &mut ExecutionContext,
        config: SwallowConfig,
    ) -> Result<(), BodyError> {
        let mut outputs: Vec<ExportSlot> = Vec::new();
        {
            let future = invoke_boundary(
                ctx,
                &[],
                &[],
                &mut outputs,
                SwallowChild {
                    temp: config.temp.clone(),
                    drops: Arc::clone(&config.drops),
                    gate: config.gate.clone(),
                    slot: Rc::clone(&config.slot),
                },
                swallow_child_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }
        // 外层接住取消并返回 Ok：Root 收口仍必须走受控失败退出。
        Ok(())
    }

    struct SwallowChild {
        temp: RefId,
        drops: Arc<AtomicUsize>,
        gate: Gate,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn swallow_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: SwallowChild,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(1, &config.drops))?;
        config.gate.wait().await;
        Ok(())
    }

    #[test]
    fn r9_swallowed_cancel_still_enters_controlled_root_exit() {
        let mut f = Fixture::new();
        let (_, _) = f.root_input(f.tracked(1));
        let config = SwallowConfig {
            temp: f.ref_id(),
            drops: Arc::clone(&f.drops),
            gate: Gate::closed(),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, swallow_body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        assert!(
            exit.close_error().is_none(),
            "no normal close was attempted after cancellation"
        );
        assert!(
            exit.cleanup_events() >= 2,
            "child guard and root guard both cleaned"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "child temporary and root input each drop exactly once"
        );
    }

    struct R10Config {
        temp: RefId,
        drops: Arc<AtomicUsize>,
    }

    async fn r10_body(ctx: &mut ExecutionContext, config: R10Config) -> Result<(), BodyError> {
        let child = ctx.create_child(&ctx.root_scope())?;
        ctx.register_owned(&child, &config.temp, tracked_num(1, &config.drops))?;
        Err(BodyError::new("original detailed execution failure"))
    }

    #[test]
    fn r10_root_exit_keeps_original_diagnostic() {
        let f = Fixture::new();
        let config = R10Config {
            temp: f.ref_id(),
            drops: Arc::clone(&f.drops),
        };
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, r10_body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert_eq!(
            exit.termination_note(),
            Some("original detailed execution failure"),
            "the original failure note is preserved"
        );
        assert_eq!(
            exit.body_error().map(|error| error.note()),
            Some("original detailed execution failure"),
            "the body error object is returned, not dropped"
        );
        assert!(exit.cleanup_error().is_none());
    }

    #[test]
    fn r11_closed_or_finalizing_scope_cannot_be_entered() {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let context = execution.context_mut();
        context.abort(&root).unwrap();
        assert!(matches!(
            context.enter(InvocationKind::Root, &root, true),
            Err(ScopeError::ScopeClosed { .. }) | Err(ScopeError::ScopeNotActive { .. })
        ));

        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let context = execution.context_mut();
        let guard = context.enter(InvocationKind::Root, &root, true).unwrap();
        let mut guard = guard;
        let child = guard.create_child(&root).unwrap();
        drop(guard);
        // child 仍存活，但 Root 已因 guard 取消而进入清理/关闭路径。
        let state = context.state(&root).unwrap();
        assert_eq!(
            state,
            ScopeState::Closed,
            "the cancelled root guard cleaned the subtree"
        );
        assert_eq!(
            context.state(&child).unwrap(),
            ScopeState::Closed,
            "the cancelled guard cleaned the whole subtree"
        );
        // 已关闭的 Scope 与已终止的 Execution 都不能再进入调用。
        assert!(matches!(
            context.enter(InvocationKind::Boundary, &child, true),
            Err(ScopeError::ScopeClosed { .. })
                | Err(ScopeError::ScopeNotActive { .. })
                | Err(ScopeError::OutsideInvocation { .. })
                | Err(ScopeError::Terminated { .. })
        ));
    }

    // ---- R12：补齐的验收证据 ----

    struct D01ChildConfig {
        identity: *const (),
        coordinator: *const (),
        container: *const (),
        gate: Gate,
    }

    async fn d01_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: D01ChildConfig,
    ) -> Result<(), BodyError> {
        // 真实的多层调用里仍然共享同一身份根、协调组件与 Container。
        assert_eq!(ctx.identity_probe(), config.identity);
        assert_eq!(ctx.coordinator_probe(), config.coordinator);
        assert_eq!(ctx.container_probe(), config.container);
        assert_eq!(ctx.frame_depth(), 2, "boundary frame on the root frame");
        let gate = config.gate.clone();
        gate.wait().await;

        // grandchild 边界：第三层 frame 里再观测同一组地址，且不新建任何一份。
        let mut grandchild_outputs: Vec<ExportSlot> = Vec::new();
        let grandchild = invoke_boundary(
            ctx,
            &[],
            &[],
            &mut grandchild_outputs,
            D01GrandchildConfig {
                identity: config.identity,
                coordinator: config.coordinator,
                container: config.container,
            },
            d01_grandchild_body,
        )
        .await?;
        assert_eq!(ctx.state(&grandchild)?, ScopeState::Closed);
        assert_eq!(ctx.frame_depth(), 2, "back inside the child invocation");
        let _ = child;
        Ok(())
    }

    struct D01GrandchildConfig {
        identity: *const (),
        coordinator: *const (),
        container: *const (),
    }

    async fn d01_grandchild_body(
        ctx: &mut ExecutionContext,
        _scope: &crate::core::identity::ScopeId,
        config: D01GrandchildConfig,
    ) -> Result<(), BodyError> {
        assert_eq!(ctx.frame_depth(), 3, "grandchild frame");
        assert_eq!(ctx.identity_probe(), config.identity);
        assert_eq!(ctx.coordinator_probe(), config.coordinator);
        assert_eq!(ctx.container_probe(), config.container);
        Ok(())
    }

    struct D01Config {
        gate: Gate,
    }

    async fn d01_outer(ctx: &mut ExecutionContext, config: D01Config) -> Result<(), BodyError> {
        // Context owner 已固定在 Root 驱动内：在真实调用层级里采集并跨 frame 比较地址。
        let identity = ctx.identity_probe();
        let coordinator = ctx.coordinator_probe();
        let container = ctx.container_probe();
        let mut outputs: Vec<ExportSlot> = Vec::new();
        // Root -> child Boundary -> grandchild Boundary：三层真实调用里观测同一组地址。
        let child = invoke_boundary(
            ctx,
            &[],
            &[],
            &mut outputs,
            D01ChildConfig {
                identity,
                coordinator,
                container,
                gate: config.gate.clone(),
            },
            d01_child_body,
        )
        .await?;
        assert_eq!(ctx.state(&child)?, ScopeState::Closed);
        assert_eq!(
            ctx.identity_probe(),
            identity,
            "identity is shared after nested calls"
        );
        assert_eq!(ctx.coordinator_probe(), coordinator);
        assert_eq!(ctx.container_probe(), container);
        Ok(())
    }

    #[test]
    fn d01_addresses_are_stable_across_a_real_nested_invocation() {
        let f = Fixture::new();
        let _root = f.root();
        let before = creation_counts::snapshot();
        let gate = Gate::closed();
        let execution = f.take_execution();
        let exit = drive_to_ready(
            run_root(execution, D01Config { gate: gate.clone() }, d01_outer),
            |pending| {
                if pending >= 1 {
                    gate.release();
                }
            },
        );
        assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
        assert!(exit.close_error().is_none());
        assert_eq!(
            creation_counts::snapshot(),
            before,
            "the nested invocation created no new context, coordinator or container"
        );
    }

    /// 两层嵌套边界、每层两项声明输出（D04／D07）。
    struct NestedInnerConfig {
        inner_a: RefId,
        inner_b: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
    }

    async fn nested_inner_body(
        ctx: &mut ExecutionContext,
        inner: &crate::core::identity::ScopeId,
        config: NestedInnerConfig,
    ) -> Result<(), BodyError> {
        assert_eq!(
            ctx.frame_depth(),
            3,
            "inner frame sits inside the child frame"
        );
        let gate = config.gate.clone();
        gate.wait().await;
        ctx.register_owned(inner, &config.inner_a, tracked_num(1, &config.drops))?;
        ctx.register_owned(inner, &config.inner_b, Other(2))?;
        Ok(())
    }

    struct NestedChildConfig {
        inner_a: RefId,
        inner_b: RefId,
        child_a: RefId,
        child_b: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
    }

    async fn nested_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: NestedChildConfig,
    ) -> Result<(), BodyError> {
        assert_eq!(ctx.frame_depth(), 2);
        // 内层边界：两项声明输出 Export 到 child。
        let mut inner_outputs = vec![
            ExportSlot::new::<Tracked>(&config.inner_a, &config.child_a),
            ExportSlot::new::<Other>(&config.inner_b, &config.child_b),
        ];
        let inner_declared = vec![config.inner_a.clone(), config.inner_b.clone()];
        invoke_boundary(
            ctx,
            &[],
            &inner_declared,
            &mut inner_outputs,
            NestedInnerConfig {
                inner_a: config.inner_a.clone(),
                inner_b: config.inner_b.clone(),
                gate: config.gate.clone(),
                drops: Arc::clone(&config.drops),
            },
            nested_inner_body,
        )
        .await?;
        assert_eq!(
            ctx.frame_depth(),
            2,
            "inner boundary restored the child frame"
        );
        assert_eq!(ctx.resolve::<Tracked>(child, &config.child_a)?.num, 1);
        assert_eq!(ctx.resolve::<Other>(child, &config.child_b)?.0, 2);
        // child 的两项声明输出由本边界（caller 侧）整组 Export 到 Root。
        Ok(())
    }

    #[test]
    fn d04_d07_nested_boundaries_export_two_outputs_per_level() {
        struct Config {
            root: crate::core::identity::ScopeId,
            inner_a: RefId,
            inner_b: RefId,
            child_a: RefId,
            child_b: RefId,
            root_a: RefId,
            root_b: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs = vec![
                ExportSlot::new::<Tracked>(&config.child_a, &config.root_a),
                ExportSlot::new::<Other>(&config.child_b, &config.root_b),
            ];
            let declared = vec![config.child_a.clone(), config.child_b.clone()];
            let child = invoke_boundary(
                ctx,
                &[],
                &declared,
                &mut outputs,
                NestedChildConfig {
                    inner_a: config.inner_a.clone(),
                    inner_b: config.inner_b.clone(),
                    child_a: config.child_a.clone(),
                    child_b: config.child_b.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                },
                nested_child_body,
            )
            .await?;
            let _ = child;
            assert_eq!(ctx.resolve::<Tracked>(&root, &config.root_a)?.num, 1);
            assert_eq!(ctx.resolve::<Other>(&root, &config.root_b)?.0, 2);
            Ok(())
        }

        let f = Fixture::new();
        let root = f.root();
        let gate = Gate::closed();
        let config = Config {
            root: root.clone(),
            inner_a: f.ref_id(),
            inner_b: f.ref_id(),
            child_a: f.ref_id(),
            child_b: f.ref_id(),
            root_a: f.ref_id(),
            root_b: f.ref_id(),
            gate: gate.clone(),
            drops: Arc::clone(&f.drops),
        };
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |pending| {
            if pending >= 1 {
                gate.release();
            }
        });
        assert!(
            exit.terminated().is_none(),
            "terminated={:?} body={:?} scope={:?}",
            exit.terminated(),
            exit.body_error().map(|error| error.note()),
            exit.body_error()
                .and_then(|error| error.scope_error().map(|scope| scope.to_string()))
        );
        assert!(exit.close_error().is_none(), "{:?}", exit.close_error());
    }

    #[test]
    fn d07_failed_export_leaves_no_partial_caller_binding_or_transfer() {
        struct Config {
            root: crate::core::identity::ScopeId,
            first: RefId,
            second: RefId,
            out_a: RefId,
            out_b: RefId,
            gate: Gate,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let refs_before = ctx.coordinator.refs_len_probe(&root)?;
            let mut outputs = vec![
                ExportSlot::new::<Tracked>(&config.first, &config.out_a),
                ExportSlot::new::<Tracked>(&config.second, &config.out_b),
            ];
            let declared = vec![config.first.clone(), config.second.clone()];
            let failure = invoke_boundary(
                ctx,
                &[],
                &declared,
                &mut outputs,
                ChildTwo {
                    first: config.first.clone(),
                    second: config.second.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                two_output_child,
            )
            .await
            .expect_err("second output declares the wrong type");
            assert!(failure.scope_error().is_some());
            let child = config.slot.take().expect("child scope recorded");
            assert_eq!(ctx.state(&child)?, ScopeState::Closed);
            // caller 两个输出位置都未绑定，也没有任何责任被部分转移。
            assert_eq!(ctx.coordinator.refs_len_probe(&root)?, refs_before);
            let _ = &failure;
            Ok(())
        }

        let f = Fixture::new();
        let root = f.root();
        let gate = Gate::closed();
        let config = Config {
            root: root.clone(),
            first: f.ref_id(),
            second: f.ref_id(),
            out_a: f.ref_id(),
            out_b: f.ref_id(),
            gate: gate.clone(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |pending| {
            if pending >= 1 {
                gate.release();
            }
        });
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert!(
            exit.cleanup_events() >= 2,
            "child guard and root guard both cleaned"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the child's counted temporary drops exactly once (Other has no drop probe)"
        );
    }

    struct ChildTwo {
        first: RefId,
        second: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn two_output_child(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ChildTwo,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        let gate = config.gate.clone();
        gate.wait().await;
        ctx.register_owned(child, &config.first, tracked_num(1, &config.drops))?;
        ctx.register_owned(child, &config.second, Other(2))?;
        Ok(())
    }

    // ---- R12：更多补齐证据（D08／D09、D15～D18、D19、D20） ----

    struct TwoItemConfig {
        first_out: RefId,
        second_out: RefId,
        state_local: RefId,
        state_round: RefId,
        expose_a: RefId,
        expose_b: RefId,
        vec_out: RefId,
        first_id: Rc<Cell<Option<crate::core::identity::DataId>>>,
        second_id: Rc<Cell<Option<crate::core::identity::DataId>>>,
        replaced_id: Rc<Cell<Option<crate::core::identity::DataId>>>,
        drops: Arc<AtomicUsize>,
    }

    async fn two_item_body(
        ctx: &mut ExecutionContext,
        controller: &crate::core::identity::ScopeId,
        config: TwoItemConfig,
    ) -> Result<(), BodyError> {
        let collector = ctx.begin_collector::<Tracked>(controller)?;

        // 两项合法 owned 输出顺序 Consume；旧 DataId 立即失效。
        let first_item = ctx.create_child(controller)?;
        let first = ctx.register_owned(
            &first_item,
            &config.first_out,
            tracked_num(1, &config.drops),
        )?;
        ctx.consume_item(&first_item, &config.first_out, &collector)?;
        assert_eq!(ctx.state(&first_item)?, ScopeState::Closed);
        assert!(
            !ctx.coordinator.alive_probe(&first),
            "consumed identity is invalid"
        );

        let second_item = ctx.create_child(controller)?;
        let second = ctx.register_owned(
            &second_item,
            &config.second_out,
            tracked_num(2, &config.drops),
        )?;
        ctx.consume_item(&second_item, &config.second_out, &collector)?;
        assert_eq!(ctx.state(&second_item)?, ScopeState::Closed);
        assert!(!ctx.coordinator.alive_probe(&second));
        assert!(!ctx.coordinator.alive_probe(&first));
        assert_eq!(
            ctx.coordinator.refs_len_probe(controller)?,
            1,
            "no per-item parent Ref binding"
        );
        config.first_id.set(Some(first));
        config.second_id.set(Some(second));

        // 两个不同的 controller-owned 状态：第二次替换形成 pending 旧值并受控回收。
        let state = ctx.register_state::<Tracked>(controller, &config.state_local)?;
        let round1 = ctx.create_child(controller)?;
        let replaced =
            ctx.register_owned(&round1, &config.state_round, tracked_num(3, &config.drops))?;
        ctx.promote(&round1, &config.state_round, &state)?;
        config.replaced_id.set(Some(replaced.clone()));

        let round2 = ctx.create_child(controller)?;
        ctx.register_owned(&round2, &config.state_round, tracked_num(4, &config.drops))?;
        ctx.promote(&round2, &config.state_round, &state)?;
        ctx.recycle_pending(controller)?;
        assert!(
            !ctx.coordinator.alive_probe(&replaced),
            "the replaced owned state is recycled exactly once"
        );
        assert_eq!(config.drops.load(Ordering::SeqCst), 1);

        // same-DataId：来源原样输出当前状态 target，不销毁、不新增 owner。
        let round3 = ctx.create_child(controller)?;
        ctx.import_batch_with_states(
            &round3,
            controller,
            &[],
            &[
                StateImportSlot::new::<Tracked>(&state, &config.expose_a),
                StateImportSlot::new::<Tracked>(&state, &config.expose_b),
            ],
        )?;
        ctx.promote(&round3, &config.expose_b, &state)?;
        assert_eq!(config.drops.load(Ordering::SeqCst), 1);

        // 完成后 Vec 绑定控制器输出位置，顺序为收集顺序。
        ctx.finish_collector(controller, &collector, &config.vec_out)?;
        let collected = ctx.resolve::<Vec<Tracked>>(controller, &config.vec_out)?;
        assert_eq!(
            collected.iter().map(|item| item.num).collect::<Vec<_>>(),
            vec![1, 2]
        );
        Ok(())
    }

    #[test]
    fn d08_d09_two_items_and_replaced_owned_state() {
        struct Config {
            root: crate::core::identity::ScopeId,
            rules: RefId,
            child_rules: RefId,
            first_out: RefId,
            second_out: RefId,
            state_round: RefId,
            expose_a: RefId,
            expose_b: RefId,
            vec_out: RefId,
            root_export: RefId,
            drops: Arc<AtomicUsize>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let mut outputs = vec![ExportSlot::new::<Vec<Tracked>>(
                &config.vec_out,
                &config.root_export,
            )];
            let declared = vec![config.vec_out.clone()];
            let imports = [ImportSlot::new::<Tracked>(
                &config.rules,
                &config.child_rules,
            )];
            let controller = invoke_boundary(
                ctx,
                &imports,
                &declared,
                &mut outputs,
                TwoItemConfig {
                    first_out: config.first_out.clone(),
                    second_out: config.second_out.clone(),
                    state_local: config.child_rules.clone(),
                    state_round: config.state_round.clone(),
                    expose_a: config.expose_a.clone(),
                    expose_b: config.expose_b.clone(),
                    vec_out: config.vec_out.clone(),
                    first_id: Rc::new(Cell::new(None)),
                    second_id: Rc::new(Cell::new(None)),
                    replaced_id: Rc::new(Cell::new(None)),
                    drops: Arc::clone(&config.drops),
                },
                two_item_body,
            )
            .await?;
            assert_eq!(ctx.state(&controller)?, ScopeState::Closed);
            assert_eq!(
                ctx.resolve::<Vec<Tracked>>(&root, &config.root_export)?
                    .iter()
                    .map(|item| item.num)
                    .collect::<Vec<_>>(),
                vec![1, 2]
            );
            Ok(())
        }

        let mut f = Fixture::new();
        let root = f.root();
        let (rules, _) = f.root_input(f.tracked(11));
        let config = Config {
            root: root.clone(),
            rules: rules.clone(),
            child_rules: f.ref_id(),
            first_out: f.ref_id(),
            second_out: f.ref_id(),
            state_round: f.ref_id(),
            expose_a: f.ref_id(),
            expose_b: f.ref_id(),
            vec_out: f.ref_id(),
            root_export: f.ref_id(),
            drops: Arc::clone(&f.drops),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
        assert!(exit.close_error().is_none(), "{:?}", exit.close_error());
        assert_eq!(
            drops.load(Ordering::SeqCst),
            5,
            "two collected elements, replaced state, current state and root input"
        );
        let _ = root;
    }

    // ---- D15／D16／D18：析构先后关系的事件证据 ----

    struct OrderingConfig {
        temp: RefId,
        leaf_out: RefId,
        input: RefId,
        root_input: RefId,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    struct OrderingChild {
        temp: RefId,
        leaf_out: RefId,
        input: RefId,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn ordering_leaf(_input: &Tracked, _config: Probe) -> Tracked {
        let _probe = Probe("inner-future-drop");
        std::future::poll_fn(|_| Poll::<()>::Pending).await;
        unreachable!()
    }

    async fn ordering_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: OrderingChild,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(7, &config.drops))?;
        // 叶子持有输入借用并挂起：内层 Future 在取消时先析构。
        let _ = invoke_leaf(
            ctx,
            child,
            &config.input,
            &config.leaf_out,
            Probe("leaf-start"),
            ordering_leaf,
        )
        .await;
        Ok(())
    }

    async fn ordering_outer(
        ctx: &mut ExecutionContext,
        config: OrderingConfig,
    ) -> Result<(), BodyError> {
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let imports = [ImportSlot::new::<Tracked>(
            &config.input,
            &config.root_input,
        )];
        let future = invoke_boundary(
            ctx,
            &imports,
            &[],
            &mut outputs,
            OrderingChild {
                temp: config.temp.clone(),
                leaf_out: config.leaf_out.clone(),
                input: config.root_input.clone(),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&config.slot),
            },
            ordering_child_body,
        );
        let boxed = advance_to_pending(future, 1);
        drop(boxed);
        Ok(())
    }

    #[test]
    fn d15_d16_d18_destruction_order_is_observed() {
        // 清空本线程此前的事件，避免跨测试残留。
        let _ = creation_counts::take_events();
        let mut f = Fixture::new();
        let (root_input, _) = f.root_input(f.tracked(9));
        let config = OrderingConfig {
            temp: f.ref_id(),
            leaf_out: f.ref_id(),
            input: root_input.clone(),
            root_input: f.ref_id(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        // body 内部把子调用推进到 Pending 后丢弃本体，再返回 Ok；root 收口因此走受控
        // 失败退出，Context 在 run_root 返回时析构。
        let exit = drive_to_ready(run_root(execution, config, ordering_outer), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        creation_counts::record_event("after-context-drop");

        let events = creation_counts::take_events();
        let inner = events.iter().position(|e| e == "inner-future-drop");
        let guard_child = events.iter().position(|e| e.starts_with("guard-cleanup:"));
        let value = events.iter().position(|e| e == "value:7");
        let teardown = events.iter().position(|e| e == "after-context-drop");
        assert!(
            inner.is_some() && guard_child.is_some() && value.is_some() && teardown.is_some(),
            "events={events:?}"
        );
        assert!(
            inner < guard_child,
            "inner future drops before the guard cleans: {events:?}"
        );
        assert!(
            value < teardown,
            "data drops before the context teardown: {events:?}"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "child temporary and root input"
        );
    }

    #[test]
    fn d19_real_collector_is_untouched_by_failed_cleanup() {
        struct Config {
            root: crate::core::identity::ScopeId,
            other_item_out: RefId,
            other_collector: Rc<Cell<Option<crate::core::identity::CollectorId>>>,
            temp: RefId,
            broken: RefId,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            // 另一份真实 collector：在兄弟边界建立并收集一项。
            let other_controller = ctx.create_child(&config.root)?;
            let collector = ctx.begin_collector::<Tracked>(&other_controller)?;
            let item = ctx.create_child(&other_controller)?;
            ctx.register_owned(&item, &config.other_item_out, tracked_num(5, &config.drops))?;
            ctx.consume_item(&item, &config.other_item_out, &collector)?;
            config.other_collector.set(Some(collector.clone()));

            let mut outputs: Vec<ExportSlot> = Vec::new();
            {
                let future = invoke_boundary(
                    ctx,
                    &[],
                    &[],
                    &mut outputs,
                    BrokenChild {
                        temp: config.temp.clone(),
                        broken: config.broken.clone(),
                        drops: Arc::clone(&config.drops),
                        slot: Rc::clone(&config.slot),
                    },
                    broken_child_body,
                );
                let boxed = advance_to_pending(future, 1);
                drop(boxed);
            }

            assert_eq!(
                ctx.termination().unwrap().kind(),
                TerminationKind::Cancelled
            );
            let cleanup = ctx.cleanup_failure().expect("cleanup failure recorded");
            assert!(ctx.cleanup_report().is_some());
            assert!(
                ctx.cleanup_failure().is_some(),
                "reporting must not erase the cleanup diagnostic"
            );
            let faulty = config.slot.take().expect("child recorded");
            assert_eq!(cleanup.scope(), &faulty);
            assert_ne!(ctx.state(&faulty)?, ScopeState::Closed);
            // 另一份 collector 的身份、内容与责任都不受影响。
            assert_eq!(
                ctx.coordinator.collector_len(&collector).unwrap(),
                1,
                "the sibling collector keeps its partial result"
            );
            assert_eq!(
                ctx.state(&other_controller)?,
                ScopeState::Active,
                "the sibling controller is untouched"
            );
            assert!(matches!(
                ctx.register_owned(&config.root, &config.broken, tracked_num(9, &config.drops)),
                Err(ScopeError::Terminated { .. })
            ));
            Ok(())
        }

        let f = Fixture::new();
        let root = f.root();
        let config = Config {
            root: root.clone(),
            other_item_out: f.ref_id(),
            other_collector: Rc::new(Cell::new(None)),
            temp: f.ref_id(),
            broken: f.ref_id(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        assert!(
            exit.cleanup_scope().is_some(),
            "cleanup failure is preserved in the exit"
        );
    }

    struct BrokenChild {
        temp: RefId,
        broken: RefId,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn broken_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: BrokenChild,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        let broken = ctx.register_owned(child, &config.broken, tracked_num(1, &config.drops))?;
        ctx.register_owned(child, &config.temp, tracked_num(2, &config.drops))?;
        ctx.coordinator.destroy_probe(&broken);
        std::future::poll_fn(|_| Poll::<()>::Pending).await;
        unreachable!()
    }

    /// D07：caller 输出位置冲突，且失败时 owner 没有部分转移。
    struct D07ConflictChild {
        declared: RefId,
        id_slot: Rc<Cell<Option<crate::core::identity::DataId>>>,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn conflict_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: D07ConflictChild,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        let id = ctx.register_owned(child, &config.declared, tracked_num(3, &config.drops))?;
        // 出口尚未提交：本值仍由 child 唯一负责（没有部分转移）。
        assert_eq!(observe_owner(ctx, &id)?, child.clone());
        assert!(ctx.coordinator.alive_probe(&id));
        config.id_slot.set(Some(id));
        Ok(())
    }

    #[test]
    fn d07_caller_position_conflict_leaves_no_partial_transfer() {
        struct Config {
            root: crate::core::identity::ScopeId,
            caller_taken: RefId,
            child_declared: RefId,
            drops: Arc<AtomicUsize>,
            id_slot: Rc<Cell<Option<crate::core::identity::DataId>>>,
            scope_slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            // 预先占用 caller 输出位置，制造真实绑定冲突。
            ctx.register_owned(&root, &config.caller_taken, tracked_num(1, &config.drops))?;
            let refs_before = ctx.coordinator.refs_len_probe(&root)?;

            let mut outputs = vec![ExportSlot::new::<Tracked>(
                &config.child_declared,
                &config.caller_taken,
            )];
            let declared = vec![config.child_declared.clone()];
            let failure = invoke_boundary(
                ctx,
                &[],
                &declared,
                &mut outputs,
                D07ConflictChild {
                    declared: config.child_declared.clone(),
                    id_slot: Rc::clone(&config.id_slot),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.scope_slot),
                },
                conflict_child_body,
            )
            .await
            .expect_err("caller output position is already bound");
            assert!(
                matches!(
                    failure.scope_error(),
                    Some(ScopeError::RefAlreadyBound { .. })
                ),
                "got {failure:?}"
            );
            assert_eq!(
                ctx.coordinator.refs_len_probe(&root)?,
                refs_before,
                "no caller binding was added and none was removed"
            );
            let child = config.scope_slot.take().expect("child recorded");
            assert_eq!(ctx.state(&child)?, ScopeState::Closed);
            let value = config.id_slot.take().expect("child value recorded");
            assert!(
                !ctx.coordinator.alive_probe(&value),
                "the rejected child value was cleaned with its scope"
            );
            Ok(())
        }

        let f = Fixture::new();
        let root = f.root();
        let config = Config {
            root: root.clone(),
            caller_taken: f.ref_id(),
            child_declared: f.ref_id(),
            drops: Arc::clone(&f.drops),
            id_slot: Rc::new(Cell::new(None)),
            scope_slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "the rejected child value and the pre-bound caller value drop once each"
        );
    }

    /// D15／D16／D18：借用见证 + 清理／frame／存储析构的准确事件序列。
    struct BorrowWitness<'a>(&'a Tracked);

    impl Drop for BorrowWitness<'_> {
        fn drop(&mut self) {
            // 在析构见证里读取输入：证明借用结束时数据仍存活、清理尚未销毁它。
            let observed = self.0.num;
            creation_counts::record_event(&format!("borrow-witness-drop:{observed}"));
        }
    }

    async fn witness_leaf(input: &Tracked, _config: LeafConfig) -> Tracked {
        let witness = BorrowWitness(input);
        let probe = Probe("inner-future-drop");
        std::future::poll_fn(|_| Poll::<()>::Pending).await;
        let _ = (&witness, &probe);
        unreachable!()
    }

    async fn witness_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: OrderingChild,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(7, &config.drops))?;
        let _ = invoke_leaf(
            ctx,
            child,
            &config.input,
            &config.leaf_out,
            LeafConfig {
                gate: Gate::closed(),
            },
            witness_leaf,
        )
        .await;
        Ok(())
    }

    async fn witness_outer(
        ctx: &mut ExecutionContext,
        config: OrderingConfig,
    ) -> Result<(), BodyError> {
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let imports = [ImportSlot::new::<Tracked>(
            &config.input,
            &config.root_input,
        )];
        {
            let future = invoke_boundary(
                ctx,
                &imports,
                &[],
                &mut outputs,
                OrderingChild {
                    temp: config.temp.clone(),
                    leaf_out: config.leaf_out.clone(),
                    input: config.root_input.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                witness_child_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }
        Ok(())
    }

    #[test]
    fn d15_d16_d18_full_destruction_sequence_is_observed() {
        let _ = creation_counts::take_events();
        let mut f = Fixture::new();
        let (root_input, _) = f.root_input(f.tracked(9));
        let config = OrderingConfig {
            temp: f.ref_id(),
            leaf_out: f.ref_id(),
            input: root_input.clone(),
            root_input: f.ref_id(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, witness_outer), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));

        let events = creation_counts::take_events();
        let pos = |needle: &str| {
            events
                .iter()
                .position(|event| event == needle)
                .unwrap_or_else(|| panic!("event {needle} missing in {events:?}"))
        };
        let inner = pos("inner-future-drop");
        let witness = pos("borrow-witness-drop:9");
        let leaf_start = pos("cleanup-start:none");
        let leaf_end = pos("cleanup-end:none");
        let leaf_frame = pos("frame-exit:leaf:1");
        let child_start = pos("cleanup-start:1");
        let child_end = pos("cleanup-end:1");
        let child_frame = pos("frame-exit:boundary:1");
        let root_start = pos("cleanup-start:0");
        let root_end = pos("cleanup-end:0");
        let root_frame = pos("frame-exit:root:0");
        let child_value = pos("value:7");
        let root_value = pos("value:9");
        let context_drop = pos("context-drop");
        let container_drop = pos("container-drop");

        assert!(
            inner < witness,
            "inner future locals drop before the borrow witness: {events:?}"
        );
        assert!(
            witness < leaf_start,
            "the input borrow ends before any cleanup begins: {events:?}"
        );
        assert!(
            leaf_end < child_start,
            "the leaf frame finishes before the boundary guard cleans: {events:?}"
        );
        assert!(
            leaf_frame < child_start,
            "the leaf frame exits before the boundary cleanup starts: {events:?}"
        );
        assert!(
            child_value > child_start && child_value < child_end,
            "the child temporary is destroyed inside the boundary guard cleanup: {events:?}"
        );
        assert!(
            child_end < child_frame,
            "the boundary frame exits only after its cleanup finished: {events:?}"
        );
        assert!(
            root_value > root_start && root_value < root_end,
            "the root input is destroyed inside the root guard cleanup: {events:?}"
        );
        assert!(
            root_end < root_frame,
            "the root frame exits after its cleanup"
        );
        assert!(
            child_frame < root_frame,
            "the inner frame exits before the root frame"
        );
        assert!(
            root_frame < context_drop,
            "the root frame exits before the context drop"
        );
        assert!(
            context_drop < container_drop,
            "storage drops with the context fields"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "child temporary and root input"
        );
    }

    // ---- 第三次复审：R10／R11 与 R12 剩余证据 ----

    struct R10CancelConfig {
        input: RefId,
        child_input: RefId,
        leaf_out: RefId,
        drops: Arc<AtomicUsize>,
        scope_seen: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    struct R10Child {
        input: RefId,
        leaf_out: RefId,
        drops: Arc<AtomicUsize>,
        scope_seen: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }

    async fn r10_holding_leaf(_input: &Tracked, _config: LeafConfig) -> Tracked {
        let _probe = Probe("r10-inner-future-drop");
        std::future::poll_fn(|_| Poll::<()>::Pending).await;
        unreachable!()
    }

    async fn r10_child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: R10Child,
    ) -> Result<(), BodyError> {
        config.scope_seen.set(Some(child.clone()));
        // leaf 沿用 child 的 Scope 且不承担其销毁责任：取消定位必须来自 frame。
        let _ = invoke_leaf(
            ctx,
            child,
            &config.input,
            &config.leaf_out,
            LeafConfig {
                gate: Gate::closed(),
            },
            r10_holding_leaf,
        )
        .await;
        let _ = &config.drops;
        Ok(())
    }

    async fn r10_outer(
        ctx: &mut ExecutionContext,
        config: R10CancelConfig,
    ) -> Result<(), BodyError> {
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let imports = [ImportSlot::new::<Tracked>(
            &config.input,
            &config.child_input,
        )];
        {
            let future = invoke_boundary(
                ctx,
                &imports,
                &[],
                &mut outputs,
                R10Child {
                    input: config.child_input.clone(),
                    leaf_out: config.leaf_out.clone(),
                    drops: Arc::clone(&config.drops),
                    scope_seen: Rc::clone(&config.scope_seen),
                },
                r10_child_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }
        Ok(())
    }

    #[test]
    fn r10_leaf_cancellation_keeps_the_frame_scope_location() {
        let mut f = Fixture::new();
        let (input, _) = f.root_input(f.tracked(1));
        let config = R10CancelConfig {
            input: input.clone(),
            child_input: f.ref_id(),
            leaf_out: f.ref_id(),
            drops: Arc::clone(&f.drops),
            scope_seen: Rc::new(Cell::new(None)),
        };
        let scope_seen = Rc::clone(&config.scope_seen);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, r10_outer), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
        assert_eq!(exit.termination_note(), Some("pending future dropped"));
        assert_eq!(
            exit.termination_scope().cloned(),
            scope_seen.take(),
            "the cancellation location is the frame scope actually used, not the (empty) responsibility set"
        );
        assert!(
            exit.termination_scope_error().is_none(),
            "cancellation carries no scope error of its own"
        );
    }

    #[test]
    fn r11_finalizing_scope_cannot_be_entered_and_frame_depth_is_unchanged() {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let context = execution.context_mut();
        let guard = context.enter(InvocationKind::Root, &root, true).unwrap();
        let mut guard = guard;
        let child = guard.create_child(&root).unwrap();
        guard.complete();
        let depth_before = context.frame_depth();

        context.coordinator.set_finalizing_probe(&child).unwrap();
        assert!(matches!(
            context.enter(InvocationKind::Boundary, &child, true),
            Err(ScopeError::ScopeNotActive {
                state: ScopeState::Finalizing,
                ..
            })
        ));
        assert_eq!(context.frame_depth(), depth_before, "no frame was pushed");
        assert!(matches!(
            context.enter(InvocationKind::Leaf, &root, false),
            Err(ScopeError::Invariant { .. })
        ));
        assert_eq!(context.frame_depth(), depth_before);
    }

    #[test]
    fn r11_broken_frame_relation_is_recorded_not_silently_truncated() {
        struct Config {
            temp: RefId,
            drops: Arc<AtomicUsize>,
            slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let child = ctx.create_child(&ctx.root_scope())?;
            config.slot.set(Some(child.clone()));
            ctx.register_owned(&child, &config.temp, tracked_num(1, &config.drops))?;
            // test-only 元数据故障：把当前 Boundary frame 的 parent 破坏成 None。
            let guard = ctx.enter(InvocationKind::Boundary, &child, true).unwrap();
            guard.context.frames[1].parent = None;
            drop(guard);

            // 异常关系：记录 Invariant 并保留现场（不清理、不 truncate）。
            assert_eq!(
                ctx.frame_depth(),
                2,
                "the broken frame is preserved for diagnosis"
            );
            assert_eq!(
                ctx.termination().map(|termination| termination.kind()),
                Some(TerminationKind::BodyError)
            );
            assert_eq!(
                ctx.termination().map(|termination| termination.note()),
                Some("a nested invocation frame must point at its immediate caller frame")
            );
            assert_ne!(
                ctx.state(&child)?,
                ScopeState::Closed,
                "an abnormal frame relation does not clean its scope as if it were a normal cancel"
            );
            let _ = ctx.abort(&child);
            Ok(())
        }

        let f = Fixture::new();
        let config = Config {
            temp: f.ref_id(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let _exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the child temporary drops exactly once"
        );
    }

    /// D07：使用真实 finalize 入口验证后项失败时首项没有部分转移。
    #[test]
    fn d07_finalize_failure_leaves_no_partial_owner_transfer() {
        struct Config {
            root: crate::core::identity::ScopeId,
            child_first: RefId,
            child_second: RefId,
            caller_out_a: RefId,
            caller_out_b: RefId,
            drops: Arc<AtomicUsize>,
        }
        async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
            let root = config.root.clone();
            let child = ctx.create_child(&root)?;
            let first =
                ctx.register_owned(&child, &config.child_first, tracked_num(1, &config.drops))?;
            let second = ctx.register_owned(&child, &config.child_second, Other(2))?;
            // 提交前：两项都由 child 唯一负责，caller 未拥有它们。
            assert_eq!(observe_owner(ctx, &first)?, child.clone());
            assert_eq!(observe_owner(ctx, &second)?, child.clone());
            let caller_refs = ctx.coordinator.refs_len_probe(&root)?;
            let caller_owned = ctx.coordinator.owned_len_probe(&root)?;

            // 后项声明类型不符：整组拒绝。
            let declared = vec![config.child_first.clone(), config.child_second.clone()];
            let mut outputs = vec![
                ExportSlot::new::<Tracked>(&config.child_first, &config.caller_out_a),
                ExportSlot::new::<Tracked>(&config.child_second, &config.caller_out_b),
            ];
            let error = ctx.finalize(&child, &declared, &mut outputs).unwrap_err();
            assert!(
                matches!(error, ScopeError::TypeMismatch { .. }),
                "got {error:?}"
            );

            // caller 的 refs／owned 都没有部分改变；两项都没有被转移，随 child 清理析构。
            assert_eq!(ctx.coordinator.refs_len_probe(&root)?, caller_refs);
            assert_eq!(ctx.coordinator.owned_len_probe(&root)?, caller_owned);
            assert!(
                ctx.coordinator.owner_probe(&first).is_err(),
                "no transfer to the caller"
            );
            assert!(!ctx.coordinator.alive_probe(&first));
            assert!(!ctx.coordinator.alive_probe(&second));
            assert_eq!(ctx.state(&child)?, ScopeState::Closed);
            // 真实边界会把该失败传播出去，由受控退出收口。
            Err(error.into())
        }

        let f = Fixture::new();
        let root = f.root();
        let config = Config {
            root: root.clone(),
            child_first: f.ref_id(),
            child_second: f.ref_id(),
            caller_out_a: f.ref_id(),
            caller_out_b: f.ref_id(),
            drops: Arc::clone(&f.drops),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "only the counted temporary"
        );
        let _ = root;
    }

    // ---- R12 剩余：D16 两层 Boundary 取消、D18 Root 本体 drop ----

    #[derive(Default)]
    struct D16Slots {
        inner: Cell<Option<crate::core::identity::ScopeId>>,
        outer: Cell<Option<crate::core::identity::ScopeId>>,
    }

    struct D16Inner {
        temp: RefId,
        input: RefId,
        leaf_out: RefId,
        drops: Arc<AtomicUsize>,
        slots: Rc<D16Slots>,
    }

    async fn d16_inner_body(
        ctx: &mut ExecutionContext,
        inner: &crate::core::identity::ScopeId,
        config: D16Inner,
    ) -> Result<(), BodyError> {
        config.slots.inner.set(Some(inner.clone()));
        ctx.register_owned(inner, &config.temp, tracked_num(7, &config.drops))?;
        let _ = invoke_leaf(
            ctx,
            inner,
            &config.input,
            &config.leaf_out,
            LeafConfig {
                gate: Gate::closed(),
            },
            witness_leaf,
        )
        .await;
        Ok(())
    }

    struct D16Outer {
        outer_temp: RefId,
        import_source: RefId,
        inner: D16Inner,
        drops: Arc<AtomicUsize>,
        slots: Rc<D16Slots>,
    }

    async fn d16_outer_body(
        ctx: &mut ExecutionContext,
        outer: &crate::core::identity::ScopeId,
        config: D16Outer,
    ) -> Result<(), BodyError> {
        config.slots.outer.set(Some(outer.clone()));
        ctx.register_owned(outer, &config.outer_temp, tracked_num(5, &config.drops))?;
        let inner_input = config.inner.input.clone();
        let mut inner_outputs: Vec<ExportSlot> = Vec::new();
        let _ = invoke_boundary(
            ctx,
            &[ImportSlot::new::<Tracked>(
                &config.import_source,
                &inner_input,
            )],
            &[],
            &mut inner_outputs,
            config.inner,
            d16_inner_body,
        )
        .await;
        Ok(())
    }

    struct D16Root {
        outer_temp: RefId,
        input: RefId,
        child_input: RefId,
        inner_input: RefId,
        inner_temp: RefId,
        leaf_out: RefId,
        slots: Rc<D16Slots>,
        drops: Arc<AtomicUsize>,
    }

    async fn d16_root_body(ctx: &mut ExecutionContext, config: D16Root) -> Result<(), BodyError> {
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let imports = [ImportSlot::new::<Tracked>(
            &config.input,
            &config.child_input,
        )];
        {
            let future = invoke_boundary(
                ctx,
                &imports,
                &[],
                &mut outputs,
                D16Outer {
                    outer_temp: config.outer_temp.clone(),
                    import_source: config.child_input.clone(),
                    inner: D16Inner {
                        temp: config.inner_temp.clone(),
                        input: config.inner_input.clone(),
                        leaf_out: config.leaf_out.clone(),
                        drops: Arc::clone(&config.drops),
                        slots: Rc::clone(&config.slots),
                    },
                    drops: Arc::clone(&config.drops),
                    slots: Rc::clone(&config.slots),
                },
                d16_outer_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }
        Ok(())
    }

    #[test]
    fn d16_two_level_boundary_cancel_orders_and_cleans_every_level() {
        let _ = creation_counts::take_events();
        let mut f = Fixture::new();
        let (input, _) = f.root_input(f.tracked(9));
        let slots = Rc::new(D16Slots {
            inner: Cell::new(None),
            outer: Cell::new(None),
        });
        let config = D16Root {
            outer_temp: f.ref_id(),
            input: input.clone(),
            child_input: f.ref_id(),
            inner_input: f.ref_id(),
            inner_temp: f.ref_id(),
            leaf_out: f.ref_id(),
            slots: Rc::clone(&slots),
            drops: Arc::clone(&f.drops),
        };
        let drops = Arc::clone(&f.drops);
        let execution = f.take_execution();
        let exit = drive_to_ready(run_root(execution, config, d16_root_body), |_| {});
        assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));

        let inner = slots.inner.take().expect("inner scope recorded");
        let outer = slots.outer.take().expect("outer scope recorded");
        let events = creation_counts::take_events();
        let pos = |needle: &str| {
            events
                .iter()
                .position(|event| event == needle)
                .unwrap_or_else(|| panic!("event {needle} missing in {events:?}"))
        };
        let witness = pos("borrow-witness-drop:9");
        let inner_cleanup = pos(&format!("cleanup-start:{}", inner.seq()));
        let inner_frame = pos(&format!("frame-exit:boundary:{}", inner.seq()));
        let outer_cleanup = pos(&format!("cleanup-start:{}", outer.seq()));
        let outer_frame = pos(&format!("frame-exit:boundary:{}", outer.seq()));
        let context_drop = pos("context-drop");
        assert!(
            witness < inner_cleanup,
            "the input borrow ends before the deepest cleanup: {events:?}"
        );
        assert!(
            inner_cleanup < inner_frame,
            "inner cleanup precedes its frame exit: {events:?}"
        );
        assert!(
            inner_frame < outer_cleanup,
            "the inner level is fully exited before the outer cleanup: {events:?}"
        );
        assert!(
            outer_cleanup < outer_frame,
            "outer cleanup precedes its frame exit: {events:?}"
        );
        assert!(
            outer_frame < context_drop,
            "frames exit before the context drop"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            3,
            "outer temp, inner temp and root input, each exactly once"
        );
        let _ = (inner, outer);
    }

    async fn d18_root_body(ctx: &mut ExecutionContext, config: D16Root) -> Result<(), BodyError> {
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let imports = [ImportSlot::new::<Tracked>(
            &config.input,
            &config.child_input,
        )];
        // 直接 await：整个 Root Future 停在最深层的真实 Pending。
        let _ = invoke_boundary(
            ctx,
            &imports,
            &[],
            &mut outputs,
            D16Outer {
                outer_temp: config.outer_temp.clone(),
                import_source: config.child_input.clone(),
                inner: D16Inner {
                    temp: config.inner_temp.clone(),
                    input: config.inner_input.clone(),
                    leaf_out: config.leaf_out.clone(),
                    drops: Arc::clone(&config.drops),
                    slots: Rc::clone(&config.slots),
                },
                drops: Arc::clone(&config.drops),
                slots: Rc::clone(&config.slots),
            },
            d16_outer_body,
        )
        .await;
        Ok(())
    }

    #[test]
    fn d18_dropping_the_root_future_owner_runs_the_same_ordered_cleanup() {
        let _ = creation_counts::take_events();
        let mut f = Fixture::new();
        let (input, _) = f.root_input(f.tracked(9));
        let slots = Rc::new(D16Slots {
            inner: Cell::new(None),
            outer: Cell::new(None),
        });
        let config = D16Root {
            outer_temp: f.ref_id(),
            input: input.clone(),
            child_input: f.ref_id(),
            inner_input: f.ref_id(),
            inner_temp: f.ref_id(),
            leaf_out: f.ref_id(),
            slots: Rc::clone(&slots),
            drops: Arc::clone(&f.drops),
        };
        let drops = Arc::clone(&f.drops);
        let other = Fixture::new();
        let execution = f.take_execution();
        // 真正丢弃拥有 Context 的 Root Future 本体（尚未 Ready）。
        let boxed = advance_to_pending(run_root(execution, config, d18_root_body), 1);
        drop(boxed);

        let events = creation_counts::take_events();
        let pos = |needle: &str| {
            events
                .iter()
                .position(|event| event == needle)
                .unwrap_or_else(|| panic!("event {needle} missing in {events:?}"))
        };
        let witness = pos("borrow-witness-drop:9");
        let child_cleanup = pos(&format!(
            "cleanup-start:{}",
            slots.inner.take().expect("inner scope recorded").seq()
        ));
        let child_frame = events
            .iter()
            .position(|event| event.starts_with("frame-exit:boundary:"))
            .unwrap_or_else(|| panic!("no boundary frame exit in {events:?}"));
        let root_frame = pos("frame-exit:root:0");
        let context_drop = pos("context-drop");
        let container_drop = pos("container-drop");
        assert!(
            witness < child_cleanup,
            "borrow ends before cleanup: {events:?}"
        );
        assert!(
            child_cleanup < child_frame,
            "cleanup precedes the frame exit"
        );
        assert!(
            child_frame < root_frame,
            "child frame exits before the root frame"
        );
        assert!(
            root_frame < context_drop,
            "frames exit before the context drop"
        );
        assert!(
            context_drop < container_drop,
            "storage drops with the context fields"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            3,
            "outer temp, inner temp and root input, each exactly once"
        );
        assert_eq!(other.drops(), 0, "another execution is unaffected");
    }

    // ---- 第四次复审：R11 的 Scope／调用类别关系反例固化 ----

    #[test]
    fn r11_leaf_scope_tampering_is_rejected_and_keeps_the_site() {
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let context = execution.context_mut();
        let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
        let child = root_guard.create_child(&root).unwrap();

        // 正常建立的 Leaf guard：沿用 caller（Root）的 Scope，parent 指向 Root frame。
        {
            let leaf_guard = root_guard
                .enter(InvocationKind::Leaf, &root, false)
                .unwrap();
            // test-only 元数据故障：Scope 改成已存在的 child Scope，其余字段保持合法形态。
            leaf_guard.context.frames[1].scope = Some(child.clone());
            drop(leaf_guard);
        }

        assert_eq!(
            root_guard.context.frames.len(),
            2,
            "the broken frame is preserved for diagnosis"
        );
        let termination = root_guard
            .context
            .termination()
            .expect("relation violation recorded");
        assert_eq!(termination.kind(), TerminationKind::BodyError);
        assert_eq!(
            termination.note(),
            "a leaf invocation must reuse the caller scope",
            "the wrong Scope relation is reported explicitly"
        );
        assert!(matches!(
            termination.scope_error(),
            Some(ScopeError::Invariant { .. })
        ));
        assert_ne!(
            root_guard.state(&child).unwrap(),
            ScopeState::Closed,
            "an abnormal leaf frame must not clean the tampered scope as a normal cancel"
        );

        // 收尾：丢弃残留 frame 并正常结束 Root frame。
        root_guard.context.frames.truncate(1);
        root_guard.complete();
    }

    #[test]
    fn r11_root_and_boundary_scope_relations_are_checked() {
        // Root 分支：frame 必须使用真正的 RootScope。
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let context = execution.context_mut();
        let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
        let child = root_guard.create_child(&root).unwrap();
        root_guard.context.frames[0].scope = Some(child.clone());
        root_guard.exit_for_probe();
        assert_eq!(
            context.termination().map(|termination| termination.note()),
            Some("a root invocation frame must use the actual RootScope")
        );
        assert_eq!(
            context.frame_depth(),
            1,
            "the broken root frame is preserved"
        );
        assert!(context.coordinator.abort(&child).is_ok());

        // Boundary 分支：frame 的 Scope 必须是 caller Scope 的直接 child。
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let context = execution.context_mut();
        let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
        let child = root_guard.create_child(&root).unwrap();
        let grandchild = root_guard.create_child(&child).unwrap();
        {
            let boundary = root_guard
                .enter(InvocationKind::Boundary, &child, true)
                .unwrap();
            // 把 Scope 换成 caller 的孙 Scope：parent 字段仍合法，但关系错误。
            boundary.context.frames[1].scope = Some(grandchild.clone());
            drop(boundary);
        }
        assert_eq!(
            root_guard
                .context
                .termination()
                .map(|termination| termination.note()),
            Some("a boundary invocation scope must be a direct child of the caller scope")
        );
        assert_eq!(
            root_guard.context.frames.len(),
            2,
            "the broken boundary frame is preserved"
        );
        assert_ne!(root_guard.state(&child).unwrap(), ScopeState::Closed);
        // 当前 frame 的 Scope 已被破坏，因此收尾直接经协调组件清理两个 Scope。
        assert!(root_guard.context.coordinator.abort(&child).is_ok());
        assert!(root_guard.context.coordinator.abort(&grandchild).is_ok());
        // 收尾：丢弃残留 frame 后正常结束 Root frame。
        root_guard.context.frames.truncate(1);
        root_guard.complete();
    }
}
