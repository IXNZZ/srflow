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
    AliasSlot, ConsumeGroupOutcome, ControlStateId, ExportSlot, ImportSlot, PreparedRootExtraction,
    PromoteGroupOutcome, ScopeCoordinator, ScopeState, StateImportSlot,
};
/// 首次终止类别：执行失败或取消。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub(crate) struct ExecutionTermination {
    kind: TerminationKind,
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    scope: Option<ScopeId>,
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    note: &'static str,
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    scope_error: Option<ScopeError>,
}
impl ExecutionTermination {
    /// 终止类别。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn kind(&self) -> TerminationKind {
        self.kind
    }

    /// 相关 Scope（如有）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn scope(&self) -> Option<&ScopeId> {
        self.scope.as_ref()
    }

    /// 说明文本。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn note(&self) -> &'static str {
        self.note
    }

    /// 首次失败时的原始 Scope／执行诊断（如有），不是通用字符串。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn scope_error(&self) -> Option<&ScopeError> {
        self.scope_error.as_ref()
    }
}
/// 清理失败诊断：故障 Scope 与错误。
///
/// 与终止原因并存，彼此不覆盖；有故障的 Scope 不会被标成 Closed。
#[derive(Debug, Clone)]
pub(crate) struct CleanupDiagnostic {
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    scope: ScopeId,
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    error: ScopeError,
}
impl CleanupDiagnostic {
    /// 清理失败的 Scope。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn scope(&self) -> &ScopeId {
        &self.scope
    }

    /// 清理失败的诊断。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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
pub(crate) enum InvocationKind {
    /// 本次 Root Execution 的调用。
    Root,
    /// 建立生命周期边界的调用。
    Boundary,
    /// 沿用 caller Scope 的轻量叶子调用。
    Leaf,
}
/// 成组 Item 边界的私有收口许可：只授权"当前 Item frame 把**完整声明输出组**一次消费进
/// 其直接 parent（EachScope）的对应 collector 组"。
///
/// 与单项 Item 收口许可同形，但承载整组：`group` 是本次统一静态 Body 的完整声明
/// 输出位置组，`collectors` 与之一一对应，二者都只能由 Each 的受控会话在每个 Item 边界内
/// 构造。许可只授予"本次收口把固定声明组写入固定 parent owner 的 collector 组"，不授予
/// 任意读取、abort、owned 注入、替换其它 collector 或修改 ancestor 的能力，也不放宽
/// `require_call_scope`：它不通过泛化 ancestor 修改权修改 Item frame 访问 parent 的门禁。
///
/// 不提供任何公开／`pub` 构造器：本类型只在 crate 内可见，字段即 `pub(crate)`，构造需同时
/// 持有真实的 item／each 句柄、完整声明组位置与已登记的目的 collector。
#[derive(Debug, Clone)]
pub(crate) struct ItemGroupPermit {
    /// 本次 Item 的 Scope（当前 frame 必须等于它）。
    pub(crate) item: ScopeId,
    /// Each 调用的 Scope（collector 组 owner、Item frame 的调用父 Scope）。
    pub(crate) each: ScopeId,
    /// 本次统一静态 Body 的完整声明输出位置组。
    pub(crate) group: Vec<RefId>,
    /// 与声明组逐槽对应的 collector。
    pub(crate) collectors: Vec<CollectorId>,
}

/// 成组 Round 边界的私有收口许可：只授权"当前 Round frame 把**完整声明输出组**一次
/// 提交到固定 parent controller 的多个控制状态"（成组 Promote），或"只关闭当前 Round、
/// 不更新任何状态"（close）。
///
/// 来源是**结构性**的：`group` 只能取自本次统一静态 Body 的完整声明组，`states` 只能由
/// 受控会话在固定 controller 上登记得到；普通调用者既拿不到可变 Context，也无法凭空
/// 取得合法 `ControlStateId`（该句柄没有按字段构造器）。与前代单项许可
/// 的单 state／单输出硬编码不同，本许可承载整组，但不授予任何 ancestor 读取、abort、
/// owned 注入、替换其它 state 或扩大访问的能力，也不放宽 [`ExecutionContext`] 的
/// `require_call_scope`。
///
/// 不提供任何公开／`pub` 构造器：本类型只在 crate 内可见，字段即 `pub(crate)`，构造需
/// 同时持有真实的 controller／round 句柄、声明组位置与正式登记的控制状态。
#[derive(Clone)]
pub(crate) struct RoundGroupPermit {
    /// 本次 Round 的 controller Scope（全部目标 state 的 owner、Round frame 的调用父）。
    pub(crate) controller: ScopeId,
    /// 本次 Round 的 Scope。
    pub(crate) round: ScopeId,
    /// 本次统一静态 Body 的完整声明输出位置组。
    pub(crate) group: Vec<RefId>,
    /// 与声明组逐槽对应的目标控制状态；"只关闭"路径为空。
    pub(crate) states: Vec<ControlStateId>,
}

/// 一个调用 frame 的元数据。
#[derive(Debug)]
pub(crate) struct InvocationFrame {
    parent: Option<usize>,
    kind: InvocationKind,
    scope: Option<ScopeId>,
    owns_scope: bool,
}
impl InvocationFrame {
    /// 本 frame 的调用类别。
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
pub(crate) struct ExecutionContext {
    coordinator: ScopeCoordinator,
    frames: Vec<InvocationFrame>,
    termination: Option<ExecutionTermination>,
    cleanup_failure: Option<CleanupDiagnostic>,
    #[cfg(test)]
    cleanup_events: usize,
}

impl ExecutionContext {
    /// 以给定身份根建立 Context 与内部协调组件（唯一 DataContainer）。
    ///
    /// 只由 `runtime` 的 Root 设施在创建一次 Root Execution 时调用。
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn current_frame(&self) -> Option<&InvocationFrame> {
        self.frames.last()
    }

    /// 是否已经终止（首次执行失败或取消）。
    pub(crate) fn is_terminated(&self) -> bool {
        self.termination.is_some()
    }

    /// 首次终止原因。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn termination(&self) -> Option<&ExecutionTermination> {
        self.termination.as_ref()
    }

    /// 首个清理失败诊断。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn cleanup_failure(&self) -> Option<&CleanupDiagnostic> {
        self.cleanup_failure.as_ref()
    }

    /// 首次终止原因的副本，供 Root 收口写入 [`RootExit`](super::runtime::RootExit)。
    ///
    /// 只返回副本：终止状态本身不可被活跃调用或诊断移交清除。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn termination_report(&self) -> Option<ExecutionTermination> {
        self.termination.clone()
    }

    /// 首个清理失败诊断的副本，同上。
    pub(crate) fn cleanup_report(&self) -> Option<CleanupDiagnostic> {
        self.cleanup_failure.clone()
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
            let termination = ExecutionTermination {
                kind,
                scope,
                note,
                scope_error,
            };
            #[cfg(test)]
            super::test_support::record_termination_saved(
                termination.kind(),
                termination.scope().cloned(),
                termination.note(),
            );
            self.termination = Some(termination);
        }
    }

    /// 记录首个清理失败；后续清理故障不覆盖首个诊断。
    pub(crate) fn record_cleanup_failure(&mut self, scope: ScopeId, error: ScopeError) {
        let stored_first = self.cleanup_failure.is_none();
        #[cfg(test)]
        super::test_support::record_cleanup_failure_probe(
            scope.clone(),
            error.clone(),
            stored_first,
        );
        if stored_first {
            self.cleanup_failure = Some(CleanupDiagnostic { scope, error });
            #[cfg(test)]
            observe::apply_later_cleanup_diagnostic(self);
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

    /// 整组本地 alias 装配：来源必须是本 Scope 已绑定的位置；不移动业务值。
    pub(crate) fn bind_local_aliases(
        &mut self,
        scope: &ScopeId,
        aliases: &[AliasSlot],
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        self.require_call_scope(scope)?;
        self.coordinator.bind_local_aliases(scope, aliases)
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

    /// 登记一个已擦除的 owned Data 并绑定本地输出位置（Root 输入登记的正式入口）。
    ///
    /// 与 [`Self::register_owned`] 同一终止与调用范围检查；擦除值只在本次 Execution 的
    /// Container 中存活，`type_name` 用于后续类型诊断。
    pub(crate) fn register_owned_erased(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        type_name: &'static str,
        value: Box<dyn Any>,
    ) -> Result<DataId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(scope)?;
        self.coordinator
            .register_owned_erased(scope, position, type_name, value)
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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

    /// 受控成组 Item 收口：把当前 Item 的**完整声明输出组**一次预检、一次消费进对应的
    /// collector 组，并在整组提交后只关闭 Item 一次。
    ///
    /// 与单项收口同序，但承载整组：许可 [`ItemGroupPermit`]
    /// 的声明组与本次 `selected` 必须逐槽一致、长度相等且非空，全部 collector 必须由该
    /// Each 负责。许可只授予"本次收口把固定声明组写入固定 parent owner 的 collector 组"，
    /// 不授予任意读取、abort、owned 注入、替换其它 collector 的能力，也不放宽通用门禁。
    pub(crate) fn consume_group_in_item_boundary(
        &mut self,
        item: &ScopeId,
        selected: &[RefId],
        permit: &ItemGroupPermit,
    ) -> ConsumeGroupOutcome {
        if let Err(violated) = self.require_item_boundary_group(item, selected, permit) {
            return ConsumeGroupOutcome::Rejected {
                primary: violated,
                cleanup_failure: None,
            };
        }
        self.coordinator
            .consume_item_group_report(item, selected, &permit.collectors)
    }

    /// 受控成组 Round 收口：把当前 Round 的**完整声明输出组**一次提交到固定 parent
    /// controller 的多个控制状态。
    ///
    /// 与单项收口同序，但承载整组：许可
    /// [`RoundGroupPermit`] 的声明组与本次 `selected` 必须逐槽一致，全部目标 state 由
    /// 许可登记的 controller 负责。许可只授予"本次收口写入固定 controller 的登记状态
    /// 组"，不授予任意读取、abort、owned 注入或替换其它 state 的能力，也不放宽通用门禁。
    pub(crate) fn promote_group_in_round_boundary(
        &mut self,
        round: &ScopeId,
        selected: &[RefId],
        permit: &RoundGroupPermit,
    ) -> PromoteGroupOutcome {
        if let Err(violated) = self.require_round_boundary_group(round, selected, permit) {
            return PromoteGroupOutcome::Rejected {
                primary: violated,
                cleanup_failure: None,
            };
        }
        self.coordinator
            .promote_group_report(round, selected, &permit.states)
    }

    /// 成组 Round 收口的许可检查：在公共结构核对之上，再要求本次 `selected` 与许可的
    /// 完整声明组逐槽一致、长度相等且非空（空组不得读成 unit Iter 合法）。
    fn require_round_boundary_group(
        &self,
        round: &ScopeId,
        selected: &[RefId],
        permit: &RoundGroupPermit,
    ) -> Result<(), ScopeError> {
        self.require_round_boundary_group_shape(round, permit)?;
        if permit.group.is_empty() {
            return Err(ScopeError::Invariant {
                violated: "group round collection requires a non-empty declared group",
            });
        }
        if selected.len() != permit.group.len() {
            return Err(ScopeError::Invariant {
                violated: "group round collection length does not match the declared group",
            });
        }
        for (index, position) in selected.iter().enumerate() {
            if position != &permit.group[index] {
                return Err(ScopeError::Invariant {
                    violated: "group round collection slot does not match the declared group position",
                });
            }
        }
        Ok(())
    }

    /// 成组许可的公共结构核对：当前 frame 就是该 Round，其父 frame 是许可登记的
    /// controller，Round 是该 controller 的直接 child，许可的声明组与目标 state 逐槽
    /// 一一对应且全部 state 由该 controller 负责。空 `states`（配合空 `group`）表示
    /// "只关闭不更新状态"。
    fn require_round_boundary_group_shape(
        &self,
        round: &ScopeId,
        permit: &RoundGroupPermit,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        if round != &permit.round {
            return Err(ScopeError::Invariant {
                violated: "group round collect requires the permitted round scope",
            });
        }
        let Some(frame) = self.frames.last() else {
            return Err(ScopeError::Invariant {
                violated: "group round collection requires an active round frame",
            });
        };
        if frame.scope.as_ref() != Some(round) {
            return Err(ScopeError::Invariant {
                violated: "group round collection requires the current frame to be the round scope",
            });
        }
        let Some(parent_index) = frame.parent else {
            return Err(ScopeError::Invariant {
                violated: "group round frame must have a caller frame",
            });
        };
        if self.frames[parent_index].scope.as_ref() != Some(&permit.controller) {
            return Err(ScopeError::Invariant {
                violated: "group round frame caller is not the registered controller scope",
            });
        }
        if self.coordinator.parent_of(round)? != Some(permit.controller.clone()) {
            return Err(ScopeError::Invariant {
                violated: "round is not a direct child of the registered controller scope",
            });
        }
        if permit.states.len() != permit.group.len() {
            return Err(ScopeError::Invariant {
                violated: "group round permit state count does not match the declared group",
            });
        }
        for state in &permit.states {
            if self.coordinator.state_owner(state)? != permit.controller {
                return Err(ScopeError::Invariant {
                    violated: "group round permit state is not owned by the registered controller",
                });
            }
        }
        Ok(())
    }

    /// 受控 Item 创建：为刚建立的直接 child（ItemScope）绑定集合元素目标。
    ///
    /// 只允许当前调用 Scope 自己调用（调用方必须是 frame 栈顶的 Scope），只接受其本地
    /// 已绑定的 `Vec<T>` 集合来源——完整 Data，或合法的受限 item 借用（其元素自身是
    /// `Vec<T>` 的嵌套集合）；不接受任意 DataId／ScopeId／owned item 注入。
    pub(crate) fn bind_item_input<T: Any>(
        &mut self,
        item_scope: &ScopeId,
        caller: &ScopeId,
        source: &RefId,
        target: &RefId,
        index: usize,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        if self.current_scope().as_ref() != Some(caller) {
            return Err(ScopeError::OutsideInvocation {
                scope: caller.clone(),
                current: self.current_scope(),
            });
        }
        self.require_call_scope(item_scope)?;
        self.coordinator
            .bind_item_input::<T>(item_scope, caller, source, target, index)
    }

    /// 成组 Item 收口的许可检查：当前 frame 就是该 Item，其父 frame 是许可登记的 Each，
    /// Item 是该 Each 的直接 child，许可的声明组与本次 `selected` 逐槽一致、长度相等且
    /// 非空，且全部目的 collector 由该 Each 负责。
    ///
    /// 声明组不能被裁剪、重排或换项：`selected` 必须与 `permit.group` 逐槽相等。空组在此
    /// 拒绝，不得读成 unit（unit 走既有合法正常关闭路径，不调用本入口）。
    fn require_item_boundary_group(
        &self,
        item: &ScopeId,
        selected: &[RefId],
        permit: &ItemGroupPermit,
    ) -> Result<(), ScopeError> {
        self.require_running()?;
        if item != &permit.item {
            return Err(ScopeError::Invariant {
                violated: "group item collect requires the permitted item scope",
            });
        }
        let Some(frame) = self.frames.last() else {
            return Err(ScopeError::Invariant {
                violated: "group item consumption requires an active item frame",
            });
        };
        if frame.scope.as_ref() != Some(item) {
            return Err(ScopeError::Invariant {
                violated: "group item consumption requires the current frame to be the item scope",
            });
        }
        let Some(parent_index) = frame.parent else {
            return Err(ScopeError::Invariant {
                violated: "group item frame must have a caller frame",
            });
        };
        if self.frames[parent_index].scope.as_ref() != Some(&permit.each) {
            return Err(ScopeError::Invariant {
                violated: "group item frame caller is not the registered each scope",
            });
        }
        if self.coordinator.parent_of(item)? != Some(permit.each.clone()) {
            return Err(ScopeError::Invariant {
                violated: "item is not a direct child of the registered each scope",
            });
        }
        if permit.group.is_empty() {
            return Err(ScopeError::Invariant {
                violated: "group item collection requires a non-empty declared group",
            });
        }
        if permit.collectors.len() != permit.group.len() {
            return Err(ScopeError::Invariant {
                violated: "group item permit collector count does not match the declared group",
            });
        }
        if selected.len() != permit.group.len() {
            return Err(ScopeError::Invariant {
                violated: "group item collection length does not match the declared group",
            });
        }
        for (index, position) in selected.iter().enumerate() {
            if position != &permit.group[index] {
                return Err(ScopeError::Invariant {
                    violated: "group item collection slot does not match the declared group position",
                });
            }
        }
        for collector in &permit.collectors {
            if self.coordinator.collector_owner(collector)? != permit.each {
                return Err(ScopeError::Invariant {
                    violated: "group item permit collector is not owned by the registered each scope",
                });
            }
        }
        Ok(())
    }

    /// Promote：把来源 child 的选定结果保留到父控制器的控制状态。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn register_uninitialized_state<T: Any>(
        &mut self,
        controller: &ScopeId,
    ) -> Result<ControlStateId, ScopeError> {
        self.require_running()?;
        self.require_call_scope(controller)?;
        self.coordinator
            .register_uninitialized_state::<T>(controller)
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn parent_of(&self, scope: &ScopeId) -> Result<Option<ScopeId>, ScopeError> {
        self.coordinator.parent_of(scope)
    }
}

/// 调用 guard：持有对当前 Context 的可变借用与本人负责退出的 Scope。
///
/// `Deref`／`DerefMut` 直接把 Context 暴露给同一调用链；guard 不持有业务值、输入
/// Future 本体或第二份 Container。
pub(crate) struct InvocationGuard<'ctx> {
    context: &'ctx mut ExecutionContext,
    frame_index: usize,
    responsible: Vec<ScopeId>,
    exit: ExitDisposition,
}
impl Deref for InvocationGuard<'_> {
    type Target = ExecutionContext;

    fn deref(&self) -> &ExecutionContext {
        self.context
    }
}
impl DerefMut for InvocationGuard<'_> {
    fn deref_mut(&mut self) -> &mut ExecutionContext {
        self.context
    }
}
impl InvocationGuard<'_> {
    /// 本 guard 对应 frame 的只读元数据。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn frame(&self) -> &InvocationFrame {
        &self.context.frames[self.frame_index]
    }

    /// 本人仍负责退出的 Scope 列表。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn responsible_scopes(&self) -> &[ScopeId] {
        &self.responsible
    }

    /// Root 收口权限：只允许当前未终止的 Root frame 在本 Context 的 RootScope 上发起。
    ///
    /// 判据彼此独立：身份（本 Execution 的 RootScope）、终止状态、调用类别（必须是
    /// Root frame；Boundary／Leaf 即使拿着 RootScope 身份也拒绝）、无未结束的 child／
    /// Leaf 调用、以及本 guard 确实承担 RootScope 的退出责任。它不放宽
    /// `require_call_scope`：可见性许可是读取范围，不是提取权。
    fn require_root_close(&self, root: &ScopeId) -> Result<(), ScopeError> {
        self.require_running()?;
        if *root != self.context.root_scope() {
            return Err(ScopeError::ForeignExecution {
                scope: root.clone(),
            });
        }
        let Some(frame) = self.context.frames.get(self.frame_index) else {
            return Err(ScopeError::Invariant {
                violated: "a root close needs its own invocation frame",
            });
        };
        if frame.kind() != InvocationKind::Root {
            return Err(ScopeError::OutsideInvocation {
                scope: root.clone(),
                current: self.context.current_scope(),
            });
        }
        if self.context.frame_depth() != self.frame_index + 1 {
            return Err(ScopeError::Invariant {
                violated: "a root close requires no live child invocation",
            });
        }
        if !self.responsible.iter().any(|known| known == root) {
            return Err(ScopeError::Invariant {
                violated: "a root close requires the root frame's own responsibility",
            });
        }
        Ok(())
    }

    /// Root 收口第一步：冻结 Root（Active → Finalizing，仅一次）并整组预检全部输出。
    ///
    /// 权限、冻结与预检都发生在任何 take 之前；失败时 Root 保持已冻结状态，由调用方
    /// 进入受控失败清理（不恢复为可继续业务执行的 Active）。
    pub(crate) fn prepare_root_extraction(
        &mut self,
        root: &ScopeId,
        ports: &[super::signature::DeclaredPort],
    ) -> Result<PreparedRootExtraction, ScopeError> {
        self.require_root_close(root)?;
        self.coordinator.freeze_root(root)?;
        self.coordinator.prepare_root_extraction(root, ports)
    }

    /// Root 收口提交：同步 take 全组并在同一边界解除 Root 责任；无可恢复失败分支。
    ///
    /// 返回按声明顺序的擦除值与关闭阶段仍需销毁的剩余 owned。
    pub(crate) fn commit_root_extraction(
        &mut self,
        root: &ScopeId,
        plan: PreparedRootExtraction,
    ) -> (Vec<Box<dyn Any>>, Vec<DataId>) {
        self.coordinator.commit_root_extraction(root, plan)
    }

    /// Root 收口关闭尾段：整组失效引用、销毁预检过的剩余 owned、保留 tombstone。
    pub(crate) fn close_root(
        &mut self,
        root: &ScopeId,
        targets: Vec<DataId>,
    ) -> Result<(), ScopeError> {
        self.coordinator.close_root_validated(root, targets)
    }

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
        #[cfg(test)]
        super::test_support::record_guard_failure(scope.clone(), this.responsible.clone());
        this.context.terminate(
            TerminationKind::BodyError,
            scope,
            error.note(),
            error.scope_error().cloned(),
        );
    }
}
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
            observe::record_cleanup_start(&scopes);

            let mut failure: Option<(ScopeId, ScopeError)> = None;
            for scope in &scopes {
                let cleanup = self.context.coordinator.abort(scope);
                #[cfg(test)]
                super::test_support::record_guard_cleanup(
                    frame_scope.clone(),
                    scope.clone(),
                    cleanup.is_ok(),
                );
                if let Err(error) = cleanup
                    && failure.is_none()
                {
                    failure = Some((scope.clone(), error));
                }
                #[cfg(test)]
                creation_counts::record_event(&format!("guard-cleanup:{}", scope.seq()));
            }
            #[cfg(test)]
            observe::note_cleanup_end(self.context, &scopes);

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
            #[cfg(test)]
            observe::record_root_after_failure_cleanup(
                self.context,
                &frame_scope,
                self.frame_index,
            );
        }

        #[cfg(test)]
        observe::observe_frame_exit(self.context, self.frame_index);

        #[cfg(not(test))]
        self.context.frames.truncate(self.frame_index);
    }
}

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

/// 业务执行错误：稳定的说明文本与可选框架诊断。
///
/// 业务 Node 用 [`BodyError::new`] 返回自己的失败说明；`Display` 输出该说明。
/// 框架内部的 Scope 诊断不进入公开访问面（访问器保持 crate 内可见）。
#[derive(Debug)]
pub struct BodyError {
    note: &'static str,
    scope: Option<ScopeError>,
}
impl BodyError {
    /// 以说明文本构造。
    pub fn new(note: &'static str) -> Self {
        Self { note, scope: None }
    }

    /// 说明文本。
    pub fn note(&self) -> &'static str {
        self.note
    }

    /// 内的 Scope 诊断（如有）。
    pub(crate) fn scope_error(&self) -> Option<&ScopeError> {
        self.scope.as_ref()
    }
}

impl std::fmt::Display for BodyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.note)
    }
}

impl std::error::Error for BodyError {}
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
#[allow(dead_code)] // 由 tests/ui/d22 等按真实非 test 入口编译；crate 内无调用点
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
#[allow(dead_code)] // 由 tests/ui/d22 等按真实非 test 入口编译；crate 内无调用点
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
#[cfg(test)]
#[path = "../../tests/internal/core/context_creation_counts.rs"]
pub(crate) mod creation_counts;

#[cfg(test)]
#[path = "../../tests/internal/core/context_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/internal/core/context_observe.rs"]
pub(crate) mod observe;
