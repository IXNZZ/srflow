//! 内部数据底座操作的诊断错误。
//!
//! 这些错误用于 Runtime 内部诊断，不是业务 Execution Error API：本任务不预建
//! 正式错误出口，错误只在 crate 内传播。错误分类依赖检查顺序——归属、存活、
//! 类型依次判定，因此"来源 Execution 不符"不会塌缩成"数据不存在"。

use std::fmt;
use std::sync::Arc;

use super::context::TerminationKind;
use super::identity::{CollectorId, DataId, ExecutionIdentity, ScopeId};
use super::ref_id::RefId;
use super::scope::{ControlStateId, ScopeState};

/// 身份序号空间的类别。
///
/// 只用于诊断；`DataId`、`ScopeId`、`CollectorId` 与控制状态位置各持有独立计数器。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdKind {
    /// `DataId` 序号空间。
    Data,
    /// `ScopeId` 序号空间。
    Scope,
    /// `RefId` 序号空间。
    Ref,
    /// `CollectorId` 序号空间。
    Collector,
    /// 单个控制器 Scope 内的控制状态位置序号空间。
    ControlState,
}

impl fmt::Display for IdKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data => f.write_str("DataId"),
            Self::Scope => f.write_str("ScopeId"),
            Self::Ref => f.write_str("RefId"),
            Self::Collector => f.write_str("CollectorId"),
            Self::ControlState => f.write_str("control state position"),
        }
    }
}

/// 内部存储与身份操作的诊断错误。
#[derive(Debug, Clone)]
pub(crate) enum InternalError {
    /// 请求的 ID 由另一次 Execution 分配。
    ForeignExecution {
        /// 被拒绝的 ID。
        requested: DataId,
        /// 当前 Container 的身份根。
        container_execution: Arc<ExecutionIdentity>,
    },
    /// ID 在本 Container 中不存在，或已经因为移除／销毁而失效。
    DataNotFound {
        /// 被拒绝的 ID。
        requested: DataId,
    },
    /// 请求类型与 entry 中实际存储的类型不符。
    TypeMismatch {
        /// 被拒绝的 ID。
        requested: DataId,
        /// 请求方期望的类型名。
        expected: &'static str,
        /// entry 中实际存储的类型名。
        actual: &'static str,
    },
    /// `DataId`／`ScopeId` 序号空间已经耗尽。
    IdSpaceExhausted {
        /// 耗尽的身份类别。
        kind: IdKind,
    },
    /// `()` 不作为业务 Data 登记，也不作为 collector 元素类型。
    UnitNotStorable,
    /// 请求的 CollectorId 由另一次 Execution 分配。
    CollectorForeignExecution {
        /// 被拒绝的 CollectorId。
        requested: CollectorId,
        /// 当前 Container 的身份根。
        container_execution: Arc<ExecutionIdentity>,
    },
    /// CollectorId 在本 Container 中不存在，或已经完成／清理而失效。
    CollectorNotFound {
        /// 被拒绝的 CollectorId。
        requested: CollectorId,
    },
    /// collector 的登记元素类型为 `()`；空 collector 仍需真实元素类型。
    UnitNotCollectible,
}

impl fmt::Display for InternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignExecution {
                requested,
                container_execution,
            } => write!(
                f,
                "id {requested} belongs to another execution: container {container_execution}, \
                 requested {}",
                requested.execution()
            ),
            Self::DataNotFound { requested } => {
                write!(f, "id {requested} does not exist or is no longer valid")
            }
            Self::TypeMismatch {
                requested,
                expected,
                actual,
            } => write!(
                f,
                "id {requested} stores `{actual}`, but `{expected}` was requested"
            ),
            Self::IdSpaceExhausted { kind } => {
                write!(f, "{kind} sequence space exhausted without reuse")
            }
            Self::UnitNotStorable => f.write_str("`()` is not a storable business Data output"),
            Self::CollectorForeignExecution {
                requested,
                container_execution,
            } => write!(
                f,
                "{requested} belongs to another execution: container {container_execution}, \
                 requested {}",
                requested.execution()
            ),
            Self::CollectorNotFound { requested } => write!(
                f,
                "{requested} does not exist or is no longer usable (finished or cleaned)"
            ),
            Self::UnitNotCollectible => f.write_str("`()` is not a usable collector element type"),
        }
    }
}

/// Scope 机制的诊断错误。
///
/// 与 [`InternalError`] 一样只用于 Runtime 内部诊断，不是业务 Execution Error API。
/// 分类对应拒绝阶段：来源不符在查表前判定；不存在与已关闭（tombstone）区分；其余
/// 均发生在任何 caller 绑定或责任转移之前的预检阶段。
#[derive(Debug, Clone)]
pub(crate) enum ScopeError {
    /// `ScopeId` 属于另一次 Execution；在 registry 查表前拒绝。
    ForeignExecution {
        /// 被拒绝的 ScopeId。
        scope: ScopeId,
    },
    /// 本 Execution 从未登记该 ScopeId（区别于已关闭的 tombstone）。
    ScopeNotFound {
        /// 被拒绝的 ScopeId。
        scope: ScopeId,
    },
    /// Scope 已关闭：本地引用与责任均已处置，身份只保留为 tombstone。
    ScopeClosed {
        /// 已关闭的 ScopeId。
        scope: ScopeId,
    },
    /// Scope 处于 Finalizing 等非 Active 状态，不再接受业务操作。
    ScopeNotActive {
        /// 当前状态。
        state: ScopeState,
        /// 被拒绝的 ScopeId。
        scope: ScopeId,
    },
    /// caller 不是 child 的直接 parent。
    NotDirectParent {
        /// child Scope。
        child: ScopeId,
        /// 被拒绝的 caller。
        caller: ScopeId,
    },
    /// 仍有未关闭的 descendant，父 Scope 不能正常进入 finalization。
    ActiveDescendants {
        /// 父 Scope。
        scope: ScopeId,
    },
    /// 内部 RootScope 不声明对外输出；Root 输出提取属 V21-10。
    RootOutputNotSupported {
        /// Root Scope。
        scope: ScopeId,
    },
    /// Root 的多个声明输出解析到同一物理 `DataId`。
    ///
    /// 与 [`Self::DuplicateOwner`] 区分：这里每个位置都合法、owner 唯一，只是同一实例被
    /// 要求提取两次；Root 提取必须给 Application 两份互不重复的 owned 值，因此在任何
    /// take 之前整体拒绝，不隐式 `Clone`。
    DuplicateRootDataId {
        /// 声明顺序中首个解析到该实例的位置。
        first_ref: RefId,
        /// 重复解析到同一实例的位置。
        duplicate_ref: RefId,
        /// 被重复选择的物理实例。
        data_id: DataId,
    },
    /// 本地位置尚未绑定。
    RefNotBound {
        /// 所在 Scope。
        scope: ScopeId,
        /// 未绑定的本地位置。
        position: RefId,
    },
    /// 本地位置已经绑定；任何情况下都不允许重绑，包括绑定到同一目标。
    RefAlreadyBound {
        /// 所在 Scope。
        scope: ScopeId,
        /// 已绑定的本地位置。
        position: RefId,
    },
    /// 同一批操作中出现重复位置（导入目标或 caller 输出）。
    DuplicatePosition {
        /// 重复出现的位置。
        position: RefId,
    },
    /// 输出数量与声明的输出位置数量不符。
    OutputCountMismatch {
        /// child Scope。
        scope: ScopeId,
        /// 声明的输出位置数量。
        declared: usize,
        /// 本次提供的绑定数量。
        supplied: usize,
    },
    /// 输出绑定的 child 位置与声明顺序不一致。
    OutputPositionMismatch {
        /// child Scope。
        scope: ScopeId,
        /// 声明位置。
        declared: RefId,
        /// 本次提供的位置。
        supplied: RefId,
    },
    /// 位置上的目标已经失效（被移出或销毁）。
    TargetNotAlive {
        /// 引用该目标的位置。
        position: RefId,
    },
    /// 目标与位置声明的类型不符。
    TypeMismatch {
        /// 引用该目标的位置。
        position: RefId,
        /// 声明的类型名。
        expected: &'static str,
        /// 目标实际类型名。
        actual: &'static str,
    },
    /// 该 DataId 没有任何责任 Scope。
    NoOwner {
        /// 无责任方的 DataId。
        id: DataId,
    },
    /// 该 DataId 有多个责任 Scope，破坏唯一责任。
    DuplicateOwner {
        /// 被多个 Scope 认领的 DataId。
        id: DataId,
    },
    /// 目标由当前边界无权转移的责任方拥有（例如 sibling-owned）。
    IllegalOwner {
        /// 目标 DataId。
        id: DataId,
        /// 实际责任方。
        owner: ScopeId,
        /// 尝试处置该目标的边界。
        boundary: ScopeId,
    },
    /// 内部不变量被破坏；不得静默跳过。
    Invariant {
        /// 被破坏的不变量说明。
        violated: &'static str,
    },
    /// 控制状态句柄属于另一次 Execution；在 registry 查表前拒绝。
    StateForeignExecution {
        /// 被拒绝的状态句柄。
        state: ControlStateId,
    },
    /// 该 (控制器 Scope, 状态序号) 从未登记，或已随控制器关闭而撤销。
    StateNotRegistered {
        /// 被拒绝的状态句柄。
        state: ControlStateId,
    },
    /// 状态句柄的控制器与本次操作要求的控制器不一致。
    StateWrongOwner {
        /// 被拒绝的状态句柄。
        state: ControlStateId,
        /// 本次操作要求的状态归属。
        expected_owner: ScopeId,
    },
    /// 状态尚未初始化：既没有本地初始引用，也没有完成过首次 Promote。
    StateUninitialized {
        /// 未初始化的状态句柄。
        state: ControlStateId,
    },
    /// 状态登记的元素类型与本次操作声明的类型不符。
    StateTypeMismatch {
        /// 状态句柄。
        state: ControlStateId,
        /// 状态登记的类型名。
        expected: &'static str,
        /// 本次操作声明的类型名。
        actual: &'static str,
    },
    /// CollectorId 属于另一次 Execution；在 registry 查表前拒绝。
    CollectorForeignExecution {
        /// 被拒绝的 CollectorId。
        collector: CollectorId,
    },
    /// 该 CollectorId 从未登记，或已完成／随控制器清理而撤销。
    CollectorNotRegistered {
        /// 被拒绝的 CollectorId。
        collector: CollectorId,
    },
    /// collector 的责任 Scope 与本次操作要求的控制器不一致。
    CollectorNotOwnedBy {
        /// 被拒绝的 CollectorId。
        collector: CollectorId,
        /// 本次操作要求的责任 Scope。
        expected_owner: ScopeId,
    },
    /// 该入口需要完整 Data 目标，但位置绑定的是 CollectionItem。
    NonCompleteTarget {
        /// 引用该目标的位置。
        position: RefId,
    },
    /// CollectionItem 的来源集合已失效（被移出或销毁）。
    ItemCollectionNotAlive {
        /// 引用该 item 的位置。
        position: RefId,
        /// 失效的来源集合。
        collection: DataId,
    },
    /// CollectionItem 的请求方不在其 lifetime cap 内。
    ItemOutsideCap {
        /// 引用该 item 的位置。
        position: RefId,
        /// item 的 lifetime cap。
        cap: ScopeId,
        /// 越界的请求方。
        requester: ScopeId,
    },
    /// CollectionItem 的 index 超出实际集合长度。
    ItemIndexOutOfRange {
        /// 引用该 item 的位置。
        position: RefId,
        /// 越界下标。
        index: usize,
    },
    /// CollectionItem 不能跨出其 lifetime cap（转移目的在 cap 之外）。
    ItemCapEscape {
        /// 引用该 item 的位置。
        position: RefId,
        /// item 的 lifetime cap。
        cap: ScopeId,
        /// cap 之外的目的 Scope。
        destination: ScopeId,
    },
    /// 当前 Execution 已终止（执行失败或取消），普通业务与正常提交入口拒绝。
    Terminated {
        /// 首次终止类别。
        kind: TerminationKind,
    },
    /// 目标 Scope 不在当前调用可见范围内：child 不能直接读写 caller／ancestor 的本地位置。
    OutsideInvocation {
        /// 被拒绝的 Scope。
        scope: ScopeId,
        /// 当前调用使用的 Scope（无 frame 时为 RootScope 初始化边界）。
        current: Option<ScopeId>,
    },
    /// 包裹底层存储诊断（归属、存活、类型、序号耗尽等）。
    Storage {
        /// 底层错误。
        source: InternalError,
    },
}

impl ScopeError {
    /// 把存储错误转换为带位置上下文的 Scope 诊断。
    pub(crate) fn from_storage(position: RefId, source: InternalError) -> Self {
        match source {
            InternalError::TypeMismatch {
                expected, actual, ..
            } => Self::TypeMismatch {
                position,
                expected,
                actual,
            },
            InternalError::DataNotFound { .. } => Self::TargetNotAlive { position },
            other => Self::Storage { source: other },
        }
    }
}

impl fmt::Display for ScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignExecution { scope } => {
                write!(f, "{scope} belongs to another execution")
            }
            Self::ScopeNotFound { scope } => write!(f, "{scope} is not registered"),
            Self::ScopeClosed { scope } => write!(f, "{scope} is closed"),
            Self::ScopeNotActive { scope, state } => {
                write!(f, "{scope} is not active ({state:?})")
            }
            Self::NotDirectParent { child, caller } => {
                write!(f, "{caller} is not the direct parent of {child}")
            }
            Self::ActiveDescendants { scope } => {
                write!(f, "{scope} still has active descendants")
            }
            Self::RootOutputNotSupported { scope } => {
                write!(
                    f,
                    "{scope} is the internal RootScope and declares no output"
                )
            }
            Self::DuplicateRootDataId {
                first_ref,
                duplicate_ref,
                data_id,
            } => {
                write!(
                    f,
                    "root outputs {first_ref} and {duplicate_ref} resolve to the same {data_id}"
                )
            }
            Self::RefNotBound { scope, position } => {
                write!(f, "{position} is not bound in {scope}")
            }
            Self::RefAlreadyBound { scope, position } => {
                write!(f, "{position} is already bound in {scope}")
            }
            Self::DuplicatePosition { position } => {
                write!(f, "{position} appears more than once in the same batch")
            }
            Self::OutputCountMismatch {
                scope,
                declared,
                supplied,
            } => write!(
                f,
                "{scope} declares {declared} output positions, but {supplied} bindings were supplied"
            ),
            Self::OutputPositionMismatch {
                scope,
                declared,
                supplied,
            } => write!(
                f,
                "{scope} declares {declared} at this output position, but {supplied} was supplied"
            ),
            Self::TargetNotAlive { position } => {
                write!(f, "the target of {position} is no longer alive")
            }
            Self::TypeMismatch {
                position,
                expected,
                actual,
            } => write!(
                f,
                "the target of {position} is `{actual}`, but `{expected}` was declared"
            ),
            Self::NoOwner { id } => write!(f, "{id} has no responsible scope"),
            Self::DuplicateOwner { id } => write!(f, "{id} is owned by more than one scope"),
            Self::IllegalOwner {
                id,
                owner,
                boundary,
            } => write!(
                f,
                "{id} is owned by {owner}, which {boundary} may not dispose of"
            ),
            Self::Invariant { violated } => write!(f, "internal invariant violated: {violated}"),
            Self::StateForeignExecution { state } => {
                write!(f, "{state} belongs to another execution")
            }
            Self::StateNotRegistered { state } => {
                write!(f, "{state} is not registered (or its controller is closed)")
            }
            Self::StateWrongOwner {
                state,
                expected_owner,
            } => write!(f, "{state} is not a control state of {expected_owner}"),
            Self::StateUninitialized { state } => {
                write!(f, "{state} is not initialized yet")
            }
            Self::StateTypeMismatch {
                state,
                expected,
                actual,
            } => write!(
                f,
                "{state} holds `{expected}` state, but `{actual}` was declared"
            ),
            Self::CollectorForeignExecution { collector } => {
                write!(f, "{collector} belongs to another execution")
            }
            Self::CollectorNotRegistered { collector } => write!(
                f,
                "{collector} is not registered (or already finished or cleaned)"
            ),
            Self::CollectorNotOwnedBy {
                collector,
                expected_owner,
            } => write!(f, "{collector} is not managed by {expected_owner}"),
            Self::OutsideInvocation { scope, current } => match current {
                Some(current) => write!(
                    f,
                    "{scope} is outside the current invocation ({current}) and its descendants"
                ),
                None => write!(
                    f,
                    "{scope} is not the RootScope; only Root initialization may touch it without an invocation"
                ),
            },
            Self::Terminated { kind } => write!(
                f,
                "the execution context is terminated ({kind:?}); ordinary business and commit entries are closed"
            ),
            Self::Storage { source } => write!(f, "{source}"),
            Self::NonCompleteTarget { position } => {
                write!(
                    f,
                    "{position} is bound to a collection item, not a complete Data"
                )
            }
            Self::ItemCollectionNotAlive {
                position,
                collection,
            } => write!(
                f,
                "the collection {collection} behind {position} is no longer alive"
            ),
            Self::ItemOutsideCap {
                position,
                cap,
                requester,
            } => write!(
                f,
                "{requester} is outside the lifetime cap {cap} of {position}"
            ),
            Self::ItemIndexOutOfRange { position, index } => {
                write!(f, "item index {index} of {position} is out of range")
            }
            Self::ItemCapEscape {
                position,
                cap,
                destination,
            } => write!(
                f,
                "{destination} is outside the lifetime cap {cap} of {position}"
            ),
        }
    }
}
