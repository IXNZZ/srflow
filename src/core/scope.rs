//! Scope 的可见性与生命周期责任。
//!
//! [`ScopeCoordinator`] 在同一 owner 中持有 [`ScopeRegistry`] 与本次 Execution 唯一的
//! [`DataContainer`]。registry 只登记元数据——身份、父子关系、状态、本地引用绑定
//! （`refs`）与生命周期责任集合（`owned`）——业务值始终只位于 Container。
//!
//! 规则要点：
//!
//! - **单赋值**：同一 Scope 中一个本地位置只能成功绑定一次，重复绑定（即使目标相同）
//!   都拒绝；不同位置可以形成指向同一完整 DataId 的只读 alias。
//! - **显式导入**：只能从直接 caller 的本地引用导入，且不增加 owner；业务 resolve
//!   不搜索 ancestor.refs，也不能凭"数据在同一个 Container 里"取得读取或销毁权。
//! - **唯一责任**：每份登记过的存活 Data 恰有一个责任 Scope；Export 才把 child-owned
//!   责任转交 caller，imported ancestor-owned alias 只绑定引用。
//! - **整组提交**：Export 的所有可恢复校验都在提交前完成，提交段只做不可失败的绑定与
//!   责任转移；提交后才失效本地引用、清理剩余 owned 并关闭 Scope。
//! - **状态**：Active → Finalizing → Closed；Closed 保留身份 tombstone，使"已关闭"与
//!   "从未登记"成为两类可区分的诊断。
//!
//! 本阶段只处理完整 `Data(DataId)` 目标；CollectionItem 与 cap 属 V21-08，Promote／
//! Consume 属 V21-03，ExecutionContext／Invocation 与异步清理由 V21-04 接入。

use std::any::{Any, TypeId, type_name};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::data_container::DataContainer;
use super::identity::{
    CollectorId, CollectorIdAllocator, DataId, ExecutionIdentity, ScopeId, ScopeIdAllocator,
};
use super::internal_error::{IdKind, InternalError, ScopeError};
use super::ref_id::RefId;

/// 一个控制状态位置的内部句柄：控制器 Scope + 该 Scope 内不复用的序号。
///
/// 句柄**不携带**目标：目标只存在于协调组件的登记表中，并由 [`ScopeCoordinator`]
/// 在每次操作时重新校验。因此拿着句柄并不能绕过登记取得任意 `DataId`，也不能自行
/// 声明一个可信 target。序号属于单个控制器 Scope，耗尽时返回
/// [`InternalError::IdSpaceExhausted`]，不复用、不回绕。
#[derive(Debug, Clone)]
pub(crate) struct ControlStateId {
    owner: ScopeId,
    seq: u64,
}

impl ControlStateId {
    /// 登记该状态的控制器 Scope。
    pub(crate) fn owner(&self) -> &ScopeId {
        &self.owner
    }

    /// 控制器 Scope 内的状态位置序号。
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }
}

impl PartialEq for ControlStateId {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && self.owner == other.owner
    }
}

impl Eq for ControlStateId {}

impl std::fmt::Display for ControlStateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ControlStateId({}, seq = {})", self.owner, self.seq)
    }
}

/// 一份从控制状态到 child 本地位置的导入描述，并声明 child 位置的类型。
///
/// 来源不是 RefId 而是已登记的状态句柄：状态 target 只有协调组件能解析，调用方不能
/// 用"容器里存在这个 DataId"代替合法保留。
pub(crate) struct StateImportSlot {
    state: ControlStateId,
    target: RefId,
    expected: TypeId,
    expected_name: &'static str,
}

#[allow(dead_code)] // slot 由 Definition 元数据／V21-09 Loop 构造，当前只由驱动与测试使用
impl StateImportSlot {
    /// 以 `T` 声明 child 本地目标位置的类型。
    pub(crate) fn new<T: Any>(state: &ControlStateId, target: &RefId) -> Self {
        Self {
            state: state.clone(),
            target: target.clone(),
            expected: TypeId::of::<T>(),
            expected_name: type_name::<T>(),
        }
    }
}

/// 一个控制状态位置的登记元数据。
///
/// 只保存归属、类型与 target 元数据，不保存业务 `T`；业务值仍在 Container 中由某个
/// Scope 承担责任。`pending` 记录被替换、但尚未满足回收条件的旧状态 DataId：它本身
/// 不授予读取权，也不表示业务状态仍在使用。
struct ControlState {
    owner: ScopeId,
    target: Option<RefTarget>,
    expected: TypeId,
    expected_name: &'static str,
    pending: Vec<DataId>,
}

/// 一个未完成 collector 的责任登记。
///
/// 只保存身份、责任 Scope 与元素类型元数据；`Vec<O>` 位于 Container 的建构区。
struct CollectorRecord {
    id: CollectorId,
    owner: ScopeId,
    element_type: TypeId,
    element_name: &'static str,
}

/// Execution 内部的运行时目标：完整 Data 实例，或受 lifetime cap 保护的集合元素。
///
/// 目标只是定位信息，不拥有业务 Data；CollectionItem 不取得独立 DataId，也不拥有
/// 业务值。它只在 ItemScope 存活且请求方位于 cap 内时可临时借用。
#[derive(Debug, Clone)]
pub(crate) enum RefTarget {
    /// 已存储在 DataContainer 中的完整 Data 实例。
    Data(DataId),
    /// 某个 `Vec<T>` 集合内部、有效期不超过 ItemScope 的 item。
    CollectionItem {
        /// 来源集合的完整 Data 身份。
        collection: DataId,
        /// 元素下标。
        index: usize,
        /// item 的有效期上限（ItemScope）。
        lifetime_cap: ScopeId,
        /// 封闭的元素访问描述（由 typed `Vec<T>` 工厂创建）。
        access: ItemAccess,
    },
}

impl RefTarget {
    /// 完整 Data 身份；CollectionItem 没有独立 DataId。
    fn data_id(&self) -> Option<&DataId> {
        match self {
            Self::Data(id) => Some(id),
            Self::CollectionItem { .. } => None,
        }
    }

    /// 要求目标为完整 Data：需要 owned／完整 Data 的入口用它显式拒绝 item。
    fn require_data<'a>(&'a self, position: &RefId) -> Result<&'a DataId, ScopeError> {
        self.data_id().ok_or_else(|| ScopeError::NonCompleteTarget {
            position: position.clone(),
        })
    }

    /// CollectionItem 元数据；完整 Data 返回空。
    fn item(&self) -> Option<(&DataId, usize, &ScopeId, &ItemAccess)> {
        match self {
            Self::Data(_) => None,
            Self::CollectionItem {
                collection,
                index,
                lifetime_cap,
                access,
            } => Some((collection, *index, lifetime_cap, access)),
        }
    }
}

/// 集合元素访问描述：只服务 safe 校验与临时借用。
///
/// 由框架的 typed `Vec<T>` 工厂在创建 item 目标时构造；不拥有业务值、不保存长期
/// `&T`、不接受业务 callback 或任意解析器。声明的类型元数据只用于快速比较，实际
/// 存储类型必须由 [`Self::project`] 对真实值 downcast 复核，不能靠元数据绕过。
#[derive(Debug, Clone, Copy)]
pub(crate) struct ItemAccess {
    collection_type: TypeId,
    collection_name: &'static str,
    element_type: TypeId,
    element_name: &'static str,
    element_at: fn(&dyn Any, usize) -> Option<&dyn Any>,
}

impl ItemAccess {
    /// 为集合 `Vec<T>` 构造访问描述（`T` 为元素类型）。
    pub(crate) fn for_collection<T: Any>() -> Self {
        Self {
            collection_type: TypeId::of::<Vec<T>>(),
            collection_name: type_name::<Vec<T>>(),
            element_type: TypeId::of::<T>(),
            element_name: type_name::<T>(),
            element_at: |value, index| {
                value
                    .downcast_ref::<Vec<T>>()
                    .and_then(|values| values.get(index))
                    .map(|item| item as &dyn Any)
            },
        }
    }

    /// 集合声明类型。
    pub(crate) fn collection_type(&self) -> TypeId {
        self.collection_type
    }

    /// 集合声明类型名。
    pub(crate) fn collection_name(&self) -> &'static str {
        self.collection_name
    }

    /// 元素声明类型。
    pub(crate) fn element_type(&self) -> TypeId {
        self.element_type
    }

    /// 元素声明类型名。
    pub(crate) fn element_name(&self) -> &'static str {
        self.element_name
    }

    /// 从真实集合值投影第 `index` 个元素：先按实际 `Vec<T>` downcast，再取元素并擦除。
    fn project<'a>(&self, value: &'a dyn Any, index: usize) -> Option<&'a dyn Any> {
        (self.element_at)(value, index)
    }
}

/// Scope 的生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeState {
    /// 可创建 child、导入／登记本地引用，并接受业务 resolve。
    Active,
    /// 已停止业务操作，只允许内部输出预检、提交与退出清理。
    Finalizing,
    /// 已关闭：本地引用与责任均已处置，只保留身份 tombstone。
    Closed,
}

/// 一份 Import 描述：caller 本地来源位置 → child 本地目标位置，并声明目标类型。
pub(crate) struct ImportSlot {
    source: RefId,
    target: RefId,
    expected: TypeId,
    expected_name: &'static str,
}

#[allow(dead_code)] // slot 由 Definition 元数据／V21-05 CallSite 构造，当前只由驱动与测试使用
impl ImportSlot {
    /// 以 `T` 声明 child 本地目标位置的类型。
    pub(crate) fn new<T: Any>(source: &RefId, target: &RefId) -> Self {
        Self::with_type(source, target.clone(), TypeId::of::<T>(), type_name::<T>())
    }

    /// 以运行时类型声明目标位置：Orchestrator 的 child-local 端口由内部 Definition 决定。
    pub(crate) fn with_type(
        source: &RefId,
        target: RefId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Self {
        Self {
            source: source.clone(),
            target,
            expected,
            expected_name,
        }
    }
}

/// 一份输出绑定描述：child 本地输出位置 → caller 输出位置，并声明输出类型。
///
/// 这是内部校验输入，不是正式 Signature API；一次 Invocation 的 slot 由本次预检
/// 一次性消费，不缓存为稍后提交的凭证。
pub(crate) struct ExportSlot {
    child: RefId,
    caller: RefId,
    expected: TypeId,
    expected_name: &'static str,
}

#[allow(dead_code)] // slot 由 Definition 元数据／V21-05 CallSite 构造，当前只由驱动与测试使用
impl ExportSlot {
    /// 以 `T` 声明 child 本地输出位置的类型。
    pub(crate) fn new<T: Any>(child: &RefId, caller: &RefId) -> Self {
        Self::with_type(child, caller.clone(), TypeId::of::<T>(), type_name::<T>())
    }

    /// 以运行时类型声明输出位置：Orchestrator 的 child-local 端口由内部 Definition 决定。
    pub(crate) fn with_type(
        child: &RefId,
        caller: RefId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Self {
        Self {
            child: child.clone(),
            caller,
            expected,
            expected_name,
        }
    }
}

/// 直接 Consume 的结果报告：原始拒绝与其后的清理诊断分别保留。
#[derive(Debug)]
pub(crate) enum ConsumeOutcome {
    /// 消费成功：值已移入 collector，ItemScope 已关闭。
    Consumed,
    /// 拒绝：`primary` 是原始原因，`cleanup_failure` 是其后的清理失败（若有）。
    Rejected {
        /// 原始拒绝原因。
        primary: ScopeError,
        /// prepare 拒绝后 `cleanup_subtree` 的诊断（若有）。
        cleanup_failure: Option<ScopeError>,
    },
}

/// Promote 的结果报告：原始拒绝与其后的清理诊断分别保留。
///
/// 与 [`ConsumeOutcome`] 同形：早期拒绝（来源非 Active、仍有活 descendant、查表失败）
/// 发生在冻结之前，不假装已经清理；prepare 拒绝后按既有纪律执行清理，其失败作为
/// `cleanup_failure` 独立报告，不覆盖 `primary`。
#[derive(Debug)]
pub(crate) enum PromoteOutcome {
    /// 保留成功：状态 target 已更新、责任已按需转移、来源 Scope 已关闭。
    Promoted,
    /// 拒绝：`primary` 是原始原因，`cleanup_failure` 是其后的清理失败（若有）。
    Rejected {
        /// 原始拒绝原因。
        primary: ScopeError,
        /// prepare 拒绝后清理来源的诊断（若有）。
        cleanup_failure: Option<ScopeError>,
    },
}

/// Round discard 的结果报告：不绑定任何输出，只关闭并清理来源。
///
/// 与 [`ConsumeOutcome`]／[`PromoteOutcome`] 同形；用于 Iter 之外的 Retry Continue 与
/// 内部丢弃路径。
#[derive(Debug)]
pub(crate) enum DiscardOutcome {
    /// 已关闭：本地引用失效、owned 处置、留 tombstone。
    Discarded,
    /// 拒绝：`primary` 是原始原因，`cleanup_failure` 是其后的清理失败（若有）。
    Rejected {
        /// 原始拒绝原因。
        primary: ScopeError,
        /// prepare 拒绝后清理来源的诊断（若有）。
        cleanup_failure: Option<ScopeError>,
    },
}

/// 测试观测用的 Scope 元数据快照：本地引用集合与责任集合（均按本地序号排序）。
#[cfg(test)]
pub(crate) type ScopeSnapshot = (Vec<(RefId, DataId)>, Vec<DataId>);

/// 测试观测用的 target-aware 快照类型：`(refs, owned)`。
#[cfg(test)]
pub(crate) type TargetSnapshotPair = (Vec<(RefId, TargetSnapshot)>, Vec<DataId>);

/// 测试观测用的 target 快照：明确区分完整 Data 与 CollectionItem，不把 item 扁平化成
/// 集合 DataId。
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetSnapshot {
    /// 完整 Data 实例。
    Data(DataId),
    /// 集合元素目标（含来源集合、下标、cap 与声明类型）。
    CollectionItem {
        /// 来源集合。
        collection: DataId,
        /// 元素下标。
        index: usize,
        /// item 的 lifetime cap。
        lifetime_cap: ScopeId,
        /// 声明的集合类型。
        collection_type: TypeId,
        /// 声明的元素类型。
        element_type: TypeId,
    },
}

#[cfg(test)]
impl TargetSnapshot {
    /// 由真实 target 投影（只读）。
    fn of(target: &RefTarget) -> Self {
        match target {
            RefTarget::Data(id) => Self::Data(id.clone()),
            RefTarget::CollectionItem {
                collection,
                index,
                lifetime_cap,
                access,
            } => Self::CollectionItem {
                collection: collection.clone(),
                index: *index,
                lifetime_cap: lifetime_cap.clone(),
                collection_type: access.collection_type(),
                element_type: access.element_type(),
            },
        }
    }
}

/// 一个 Scope 的元数据记录。
///
/// 只保存身份、关系、状态与绑定／责任元数据；不含业务值，也不长期保存 Rust borrow。
// 键类型 `RefId`／`DataId` 内部持有身份根的原子计数，clippy 会视为"含内部可变性的
// key"；两者的 Eq／Hash 只使用身份地址与本地序号，二者都不随分配变化，键语义安全。
#[allow(clippy::mutable_key_type)]
struct Scope {
    id: ScopeId,
    parent: Option<ScopeId>,
    children: Vec<ScopeId>,
    state: ScopeState,
    refs: HashMap<RefId, RefTarget>,
    owned: HashSet<DataId>,
    /// 本控制器 Scope 的下一个控制状态位置序号：单调、不复用、checked 耗尽。
    state_next: u64,
}

impl Scope {
    fn new(id: ScopeId, parent: Option<ScopeId>) -> Self {
        Self {
            id,
            parent,
            children: Vec::new(),
            state: ScopeState::Active,
            refs: HashMap::new(),
            owned: HashSet::new(),
            state_next: 0,
        }
    }

    /// 业务操作要求 Active；Closed 与 Finalizing 分别诊断。
    fn require_active(&self) -> Result<(), ScopeError> {
        match self.state {
            ScopeState::Active => Ok(()),
            ScopeState::Closed => Err(ScopeError::ScopeClosed {
                scope: self.id.clone(),
            }),
            ScopeState::Finalizing => Err(ScopeError::ScopeNotActive {
                scope: self.id.clone(),
                state: self.state,
            }),
        }
    }
}

/// Scope registry：Scope 树、状态与绑定／责任元数据。
///
/// 不含 Container 或业务值；唯一 Container 位于外层 [`ScopeCoordinator`]。
struct ScopeRegistry {
    execution: Arc<ExecutionIdentity>,
    scope_ids: ScopeIdAllocator,
    collector_ids: CollectorIdAllocator,
    root: ScopeId,
    scopes: HashMap<u64, Scope>,
    /// 控制状态登记表，键为 (控制器 Scope 序号, 状态位置序号)。
    ///
    /// 键只用本地序号，避免把含身份根的句柄当作 map key；句柄自身仍携带 ScopeId 用于
    /// 归属校验。
    states: HashMap<(u64, u64), ControlState>,
    /// 未完成 collector 的责任登记表，键为 CollectorId 序号。
    collectors: HashMap<u64, CollectorRecord>,
}

impl ScopeRegistry {
    /// 来源校验先于查表：另一 Execution 的同序号 ScopeId 得到"来源不符"。
    fn lookup(&self, scope: &ScopeId) -> Result<&Scope, ScopeError> {
        if !Arc::ptr_eq(scope.execution(), &self.execution) {
            return Err(ScopeError::ForeignExecution {
                scope: scope.clone(),
            });
        }
        self.scopes
            .get(&scope.seq())
            .ok_or_else(|| ScopeError::ScopeNotFound {
                scope: scope.clone(),
            })
    }

    fn lookup_mut(&mut self, scope: &ScopeId) -> Result<&mut Scope, ScopeError> {
        if !Arc::ptr_eq(scope.execution(), &self.execution) {
            return Err(ScopeError::ForeignExecution {
                scope: scope.clone(),
            });
        }
        self.scopes
            .get_mut(&scope.seq())
            .ok_or_else(|| ScopeError::ScopeNotFound {
                scope: scope.clone(),
            })
    }
}

/// Promote 的预检结果。
///
/// 只由 [`ScopeCoordinator::prepare_promote`] 生成并在同一次独占操作内提交；不对外
/// 暴露，也不能保存后择机提交。`replaced_pending` 只是待回收登记，不授予读取权。
#[derive(Debug)]
struct PreparedPromote {
    source: ScopeId,
    controller: ScopeId,
    state_key: (u64, u64),
    target: RefTarget,
    transferred: bool,
    replaced_pending: Option<DataId>,
}

/// Consume 的预检结果。
///
/// 只由 [`ScopeCoordinator::prepare_consume`] 生成并在同一次独占操作内提交。
#[derive(Debug)]
struct PreparedConsume {
    item: ScopeId,
    target: RefTarget,
    collector: CollectorId,
}

/// Export 的预检结果。
///
/// 只由 `prepare_export` 生成，并在同一次独占操作内被 `commit_export` 消费；
/// 不对外暴露，不能保存后择机提交。
#[derive(Debug)]
struct PreparedExport {
    child: ScopeId,
    caller: ScopeId,
    bindings: Vec<(RefId, RefTarget)>,
    transferred: Vec<DataId>,
}

/// Scope 协调组件：同一 owner 中的 registry 与唯一 Container。
///
/// 它提供受控的登记、导入、resolve、导出与退出入口，不启动业务调用、不创建 Invocation，
/// 也不提供任意可变 Container 的访问器。后续 ExecutionContext 复用它。
#[allow(dead_code)] // V21-04 接入真实调用链前，本组件的入口只由测试驱动
pub(crate) struct ScopeCoordinator {
    registry: ScopeRegistry,
    container: DataContainer,
}

#[allow(dead_code)] // 同上：入口在 V21-04 接入前只由测试驱动
impl ScopeCoordinator {
    /// 以驱动创建一次的 ExecutionIdentity 建立协调组件与内部 RootScope。
    ///
    /// registry、Container 与 ScopeId 分配器共享同一身份根；不创建第二个身份根或
    /// Container。RootScope 是 Scope 树起点，不是 Application Root API。
    pub(crate) fn new(execution: Arc<ExecutionIdentity>) -> Self {
        let scope_ids = ScopeIdAllocator::new(Arc::clone(&execution));
        let collector_ids = CollectorIdAllocator::new(Arc::clone(&execution));
        #[cfg(test)]
        crate::core::context::creation_counts::count_coordinator();
        let root = scope_ids
            .allocate()
            .expect("a fresh execution identity always has ScopeId space");
        let mut scopes = HashMap::new();
        scopes.insert(root.seq(), Scope::new(root.clone(), None));
        Self {
            registry: ScopeRegistry {
                execution: Arc::clone(&execution),
                scope_ids,
                collector_ids,
                root: root.clone(),
                scopes,
                states: HashMap::new(),
                collectors: HashMap::new(),
            },
            container: DataContainer::with_identity(execution),
        }
    }

    /// 内部 RootScope 身份。
    pub(crate) fn root(&self) -> ScopeId {
        self.registry.root.clone()
    }

    /// 测试观测：一个 Scope 当前的本地引用数量。
    #[cfg(test)]
    pub(crate) fn refs_len_probe(&self, scope: &ScopeId) -> Result<usize, ScopeError> {
        Ok(self.registry.lookup(scope)?.refs.len())
    }

    /// 测试故障注入：直接销毁一个已登记 entry（责任记录保留）。
    #[cfg(test)]
    pub(crate) fn destroy_probe(&mut self, id: &DataId) {
        self.container
            .destroy(id)
            .expect("fixture only destroys live entries");
    }

    /// 测试故障注入：把一个 target 直接写入 Scope 的本地位置。
    ///
    /// 只用于构造"非法 owner"等反例；不成为生产入口。
    #[cfg(test)]
    pub(crate) fn inject_target_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        target: RefTarget,
    ) {
        self.registry
            .lookup_mut(scope)
            .expect("fixture scope exists")
            .refs
            .insert(position.clone(), target);
    }

    /// 测试观测：一个 Scope 的 owned 数量。
    #[cfg(test)]
    pub(crate) fn owned_len_probe(&self, scope: &ScopeId) -> Result<usize, ScopeError> {
        Ok(self.registry.lookup(scope)?.owned.len())
    }

    /// 测试故障注入：把一个 Scope 置为 Finalizing，用于验证"禁止进入非 Active Scope"。
    ///
    /// 只用于构造进入拒绝样本；不成为生产入口。
    #[cfg(test)]
    pub(crate) fn set_finalizing_probe(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
        self.registry.lookup_mut(scope)?.state = ScopeState::Finalizing;
        Ok(())
    }

    /// 测试观测：一个 DataId 是否仍然存活（旧身份失效证据）。
    #[cfg(test)]
    pub(crate) fn alive_probe(&self, id: &DataId) -> bool {
        self.container.validate(id).is_ok()
    }

    /// 测试观测：一个 DataId 的唯一责任 Scope。
    #[cfg(test)]
    pub(crate) fn owner_probe(&self, id: &DataId) -> Result<ScopeId, ScopeError> {
        self.owner_of(id)
    }

    /// 测试观测：本协调组件创建的唯一 DataContainer 的稳定地址。
    #[cfg(test)]
    pub(crate) fn container_probe(&self) -> *const () {
        std::ptr::from_ref(&self.container).cast::<()>()
    }

    /// 测试观测：本协调组件的稳定地址。
    #[cfg(test)]
    pub(crate) fn coordinator_probe(&self) -> *const () {
        std::ptr::from_ref(self).cast::<()>()
    }

    /// 测试观测：本次 Execution 身份根的地址（用于证明 child 共享同一身份）。
    #[cfg(test)]
    pub(crate) fn identity_probe(&self) -> *const () {
        Arc::as_ptr(&self.registry.execution).cast::<()>()
    }

    /// 控制状态当前的责任控制器。
    ///
    /// 供 Context 校验"控制提交两端"的权限：不仅是来源 Scope，接收 state 的控制器
    /// 也必须属于当前调用的可见范围。
    #[allow(dead_code)] // V21-05 接入 CallSite 前只由 Context 与测试使用
    pub(crate) fn state_owner(&self, state: &ControlStateId) -> Result<ScopeId, ScopeError> {
        if !Arc::ptr_eq(state.owner().execution(), &self.registry.execution) {
            return Err(ScopeError::StateForeignExecution {
                state: state.clone(),
            });
        }
        match self
            .registry
            .states
            .get(&(state.owner().seq(), state.seq()))
        {
            Some(record) => Ok(record.owner.clone()),
            None => Err(ScopeError::StateNotRegistered {
                state: state.clone(),
            }),
        }
    }

    /// 未完成 collector 的责任 Scope。
    #[allow(dead_code)] // 同上
    pub(crate) fn collector_owner(&self, collector: &CollectorId) -> Result<ScopeId, ScopeError> {
        Ok(self.lookup_collector_owner(collector)?.clone())
    }

    /// collector 记录的责任 Scope（存在性检查后）。
    fn lookup_collector_owner(&self, collector: &CollectorId) -> Result<&ScopeId, ScopeError> {
        if !Arc::ptr_eq(collector.execution(), &self.registry.execution) {
            return Err(ScopeError::CollectorForeignExecution {
                collector: collector.clone(),
            });
        }
        match self.registry.collectors.get(&collector.seq()) {
            Some(record) => Ok(&record.owner),
            None => Err(ScopeError::CollectorNotRegistered {
                collector: collector.clone(),
            }),
        }
    }

    /// 一个 Scope 的直接 parent；用于调用边界校验 frame 与 Scope 的关系。
    ///
    /// 只读元数据访问，不改变任何绑定或责任。
    #[allow(dead_code)] // V21-04 的 Context frame 校验使用
    pub(crate) fn parent_of(&self, scope: &ScopeId) -> Result<Option<ScopeId>, ScopeError> {
        Ok(self.registry.lookup(scope)?.parent.clone())
    }

    /// 当前状态；不存在与已关闭分别诊断。
    pub(crate) fn state(&self, scope: &ScopeId) -> Result<ScopeState, ScopeError> {
        Ok(self.registry.lookup(scope)?.state)
    }

    /// 从有效 Active parent 建立 child。
    ///
    /// 状态检查先于分配，被拒绝的请求不消耗 `ScopeId`，也不改变 parent 状态。
    pub(crate) fn create_child(&mut self, parent: &ScopeId) -> Result<ScopeId, ScopeError> {
        self.registry.lookup(parent)?.require_active()?;
        let child = self
            .registry
            .scope_ids
            .allocate()
            .map_err(|source| ScopeError::Storage { source })?;
        self.registry
            .scopes
            .insert(child.seq(), Scope::new(child.clone(), Some(parent.clone())));
        self.registry
            .lookup_mut(parent)?
            .children
            .push(child.clone());
        Ok(child)
    }

    /// 登记一个新产生的 owned Data，并把它绑定到本地输出位置。
    ///
    /// 顺序为状态与位置占用检查 → Container 插入 → 登记唯一责任与绑定。插入失败不留
    /// 引用或责任；插入成功后不再有可恢复校验，因此不会留下无 owner entry。
    pub(crate) fn register_owned<T: Any>(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        value: T,
    ) -> Result<DataId, ScopeError> {
        {
            let record = self.registry.lookup(scope)?;
            record.require_active()?;
            if record.refs.contains_key(position) {
                return Err(ScopeError::RefAlreadyBound {
                    scope: scope.clone(),
                    position: position.clone(),
                });
            }
        }

        let id = self
            .container
            .insert_owned(value)
            .map_err(|source| ScopeError::Storage { source })?;

        let record = self.registry.lookup_mut(scope)?;
        record
            .refs
            .insert(position.clone(), RefTarget::Data(id.clone()));
        record.owned.insert(id.clone());
        Ok(id)
    }

    /// 业务 resolve：只使用指定 Active Scope 的本地位置。
    ///
    /// 返回的 `&T` 生命周期绑定组件的 `&self`；借用未结束时无法调用退出或清理入口。
    pub(crate) fn resolve<T: Any>(
        &self,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<&T, ScopeError> {
        let record = self.registry.lookup(scope)?;
        record.require_active()?;
        let target = record
            .refs
            .get(position)
            .ok_or_else(|| ScopeError::RefNotBound {
                scope: scope.clone(),
                position: position.clone(),
            })?;
        match target {
            RefTarget::Data(id) => {
                // 先走容器检查（归属 → 存活 → 类型），保持既有诊断顺序。
                let borrowed = self
                    .container
                    .borrow::<T>(id)
                    .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
                // 容器校验之后、返回借用之前验证责任链：只有本 Scope 自身或其后代可见的
                // 责任方才能提供读取；sibling-owned、无 owner 与重复 owner 都必须拒绝。
                self.require_visible_target(id, scope)?;
                Ok(borrowed)
            }
            RefTarget::CollectionItem { .. } => self.borrow_item::<T>(target, scope, position),
        }
    }

    /// CollectionItem 借用：cap 有效且请求方在 cap 内 → 集合存活与实际 `Vec<T>` 类型 →
    /// index／元素类型 → 责任链。返回的 `&T` 绑定组件的 `&self`，与其他借用同寿命上限。
    fn borrow_item<'a, T: Any>(
        &'a self,
        target: &'a RefTarget,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<&'a T, ScopeError> {
        let (_, _, cap, access) = target
            .item()
            .expect("borrow_item is only called for item targets");
        self.require_in_cap(position, cap, scope)?;
        self.check_item(position, target, TypeId::of::<T>(), type_name::<T>())?;
        let (collection, index, _, _) = target.item().expect("item target");
        let value = self
            .container
            .borrow_any(collection)
            .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
        let projected =
            access
                .project(value, index)
                .ok_or_else(|| ScopeError::ItemIndexOutOfRange {
                    position: position.clone(),
                    index,
                })?;
        projected
            .downcast_ref::<T>()
            .ok_or_else(|| ScopeError::TypeMismatch {
                position: position.clone(),
                expected: type_name::<T>(),
                actual: access.element_name(),
            })
    }

    /// item 目标的共享检查：cap 有效 → cap 内转移目的 → 集合存活与实际 `Vec<T>` 类型 →
    /// 元素声明类型 → index 在界 → 责任链（cap 仍能看到来源集合）。请求方范围由调用方
    /// 按语义给出诊断（业务读取用 `ItemOutsideCap`，跨 Scope 转移用 `ItemCapEscape`）。
    fn check_item(
        &self,
        position: &RefId,
        target: &RefTarget,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Result<(), ScopeError> {
        let (collection, index, cap, access) = target
            .item()
            .expect("check_item is only called for item targets");
        self.require_live_cap(position, cap)?;
        if self.container.borrow_any(collection).is_err() {
            return Err(ScopeError::ItemCollectionNotAlive {
                position: position.clone(),
                collection: collection.clone(),
            });
        }
        self.container
            .validate_type(
                collection,
                access.collection_type(),
                access.collection_name(),
            )
            .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
        if access.element_type() != expected {
            return Err(ScopeError::TypeMismatch {
                position: position.clone(),
                expected: expected_name,
                actual: access.element_name(),
            });
        }
        self.require_visible_target(collection, cap)?;
        let value = self
            .container
            .borrow_any(collection)
            .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
        if access.project(value, index).is_none() {
            return Err(ScopeError::ItemIndexOutOfRange {
                position: position.clone(),
                index,
            });
        }
        Ok(())
    }

    /// 请求方必须在 item 的 lifetime cap 内（业务读取与位置校验）。
    fn require_in_cap(
        &self,
        position: &RefId,
        cap: &ScopeId,
        requester: &ScopeId,
    ) -> Result<(), ScopeError> {
        if self.is_ancestor_or_self(cap, requester) {
            Ok(())
        } else {
            Err(ScopeError::ItemOutsideCap {
                position: position.clone(),
                cap: cap.clone(),
                requester: requester.clone(),
            })
        }
    }

    /// 转移目的必须在 item 的 lifetime cap 内（Import／Export／Promote）。
    fn require_destination_in_cap(
        &self,
        position: &RefId,
        cap: &ScopeId,
        destination: &ScopeId,
    ) -> Result<(), ScopeError> {
        if self.is_ancestor_or_self(cap, destination) {
            Ok(())
        } else {
            Err(ScopeError::ItemCapEscape {
                position: position.clone(),
                cap: cap.clone(),
                destination: destination.clone(),
            })
        }
    }

    /// cap 必须仍存在且未关闭；Closed 身份不能重新代表新 Scope，因此旧 cap 不会复活。
    fn require_live_cap(&self, position: &RefId, cap: &ScopeId) -> Result<(), ScopeError> {
        let _ = position;
        let record = self.registry.lookup(cap)?;
        if record.state == ScopeState::Closed {
            return Err(ScopeError::ScopeClosed { scope: cap.clone() });
        }
        Ok(())
    }

    /// 窄只读观察：collector 已移动进建构区的元素个数。
    ///
    /// 只读 Count，不改变 collector 状态；finish 与控制器清理都会移除计数，因此证据必须
    /// 在移除之前留存。
    #[cfg(test)]
    pub(crate) fn collector_moves_probe(
        &self,
        collector: &CollectorId,
    ) -> Result<usize, ScopeError> {
        self.container
            .collector_moves(collector)
            .map_err(|source| ScopeError::Storage { source })
    }

    /// 测试观测：只读快照某个 Scope 的本地引用与责任集合（终止后仍允许）。
    ///
    /// 只查表、不要求 Active、不授予读取权；用于证明失败的整组导出没有留下部分绑定或
    /// 责任转移。返回按本地序号排序的确定性列表。本投影**只接受完整 Data 绑定**：
    /// 出现 CollectionItem 时显式报"不适用"，不把集合 DataId 扁平化成 item 身份
    /// （target-aware 观察见 [`Self::snapshot_targets_probe`]）。
    #[cfg(test)]
    pub(crate) fn snapshot_probe(&self, scope: &ScopeId) -> Result<ScopeSnapshot, ScopeError> {
        let (refs, owned) = self.snapshot_targets_probe(scope)?;
        let mut projected: Vec<(RefId, DataId)> = Vec::with_capacity(refs.len());
        for (position, target) in refs {
            match target {
                TargetSnapshot::Data(id) => projected.push((position, id)),
                TargetSnapshot::CollectionItem { .. } => {
                    return Err(ScopeError::NonCompleteTarget { position });
                }
            }
        }
        Ok((projected, owned))
    }

    /// target-aware 只读快照：refs 区分完整 Data 与 CollectionItem（含来源集合、下标、
    /// cap 与声明类型），owned 仍是完整 DataId 集合；不做任何扁平化。
    #[cfg(test)]
    pub(crate) fn snapshot_targets_probe(
        &self,
        scope: &ScopeId,
    ) -> Result<TargetSnapshotPair, ScopeError> {
        let record = self.registry.lookup(scope)?;
        let mut refs: Vec<(RefId, TargetSnapshot)> = record
            .refs
            .iter()
            .map(|(position, target)| (position.clone(), TargetSnapshot::of(target)))
            .collect();
        refs.sort_by_key(|(position, _)| position.seq());
        let mut owned: Vec<DataId> = record.owned.iter().cloned().collect();
        owned.sort_by_key(DataId::seq);
        Ok((refs, owned))
    }

    /// 输出位置预检：只校验 Scope 状态与"该位置尚未被绑定"，不做任何变更。
    ///
    /// 叶子调用在执行业务体之前用它做预检：已知输出冲突时业务体不运行，错误带真实的
    /// `RefAlreadyBound` 诊断，供调用方以执行错误退出（而不是退化成未标记取消）。
    pub(crate) fn precheck_output_position(
        &self,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<(), ScopeError> {
        let record = self.registry.lookup(scope)?;
        record.require_active()?;
        if record.refs.contains_key(position) {
            return Err(ScopeError::RefAlreadyBound {
                scope: scope.clone(),
                position: position.clone(),
            });
        }
        Ok(())
    }

    /// 位置校验：只校验归属、存活与声明类型，不借用业务值。
    ///
    /// 供 Orchestrator 输入 pack 的擦除后校验使用：它走与 [`Self::resolve`] 相同的
    /// Scope 访问关系与容器归属 → 存活 → 类型检查路径，不建立第二条可能漂移的顺序，
    /// 也不产生长期 `&T`。
    pub(crate) fn validate_position(
        &self,
        scope: &ScopeId,
        position: &RefId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Result<(), ScopeError> {
        let record = self.registry.lookup(scope)?;
        record.require_active()?;
        let target = record
            .refs
            .get(position)
            .ok_or_else(|| ScopeError::RefNotBound {
                scope: scope.clone(),
                position: position.clone(),
            })?;
        match target {
            RefTarget::Data(id) => {
                self.container
                    .validate_type(id, expected, expected_name)
                    .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
                self.require_visible_target(id, scope)
            }
            RefTarget::CollectionItem { .. } => {
                self.validate_item(position, target, scope, expected, expected_name)
            }
        }
    }

    /// CollectionItem 的位置校验：不建立长期借用，顺序与 [`Self::borrow_item`] 一致。
    fn validate_item(
        &self,
        position: &RefId,
        target: &RefTarget,
        scope: &ScopeId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Result<(), ScopeError> {
        let (_, _, cap, _) = target
            .item()
            .expect("validate_item is only called for item targets");
        self.require_in_cap(position, cap, scope)?;
        self.check_item(position, target, expected, expected_name)
    }

    /// 整组 Import（本地引用来源）：[`Self::import_batch_with_states`] 的 convenience 入口。
    ///
    /// 全部预检（包括批内重复目标）通过后才提交；任何失败都不留下部分输入，也不改动
    /// 双方的 owned。
    pub(crate) fn import_batch(
        &mut self,
        child: &ScopeId,
        caller: &ScopeId,
        inputs: &[ImportSlot],
    ) -> Result<(), ScopeError> {
        self.import_batch_with_states(child, caller, inputs, &[])
    }

    /// 整组 Import：来源可为 caller 的本地引用或 caller 已登记的控制状态。
    ///
    /// 两类来源共用同一条预检与提交路径：全部校验（含批内重复目标）通过后才绑定，
    /// 任何失败都不留下部分 `child.refs`，也不改变 owner 或状态 target。状态来源必须
    /// 属于 caller 本身：调用方不能凭"容器中存在此 DataId"导入未经合法保留的状态。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn import_batch_with_states(
        &mut self,
        child: &ScopeId,
        caller: &ScopeId,
        local: &[ImportSlot],
        from_state: &[StateImportSlot],
    ) -> Result<(), ScopeError> {
        {
            let child_record = self.registry.lookup(child)?;
            child_record.require_active()?;
            let caller_record = self.registry.lookup(caller)?;
            caller_record.require_active()?;
            if child_record.parent.as_ref() != Some(caller) {
                return Err(ScopeError::NotDirectParent {
                    child: child.clone(),
                    caller: caller.clone(),
                });
            }
        }

        let mut planned: Vec<(RefId, RefTarget)> =
            Vec::with_capacity(local.len() + from_state.len());
        {
            for slot in local {
                let target = self.local_import_target(caller, &slot.source)?;
                self.check_child_position(child, &slot.target, &planned)?;
                match &target {
                    RefTarget::Data(id) => {
                        self.container
                            .validate_type(id, slot.expected, slot.expected_name)
                            .map_err(|source| {
                                ScopeError::from_storage(slot.target.clone(), source)
                            })?;
                        let owner = self.owner_of(id)?;
                        if !self.is_ancestor_or_self(&owner, caller) {
                            return Err(ScopeError::IllegalOwner {
                                id: id.clone(),
                                owner,
                                boundary: child.clone(),
                            });
                        }
                    }
                    RefTarget::CollectionItem { .. } => {
                        // item 只作为 alias 导入：来源与目的都必须在 cap 内，不产生 item-owned
                        // 登记；合法目的不能替非法来源背书。
                        let (_, _, cap, _) = target.item().expect("item target matched by variant");
                        self.check_item(&slot.source, &target, slot.expected, slot.expected_name)?;
                        self.require_in_cap(&slot.source, cap, caller)?;
                        self.require_destination_in_cap(&slot.source, cap, child)?;
                    }
                }
                planned.push((slot.target.clone(), target));
            }
            for slot in from_state {
                let (state_owner, state_expected, state_expected_name, state_target) = {
                    let state = self.lookup_state(&slot.state)?;
                    let target =
                        state
                            .target
                            .clone()
                            .ok_or_else(|| ScopeError::StateUninitialized {
                                state: slot.state.clone(),
                            })?;
                    (
                        state.owner.clone(),
                        state.expected,
                        state.expected_name,
                        target,
                    )
                };
                if state_owner != *caller {
                    return Err(ScopeError::StateWrongOwner {
                        state: slot.state.clone(),
                        expected_owner: caller.clone(),
                    });
                }
                if state_expected != slot.expected {
                    return Err(ScopeError::StateTypeMismatch {
                        state: slot.state.clone(),
                        expected: state_expected_name,
                        actual: slot.expected_name,
                    });
                }
                self.check_child_position(child, &slot.target, &planned)?;
                match &state_target {
                    RefTarget::Data(id) => {
                        self.container
                            .validate_type(id, slot.expected, slot.expected_name)
                            .map_err(|source| {
                                ScopeError::from_storage(slot.target.clone(), source)
                            })?;
                        let owner = self.owner_of(id)?;
                        if !self.is_ancestor_or_self(&owner, caller) {
                            return Err(ScopeError::IllegalOwner {
                                id: id.clone(),
                                owner,
                                boundary: child.clone(),
                            });
                        }
                    }
                    RefTarget::CollectionItem { .. } => {
                        let (_, _, cap, _) =
                            state_target.item().expect("item target matched by variant");
                        self.check_item(
                            &slot.target,
                            &state_target,
                            slot.expected,
                            slot.expected_name,
                        )?;
                        // 状态来源的持有者是 caller 本身：来源也必须位于 cap 内。
                        self.require_in_cap(&slot.target, cap, caller)?;
                        self.require_destination_in_cap(&slot.target, cap, child)?;
                    }
                }
                planned.push((slot.target.clone(), state_target));
            }
        }

        let child_record = self.registry.lookup_mut(child)?;
        for (position, target) in planned {
            child_record.refs.insert(position, target);
        }
        Ok(())
    }

    /// 以控制器本地已绑定的引用初始化一个控制状态位置。
    ///
    /// `T` 是状态声明的类型；状态只保存 target 元数据，不保存业务值。初始化只解析
    /// 控制器自身的本地引用，不接受任意 DataId／RefTarget 注入，也不消耗 Definition
    /// RefId 或重绑任何本地位置。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn register_state<T: Any>(
        &mut self,
        controller: &ScopeId,
        from_local: &RefId,
    ) -> Result<ControlStateId, ScopeError> {
        let target = {
            let record = self.registry.lookup(controller)?;
            record.require_active()?;
            let target = record
                .refs
                .get(from_local)
                .ok_or_else(|| ScopeError::RefNotBound {
                    scope: controller.clone(),
                    position: from_local.clone(),
                })?;
            match target {
                RefTarget::Data(id) => {
                    self.container
                        .validate_type(id, TypeId::of::<T>(), type_name::<T>())
                        .map_err(|source| ScopeError::from_storage(from_local.clone(), source))?;
                    self.require_visible_target(id, controller)?;
                }
                RefTarget::CollectionItem { .. } => {
                    let (_, _, cap, _) = target.item().expect("item target matched by variant");
                    self.require_in_cap(from_local, cap, controller)?;
                    self.check_item(from_local, target, TypeId::of::<T>(), type_name::<T>())?;
                }
            }
            target.clone()
        };
        self.register_state_record(
            controller,
            TypeId::of::<T>(),
            type_name::<T>(),
            Some(target),
        )
    }

    /// 登记一个尚未初始化的控制状态位置。
    ///
    /// 用于"完成前为空"的控制器状态：Iter 的当前状态由本地初始引用设置（见
    /// [`Self::register_state`]），而 Retry 的最终结果状态在首轮完成前没有目标，只能
    /// 由首次合法 Promote 初始化。该形态的推进策略属 V21-09，本任务只交付机制。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn register_uninitialized_state<T: Any>(
        &mut self,
        controller: &ScopeId,
    ) -> Result<ControlStateId, ScopeError> {
        self.register_state_record(controller, TypeId::of::<T>(), type_name::<T>(), None)
    }

    /// Promote：把来源 child 选定的完整 Data 结果保留到父控制器的控制状态。
    ///
    /// 不绑定父 Scope 的 Definition RefId，也不消耗 Definition RefId 序列。本入口是
    /// [`Self::promote_report`] 的薄兼容映射：成功为 `Ok(())`，拒绝时保持既有"清理失败
    /// 优先"规则（`Err(cleanup_failure.unwrap_or(primary))`）。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn promote(
        &mut self,
        source: &ScopeId,
        selected: &RefId,
        state: &ControlStateId,
    ) -> Result<(), ScopeError> {
        match self.promote_report(source, selected, state) {
            PromoteOutcome::Promoted => Ok(()),
            PromoteOutcome::Rejected {
                primary,
                cleanup_failure,
            } => Err(cleanup_failure.unwrap_or(primary)),
        }
    }

    /// Promote 的报告形状：与 [`Self::promote`] 复用同一 prepare／commit／cleanup 实现，
    /// 但把**原始拒绝**与其后的**清理失败**分别保留。
    ///
    /// 次序为：来源 Active 且无活跃 descendant → 冻结 → 预检（状态归属与类型、target
    /// 存活与唯一 owner、剩余 owned／collector 清理前提）→ commit（更新状态、必要时转移
    /// 责任、关闭来源、登记待回收旧状态）。早期拒绝不冻结；prepare 拒绝按既有纪律执行
    /// `cleanup_subtree`，其失败独立报告。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn promote_report(
        &mut self,
        source: &ScopeId,
        selected: &RefId,
        state: &ControlStateId,
    ) -> PromoteOutcome {
        if let Err(primary) = self.collect_source_early_check(source) {
            return PromoteOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        if let Err(primary) = self
            .registry
            .lookup_mut(source)
            .map(|record| record.state = ScopeState::Finalizing)
        {
            return PromoteOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        #[cfg(test)]
        self.record_round_collect_probe(
            super::test_support::RoundCollectSnapshotPhase::Before,
            super::test_support::RoundCollectOperation::Promote,
            source,
            Some(selected),
            Some(state),
        );
        match self.prepare_promote(source, selected, state) {
            Ok(plan) => match self.commit_promote(plan) {
                Ok(()) => PromoteOutcome::Promoted,
                Err(primary) => PromoteOutcome::Rejected {
                    primary,
                    cleanup_failure: None,
                },
            },
            Err(primary) => {
                #[cfg(test)]
                self.record_round_collect_probe(
                    super::test_support::RoundCollectSnapshotPhase::AfterReject,
                    super::test_support::RoundCollectOperation::Promote,
                    source,
                    Some(selected),
                    Some(state),
                );
                PromoteOutcome::Rejected {
                    primary,
                    cleanup_failure: self.cleanup_subtree(source).err(),
                }
            }
        }
    }

    /// Round discard：在无可绑定输出的收口里关闭来源并处置其 owned。
    ///
    /// 与 Promote 共用同一早检与清理纪律，但**不更新任何控制状态、不转移责任、不绑定
    /// Definition RefId**；用于 Retry 的 Continue（本轮结果随 Round 结束丢弃）。返回
    /// 报告形状，供真实 Round runner 区分"已关闭"与"拒绝"。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn discard_report(
        &mut self,
        source: &ScopeId,
        #[cfg(test)] selected: Option<&RefId>,
        #[cfg(test)] state: Option<&ControlStateId>,
    ) -> DiscardOutcome {
        if let Err(primary) = self.collect_source_early_check(source) {
            return DiscardOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        if let Err(primary) = self
            .registry
            .lookup_mut(source)
            .map(|record| record.state = ScopeState::Finalizing)
        {
            return DiscardOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        #[cfg(test)]
        self.record_round_collect_probe(
            super::test_support::RoundCollectSnapshotPhase::Before,
            super::test_support::RoundCollectOperation::Discard,
            source,
            selected,
            state,
        );
        let prepared = self.cleanup_targets(source);
        match prepared {
            Ok(targets) => match self.close_validated(source, targets) {
                Ok(()) => DiscardOutcome::Discarded,
                Err(primary) => DiscardOutcome::Rejected {
                    primary,
                    cleanup_failure: None,
                },
            },
            Err(primary) => {
                #[cfg(test)]
                self.record_round_collect_probe(
                    super::test_support::RoundCollectSnapshotPhase::AfterReject,
                    super::test_support::RoundCollectOperation::Discard,
                    source,
                    selected,
                    state,
                );
                DiscardOutcome::Rejected {
                    primary,
                    cleanup_failure: self.cleanup_subtree(source).err(),
                }
            }
        }
    }

    /// 收口早检：来源存在、Active、且没有仍存活的 descendant。不做任何状态变更。
    fn collect_source_early_check(&self, source: &ScopeId) -> Result<(), ScopeError> {
        self.registry.lookup(source)?.require_active()?;
        if self.has_live_descendants(source)? {
            return Err(ScopeError::ActiveDescendants {
                scope: source.clone(),
            });
        }
        Ok(())
    }

    /// 受控回收：检查该控制器负责的待回收旧状态，满足条件即销毁。
    ///
    /// 回收条件为：仍由该控制器负责、没有任何存活 Scope 的本地引用指向它、没有其它
    /// 控制状态保留它。Promote 在替换旧状态时只登记 pending（不做全量扫描），本入口
    /// 执行检查与销毁；控制器退出时的清理是兜底。本入口不删除仍被使用的 alias，也不
    /// 通过制造"无人引用"推进状态。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn recycle_pending(&mut self, controller: &ScopeId) -> Result<(), ScopeError> {
        self.registry.lookup(controller)?.require_active()?;

        let keys: Vec<(u64, u64)> = self
            .registry
            .states
            .iter()
            .filter(|(_, state)| state.owner == *controller)
            .map(|(key, _)| *key)
            .collect();

        for key in keys {
            let pending = std::mem::take(
                &mut self
                    .registry
                    .states
                    .get_mut(&key)
                    .expect("state key came from this registry")
                    .pending,
            );
            let mut kept: Vec<DataId> = Vec::new();
            for id in pending {
                if self.container.validate(&id).is_err() {
                    continue;
                }
                match self.owner_of(&id) {
                    // 不再由本控制器负责（例如已被后续 Promote 转交）：不销毁。
                    Ok(owner) if owner != *controller => kept.push(id),
                    Ok(_) => {
                        if self.has_live_reference(&id) || self.state_holds(&id) {
                            kept.push(id);
                            continue;
                        }
                        self.registry.lookup_mut(controller)?.owned.remove(&id);
                        self.container
                            .destroy(&id)
                            .map_err(|source| ScopeError::Storage { source })?;
                    }
                    // 责任集合破坏：不授予销毁权，保留待回收记录。
                    Err(_) => kept.push(id),
                }
            }
            self.registry
                .states
                .get_mut(&key)
                .expect("state key came from this registry")
                .pending = kept;
        }
        Ok(())
    }

    /// 把控制状态的当前 target 一次性绑定到控制器最终声明的本地输出位置。
    ///
    /// 只用于最终输出准备：状态必须属于该控制器且已初始化，target 必须存活、类型相符
    /// 且责任链对该控制器可见；位置保持单赋值，不存在解绑／重绑接口。逐轮可变位置不
    /// 使用本入口。调用时机与决策属 V21-09。
    #[allow(dead_code)] // V21-09 接入真实 Loop 前只由驱动与测试使用
    pub(crate) fn bind_state_output(
        &mut self,
        controller: &ScopeId,
        state: &ControlStateId,
        position: &RefId,
    ) -> Result<(), ScopeError> {
        {
            let record = self.registry.lookup(controller)?;
            record.require_active()?;
            if record.refs.contains_key(position) {
                return Err(ScopeError::RefAlreadyBound {
                    scope: controller.clone(),
                    position: position.clone(),
                });
            }
        }

        let target = {
            let state_record = self.lookup_state(state)?;
            if state_record.owner != *controller {
                return Err(ScopeError::StateWrongOwner {
                    state: state.clone(),
                    expected_owner: controller.clone(),
                });
            }
            let target =
                state_record
                    .target
                    .clone()
                    .ok_or_else(|| ScopeError::StateUninitialized {
                        state: state.clone(),
                    })?;
            match &target {
                RefTarget::Data(id) => {
                    self.container
                        .validate_type(id, state_record.expected, state_record.expected_name)
                        .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
                    self.require_visible_target(id, controller)?;
                }
                RefTarget::CollectionItem { .. } => {
                    let (_, _, cap, _) = target.item().expect("item target matched by variant");
                    self.require_in_cap(position, cap, controller)?;
                    self.check_item(
                        position,
                        &target,
                        state_record.expected,
                        state_record.expected_name,
                    )?;
                }
            }
            target
        };

        self.registry
            .lookup_mut(controller)?
            .refs
            .insert(position.clone(), target);
        Ok(())
    }

    /// 在一个有效 Active 控制器 Scope 下建立空 collector。
    ///
    /// 协调组件只登记责任 Scope 与元素真实 `TypeId`；建构值位于 Container 的建构区。
    /// 调用方不能预装元素：元素只能来自合法 Consume。`()` 不是可用的元素类型。
    #[allow(dead_code)] // V21-08 接入真实 Each 前只由驱动与测试使用
    pub(crate) fn begin_collector<O: Any>(
        &mut self,
        owner: &ScopeId,
    ) -> Result<CollectorId, ScopeError> {
        self.registry.lookup(owner)?.require_active()?;
        let id = self
            .registry
            .collector_ids
            .allocate()
            .map_err(|source| ScopeError::Storage { source })?;
        self.container
            .begin_collector::<O>(&id)
            .map_err(|source| ScopeError::Storage { source })?;
        self.registry.collectors.insert(
            id.seq(),
            CollectorRecord {
                id: id.clone(),
                owner: owner.clone(),
                element_type: TypeId::of::<O>(),
                element_name: type_name::<O>(),
            },
        );
        Ok(id)
    }

    /// 观测：未完成 collector 的登记元数据（元素类型名、责任 Scope）。
    #[allow(dead_code)] // 观测入口，同上
    pub(crate) fn collector_metadata(
        &self,
        collector: &CollectorId,
    ) -> Result<(&'static str, ScopeId), ScopeError> {
        let record = self.lookup_collector(collector)?;
        Ok((record.element_name, record.owner.clone()))
    }

    /// 观测：未完成 collector 已收集的元素数量。
    #[allow(dead_code)] // 观测入口，同上
    pub(crate) fn collector_len(&self, collector: &CollectorId) -> Result<usize, ScopeError> {
        let _ = self.lookup_collector(collector)?;
        self.container
            .collector_len(collector)
            .map_err(|source| ScopeError::Storage { source })
    }

    /// 测试观测：真实物理移动路径上的追加次数。
    #[cfg(test)]
    pub(crate) fn collector_moves(&self, collector: &CollectorId) -> Result<usize, ScopeError> {
        let _ = self.lookup_collector(collector)?;
        self.container
            .collector_moves(collector)
            .map_err(|source| ScopeError::Storage { source })
    }

    /// 直接 Consume：把 ItemScope 的一个完整 owned 输出移入其直接 parent 的 collector。
    ///
    /// 不绑定 EachScope 的 Definition RefId，也不建立 collectible 集合：值在 Container
    /// 内部从普通 entry 移入建构值。全部预检（含剩余 owned 清理前提）在冻结与移动前
    /// 完成；Imported／parent-owned／sibling-owned 输出与类型不符都在移动前拒绝。
    #[allow(dead_code)] // V21-08 接入真实 Each 前只由驱动与测试使用
    pub(crate) fn consume_item(
        &mut self,
        item: &ScopeId,
        selected: &RefId,
        collector: &CollectorId,
    ) -> Result<(), ScopeError> {
        match self.consume_item_report(item, selected, collector) {
            ConsumeOutcome::Consumed => Ok(()),
            ConsumeOutcome::Rejected {
                primary,
                cleanup_failure,
            } => Err(cleanup_failure.unwrap_or(primary)),
        }
    }

    /// 直接 Consume 的报告形状：与 [`Self::consume_item`] 复用同一 prepare／commit／
    /// cleanup 实现，但把**原始拒绝**与其后的**清理失败**分别保留。
    ///
    /// 早期前置拒绝（Item 非 Active、仍有活 descendant、查表失败）发生在冻结之前，
    /// 不假装已经清理；prepare 拒绝后按既有纪律执行 `cleanup_subtree`，其失败作为
    /// `cleanup_failure` 独立报告，不覆盖 `primary`。
    pub(crate) fn consume_item_report(
        &mut self,
        item: &ScopeId,
        selected: &RefId,
        collector: &CollectorId,
    ) -> ConsumeOutcome {
        match self.consume_item_early_check(item) {
            Ok(()) => {}
            Err(primary) => {
                return ConsumeOutcome::Rejected {
                    primary,
                    cleanup_failure: None,
                };
            }
        }
        if let Err(primary) = self
            .registry
            .lookup_mut(item)
            .map(|record| record.state = ScopeState::Finalizing)
        {
            return ConsumeOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        #[cfg(test)]
        self.record_pre_cleanup_probe(
            super::test_support::ConsumeSnapshotPhase::Before,
            item,
            selected,
            collector,
        );
        match self.prepare_consume(item, selected, collector) {
            Ok(plan) => match self.commit_consume(plan) {
                Ok(()) => ConsumeOutcome::Consumed,
                Err(primary) => ConsumeOutcome::Rejected {
                    primary,
                    cleanup_failure: None,
                },
            },
            Err(primary) => {
                #[cfg(test)]
                self.record_pre_cleanup_probe(
                    super::test_support::ConsumeSnapshotPhase::AfterReject,
                    item,
                    selected,
                    collector,
                );
                ConsumeOutcome::Rejected {
                    primary,
                    cleanup_failure: self.cleanup_subtree(item).err(),
                }
            }
        }
    }

    /// cfg(test) 元数据故障注入：把某位置上的 CollectionItem 目标替换为给定的访问描述／下标。
    ///
    /// 只改目标**元数据**，不新增业务数据通道；解析仍走真实向量／元素校验。
    #[cfg(test)]
    pub(crate) fn corrupt_item_access_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        access: ItemAccess,
        index: Option<usize>,
    ) -> Result<(), ScopeError> {
        let record = self.registry.lookup_mut(scope)?;
        let target = record
            .refs
            .get_mut(position)
            .ok_or_else(|| ScopeError::RefNotBound {
                scope: scope.clone(),
                position: position.clone(),
            })?;
        match target {
            RefTarget::CollectionItem {
                access: slot_access,
                index: slot_index,
                ..
            } => {
                *slot_access = access;
                if let Some(index) = index {
                    *slot_index = index;
                }
                Ok(())
            }
            RefTarget::Data(_) => Err(ScopeError::Invariant {
                violated: "metadata fault injection requires a collection item target",
            }),
        }
    }

    /// cfg(test) 只读观察：下一个将被分配的 `DataId` 序号。
    #[cfg(test)]
    pub(crate) fn next_data_id_probe(&self) -> Option<u64> {
        self.container.next_data_id_probe()
    }

    /// cfg(test) 窄只读观察：某位置的完整 Data 身份（item 目标返回空）。
    #[cfg(test)]
    pub(crate) fn target_data_id_probe(
        &self,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<Option<DataId>, ScopeError> {
        let record = self.registry.lookup(scope)?;
        Ok(record
            .refs
            .get(position)
            .and_then(|target| target.data_id().cloned()))
    }

    /// cfg(test) 完整只读观察（Round 收口）：`Before` 在冻结之后的 prepare 之前，
    /// `AfterReject` 在 prepare 拒绝之后、内部 cleanup 之前。
    ///
    /// 记录来源侧与控制器侧的完整 target-aware refs／owned、来源状态、被选输出身份／
    /// 责任方／存活、控制状态 target／pending 与下一个 `DataId` 序号；观察失败以
    /// `observation_error` 显式记录，不静默降级为默认值。
    #[cfg(test)]
    pub(crate) fn record_round_collect_probe(
        &self,
        phase: super::test_support::RoundCollectSnapshotPhase,
        operation: super::test_support::RoundCollectOperation,
        source: &ScopeId,
        selected: Option<&RefId>,
        state: Option<&ControlStateId>,
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let source_state = self.registry.lookup(source).ok().map(|record| record.state);
        let (source_refs, source_owned) = match self.snapshot_targets_probe(source) {
            Ok((refs, owned)) => (Some(refs), Some(owned)),
            Err(err) => {
                note(format!("source snapshot: {err:?}"));
                (None, None)
            }
        };
        let controller = state.map(|state| state.owner().clone()).or_else(|| {
            self.registry
                .lookup(source)
                .ok()
                .and_then(|record| record.parent.clone())
        });
        let (controller_refs, controller_owned) = match controller.as_ref() {
            Some(controller) => match self.snapshot_targets_probe(controller) {
                Ok((refs, owned)) => (Some(refs), Some(owned)),
                Err(err) => {
                    note(format!("controller snapshot: {err:?}"));
                    (None, None)
                }
            },
            None => (None, None),
        };
        let selected_target_value = selected.and_then(|selected| {
            self.registry
                .lookup(source)
                .ok()
                .and_then(|record| record.refs.get(selected).cloned())
        });
        let selected_target = selected_target_value.as_ref().map(TargetSnapshot::of);
        let selected_collection_owner = match selected_target.as_ref() {
            Some(TargetSnapshot::CollectionItem { collection, .. }) => {
                self.owner_of(collection).ok()
            }
            _ => None,
        };
        let (selected_data, selected_owner, selected_alive) = match selected {
            Some(selected) => match self
                .registry
                .lookup(source)
                .ok()
                .and_then(|record| record.refs.get(selected).cloned())
            {
                Some(RefTarget::Data(id)) => (
                    Some(id.clone()),
                    self.owner_of(&id).ok(),
                    self.container.borrow_any(&id).is_ok(),
                ),
                Some(RefTarget::CollectionItem { .. }) => (None, None, false),
                None => {
                    note(format!(
                        "selected position {:?} is not bound",
                        selected.seq()
                    ));
                    (None, None, false)
                }
            },
            None => (None, None, false),
        };
        let (state_target, state_pending) = match state {
            Some(state) => match self.lookup_state(state) {
                Ok(record) => (
                    record
                        .target
                        .clone()
                        .map(|target| TargetSnapshot::of(&target)),
                    Some(record.pending.clone()),
                ),
                Err(err) => {
                    note(format!("state lookup: {err:?}"));
                    (None, None)
                }
            },
            None => (None, None),
        };
        super::test_support::record_round_collect_pre_cleanup(
            super::test_support::RoundCollectPreCleanupSnapshot {
                phase,
                operation,
                source: Some(source.clone()),
                source_state,
                source_refs,
                source_owned,
                controller,
                controller_refs,
                controller_owned,
                selected_data,
                selected_target,
                selected_collection_owner,
                selected_owner,
                selected_alive,
                state_target,
                state_pending,
                next_data_id: self.next_data_id_probe(),
                observation_error: error,
            },
        );
    }

    /// cfg(test) 完整只读观察：`phase = Before` 在 prepare 之前，`AfterReject` 在 prepare
    /// 拒绝之后、内部 cleanup 之前。记录两侧完整 target-aware refs／owned（含身份）、
    /// collector 状态与被选输出责任方；观察失败以 `observation_error` 显式记录，不静默降级。
    #[cfg(test)]
    fn record_pre_cleanup_probe(
        &self,
        phase: super::test_support::ConsumeSnapshotPhase,
        item: &ScopeId,
        selected: &RefId,
        collector: &CollectorId,
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let item_state = self.snapshot_targets_probe(item);
        let (item_refs, item_owned) = match item_state {
            Ok((refs, owned)) => (Some(refs), Some(owned)),
            Err(err) => {
                note(format!("item snapshot: {err:?}"));
                (None, None)
            }
        };
        let parent = self
            .registry
            .lookup(item)
            .ok()
            .and_then(|record| record.parent.clone());
        let (parent_refs, parent_owned) = match parent.as_ref() {
            Some(parent) => match self.snapshot_targets_probe(parent) {
                Ok((refs, owned)) => (Some(refs), Some(owned)),
                Err(err) => {
                    note(format!("parent snapshot: {err:?}"));
                    (None, None)
                }
            },
            None => (None, None),
        };
        let selected_target = self
            .registry
            .lookup(item)
            .ok()
            .and_then(|record| record.refs.get(selected).cloned());
        let (selected_data, selected_owner, selected_alive) = match selected_target {
            Some(RefTarget::Data(id)) => (
                Some(id.clone()),
                self.owner_of(&id).ok(),
                self.container.borrow_any(&id).is_ok(),
            ),
            _ => (None, None, false),
        };
        let (collector_element, collector_owner, collector_moves) =
            match self.lookup_collector(collector) {
                Ok(record) => (
                    Some(record.element_name),
                    Some(record.owner.clone()),
                    self.container.collector_moves(collector).ok(),
                ),
                Err(err) => {
                    note(format!("collector lookup: {err:?}"));
                    (None, None, None)
                }
            };
        super::test_support::record_consume_pre_cleanup(
            super::test_support::ConsumePreCleanupSnapshot {
                phase,
                item_refs,
                item_owned,
                parent_refs,
                parent_owned,
                selected_data,
                selected_owner,
                selected_alive,
                collector_element,
                collector_owner,
                collector_moves,
                observation_error: error,
            },
        );
    }

    /// Consume 的冻结前置检查：Item 必须 Active 且没有活的 descendant。
    fn consume_item_early_check(&self, item: &ScopeId) -> Result<(), ScopeError> {
        let record = self.registry.lookup(item)?;
        record.require_active()?;
        if self.has_live_descendants(item)? {
            return Err(ScopeError::ActiveDescendants {
                scope: item.clone(),
            });
        }
        Ok(())
    }

    /// 在 `item_scope` 中绑定一个 CollectionItem 输入位置（item 创建入口）。
    ///
    /// 只接受 `caller`（EachScope）本地已绑定的完整 `Vec<T>` 集合：cap 设为刚建立的
    /// `item_scope`，不分配 DataId、不移动业务值、不增加 owner。`index` 必须在实际
    /// 长度内；目标位置必须尚未绑定。
    pub(crate) fn bind_item_input<T: Any>(
        &mut self,
        item_scope: &ScopeId,
        caller: &ScopeId,
        source: &RefId,
        target: &RefId,
        index: usize,
    ) -> Result<(), ScopeError> {
        {
            let item_record = self.registry.lookup(item_scope)?;
            item_record.require_active()?;
            self.registry.lookup(caller)?.require_active()?;
            if item_record.parent.as_ref() != Some(caller) {
                return Err(ScopeError::NotDirectParent {
                    child: item_scope.clone(),
                    caller: caller.clone(),
                });
            }
        }
        let collection = {
            let caller_record = self.registry.lookup(caller)?;
            let bound = caller_record
                .refs
                .get(source)
                .ok_or_else(|| ScopeError::RefNotBound {
                    scope: caller.clone(),
                    position: source.clone(),
                })?;
            let id = bound.require_data(source)?.clone();
            self.container
                .validate_type(&id, TypeId::of::<Vec<T>>(), type_name::<Vec<T>>())
                .map_err(|source_error| ScopeError::from_storage(source.clone(), source_error))?;
            self.require_visible_target(&id, caller)?;
            id
        };
        let access = ItemAccess::for_collection::<T>();
        let length = {
            let value = self
                .container
                .borrow_any(&collection)
                .map_err(|source_error| ScopeError::from_storage(source.clone(), source_error))?;
            value
                .downcast_ref::<Vec<T>>()
                .expect("validate_type checked the collection type")
                .len()
        };
        if index >= length {
            return Err(ScopeError::ItemIndexOutOfRange {
                position: target.clone(),
                index,
            });
        }
        {
            let item_record = self.registry.lookup(item_scope)?;
            if item_record.refs.contains_key(target) {
                return Err(ScopeError::RefAlreadyBound {
                    scope: item_scope.clone(),
                    position: target.clone(),
                });
            }
        }
        let item_record = self.registry.lookup_mut(item_scope)?;
        item_record.refs.insert(
            target.clone(),
            RefTarget::CollectionItem {
                collection,
                index,
                lifetime_cap: item_scope.clone(),
                access,
            },
        );
        Ok(())
    }

    /// 完成 collector：建构值成为新的普通 `Vec<O>` Data，登记控制器 owned 并一次性
    /// 绑定最终本地输出位置。
    ///
    /// 新的普通 `DataId` 在任何移除 collector 或绑定之前分配，因此序号耗尽时 collector
    /// 仍保持未完成、内容不变，控制器不出现新 ref／owned／普通 entry。最终集合的类型
    /// 由后续 Export 的声明输出位置校验。
    #[allow(dead_code)] // V21-08／V21-10 接入真实 Each／Root 前只由驱动与测试使用
    pub(crate) fn finish_collector(
        &mut self,
        controller: &ScopeId,
        collector: &CollectorId,
        position: &RefId,
    ) -> Result<DataId, ScopeError> {
        {
            let record = self.registry.lookup(controller)?;
            record.require_active()?;
            if record.refs.contains_key(position) {
                return Err(ScopeError::RefAlreadyBound {
                    scope: controller.clone(),
                    position: position.clone(),
                });
            }
        }
        if self.has_live_descendants(controller)? {
            return Err(ScopeError::ActiveDescendants {
                scope: controller.clone(),
            });
        }
        let record = self.lookup_collector(collector)?;
        if record.owner != *controller {
            return Err(ScopeError::CollectorNotOwnedBy {
                collector: collector.clone(),
                expected_owner: controller.clone(),
            });
        }
        self.require_collector_consistency(record)?;

        let id = self
            .container
            .finish_collector(collector)
            .map_err(|source| ScopeError::Storage { source })?;
        self.registry.collectors.remove(&collector.seq());
        let record = self.registry.lookup_mut(controller)?;
        record.owned.insert(id.clone());
        record
            .refs
            .insert(position.clone(), RefTarget::Data(id.clone()));
        Ok(id)
    }

    /// 正常退出：冻结、整组输出预检与提交、失效本地引用、清理剩余 owned、关闭。
    ///
    /// descendant 未全部关闭时拒绝，且不改变任何状态。预检失败走正式失败退出：先清理
    /// 整棵子树，再返回原失败诊断。
    pub(crate) fn finalize(
        &mut self,
        scope: &ScopeId,
        declared: &[RefId],
        outputs: &mut Vec<ExportSlot>,
    ) -> Result<(), ScopeError> {
        self.registry.lookup(scope)?.require_active()?;
        if self.has_live_descendants(scope)? {
            return Err(ScopeError::ActiveDescendants {
                scope: scope.clone(),
            });
        }
        self.registry.lookup_mut(scope)?.state = ScopeState::Finalizing;
        #[cfg(test)]
        self.record_export_probe(
            super::test_support::ExportSnapshotPhase::Before,
            scope,
            declared,
            outputs,
        );

        self.complete_exit(scope, declared, outputs)
    }

    /// 从 Finalizing 完成退出：整组输出预检 → 提交 → 失效引用 → 清理 → Closed。
    ///
    /// Finalizing 只允许内部输出预检、提交与退出清理，因此退出的后半段在这里完成；
    /// 业务操作与新建 child 都已在该状态被拒绝。预检失败走正式失败退出：先做整组责任
    /// 校验再清理（责任集合破坏时不销毁任何值），并返回原失败诊断。
    fn complete_exit(
        &mut self,
        scope: &ScopeId,
        declared: &[RefId],
        outputs: &mut Vec<ExportSlot>,
    ) -> Result<(), ScopeError> {
        // 本次 Invocation 的 slot 由预检一次性消费：调用后列表为空，不能再次提交。
        // `declared` 是 Definition 声明元数据，可复用于后续 Invocation，但每次都必须
        // 重新组装并重新校验本次 slot。
        let slots = std::mem::take(outputs);
        let caller = self.registry.lookup(scope)?.parent.clone();

        let prepared = match caller {
            Some(caller) => match self.prepare_export(scope, &caller, declared, &slots) {
                Ok(plan) => Some(plan),
                Err(error) => {
                    #[cfg(test)]
                    self.record_export_probe(
                        super::test_support::ExportSnapshotPhase::AfterReject,
                        scope,
                        declared,
                        &slots,
                    );
                    // 失败退出：清理前先做整组责任校验。若清理本身失败（例如 owned 中
                    // 已有失效 entry，或责任集合被破坏），必须让调用方看到清理诊断——
                    // 它表示清理**未完成**、来源 Scope 仍是 Finalizing；此时优先传播
                    // 清理错误，而不是只返回普通 Export 错误。清理成功才返回原诊断。
                    self.cleanup_subtree(scope)?;
                    return Err(error);
                }
            },
            None => {
                if !declared.is_empty() || !slots.is_empty() {
                    return Err(ScopeError::RootOutputNotSupported {
                        scope: scope.clone(),
                    });
                }
                None
            }
        };

        // 剩余 owned 的清理前提必须在输出提交前校验，避免导出后才发现临时值失效；
        // 真正销毁的集合在提交之后按最新 owned 重新计算（已转交的值不再属于本 Scope）。
        self.cleanup_targets(scope)?;

        if let Some(plan) = prepared {
            self.commit_export(plan)?;
        }
        self.close_scope(scope)
    }

    /// 同步失败清理入口：从最深 descendant 开始清理整棵子树。
    pub(crate) fn abort(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
        self.cleanup_subtree(scope)
    }

    /// Export 预检：核对声明元数据与本次 slot，并验证全部可恢复条件；不做任何状态变更。
    fn prepare_export(
        &self,
        child: &ScopeId,
        caller: &ScopeId,
        declared: &[RefId],
        outputs: &[ExportSlot],
    ) -> Result<PreparedExport, ScopeError> {
        let child_record = self.registry.lookup(child)?;
        match child_record.state {
            ScopeState::Active | ScopeState::Finalizing => {}
            ScopeState::Closed => {
                return Err(ScopeError::ScopeClosed {
                    scope: child.clone(),
                });
            }
        }
        if child_record.parent.as_ref() != Some(caller) {
            return Err(ScopeError::NotDirectParent {
                child: child.clone(),
                caller: caller.clone(),
            });
        }
        let caller_record = self.registry.lookup(caller)?;
        caller_record.require_active()?;

        // 位置／数量契约：声明元数据独立于本次 slot 列表核对，缺项或多项都必须在这里
        // 被识别，不能依赖 caller 位置的绑定状态。
        for (index, position) in declared.iter().enumerate() {
            if declared[..index].iter().any(|seen| seen == position) {
                return Err(ScopeError::DuplicatePosition {
                    position: position.clone(),
                });
            }
        }
        if declared.len() != outputs.len() {
            return Err(ScopeError::OutputCountMismatch {
                scope: child.clone(),
                declared: declared.len(),
                supplied: outputs.len(),
            });
        }
        for (declared_position, slot) in declared.iter().zip(outputs) {
            if declared_position != &slot.child {
                return Err(ScopeError::OutputPositionMismatch {
                    scope: child.clone(),
                    declared: declared_position.clone(),
                    supplied: slot.child.clone(),
                });
            }
        }

        let mut bindings: Vec<(RefId, RefTarget)> = Vec::with_capacity(outputs.len());
        let mut transferred: Vec<DataId> = Vec::new();
        let mut caller_positions: Vec<RefId> = Vec::with_capacity(outputs.len());

        for slot in outputs {
            let target =
                child_record
                    .refs
                    .get(&slot.child)
                    .ok_or_else(|| ScopeError::RefNotBound {
                        scope: child.clone(),
                        position: slot.child.clone(),
                    })?;
            match target {
                RefTarget::Data(id) => {
                    self.container
                        .validate_type(id, slot.expected, slot.expected_name)
                        .map_err(|source| ScopeError::from_storage(slot.child.clone(), source))?;
                }
                RefTarget::CollectionItem { .. } => {
                    // item 不能逃出 cap：来源 child 与目的 caller 都必须位于 cap 内。
                    let (_, _, cap, _) = target.item().expect("item target matched by variant");
                    self.check_item(&slot.child, target, slot.expected, slot.expected_name)?;
                    self.require_in_cap(&slot.child, cap, child)?;
                    self.require_destination_in_cap(&slot.child, cap, caller)?;
                }
            }
            if caller_record.refs.contains_key(&slot.caller) {
                return Err(ScopeError::RefAlreadyBound {
                    scope: caller.clone(),
                    position: slot.caller.clone(),
                });
            }
            if caller_positions.iter().any(|seen| seen == &slot.caller) {
                return Err(ScopeError::DuplicatePosition {
                    position: slot.caller.clone(),
                });
            }
            caller_positions.push(slot.caller.clone());

            match target {
                RefTarget::Data(id) => {
                    let owner = self.owner_of(id)?;
                    if owner == *child {
                        // 同一 DataId 出现在多个输出位置时只转移一次责任。
                        if !transferred.iter().any(|moved| moved == id) {
                            transferred.push(id.clone());
                        }
                    } else if !self.is_ancestor_or_self(&owner, caller) {
                        return Err(ScopeError::IllegalOwner {
                            id: id.clone(),
                            owner,
                            boundary: child.clone(),
                        });
                    }
                }
                RefTarget::CollectionItem { .. } => {
                    // item 是 alias 目标：不转移来源集合责任，也不产生第二个 owner。
                }
            }
            bindings.push((slot.caller.clone(), target.clone()));
        }

        Ok(PreparedExport {
            child: child.clone(),
            caller: caller.clone(),
            bindings,
            transferred,
        })
    }

    /// Export 提交段：只做绑定与责任转移，不含可恢复失败分支。
    fn commit_export(&mut self, plan: PreparedExport) -> Result<(), ScopeError> {
        let PreparedExport {
            child,
            caller,
            bindings,
            transferred,
        } = plan;
        {
            let caller_record = self.registry.lookup_mut(&caller)?;
            for (position, target) in bindings {
                caller_record.refs.insert(position, target);
            }
        }
        {
            let child_record = self.registry.lookup_mut(&child)?;
            for id in &transferred {
                child_record.owned.remove(id);
            }
        }
        let caller_record = self.registry.lookup_mut(&caller)?;
        for id in transferred {
            caller_record.owned.insert(id);
        }
        Ok(())
    }

    /// caller 本地位置的绑定目标；未绑定即拒绝。
    fn local_import_target(
        &self,
        caller: &ScopeId,
        source: &RefId,
    ) -> Result<RefTarget, ScopeError> {
        self.registry
            .lookup(caller)?
            .refs
            .get(source)
            .cloned()
            .ok_or_else(|| ScopeError::RefNotBound {
                scope: caller.clone(),
                position: source.clone(),
            })
    }

    /// child 目标位置的单赋值与批内重复检查。
    ///
    /// 顺序与原实现一致：先判"已绑定"，再判"同一批内重复"，因此两种诊断可区分。
    fn check_child_position(
        &self,
        child: &ScopeId,
        position: &RefId,
        planned: &[(RefId, RefTarget)],
    ) -> Result<(), ScopeError> {
        if self.registry.lookup(child)?.refs.contains_key(position) {
            return Err(ScopeError::RefAlreadyBound {
                scope: child.clone(),
                position: position.clone(),
            });
        }
        if planned.iter().any(|(seen, _)| seen == position) {
            return Err(ScopeError::DuplicatePosition {
                position: position.clone(),
            });
        }
        Ok(())
    }

    /// 是否还有未关闭的 descendant。
    ///
    /// 正常退出入口在冻结与任何 mutation 之前使用该判据；Closed tombstone 不算存活。
    fn has_live_descendants(&self, scope: &ScopeId) -> Result<bool, ScopeError> {
        let record = self.registry.lookup(scope)?;
        Ok(record.children.iter().any(|child| {
            self.registry
                .scopes
                .get(&child.seq())
                .is_some_and(|record| record.state != ScopeState::Closed)
        }))
    }

    /// 是否有任何存活 Scope 的本地引用指向该 DataId。
    ///
    /// 回收判据的一部分：Controller 自己的本地 alias 同样计入，因此不会为了回收而
    /// 删除仍在使用的引用。已关闭 Scope 的 refs 已清空，不再计入。
    fn has_live_reference(&self, id: &DataId) -> bool {
        self.registry.scopes.values().any(|record| {
            record
                .refs
                .values()
                .any(|target| matches!(target.data_id(), Some(bound) if bound == id))
        })
    }

    /// 是否有任何控制状态仍把该 DataId 作为当前 target 保留。
    fn state_holds(&self, id: &DataId) -> bool {
        self.registry.states.values().any(|state| {
            state
                .target
                .as_ref()
                .is_some_and(|target| matches!(target.data_id(), Some(bound) if bound == id))
        })
    }

    /// 控制状态句柄的归属与存在检查。
    fn lookup_state(&self, state: &ControlStateId) -> Result<&ControlState, ScopeError> {
        if !Arc::ptr_eq(state.owner().execution(), &self.registry.execution) {
            return Err(ScopeError::StateForeignExecution {
                state: state.clone(),
            });
        }
        let record = self.registry.lookup(state.owner())?;
        if record.state == ScopeState::Closed {
            return Err(ScopeError::ScopeClosed {
                scope: state.owner().clone(),
            });
        }
        self.registry
            .states
            .get(&(state.owner().seq(), state.seq()))
            .ok_or_else(|| ScopeError::StateNotRegistered {
                state: state.clone(),
            })
    }

    /// CollectorId 的归属与存在检查。
    ///
    /// 已完成或已清理的 collector 记录已被移除，因此旧句柄得到"不存在／不可用"诊断，
    /// 不会重新定位到新值。
    fn lookup_collector(&self, collector: &CollectorId) -> Result<&CollectorRecord, ScopeError> {
        if !Arc::ptr_eq(collector.execution(), &self.registry.execution) {
            return Err(ScopeError::CollectorForeignExecution {
                collector: collector.clone(),
            });
        }
        let record = self
            .registry
            .collectors
            .get(&collector.seq())
            .ok_or_else(|| ScopeError::CollectorNotRegistered {
                collector: collector.clone(),
            })?;
        // 查询句柄必须与登记身份一致：否则句柄会"重定向"到另一个 collector 的责任与
        // 物理建构值，Consume／完成／清理都可能作用在错误的身份上。
        if record.id != *collector {
            return Err(ScopeError::Invariant {
                violated: "collector handle must equal the recorded registration identity",
            });
        }
        Ok(record)
    }

    /// 按登记键取出 collector 记录，并校验键、记录身份与来源 Execution 一致。
    ///
    /// 登记键是唯一可信的定位与删除依据：记录中的 `id` 只有与键一致时才是可信身份。
    /// 清理遍历必须经本方法，不能把未经验证的 `record.id` 当作删除键——否则身份故障
    /// 会把另一 Scope 的 collector 当成本 Scope 的值销毁，并按错误序号撤销登记。
    fn validated_collector(&self, key: u64) -> Result<&CollectorRecord, ScopeError> {
        let record = self
            .registry
            .collectors
            .get(&key)
            .ok_or(ScopeError::Invariant {
                violated: "collector registration key must exist while it is iterated",
            })?;
        if record.id.seq() != key || !Arc::ptr_eq(record.id.execution(), &self.registry.execution) {
            return Err(ScopeError::Invariant {
                violated: "collector registration key and recorded identity must agree",
            });
        }
        Ok(record)
    }

    /// 登记一个控制状态位置；序号单调、checked 耗尽、不复用。
    fn register_state_record(
        &mut self,
        controller: &ScopeId,
        expected: TypeId,
        expected_name: &'static str,
        target: Option<RefTarget>,
    ) -> Result<ControlStateId, ScopeError> {
        let owner = controller.clone();
        let seq = {
            let record = self.registry.lookup_mut(controller)?;
            record.require_active()?;
            let seq = record.state_next;
            record.state_next = seq.checked_add(1).ok_or(ScopeError::Storage {
                source: InternalError::IdSpaceExhausted {
                    kind: IdKind::ControlState,
                },
            })?;
            seq
        };
        self.registry.states.insert(
            (owner.seq(), seq),
            ControlState {
                owner: owner.clone(),
                target,
                expected,
                expected_name,
                pending: Vec::new(),
            },
        );
        Ok(ControlStateId { owner, seq })
    }

    /// cfg(test) Export 收口的完整前后观察：来源／caller 双侧 refs／owned、slot 身份、
    /// caller 冲突位置的目标／owner／存活、caller 状态与下一个 `DataId`。
    #[cfg(test)]
    pub(crate) fn record_export_probe(
        &self,
        phase: super::test_support::ExportSnapshotPhase,
        child: &ScopeId,
        declared: &[RefId],
        outputs: &[ExportSlot],
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let caller = self
            .registry
            .lookup(child)
            .ok()
            .and_then(|r| r.parent.clone());
        let (child_refs, child_owned) = match self.snapshot_targets_probe(child) {
            Ok((refs, owned)) => (Some(refs), Some(owned)),
            Err(err) => {
                note(format!("child snapshot: {err:?}"));
                (None, None)
            }
        };
        let (caller_refs, caller_owned) = match caller.as_ref() {
            Some(caller) => match self.snapshot_targets_probe(caller) {
                Ok((refs, owned)) => (Some(refs), Some(owned)),
                Err(err) => {
                    note(format!("caller snapshot: {err:?}"));
                    (None, None)
                }
            },
            None => (None, None),
        };
        let slots: Vec<(RefId, RefId, &'static str)> = outputs
            .iter()
            .map(|slot| (slot.child.clone(), slot.caller.clone(), slot.expected_name))
            .collect();
        let conflict = outputs
            .iter()
            .find(|slot| {
                caller
                    .as_ref()
                    .and_then(|caller| self.registry.lookup(caller).ok())
                    .is_some_and(|record| record.refs.contains_key(&slot.caller))
            })
            .map(|slot| slot.caller.clone());
        let conflict_target = match conflict.as_ref() {
            Some(position) => caller
                .as_ref()
                .and_then(|caller| self.registry.lookup(caller).ok())
                .and_then(|record| record.refs.get(position).cloned())
                .map(|target| TargetSnapshot::of(&target)),
            None => None,
        };
        let (conflict_owner, conflict_alive) = match conflict_target.as_ref() {
            Some(TargetSnapshot::Data(id)) => (
                self.owner_of(id).ok(),
                self.container.borrow_any(id).is_ok(),
            ),
            _ => (None, false),
        };
        let caller_state = caller
            .as_ref()
            .and_then(|caller| self.registry.lookup(caller).ok())
            .map(|record| record.state);
        super::test_support::record_export_pre_cleanup(
            super::test_support::ExportPreCleanupSnapshot {
                phase,
                child: Some(child.clone()),
                caller,
                child_refs,
                child_owned,
                caller_refs,
                caller_owned,
                slots,
                conflict_target,
                conflict_owner,
                conflict_alive,
                caller_state,
                next_data_id: self.next_data_id_probe(),
                observation_error: error,
            },
        );
        let _ = declared;
    }

    /// cfg(test) 只读：控制器所有控制状态的待回收旧值（按登记顺序合并）。
    #[cfg(test)]
    pub(crate) fn pending_probe(&self, controller: &ScopeId) -> Option<Vec<DataId>> {
        let mut pending: Vec<DataId> = Vec::new();
        for state in self.registry.states.values() {
            if state.owner == *controller {
                pending.extend(state.pending.iter().cloned());
            }
        }
        Some(pending)
    }

    /// cfg(test) 最终绑定的完整前后观察：控制器 refs／owned／状态、控制状态 target／pending、
    /// 目标位置当前身份／owner／存活与下一个 `DataId` 序号；观察失败显式记录。
    #[cfg(test)]
    pub(crate) fn record_final_bind_probe(
        &self,
        phase: super::test_support::FinalBindSnapshotPhase,
        controller: &ScopeId,
        state: &ControlStateId,
        position: &RefId,
    ) {
        let mut error: Option<String> = None;
        let mut note = |message: String| {
            if error.is_none() {
                error = Some(message);
            }
        };
        let controller_state = self
            .registry
            .lookup(controller)
            .ok()
            .map(|record| record.state);
        let (controller_refs, controller_owned) = match self.snapshot_targets_probe(controller) {
            Ok((refs, owned)) => (Some(refs), Some(owned)),
            Err(err) => {
                note(format!("controller snapshot: {err:?}"));
                (None, None)
            }
        };
        let (state_target, state_pending) = match self.lookup_state(state) {
            Ok(record) => (
                record
                    .target
                    .clone()
                    .map(|target| TargetSnapshot::of(&target)),
                Some(record.pending.clone()),
            ),
            Err(err) => {
                note(format!("state lookup: {err:?}"));
                (None, None)
            }
        };
        let bound = self
            .registry
            .lookup(controller)
            .ok()
            .and_then(|record| record.refs.get(position).cloned());
        let position_target = bound.as_ref().map(TargetSnapshot::of);
        let (position_data, position_owner, position_alive) = match bound {
            Some(RefTarget::Data(id)) => (
                Some(id.clone()),
                self.owner_of(&id).ok(),
                self.container.borrow_any(&id).is_ok(),
            ),
            Some(RefTarget::CollectionItem { .. }) => (None, None, false),
            None => (None, None, false),
        };
        super::test_support::record_final_bind_pre_cleanup(
            super::test_support::FinalBindPreCleanupSnapshot {
                phase,
                controller: Some(controller.clone()),
                controller_state,
                controller_refs,
                controller_owned,
                state_target,
                state_pending,
                position_data,
                position_target,
                position_owner,
                position_alive,
                next_data_id: self.next_data_id_probe(),
                observation_error: error,
            },
        );
    }

    /// cfg(test) 窄故障：把控制状态改回"未初始化"（最终绑定前置反例）。
    #[cfg(test)]
    pub(crate) fn uninitialize_state_probe(
        &mut self,
        state: &ControlStateId,
    ) -> Result<(), ScopeError> {
        let key = (state.owner().seq(), state.seq());
        let record =
            self.registry
                .states
                .get_mut(&key)
                .ok_or_else(|| ScopeError::StateNotRegistered {
                    state: state.clone(),
                })?;
        record.target = None;
        Ok(())
    }

    /// cfg(test) 窄故障：把控制状态 target 的声明类型改为指定元数据。
    #[cfg(test)]
    pub(crate) fn corrupt_state_type_probe(
        &mut self,
        state: &ControlStateId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Result<(), ScopeError> {
        let key = (state.owner().seq(), state.seq());
        let record =
            self.registry
                .states
                .get_mut(&key)
                .ok_or_else(|| ScopeError::StateNotRegistered {
                    state: state.clone(),
                })?;
        record.expected = expected;
        record.expected_name = expected_name;
        Ok(())
    }

    /// cfg(test) 窄故障：把一个**本地位置**上的 item target 的 cap 换成指定 Scope。
    #[cfg(test)]
    pub(crate) fn corrupt_item_cap_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        cap: ScopeId,
    ) -> Result<(), ScopeError> {
        let record = self.registry.lookup_mut(scope)?;
        match record.refs.get_mut(position) {
            Some(RefTarget::CollectionItem { lifetime_cap, .. }) => {
                *lifetime_cap = cap;
                Ok(())
            }
            _ => Err(ScopeError::Invariant {
                violated: "item cap fault requires a collection item target",
            }),
        }
    }

    /// cfg(test) 窄故障：把控制状态 item target 的 cap 换成指定 Scope。
    #[cfg(test)]
    pub(crate) fn corrupt_state_cap_probe(
        &mut self,
        state: &ControlStateId,
        cap: ScopeId,
    ) -> Result<(), ScopeError> {
        let key = (state.owner().seq(), state.seq());
        let record =
            self.registry
                .states
                .get_mut(&key)
                .ok_or_else(|| ScopeError::StateNotRegistered {
                    state: state.clone(),
                })?;
        match record.target.as_mut() {
            Some(RefTarget::CollectionItem { lifetime_cap, .. }) => {
                *lifetime_cap = cap;
                Ok(())
            }
            _ => Err(ScopeError::Invariant {
                violated: "state cap fault requires a collection item target",
            }),
        }
    }

    /// cfg(test) 窄构造：把一个已存活 DataId 作为**真实本地 alias** 绑定到该 Scope 的位置。
    ///
    /// 只用于构造"旧值仍被合法别名引用"的延迟回收条件；要求该 DataId 的 owner 是本 Scope
    /// 的祖先或自身，否则拒绝（不制造跨边界 alias）。
    #[cfg(test)]
    pub(crate) fn bind_alias_probe(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        id: &DataId,
    ) -> Result<(), ScopeError> {
        self.registry.lookup(scope)?.require_active()?;
        let owner = self.owner_of(id)?;
        if !self.is_ancestor_or_self(&owner, scope) {
            return Err(ScopeError::IllegalOwner {
                id: id.clone(),
                owner,
                boundary: scope.clone(),
            });
        }
        self.registry
            .lookup_mut(scope)?
            .refs
            .insert(position.clone(), RefTarget::Data(id.clone()));
        Ok(())
    }

    /// 测试夹具：把一个控制器 Scope 的状态位置序号设为近上限起点。
    ///
    /// 只用于验证控制状态序号的 checked 耗尽；不提供重置、回退或复用能力，也不改写
    /// 已登记的状态记录。
    #[cfg(test)]
    pub(crate) fn with_state_start(
        &mut self,
        controller: &ScopeId,
        start: u64,
    ) -> Result<(), ScopeError> {
        self.registry.lookup_mut(controller)?.state_next = start;
        Ok(())
    }

    /// Promote 预检：不做任何状态变更，失败时调用方走正式失败退出。
    fn prepare_promote(
        &self,
        source: &ScopeId,
        selected: &RefId,
        state: &ControlStateId,
    ) -> Result<PreparedPromote, ScopeError> {
        let source_record = self.registry.lookup(source)?;
        match source_record.state {
            ScopeState::Active | ScopeState::Finalizing => {}
            ScopeState::Closed => {
                return Err(ScopeError::ScopeClosed {
                    scope: source.clone(),
                });
            }
        }
        let parent = source_record
            .parent
            .clone()
            .ok_or_else(|| ScopeError::StateWrongOwner {
                state: state.clone(),
                expected_owner: source.clone(),
            })?;

        let state_record = self.lookup_state(state)?;
        if state_record.owner != parent {
            return Err(ScopeError::StateWrongOwner {
                state: state.clone(),
                expected_owner: parent.clone(),
            });
        }
        let controller_record = self.registry.lookup(&parent)?;
        controller_record.require_active()?;

        let target =
            source_record
                .refs
                .get(selected)
                .cloned()
                .ok_or_else(|| ScopeError::RefNotBound {
                    scope: source.clone(),
                    position: selected.clone(),
                })?;
        let transferred = match &target {
            RefTarget::Data(id) => {
                self.container
                    .validate_type(id, state_record.expected, state_record.expected_name)
                    .map_err(|source_error| {
                        ScopeError::from_storage(selected.clone(), source_error)
                    })?;
                let owner = self.owner_of(id)?;
                if owner == *source {
                    true
                } else if self.is_ancestor_or_self(&owner, &parent) {
                    false
                } else {
                    return Err(ScopeError::IllegalOwner {
                        id: id.clone(),
                        owner,
                        boundary: source.clone(),
                    });
                }
            }
            RefTarget::CollectionItem { .. } => {
                let (_, _, cap, _) = target.item().expect("item target matched by variant");
                // 来源与目的都必须位于 cap 内。
                self.require_in_cap(selected, cap, source)?;
                self.require_in_cap(selected, cap, &parent)?;
                self.check_item(
                    selected,
                    &target,
                    state_record.expected,
                    state_record.expected_name,
                )?;
                // item 没有独立 owner：不转移任何责任。
                false
            }
        };

        // 被替换的旧状态：只有仍由该控制器负责的值才进入待回收登记；imported 值保持
        // 原 owner。这里只做判定，不改状态。
        let replaced_pending = match (state_record.target.clone(), target.data_id()) {
            (Some(RefTarget::Data(old)), Some(new)) if old != *new => {
                if self.owner_of(&old)? == parent {
                    Some(old)
                } else {
                    None
                }
            }
            _ => None,
        };

        // 剩余 owned 的清理前提必须在提交前校验。
        self.cleanup_targets(source)?;

        Ok(PreparedPromote {
            source: source.clone(),
            controller: parent,
            state_key: (state.owner().seq(), state.seq()),
            target,
            transferred,
            replaced_pending,
        })
    }

    /// Promote 提交段：更新状态、必要时转移责任、关闭来源、登记待回收旧值。
    ///
    /// 提交段没有可恢复失败分支：预检（含清理前提）已全部完成。
    fn commit_promote(&mut self, plan: PreparedPromote) -> Result<(), ScopeError> {
        if plan.transferred {
            let id = plan
                .target
                .data_id()
                .expect("promote transfers responsibility only for complete Data targets");
            self.registry.lookup_mut(&plan.source)?.owned.remove(id);
            self.registry
                .lookup_mut(&plan.controller)?
                .owned
                .insert(id.clone());
        }
        {
            let state = self
                .registry
                .states
                .get_mut(&plan.state_key)
                .expect("promote plan came from this registry");
            state.target = Some(plan.target.clone());
            if let Some(old) = plan.replaced_pending
                && !state.pending.iter().any(|pending| pending == &old)
            {
                state.pending.push(old);
            }
        }
        self.close_scope(&plan.source)
    }

    /// Consume 预检：不做任何状态变更，失败时调用方走正式失败退出。
    fn prepare_consume(
        &self,
        item: &ScopeId,
        selected: &RefId,
        collector: &CollectorId,
    ) -> Result<PreparedConsume, ScopeError> {
        let item_record = self.registry.lookup(item)?;
        match item_record.state {
            ScopeState::Active | ScopeState::Finalizing => {}
            ScopeState::Closed => {
                return Err(ScopeError::ScopeClosed {
                    scope: item.clone(),
                });
            }
        }
        let parent = item_record
            .parent
            .clone()
            .ok_or_else(|| ScopeError::CollectorNotOwnedBy {
                collector: collector.clone(),
                expected_owner: item.clone(),
            })?;
        self.registry.lookup(&parent)?.require_active()?;

        let collector_record = self.lookup_collector(collector)?;
        if collector_record.owner != parent {
            return Err(ScopeError::CollectorNotOwnedBy {
                collector: collector.clone(),
                expected_owner: parent.clone(),
            });
        }
        self.require_collector_consistency(collector_record)?;

        let target =
            item_record
                .refs
                .get(selected)
                .cloned()
                .ok_or_else(|| ScopeError::RefNotBound {
                    scope: item.clone(),
                    position: selected.clone(),
                })?;
        let Some(data_id) = target.data_id().cloned() else {
            // CollectionItem 不是完整 owned Data：在任何内部取值前拒绝。
            return Err(ScopeError::NonCompleteTarget {
                position: selected.clone(),
            });
        };
        self.container
            .validate_type(
                &data_id,
                collector_record.element_type,
                collector_record.element_name,
            )
            .map_err(|source| ScopeError::from_storage(selected.clone(), source))?;

        if !item_record.owned.contains(&data_id) {
            return Err(ScopeError::IllegalOwner {
                id: data_id.clone(),
                owner: self.owner_of(&data_id)?,
                boundary: item.clone(),
            });
        }
        if self.owner_of(&data_id)? != *item {
            return Err(ScopeError::IllegalOwner {
                id: data_id.clone(),
                owner: self.owner_of(&data_id)?,
                boundary: item.clone(),
            });
        }

        // 剩余 owned 的清理前提必须在移动前校验。
        self.cleanup_targets(item)?;

        Ok(PreparedConsume {
            item: item.clone(),
            target,
            collector: collector.clone(),
        })
    }

    /// Consume 提交段：Container 内部移动 → 撤销 ItemScope 责任 → 清理并关闭 ItemScope。
    ///
    /// 预检已确认目标为 ItemScope 唯一负责的完整 Data 且元素类型相符，因此移动段没有
    /// 可恢复失败分支；容器诊断只用于报告不变量破坏。
    fn commit_consume(&mut self, plan: PreparedConsume) -> Result<(), ScopeError> {
        let data_id = plan
            .target
            .data_id()
            .expect("prepare_consume only plans complete Data targets");
        self.container
            .move_into_collector(&plan.collector, data_id)
            .map_err(|source| ScopeError::Storage { source })?;
        self.registry.lookup_mut(&plan.item)?.owned.remove(data_id);
        self.close_scope(&plan.item)
    }

    /// 一个 DataId 的唯一责任 Scope。
    ///
    /// 直接扫描各 Scope 的 owned 集合：没有第二份索引，也就不存在索引不同步的问题；
    /// 出现两个责任方按不变量破坏诊断，不静默择一。
    fn owner_of(&self, id: &DataId) -> Result<ScopeId, ScopeError> {
        let mut owner: Option<ScopeId> = None;
        for record in self.registry.scopes.values() {
            if record.owned.contains(id) {
                if owner.is_some() {
                    return Err(ScopeError::DuplicateOwner { id: id.clone() });
                }
                owner = Some(record.id.clone());
            }
        }
        owner.ok_or_else(|| ScopeError::NoOwner { id: id.clone() })
    }

    /// `candidate` 是否是 `scope` 自身或其祖先。
    fn is_ancestor_or_self(&self, candidate: &ScopeId, scope: &ScopeId) -> bool {
        let mut cursor = Some(scope.clone());
        while let Some(current) = cursor {
            if current == *candidate {
                return true;
            }
            cursor = self
                .registry
                .scopes
                .get(&current.seq())
                .and_then(|record| record.parent.clone());
        }
        false
    }

    /// 目标必须由 `scope` 自身或其后代可见的责任方拥有。
    ///
    /// 业务读取与责任处置共用同一条可见性判据：sibling-owned、无 owner 与重复 owner
    /// 都在返回业务借用之前被拒绝。
    fn require_visible_target(&self, id: &DataId, scope: &ScopeId) -> Result<(), ScopeError> {
        let owner = self.owner_of(id)?;
        if !self.is_ancestor_or_self(&owner, scope) {
            return Err(ScopeError::IllegalOwner {
                id: id.clone(),
                owner,
                boundary: scope.clone(),
            });
        }
        Ok(())
    }

    /// 后序清理整棵子树：最深 descendant 先失效并清理。
    fn cleanup_subtree(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
        let children = self.registry.lookup(scope)?.children.clone();
        for child in children {
            self.cleanup_subtree(&child)?;
        }
        self.close_scope(scope)
    }

    /// 关闭一个 Scope：先整组校验待清理集合，再撤销引用、销毁 owned 并保留 tombstone。
    fn close_scope(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
        let targets = self.cleanup_targets(scope)?;
        self.close_validated(scope, targets)
    }

    /// 校验待清理集合：owned ID 的存活与唯一责任，以及本 Scope 未完成 collector 的
    /// 身份、物理建构值存活与登记类型一致性。
    ///
    /// 这是 Export／Promote／Consume／正常退出／abort 共用的**提交前**清理前提检查：
    /// 责任集合或 collector 登记被破坏时返回诊断且**不授予任何销毁权**，调用方在任何
    /// 绑定、责任转移、状态更新或移动之前完成校验，因此不会清掉其它 Scope 或 ancestor
    /// 的值，也不会在发现故障前撤销 collector 登记。
    fn cleanup_targets(&self, scope: &ScopeId) -> Result<Vec<DataId>, ScopeError> {
        let record = self.registry.lookup(scope)?;
        let mut targets = Vec::with_capacity(record.owned.len());
        for id in &record.owned {
            if self.container.validate(id).is_err() {
                return Err(ScopeError::Invariant {
                    violated: "owned entry must exist until its scope closes",
                });
            }
            if self.owner_of(id)? != *scope {
                return Err(ScopeError::Invariant {
                    violated: "owned data must be uniquely owned by the closing scope",
                });
            }
            targets.push(id.clone());
        }
        let collector_keys: Vec<u64> = self
            .registry
            .collectors
            .iter()
            .filter(|(_, record)| record.owner == *scope)
            .map(|(key, _)| *key)
            .collect();
        for key in collector_keys {
            let collector = self.validated_collector(key)?;
            self.require_collector_consistency(collector)?;
        }
        Ok(targets)
    }

    /// 登记与物理建构值的一致性检查。
    ///
    /// collector 必须在 Container 的建构区真实存在（未完成状态），且其真实元素
    /// `TypeId` 与 registry 登记一致。不一致属于内部不变量破坏：Consume／完成在提交前
    /// 拒绝，Scope 退出在撤销登记之前拒绝。容器自身的防御性校验仍然保留，但 Scope 层
    /// 不能依赖它来代替提交前预检。
    fn require_collector_consistency(&self, collector: &CollectorRecord) -> Result<(), ScopeError> {
        match self.container.collector_element_type(&collector.id) {
            Ok((element_type, element_name))
                if element_type == collector.element_type
                    && element_name == collector.element_name =>
            {
                Ok(())
            }
            Ok(_) => Err(ScopeError::Invariant {
                violated: "collector element type differs between registry and container building value",
            }),
            Err(_) => Err(ScopeError::Invariant {
                violated: "registered collector has no live building value",
            }),
        }
    }

    /// 执行清理：撤销本地引用 → 销毁已校验的 owned → 保留 tombstone。
    ///
    /// 只接受 [`Self::cleanup_targets`] 的校验结果，因此销毁阶段没有可恢复失败分支。
    fn close_validated(&mut self, scope: &ScopeId, targets: Vec<DataId>) -> Result<(), ScopeError> {
        let parent = self.registry.lookup(scope)?.parent.clone();

        self.registry.lookup_mut(scope)?.refs.clear();
        for id in targets {
            if self.container.destroy(&id).is_err() {
                return Err(ScopeError::Invariant {
                    violated: "validated owned entry disappeared before destruction",
                });
            }
        }

        // 未完成 collector 与控制状态随控制器 Scope 一起撤销：旧句柄此后只能得到
        // Closed／不存在诊断，不能读取、追加或重新激活。待回收旧值与 collector 的
        // 部分结果都在本 Scope 的 owned／建构区中，按同一条入口一并析构。
        // 顺序为"先销毁物理建构值，成功后才撤销登记"：任何失败都不留下"登记已消失、
        // 建构值仍在"的中间状态。
        let collector_keys: Vec<u64> = self
            .registry
            .collectors
            .iter()
            .filter(|(_, record)| record.owner == *scope)
            .map(|(key, _)| *key)
            .collect();
        for key in collector_keys {
            // 键与记录身份已由 `cleanup_targets` 的预检校验；这里再核对一次，
            // 并按同一个键撤销登记，保证不会删到祖先／兄弟的 collector。
            let collector = self.validated_collector(key)?.id.clone();
            if self.container.destroy_collector(&collector).is_err() {
                return Err(ScopeError::Invariant {
                    violated: "registered collector disappeared before destruction",
                });
            }
            self.registry.collectors.remove(&key);
        }
        self.registry
            .states
            .retain(|(owner_seq, _), _| *owner_seq != scope.seq());

        {
            let record = self.registry.lookup_mut(scope)?;
            record.owned.clear();
            record.children.clear();
            record.state = ScopeState::Closed;
        }
        if let Some(parent) = parent
            && let Some(parent_record) = self.registry.scopes.get_mut(&parent.seq())
        {
            parent_record.children.retain(|child| child != scope);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::any::{Any, TypeId};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{
        CollectorRecord, ControlState, ControlStateId, ExportSlot, ImportSlot, RefTarget, Scope,
        ScopeCoordinator, ScopeRegistry, ScopeState, StateImportSlot,
    };
    use crate::core::identity::{
        CollectorId, DataId, ExecutionIdentity, ScopeId, ScopeIdAllocator,
    };
    use crate::core::internal_error::{IdKind, InternalError, ScopeError};
    use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};

    /// 非 Clone 业务值，带 Drop 观测。
    struct Tracked {
        num: u32,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// 另一类型，用于类型不符与异构多输出。
    struct Other(u32);

    struct Fixture {
        coordinator: ScopeCoordinator,
        ids: RefIdAllocator,
        drops: Arc<AtomicUsize>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                coordinator: ScopeCoordinator::new(ExecutionIdentity::new()),
                ids: RefIdAllocator::new(RefIdSource::new()),
                drops: Arc::new(AtomicUsize::new(0)),
            }
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

        fn root(&self) -> ScopeId {
            self.coordinator.root()
        }

        fn drops(&self) -> usize {
            self.drops.load(Ordering::SeqCst)
        }

        /// 以单输出声明调用正常退出：声明位置即该 slot 的 child 位置。
        fn finalize_one<T: Any>(
            &mut self,
            scope: &ScopeId,
            child: &RefId,
            caller: &RefId,
        ) -> Result<(), ScopeError> {
            let mut slots = vec![ExportSlot::new::<T>(child, caller)];
            self.coordinator
                .finalize(scope, std::slice::from_ref(child), &mut slots)
        }

        /// 以显式多输出声明调用正常退出。
        fn finalize_many(
            &mut self,
            scope: &ScopeId,
            declared: &[RefId],
            mut slots: Vec<ExportSlot>,
        ) -> Result<(), ScopeError> {
            self.coordinator.finalize(scope, declared, &mut slots)
        }

        /// 以显式无输出调用正常退出。
        fn finalize_none(&mut self, scope: &ScopeId) -> Result<(), ScopeError> {
            let mut slots: Vec<ExportSlot> = Vec::new();
            self.coordinator.finalize(scope, &[], &mut slots)
        }

        fn owner(&self, id: &DataId) -> Result<ScopeId, ScopeError> {
            self.coordinator.owner_of(id)
        }

        fn child(&mut self, parent: &ScopeId) -> ScopeId {
            self.coordinator.create_child(parent).unwrap()
        }

        /// 以本地引用初始化控制状态。
        fn state<T: Any>(&mut self, controller: &ScopeId, from: &RefId) -> ControlStateId {
            self.coordinator
                .register_state::<T>(controller, from)
                .unwrap()
        }

        /// 登记尚未初始化的控制状态。
        fn uninit_state<T: Any>(&mut self, controller: &ScopeId) -> ControlStateId {
            self.coordinator
                .register_uninitialized_state::<T>(controller)
                .unwrap()
        }

        fn collector<O: Any>(&mut self, owner: &ScopeId) -> CollectorId {
            self.coordinator.begin_collector::<O>(owner).unwrap()
        }

        /// 是否仍能按类型读取该 DataId（用于观测旧身份是否失效）。
        fn readable_as<T: Any>(&self, id: &DataId) -> bool {
            self.coordinator.container.borrow::<T>(id).is_ok()
        }

        fn alive(&self, id: &DataId) -> bool {
            self.coordinator.container.validate(id).is_ok()
        }

        fn scope_state(&self, scope: &ScopeId) -> ScopeState {
            self.coordinator.state(scope).unwrap()
        }

        fn refs_len(&self, scope: &ScopeId) -> usize {
            self.coordinator.registry.lookup(scope).unwrap().refs.len()
        }

        fn owned_len(&self, scope: &ScopeId) -> usize {
            self.coordinator.registry.lookup(scope).unwrap().owned.len()
        }

        /// 测试专用故障：把一个 target 直接注入 Scope 的本地位置。
        fn inject_target(&mut self, scope: &ScopeId, position: &RefId, target: RefTarget) {
            self.coordinator
                .registry
                .lookup_mut(scope)
                .unwrap()
                .refs
                .insert(position.clone(), target);
        }

        /// 测试专用故障：撤销一个 Scope 的 owned 责任记录（entry 仍在容器中）。
        fn drop_ownership(&mut self, scope: &ScopeId, id: &DataId) {
            assert!(
                self.coordinator
                    .registry
                    .lookup_mut(scope)
                    .unwrap()
                    .owned
                    .remove(id)
            );
        }

        /// 测试专用故障：把责任记录加到一个 Scope 上，制造重复 owner。
        fn add_ownership(&mut self, scope: &ScopeId, id: &DataId) {
            assert!(
                self.coordinator
                    .registry
                    .lookup_mut(scope)
                    .unwrap()
                    .owned
                    .insert(id.clone())
            );
        }
    }

    #[test]
    fn b01_execution_identity_is_shared_and_foreign_scopes_are_rejected_first() {
        let mut first = Fixture::new();
        let mut second = Fixture::new();

        // 两个 Execution 的 RootScope 序号相同，各自的第一个 child 序号也相同。
        assert_eq!(first.root().seq(), second.root().seq());
        let child = first.coordinator.create_child(&first.root()).unwrap();
        let second_child = second.coordinator.create_child(&second.root()).unwrap();
        assert_eq!(child.seq(), second_child.seq());

        // 来源校验先于查表：不会误命中第二份 registry 的同序号记录。
        assert!(matches!(
            second.coordinator.state(&first.root()),
            Err(ScopeError::ForeignExecution { .. })
        ));
        assert!(matches!(
            second.coordinator.create_child(&first.root()),
            Err(ScopeError::ForeignExecution { .. })
        ));

        // 只有 Active parent 能建立 child；关闭后的身份不复用。
        first.finalize_none(&child).unwrap();
        assert_eq!(first.coordinator.state(&child).unwrap(), ScopeState::Closed);
        assert!(matches!(
            first.coordinator.create_child(&child),
            Err(ScopeError::ScopeClosed { .. })
        ));
        let next = first.coordinator.create_child(&first.root()).unwrap();
        assert_ne!(next.seq(), child.seq());

        // foreign DataId 的拒绝发生在存储校验，且诊断是"来源不符"而不是"不存在"。
        let foreign = second
            .coordinator
            .register_owned(&second.root(), &second.ref_id(), Other(9))
            .unwrap();
        let position = first.ref_id();
        first
            .coordinator
            .registry
            .lookup_mut(&first.root())
            .unwrap()
            .refs
            .insert(position.clone(), RefTarget::Data(foreign));
        assert!(matches!(
            first.coordinator.resolve::<Other>(&first.root(), &position),
            Err(ScopeError::Storage {
                source: InternalError::ForeignExecution { .. }
            })
        ));
    }

    #[test]
    fn b02_definition_ref_ids_are_reusable_across_independent_scopes() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let first_child = fixture.coordinator.create_child(&root).unwrap();
        let second_child = fixture.coordinator.create_child(&root).unwrap();

        // 同一 Definition 逻辑位置在两个 child 实例中分别绑定到不同 DataId。
        let shared_position = fixture.ref_id();
        let first_id = fixture
            .coordinator
            .register_owned(&first_child, &shared_position, fixture.tracked(1))
            .unwrap();
        let second_id = fixture
            .coordinator
            .register_owned(&second_child, &shared_position, fixture.tracked(2))
            .unwrap();

        assert_ne!(first_id, second_id);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&first_child, &shared_position)
                .unwrap()
                .num,
            1
        );
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&second_child, &shared_position)
                .unwrap()
                .num,
            2
        );
        // 两个实例的本地 refs 互不覆写，各自登记一个位置。
        assert_eq!(
            fixture
                .coordinator
                .registry
                .lookup(&first_child)
                .unwrap()
                .refs
                .len(),
            1
        );
        assert_eq!(
            fixture
                .coordinator
                .registry
                .lookup(&second_child)
                .unwrap()
                .refs
                .len(),
            1
        );
    }

    #[test]
    fn b03_local_binding_is_single_assignment_and_aliasable() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let position = fixture.ref_id();
        let first_alias = fixture.ref_id();
        let second_alias = fixture.ref_id();

        let d1 = fixture
            .coordinator
            .register_owned(&root, &position, fixture.tracked(1))
            .unwrap();

        // 重绑同一位置（即使目标相同）在修改前拒绝，原绑定可读。
        assert!(matches!(
            fixture
                .coordinator
                .register_owned(&root, &position, fixture.tracked(2)),
            Err(ScopeError::RefAlreadyBound { .. })
        ));
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &position)
                .unwrap()
                .num,
            1
        );

        // 不同位置 alias 同一 DataId：两个位置都可读，责任仍只有一份。
        let child = fixture.coordinator.create_child(&root).unwrap();
        fixture
            .coordinator
            .import_batch(
                &child,
                &root,
                &[
                    ImportSlot::new::<Tracked>(&position, &first_alias),
                    ImportSlot::new::<Tracked>(&position, &second_alias),
                ],
            )
            .unwrap();
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&child, &first_alias)
                .unwrap()
                .num,
            1
        );
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&child, &second_alias)
                .unwrap()
                .num,
            1
        );
        assert_eq!(fixture.owner(&d1).unwrap(), root);
    }

    #[test]
    fn b04_import_grants_visibility_without_ownership() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let parent_position = fixture.ref_id();
        let child_position = fixture.ref_id();
        let sibling_position = fixture.ref_id();

        let d1 = fixture
            .coordinator
            .register_owned(&root, &parent_position, fixture.tracked(1))
            .unwrap();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let sibling = fixture.coordinator.create_child(&root).unwrap();
        fixture
            .coordinator
            .register_owned(&sibling, &sibling_position, fixture.tracked(2))
            .unwrap();

        // 未导入时 child 不能通过位置读取；不搜索 ancestor.refs，也看不到 sibling 的位置。
        assert!(matches!(
            fixture
                .coordinator
                .resolve::<Tracked>(&child, &parent_position),
            Err(ScopeError::RefNotBound { .. })
        ));
        assert!(matches!(
            fixture
                .coordinator
                .resolve::<Tracked>(&child, &sibling_position),
            Err(ScopeError::RefNotBound { .. })
        ));

        fixture
            .coordinator
            .import_batch(
                &child,
                &root,
                &[ImportSlot::new::<Tracked>(
                    &parent_position,
                    &child_position,
                )],
            )
            .unwrap();

        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&child, &child_position)
                .unwrap()
                .num,
            1
        );
        // 导入不增加 owner：责任仍在 root。
        assert_eq!(fixture.owner(&d1).unwrap(), root);
    }

    #[test]
    fn b05_batch_import_failure_leaves_no_partial_binding() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let caller_position = fixture.ref_id();
        let good_target = fixture.ref_id();
        let bad_target = fixture.ref_id();

        fixture
            .coordinator
            .register_owned(&root, &caller_position, fixture.tracked(1))
            .unwrap();

        // 第二项类型不符：第一项也不能留下绑定。
        let error = fixture
            .coordinator
            .import_batch(
                &child,
                &root,
                &[
                    ImportSlot::new::<Tracked>(&caller_position, &good_target),
                    ImportSlot::new::<Other>(&caller_position, &bad_target),
                ],
            )
            .unwrap_err();
        assert!(matches!(error, ScopeError::TypeMismatch { .. }));
        assert!(matches!(
            fixture.coordinator.resolve::<Tracked>(&child, &good_target),
            Err(ScopeError::RefNotBound { .. })
        ));
        assert!(
            fixture
                .coordinator
                .registry
                .lookup(&child)
                .unwrap()
                .refs
                .is_empty()
        );

        // 第二项重复目标位置同样整组拒绝。
        assert!(matches!(
            fixture.coordinator.import_batch(
                &child,
                &root,
                &[
                    ImportSlot::new::<Tracked>(&caller_position, &good_target),
                    ImportSlot::new::<Tracked>(&caller_position, &good_target),
                ],
            ),
            Err(ScopeError::DuplicatePosition { .. })
        ));
        assert!(
            fixture
                .coordinator
                .registry
                .lookup(&child)
                .unwrap()
                .refs
                .is_empty()
        );
    }

    #[test]
    fn b06_registration_rejects_before_insert_and_survives_exhaustion() {
        // 用近上限的全新 Execution 身份同时建立 registry 与 Container。
        let execution = ExecutionIdentity::with_starts(u64::MAX - 1, 0);
        let mut coordinator = ScopeCoordinator::new(execution);
        let ids = RefIdAllocator::new(RefIdSource::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let root = coordinator.root();

        let first = ids.allocate().unwrap();
        let second = ids.allocate().unwrap();
        let id = coordinator
            .register_owned(
                &root,
                &first,
                Tracked {
                    num: 1,
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        assert_eq!(id.seq(), u64::MAX - 1);

        // 序号耗尽：插入失败不留 refs／owned，也不影响原值与责任。
        assert!(matches!(
            coordinator.register_owned(
                &root,
                &second,
                Tracked {
                    num: 2,
                    drops: Arc::clone(&drops),
                }
            ),
            Err(ScopeError::Storage {
                source: InternalError::IdSpaceExhausted { .. }
            })
        ));
        {
            let record = coordinator.registry.lookup(&root).unwrap();
            assert_eq!(record.owned.len(), 1);
            assert_eq!(record.refs.len(), 1);
        }
        assert_eq!(
            coordinator.resolve::<Tracked>(&root, &first).unwrap().num,
            1
        );
        // 失败插入的输入值随拒绝一同 drop，不计入已登记值。
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        // 非 Active Scope 在插入前拒绝。
        coordinator.registry.lookup_mut(&root).unwrap().state = ScopeState::Finalizing;
        assert!(matches!(
            coordinator.register_owned(
                &root,
                &ids.allocate().unwrap(),
                Tracked {
                    num: 3,
                    drops: Arc::clone(&drops),
                }
            ),
            Err(ScopeError::ScopeNotActive { .. })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn b07_p02_normal_export_commits_binding_and_responsibility() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let parent_position = fixture.ref_id();
        let imported = fixture.ref_id();
        let child_output = fixture.ref_id();
        let caller_position = fixture.ref_id();
        let temporary = fixture.ref_id();

        let d1 = fixture
            .coordinator
            .register_owned(&root, &parent_position, fixture.tracked(1))
            .unwrap();
        let child = fixture.coordinator.create_child(&root).unwrap();
        fixture
            .coordinator
            .import_batch(
                &child,
                &root,
                &[ImportSlot::new::<Tracked>(&parent_position, &imported)],
            )
            .unwrap();
        let d2 = fixture
            .coordinator
            .register_owned(&child, &child_output, fixture.tracked(2))
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &temporary, fixture.tracked(3))
            .unwrap();

        fixture
            .finalize_one::<Tracked>(&child, &child_output, &caller_position)
            .unwrap();

        // 只有未导出的临时值被清理。
        assert_eq!(fixture.drops(), 1);
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
        // caller 可读 D1／D2；责任转移完成。
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &parent_position)
                .unwrap()
                .num,
            1
        );
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &caller_position)
                .unwrap()
                .num,
            2
        );
        assert_eq!(fixture.owner(&d1).unwrap(), root);
        assert_eq!(fixture.owner(&d2).unwrap(), root);
        // child 的本地引用随 Closed 失效。
        assert!(matches!(
            fixture.coordinator.resolve::<Tracked>(&child, &imported),
            Err(ScopeError::ScopeClosed { .. })
        ));

        // parent 退出时清理自己负责的 D1／D2 各一次。
        fixture.finalize_none(&root).unwrap();
        assert_eq!(fixture.drops(), 3);
    }

    #[test]
    fn b08_exported_imported_alias_keeps_the_original_owner() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let root_position = fixture.ref_id();
        let middle_position = fixture.ref_id();
        let caller_position = fixture.ref_id();

        let d1 = fixture
            .coordinator
            .register_owned(&root, &root_position, fixture.tracked(1))
            .unwrap();
        let middle = fixture.coordinator.create_child(&root).unwrap();
        fixture
            .coordinator
            .import_batch(
                &middle,
                &root,
                &[ImportSlot::new::<Tracked>(&root_position, &middle_position)],
            )
            .unwrap();

        // 中间 Scope 原样再输出 imported Data。
        fixture
            .finalize_one::<Tracked>(&middle, &middle_position, &caller_position)
            .unwrap();

        assert_eq!(fixture.drops(), 0);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &caller_position)
                .unwrap()
                .num,
            1
        );
        // 原 owner 不变，也没有第二个 owner。
        assert_eq!(fixture.owner(&d1).unwrap(), root);
    }

    #[test]
    fn b09_alias_outputs_transfer_responsibility_once() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child_first = fixture.ref_id();
        let child_second = fixture.ref_id();
        let caller_first = fixture.ref_id();
        let caller_second = fixture.ref_id();

        let child = fixture.coordinator.create_child(&root).unwrap();
        let d2 = fixture
            .coordinator
            .register_owned(&child, &child_first, fixture.tracked(1))
            .unwrap();
        // 两个不同输出位置指向同一 child-owned DataId。
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .refs
            .insert(child_second.clone(), RefTarget::Data(d2.clone()));

        fixture
            .finalize_many(
                &child,
                &[child_first.clone(), child_second.clone()],
                vec![
                    ExportSlot::new::<Tracked>(&child_first, &caller_first),
                    ExportSlot::new::<Tracked>(&child_second, &caller_second),
                ],
            )
            .unwrap();

        assert_eq!(fixture.drops(), 0);
        assert_eq!(fixture.owner(&d2).unwrap(), root);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &caller_first)
                .unwrap()
                .num,
            1
        );
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &caller_second)
                .unwrap()
                .num,
            1
        );

        // 责任只转移一次：parent 清理只 drop 一次。
        fixture.finalize_none(&root).unwrap();
        assert_eq!(fixture.drops(), 1);
    }

    #[test]
    fn b10_heterogeneous_export_rejects_before_any_commit() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let tracked_position = fixture.ref_id();
        let other_position = fixture.ref_id();
        let caller_tracked = fixture.ref_id();
        let caller_other = fixture.ref_id();

        let child = fixture.coordinator.create_child(&root).unwrap();
        fixture
            .coordinator
            .register_owned(&child, &tracked_position, fixture.tracked(1))
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &other_position, Other(7))
            .unwrap();

        // 异构两输出正常导出。
        fixture
            .finalize_many(
                &child,
                &[tracked_position.clone(), other_position.clone()],
                vec![
                    ExportSlot::new::<Tracked>(&tracked_position, &caller_tracked),
                    ExportSlot::new::<Other>(&other_position, &caller_other),
                ],
            )
            .unwrap();
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &caller_tracked)
                .unwrap()
                .num,
            1
        );
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Other>(&root, &caller_other)
                .unwrap()
                .0,
            7
        );

        // 第二项类型不符：合法第一项不得提前提交。
        let child = fixture.coordinator.create_child(&root).unwrap();
        let first_output = fixture.ref_id();
        let second_output = fixture.ref_id();
        let first_caller = fixture.ref_id();
        let second_caller = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&child, &first_output, fixture.tracked(2))
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &second_output, fixture.tracked(3))
            .unwrap();

        let error = fixture
            .coordinator
            .prepare_export(
                &child,
                &root,
                &[first_output.clone(), second_output.clone()],
                &[
                    ExportSlot::new::<Tracked>(&first_output, &first_caller),
                    ExportSlot::new::<Other>(&second_output, &second_caller),
                ],
            )
            .unwrap_err();
        assert!(matches!(error, ScopeError::TypeMismatch { .. }));
        assert!(
            !fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .refs
                .contains_key(&first_caller)
        );
        assert!(matches!(
            fixture.coordinator.resolve::<Tracked>(&root, &first_caller),
            Err(ScopeError::RefNotBound { .. })
        ));
        assert_eq!(fixture.drops(), 0);
    }

    #[test]
    fn b11_caller_position_conflicts_and_illegal_relations_are_rejected() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let sibling = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let other_output = fixture.ref_id();
        let bound_caller = fixture.ref_id();

        fixture
            .coordinator
            .register_owned(&child, &output, fixture.tracked(1))
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &other_output, fixture.tracked(3))
            .unwrap();
        fixture
            .coordinator
            .register_owned(&root, &bound_caller, fixture.tracked(2))
            .unwrap();

        // caller 位置已绑定。
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Tracked>(&output, &bound_caller)],
            ),
            Err(ScopeError::RefAlreadyBound { .. })
        ));

        // 本批重复 caller 位置：第二个 slot 才会撞上重复。
        let fresh_caller = fixture.ref_id();
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                &[output.clone(), other_output.clone()],
                &[
                    ExportSlot::new::<Tracked>(&output, &fresh_caller),
                    ExportSlot::new::<Tracked>(&other_output, &fresh_caller),
                ],
            ),
            Err(ScopeError::DuplicatePosition { .. })
        ));

        // 非直接 parent。
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &sibling,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Tracked>(&output, &fixture.ref_id())],
            ),
            Err(ScopeError::NotDirectParent { .. })
        ));

        // caller 非 Active。
        fixture
            .coordinator
            .registry
            .lookup_mut(&root)
            .unwrap()
            .state = ScopeState::Finalizing;
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Tracked>(&output, &fixture.ref_id())],
            ),
            Err(ScopeError::ScopeNotActive { .. })
        ));
    }

    #[test]
    fn b12_target_and_owner_faults_are_rejected_before_commit() {
        // 失效 target。
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let dead = fixture
            .coordinator
            .register_owned(&child, &output, fixture.tracked(1))
            .unwrap();
        fixture.coordinator.container.destroy(&dead).unwrap();
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .owned
            .remove(&dead);
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Tracked>(&output, &fixture.ref_id())],
            ),
            Err(ScopeError::TargetNotAlive { .. })
        ));

        // foreign target。
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let mut other = Fixture::new();
        let foreign = other
            .coordinator
            .register_owned(&other.root(), &other.ref_id(), Other(1))
            .unwrap();
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .refs
            .insert(output.clone(), RefTarget::Data(foreign));
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
            ),
            Err(ScopeError::Storage {
                source: InternalError::ForeignExecution { .. }
            })
        ));

        // 无 owner。
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let orphan = fixture
            .coordinator
            .container
            .insert_owned(Other(2))
            .unwrap();
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .refs
            .insert(output.clone(), RefTarget::Data(orphan));
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
            ),
            Err(ScopeError::NoOwner { .. })
        ));

        // sibling owner。
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let exporter = fixture.coordinator.create_child(&root).unwrap();
        let sibling = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let sibling_position = fixture.ref_id();
        let sibling_data = fixture
            .coordinator
            .register_owned(&sibling, &sibling_position, Other(3))
            .unwrap();
        fixture
            .coordinator
            .registry
            .lookup_mut(&exporter)
            .unwrap()
            .refs
            .insert(output.clone(), RefTarget::Data(sibling_data));
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &exporter,
                &root,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
            ),
            Err(ScopeError::IllegalOwner { .. })
        ));

        // 重复 owner。
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let exporter = fixture.coordinator.create_child(&root).unwrap();
        let sibling = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let sibling_position = fixture.ref_id();
        let shared = fixture
            .coordinator
            .register_owned(&sibling, &sibling_position, Other(4))
            .unwrap();
        {
            let record = fixture.coordinator.registry.lookup_mut(&exporter).unwrap();
            record.owned.insert(shared.clone());
            record.refs.insert(output.clone(), RefTarget::Data(shared));
        }
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &exporter,
                &root,
                std::slice::from_ref(&output),
                &[ExportSlot::new::<Other>(&output, &fixture.ref_id())],
            ),
            Err(ScopeError::DuplicateOwner { .. })
        ));
    }

    #[test]
    fn b13_precommit_snapshot_and_formal_failure_exit() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let root_position = fixture.ref_id();
        let imported = fixture.ref_id();
        let first_output = fixture.ref_id();
        let second_output = fixture.ref_id();
        let first_caller = fixture.ref_id();
        let second_caller = fixture.ref_id();

        // ancestor-owned 值显式导入 child：失败退出不得动它。
        fixture
            .coordinator
            .register_owned(&root, &root_position, fixture.tracked(9))
            .unwrap();
        fixture
            .coordinator
            .import_batch(
                &child,
                &root,
                &[ImportSlot::new::<Tracked>(&root_position, &imported)],
            )
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &first_output, fixture.tracked(1))
            .unwrap();
        let d2 = fixture
            .coordinator
            .register_owned(&child, &second_output, fixture.tracked(2))
            .unwrap();

        // 私有预检路径：失败瞬间 caller 无新绑定，D2 仍由 child 负责。
        let error = fixture
            .coordinator
            .prepare_export(
                &child,
                &root,
                &[first_output.clone(), second_output.clone()],
                &[
                    ExportSlot::new::<Tracked>(&first_output, &first_caller),
                    ExportSlot::new::<Other>(&second_output, &second_caller),
                ],
            )
            .unwrap_err();
        assert!(matches!(error, ScopeError::TypeMismatch { .. }));
        assert!(
            !fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .refs
                .contains_key(&first_caller)
        );
        assert_eq!(fixture.owner(&d2).unwrap(), child);
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Active
        );
        assert_eq!(fixture.drops(), 0);

        // 正式失败出口：清理 child 自身数据并返回原诊断，ancestor 值保留。
        let error = fixture
            .finalize_many(
                &child,
                &[first_output.clone(), second_output.clone()],
                vec![
                    ExportSlot::new::<Tracked>(&first_output, &first_caller),
                    ExportSlot::new::<Other>(&second_output, &second_caller),
                ],
            )
            .unwrap_err();
        assert!(matches!(error, ScopeError::TypeMismatch { .. }));
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
        assert_eq!(fixture.drops(), 2);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &root_position)
                .unwrap()
                .num,
            9
        );
        // 同 §7：RefId 的 Eq／Hash 只用来源地址与序号，键语义不随分配变化。
        #[allow(clippy::mutable_key_type)]
        fn caller_bindings(
            fixture: &Fixture,
            root: &ScopeId,
            first: &RefId,
            second: &RefId,
        ) -> bool {
            let refs = &fixture.coordinator.registry.lookup(root).unwrap().refs;
            !refs.contains_key(first) && !refs.contains_key(second)
        }
        assert!(caller_bindings(
            &fixture,
            &root,
            &first_caller,
            &second_caller
        ));
    }
    #[test]
    fn b14_finalizing_and_closed_states_are_distinguishable() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let position = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&child, &position, fixture.tracked(1))
            .unwrap();

        // Finalizing 禁止新 child／导入／登记／业务 resolve，但允许内部输出预检。
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .state = ScopeState::Finalizing;
        let unrelated = fixture.ref_id();
        assert!(matches!(
            fixture.coordinator.create_child(&child),
            Err(ScopeError::ScopeNotActive { .. })
        ));
        assert!(matches!(
            fixture
                .coordinator
                .register_owned(&child, &unrelated, fixture.tracked(2)),
            Err(ScopeError::ScopeNotActive { .. })
        ));
        assert!(matches!(
            fixture.coordinator.resolve::<Tracked>(&child, &position),
            Err(ScopeError::ScopeNotActive { .. })
        ));
        assert!(matches!(
            fixture.coordinator.import_batch(
                &child,
                &root,
                &[ImportSlot::new::<Tracked>(&unrelated, &unrelated)],
            ),
            Err(ScopeError::ScopeNotActive { .. })
        ));
        // 内部输出校验仍可用。
        fixture
            .coordinator
            .prepare_export(
                &child,
                &root,
                std::slice::from_ref(&position),
                &[ExportSlot::new::<Tracked>(&position, &fixture.ref_id())],
            )
            .unwrap();

        // 从 Finalizing 完成退出清理。
        let mut consumed: Vec<ExportSlot> = Vec::new();
        fixture
            .coordinator
            .complete_exit(&child, &[], &mut consumed)
            .unwrap();
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
        // 已关闭身份 → Closed；同次 Execution 已分配但未登记的身份 → 不存在。
        assert!(matches!(
            fixture.coordinator.resolve::<Tracked>(&child, &position),
            Err(ScopeError::ScopeClosed { .. })
        ));
        let unregistered = {
            let ids = ScopeIdAllocator::new(fixture.coordinator.registry.execution.clone());
            ids.allocate().unwrap();
            ids.allocate().unwrap()
        };
        assert!(matches!(
            fixture.coordinator.state(&unregistered),
            Err(ScopeError::ScopeNotFound { .. })
        ));
        // 关闭后的 tombstone 不再保留 refs／owned／活跃子关系。
        let record = fixture.coordinator.registry.lookup(&child).unwrap();
        assert!(record.refs.is_empty() && record.owned.is_empty() && record.children.is_empty());
    }

    #[test]
    fn b15_empty_output_set_is_a_normal_exit() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        fixture
            .coordinator
            .register_owned(&child, &fixture.ref_id(), fixture.tracked(1))
            .unwrap();

        fixture.finalize_none(&child).unwrap();

        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
        assert_eq!(fixture.drops(), 1);
        assert!(
            fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .refs
                .is_empty()
        );
    }

    #[test]
    fn b16_active_descendants_block_normal_exit() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let grandchild = fixture.coordinator.create_child(&child).unwrap();

        assert!(matches!(
            fixture.finalize_none(&child),
            Err(ScopeError::ActiveDescendants { .. })
        ));
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Active
        );
        assert_eq!(
            fixture.coordinator.state(&grandchild).unwrap(),
            ScopeState::Active
        );

        fixture.finalize_none(&grandchild).unwrap();
        fixture.finalize_none(&child).unwrap();
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
    }

    #[test]
    fn b17_multi_level_failure_cleanup_is_post_order() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let root_position = fixture.ref_id();
        let parent = fixture.coordinator.create_child(&root).unwrap();
        let imported_in_parent = fixture.ref_id();
        let parent_position = fixture.ref_id();
        let child = fixture.coordinator.create_child(&parent).unwrap();
        let imported_in_child = fixture.ref_id();
        let child_position = fixture.ref_id();

        fixture
            .coordinator
            .register_owned(&root, &root_position, fixture.tracked(1))
            .unwrap();
        fixture
            .coordinator
            .import_batch(
                &parent,
                &root,
                &[ImportSlot::new::<Tracked>(
                    &root_position,
                    &imported_in_parent,
                )],
            )
            .unwrap();
        fixture
            .coordinator
            .register_owned(&parent, &parent_position, fixture.tracked(2))
            .unwrap();
        fixture
            .coordinator
            .import_batch(
                &child,
                &parent,
                &[ImportSlot::new::<Tracked>(
                    &imported_in_parent,
                    &imported_in_child,
                )],
            )
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &child_position, fixture.tracked(3))
            .unwrap();

        fixture.coordinator.abort(&parent).unwrap();

        // 三层中的 parent 与 child 全部关闭，未导出的自有数据各 drop 一次。
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
        assert_eq!(
            fixture.coordinator.state(&parent).unwrap(),
            ScopeState::Closed
        );
        assert_eq!(fixture.drops(), 2);
        assert!(matches!(
            fixture
                .coordinator
                .resolve::<Tracked>(&child, &imported_in_child),
            Err(ScopeError::ScopeClosed { .. })
        ));
        // 仍存活的 ancestor 值未被清理,root 也未脱离管理。
        assert_eq!(
            fixture.coordinator.state(&root).unwrap(),
            ScopeState::Active
        );
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &root_position)
                .unwrap()
                .num,
            1
        );
        assert!(
            fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .children
                .is_empty()
        );
    }
    #[test]
    fn b18_received_values_are_disposed_by_the_receiving_scope() {
        // 两种退出路径都观测"接收方处置转交值、仍存活的 ancestor 值保留"。
        for use_abort in [false, true] {
            let mut fixture = Fixture::new();
            let root = fixture.root();
            let root_position = fixture.ref_id();
            let parent = fixture.coordinator.create_child(&root).unwrap();
            let imported_in_parent = fixture.ref_id();
            let child = fixture.coordinator.create_child(&parent).unwrap();
            let imported_in_child = fixture.ref_id();
            let child_output = fixture.ref_id();
            let parent_position = fixture.ref_id();

            fixture
                .coordinator
                .register_owned(&root, &root_position, fixture.tracked(1))
                .unwrap();
            fixture
                .coordinator
                .import_batch(
                    &parent,
                    &root,
                    &[ImportSlot::new::<Tracked>(
                        &root_position,
                        &imported_in_parent,
                    )],
                )
                .unwrap();
            fixture
                .coordinator
                .import_batch(
                    &child,
                    &parent,
                    &[ImportSlot::new::<Tracked>(
                        &imported_in_parent,
                        &imported_in_child,
                    )],
                )
                .unwrap();
            fixture
                .coordinator
                .register_owned(&child, &child_output, fixture.tracked(2))
                .unwrap();

            // child 把自有值转交 parent 后关闭；接收方与 ancestor 均存活时未发生 Drop。
            fixture
                .finalize_one::<Tracked>(&child, &child_output, &parent_position)
                .unwrap();
            assert_eq!(fixture.drops(), 0);
            assert_eq!(
                fixture
                    .coordinator
                    .resolve::<Tracked>(&parent, &parent_position)
                    .unwrap()
                    .num,
                2
            );

            // parent 正常退出或失败清理：只处置转交来的值一次，ancestor 值保留。
            if use_abort {
                fixture.coordinator.abort(&parent).unwrap();
            } else {
                fixture.finalize_none(&parent).unwrap();
            }
            assert_eq!(fixture.drops(), 1);
            assert_eq!(
                fixture
                    .coordinator
                    .resolve::<Tracked>(&root, &root_position)
                    .unwrap()
                    .num,
                1
            );
            assert!(
                !fixture
                    .coordinator
                    .registry
                    .lookup(&root)
                    .unwrap()
                    .refs
                    .is_empty()
            );

            // Container 析构只清理 root 自己的值，不再重复 Drop。
            let drops = Arc::clone(&fixture.drops);
            let coordinator = fixture.coordinator;
            drop(coordinator);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        }
    }

    #[test]
    fn b19_shortening_the_borrow_allows_exit_and_cleanup() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let position = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&root, &position, fixture.tracked(1))
            .unwrap();

        // 缩短借用后：正常退出入口可运行。
        {
            let borrowed = fixture
                .coordinator
                .resolve::<Tracked>(&root, &position)
                .unwrap();
            assert_eq!(borrowed.num, 1);
        }
        fixture.finalize_none(&root).unwrap();
        assert_eq!(fixture.drops(), 1);

        // 缩短借用后：失败清理入口可运行。
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let position = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&root, &position, fixture.tracked(2))
            .unwrap();
        {
            let borrowed = fixture
                .coordinator
                .resolve::<Tracked>(&root, &position)
                .unwrap();
            assert_eq!(borrowed.num, 2);
        }
        fixture.coordinator.abort(&root).unwrap();
        assert_eq!(fixture.drops(), 1);
    }

    #[test]
    fn b21_registry_records_hold_metadata_only() {
        // 字段类型断言：registry／Scope／RefTarget 的类型若变化，本测试不再编译。
        fn scope_fields(scope: &Scope) {
            let _: &ScopeId = &scope.id;
            let _: &Option<ScopeId> = &scope.parent;
            let _: &Vec<ScopeId> = &scope.children;
            let _: &ScopeState = &scope.state;
            let _: &HashMap<RefId, RefTarget> = &scope.refs;
            let _: &HashSet<DataId> = &scope.owned;
        }
        fn registry_fields(registry: &ScopeRegistry) {
            let _: &Arc<ExecutionIdentity> = &registry.execution;
            let _: &ScopeIdAllocator = &registry.scope_ids;
            let _: &ScopeId = &registry.root;
            let _: &HashMap<u64, Scope> = &registry.scopes;
        }
        fn target_fields(target: &RefTarget) {
            match target {
                RefTarget::Data(id) => {
                    let _: &DataId = id;
                }
                RefTarget::CollectionItem {
                    collection,
                    index,
                    lifetime_cap,
                    access,
                } => {
                    let _: &DataId = collection;
                    let _: &usize = index;
                    let _: &ScopeId = lifetime_cap;
                    let _: &super::ItemAccess = access;
                    let _: TypeId = access.element_type();
                    let _: TypeId = access.collection_type();
                }
            }
        }

        let mut fixture = Fixture::new();
        let root = fixture.root();
        let position = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&root, &position, Other(1))
            .unwrap();

        scope_fields(fixture.coordinator.registry.lookup(&root).unwrap());
        registry_fields(&fixture.coordinator.registry);
        let record = fixture.coordinator.registry.lookup(&root).unwrap();
        target_fields(record.refs.values().next().unwrap());
        // 唯一 Container 位于外层协调组件，不是 registry 的字段。
        let _: &crate::core::data_container::DataContainer = &fixture.coordinator.container;
    }

    #[test]
    fn r13_failure_and_abort_must_not_destroy_ancestor_values() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let root_position = fixture.ref_id();
        let imported = fixture.ref_id();
        let caller_position = fixture.ref_id();

        let ancestor = fixture
            .coordinator
            .register_owned(&root, &root_position, fixture.tracked(1))
            .unwrap();
        fixture
            .coordinator
            .import_batch(
                &child,
                &root,
                &[ImportSlot::new::<Tracked>(&root_position, &imported)],
            )
            .unwrap();
        // 故障构造：child 重复认领 imported ancestor 值,破坏唯一责任。
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .owned
            .insert(ancestor.clone());

        // 正式失败入口：保留原导出诊断,且不授予销毁权。
        let error = fixture
            .finalize_one::<Tracked>(&child, &imported, &caller_position)
            .unwrap_err();
        assert!(matches!(error, ScopeError::DuplicateOwner { .. }));
        assert_eq!(fixture.drops(), 0);
        assert!(fixture.coordinator.container.validate(&ancestor).is_ok());
        // caller refs 不变：没有新增输出绑定。
        {
            let root_record = fixture.coordinator.registry.lookup(&root).unwrap();
            assert!(root_record.refs.contains_key(&root_position));
            assert!(!root_record.refs.contains_key(&caller_position));
        }
        // 破坏状态不被静默清理：child 未冒充 Closed,其 owned 仍保留注入项。
        {
            let child_record = fixture.coordinator.registry.lookup(&child).unwrap();
            assert_ne!(child_record.state, ScopeState::Closed);
            assert!(child_record.owned.contains(&ancestor));
        }

        // 破坏状态不冒充正常 Closed：后续退出尝试以责任诊断报出，仍不销毁任何值。
        assert!(matches!(
            fixture.coordinator.abort(&child),
            Err(ScopeError::DuplicateOwner { .. }) | Err(ScopeError::Invariant { .. })
        ));
        assert_eq!(fixture.drops(), 0);
        assert!(fixture.coordinator.container.validate(&ancestor).is_ok());

        // 恢复被破坏的责任后,ancestor 值仍完整可读（证明未被销毁）。
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .owned
            .remove(&ancestor);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &root_position)
                .unwrap()
                .num,
            1
        );
    }

    #[test]
    fn r14_resolve_requires_a_legal_responsibility_chain() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let sibling = fixture.coordinator.create_child(&root).unwrap();
        let own_position = fixture.ref_id();
        let root_position = fixture.ref_id();
        let imported = fixture.ref_id();
        let forged = fixture.ref_id();
        let orphan_position = fixture.ref_id();
        let duplicated_position = fixture.ref_id();
        let dead_position = fixture.ref_id();

        // 合法本地登记与合法导入都可读。
        fixture
            .coordinator
            .register_owned(&child, &own_position, Other(1))
            .unwrap();
        fixture
            .coordinator
            .register_owned(&root, &root_position, Other(2))
            .unwrap();
        fixture
            .coordinator
            .import_batch(
                &child,
                &root,
                &[ImportSlot::new::<Other>(&root_position, &imported)],
            )
            .unwrap();
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Other>(&child, &own_position)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Other>(&child, &imported)
                .unwrap()
                .0,
            2
        );

        // sibling-owned：注入 child 本地位置后必须拒绝。
        let sibling_position = fixture.ref_id();
        let sibling_id = fixture
            .coordinator
            .register_owned(&sibling, &sibling_position, Other(3))
            .unwrap();
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .refs
            .insert(forged.clone(), RefTarget::Data(sibling_id.clone()));
        assert!(matches!(
            fixture.coordinator.resolve::<Other>(&child, &forged),
            Err(ScopeError::IllegalOwner { .. })
        ));

        // 无 owner。
        let orphan = fixture
            .coordinator
            .container
            .insert_owned(Other(4))
            .unwrap();
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .refs
            .insert(orphan_position.clone(), RefTarget::Data(orphan));
        assert!(matches!(
            fixture
                .coordinator
                .resolve::<Other>(&child, &orphan_position),
            Err(ScopeError::NoOwner { .. })
        ));

        // 重复 owner。
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .owned
            .insert(sibling_id.clone());
        fixture
            .coordinator
            .registry
            .lookup_mut(&child)
            .unwrap()
            .refs
            .insert(duplicated_position.clone(), RefTarget::Data(sibling_id));
        assert!(matches!(
            fixture
                .coordinator
                .resolve::<Other>(&child, &duplicated_position),
            Err(ScopeError::DuplicateOwner { .. })
        ));

        // 失效 target 仍按既有顺序先报 TargetNotAlive。
        let dead = fixture
            .coordinator
            .register_owned(&child, &dead_position, Other(5))
            .unwrap();
        fixture.coordinator.container.destroy(&dead).unwrap();
        assert!(matches!(
            fixture.coordinator.resolve::<Other>(&child, &dead_position),
            Err(ScopeError::TargetNotAlive { .. })
        ));
    }

    #[test]
    fn r15_output_position_and_count_contract_is_checked_against_declaration() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let first = fixture.ref_id();
        let second = fixture.ref_id();
        let first_caller = fixture.ref_id();
        let second_caller = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&child, &first, fixture.tracked(1))
            .unwrap();
        fixture
            .coordinator
            .register_owned(&child, &second, fixture.tracked(2))
            .unwrap();
        let declared = vec![first.clone(), second.clone()];

        // 声明两项却只提交一项（合法缩短列表）：数量契约在任何绑定与转移前拒绝。
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                &declared,
                &[ExportSlot::new::<Tracked>(&first, &first_caller)],
            ),
            Err(ScopeError::OutputCountMismatch {
                declared: 2,
                supplied: 1,
                ..
            })
        ));

        // 空提交同样不能通过非空声明。
        assert!(matches!(
            fixture
                .coordinator
                .prepare_export(&child, &root, &declared, &[]),
            Err(ScopeError::OutputCountMismatch { supplied: 0, .. })
        ));

        // 数量相同但位置顺序与声明不符。
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                &declared,
                &[
                    ExportSlot::new::<Tracked>(&second, &first_caller),
                    ExportSlot::new::<Tracked>(&first, &second_caller),
                ],
            ),
            Err(ScopeError::OutputPositionMismatch { .. })
        ));

        // 声明本身重复位置也拒绝。
        assert!(matches!(
            fixture.coordinator.prepare_export(
                &child,
                &root,
                &[first.clone(), first.clone()],
                &[ExportSlot::new::<Tracked>(&first, &first_caller)],
            ),
            Err(ScopeError::DuplicatePosition { .. })
        ));

        // 以上均未产生任何 caller 绑定、责任转移或 Drop。
        assert!(
            fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .refs
                .is_empty()
        );
        assert_eq!(fixture.drops(), 0);
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Active
        );

        // 正式入口同样拒绝缩短声明，并走失败退出清理。
        let mut shortened = vec![ExportSlot::new::<Tracked>(&first, &first_caller)];
        assert!(matches!(
            fixture
                .coordinator
                .finalize(&child, &declared, &mut shortened),
            Err(ScopeError::OutputCountMismatch { .. })
        ));
        assert_eq!(fixture.drops(), 2);
        assert!(
            fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .refs
                .is_empty()
        );
    }

    #[test]
    fn r16_invocation_slots_are_consumed_and_cannot_be_reused() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let caller_position = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&child, &output, fixture.tracked(1))
            .unwrap();

        let declared = vec![output.clone()];
        let mut slots = vec![ExportSlot::new::<Tracked>(&output, &caller_position)];

        // 第一次提交：本次 Invocation 的 slot 被消费为空，声明列表未被消耗。
        fixture
            .coordinator
            .finalize(&child, &declared, &mut slots)
            .unwrap();
        assert!(slots.is_empty(), "invocation slots must be consumed");
        assert_eq!(declared.len(), 1, "declaration metadata stays reusable");
        assert!(
            fixture
                .coordinator
                .registry
                .lookup(&root)
                .unwrap()
                .refs
                .contains_key(&caller_position)
        );

        // 复用同一份已消费列表提交另一个 Invocation：不再被接受。
        let second = fixture.coordinator.create_child(&root).unwrap();
        let second_output = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&second, &second_output, fixture.tracked(2))
            .unwrap();
        let mut recycled = slots;
        assert!(matches!(
            fixture
                .coordinator
                .finalize(&second, &[second_output], &mut recycled),
            Err(ScopeError::OutputCountMismatch {
                declared: 1,
                supplied: 0,
                ..
            })
        ));
        // 崩溃的第二次提交走失败退出：只清理它自己的数据，第一次的输出不受影响。
        assert_eq!(
            fixture.coordinator.state(&second).unwrap(),
            ScopeState::Closed
        );
        assert_eq!(fixture.drops(), 1);
        assert_eq!(
            fixture
                .coordinator
                .resolve::<Tracked>(&root, &caller_position)
                .unwrap()
                .num,
            1
        );
    }

    #[test]
    fn r18_cleanup_diagnostic_is_not_hidden_by_the_export_error() {
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let temporary = fixture.ref_id();
        let caller_position = fixture.ref_id();

        fixture
            .coordinator
            .register_owned(&child, &output, Other(42))
            .unwrap();
        let stale = fixture
            .coordinator
            .register_owned(&child, &temporary, fixture.tracked(9))
            .unwrap();
        // 故障构造：临时 entry 失效，但 owned 责任记录仍在（该值已随 destroy 析构）。
        fixture.coordinator.container.destroy(&stale).unwrap();
        let drops_after_fault = fixture.drops();

        // 组合故障：Export 类型错误 + 清理前提破坏。
        // 必须报告清理诊断（Invariant），不能只让调用方看到 TypeMismatch。
        let mut slots = vec![ExportSlot::new::<Tracked>(&output, &caller_position)];
        let error = fixture
            .coordinator
            .finalize(&child, std::slice::from_ref(&output), &mut slots)
            .unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "cleanup failure was hidden: {error:?}"
        );
        // 未冒充正常 Closed，也没有越权销毁（失败清理未再析构任何值）。
        assert_ne!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
        assert_eq!(fixture.drops(), drops_after_fault);

        // 对照：普通 Export 类型错误且清理前提完好时，仍返回原诊断并完成清理。
        let mut fixture = Fixture::new();
        let root = fixture.root();
        let child = fixture.coordinator.create_child(&root).unwrap();
        let output = fixture.ref_id();
        let caller_position = fixture.ref_id();
        fixture
            .coordinator
            .register_owned(&child, &output, fixture.tracked(42))
            .unwrap();
        let mut slots = vec![ExportSlot::new::<Other>(&output, &caller_position)];
        let error = fixture
            .coordinator
            .finalize(&child, std::slice::from_ref(&output), &mut slots)
            .unwrap_err();
        assert!(matches!(error, ScopeError::TypeMismatch { .. }));
        assert_eq!(
            fixture.coordinator.state(&child).unwrap(),
            ScopeState::Closed
        );
        // 失败清理完成：child 自有的 Tracked 值析构一次。
        assert_eq!(fixture.drops(), 1);
    }

    // ---- V21-03：控制状态登记、导入与 Promote（C01～C11） ----

    /// C01：控制状态登记、句柄约束与归属／存在诊断。
    #[test]
    fn c01_control_state_registration_and_handles() {
        let mut f = Fixture::new();
        let root = f.root();
        let local = f.ref_id();
        f.coordinator
            .register_owned(&root, &local, f.tracked(1))
            .unwrap();

        // 从合法本地引用初始化：不分配 Definition RefId，也不重绑任何本地位置。
        let before = f.ids.allocate().unwrap().seq();
        let state = f.state::<Tracked>(&root, &local);
        let after = f.ids.allocate().unwrap().seq();
        assert_eq!(
            after - before,
            1,
            "register_state must not allocate or rebind Definition RefIds"
        );
        assert_eq!(state.owner(), &root);
        assert_eq!(state.seq(), 0);
        assert_eq!(f.refs_len(&root), 1);
        assert_eq!(f.uninit_state::<Tracked>(&root).seq(), 1);

        // 未初始化状态不能作为最终输出绑定。
        let out = f.ref_id();
        let uninit = f.uninit_state::<Tracked>(&root);
        assert!(matches!(
            f.coordinator.bind_state_output(&root, &uninit, &out),
            Err(ScopeError::StateUninitialized { .. })
        ));

        // 错误 owner：状态属于 root，被要求用于另一个控制器。
        let other = f.child(&root);
        let other_pos = f.ref_id();
        assert!(matches!(
            f.coordinator.bind_state_output(&other, &state, &other_pos),
            Err(ScopeError::StateWrongOwner { .. })
        ));

        // 来源不是本地引用时，登记直接拒绝。
        assert!(matches!(
            f.coordinator.register_state::<Tracked>(&root, &f.ref_id()),
            Err(ScopeError::RefNotBound { .. })
        ));

        // foreign 句柄：另一次 Execution 的状态不能被本协调组件解析。
        let mut foreign = Fixture::new();
        let foreign_root = foreign.root();
        let foreign_local = foreign.ref_id();
        foreign
            .coordinator
            .register_owned(&foreign_root, &foreign_local, foreign.tracked(7))
            .unwrap();
        let foreign_state = foreign.state::<Tracked>(&foreign_root, &foreign_local);
        assert!(matches!(
            f.coordinator.bind_state_output(&root, &foreign_state, &out),
            Err(ScopeError::StateForeignExecution { .. })
        ));
        assert!(matches!(
            f.coordinator.import_batch_with_states(
                &other,
                &root,
                &[],
                &[StateImportSlot::new::<Tracked>(&foreign_state, &other_pos)],
            ),
            Err(ScopeError::StateForeignExecution { .. })
        ));

        // 同 Execution 内从未登记的序号：StateNotRegistered（不授予任何 target）。
        let phantom = ControlStateId {
            owner: root.clone(),
            seq: 42,
        };
        assert!(matches!(
            f.coordinator.bind_state_output(&root, &phantom, &out),
            Err(ScopeError::StateNotRegistered { .. })
        ));

        // 控制器关闭后：旧句柄只得到 Closed，不复活。
        f.coordinator.abort(&other).unwrap();
        let declared: Vec<RefId> = Vec::new();
        let mut slots: Vec<ExportSlot> = Vec::new();
        f.coordinator
            .finalize(&root, &declared, &mut slots)
            .unwrap();
        assert!(matches!(
            f.coordinator.bind_state_output(&root, &state, &out),
            Err(ScopeError::ScopeClosed { .. })
        ));
        assert!(matches!(
            f.coordinator.promote(&other, &other_pos, &state),
            Err(ScopeError::ScopeClosed { .. })
        ));
        assert_eq!(f.drops(), 1, "root exit destroyed its own value once");
    }

    /// C02：状态来源与本地来源混合的整组 Import，失败不留部分绑定。
    #[test]
    fn c02_state_import_is_group_atomic_and_mixed() {
        let mut f = Fixture::new();
        let root = f.root();
        let d1_pos = f.ref_id();
        let d2_pos = f.ref_id();
        let d1 = f
            .coordinator
            .register_owned(&root, &d1_pos, f.tracked(1))
            .unwrap();
        f.coordinator
            .register_owned(&root, &d2_pos, f.tracked(2))
            .unwrap();

        let controller = f.child(&root);
        let c1 = f.ref_id();
        let c2 = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[
                    ImportSlot::new::<Tracked>(&d1_pos, &c1),
                    ImportSlot::new::<Tracked>(&d2_pos, &c2),
                ],
            )
            .unwrap();
        let state = f.state::<Tracked>(&controller, &c1);

        // 混合批次：本地引用与状态来源同组导入并全部成功。
        let round = f.child(&controller);
        let local_target = f.ref_id();
        let state_target = f.ref_id();
        f.coordinator
            .import_batch_with_states(
                &round,
                &controller,
                &[ImportSlot::new::<Tracked>(&c2, &local_target)],
                &[StateImportSlot::new::<Tracked>(&state, &state_target)],
            )
            .unwrap_or_else(|error| panic!("mixed import rejected: {error}"));
        assert_eq!(f.refs_len(&round), 2);
        assert_eq!(
            f.owner(&d1).unwrap(),
            root,
            "state import never changes owner"
        );
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&round, &state_target)
                .unwrap()
                .num,
            1
        );
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&round, &local_target)
                .unwrap()
                .num,
            2
        );

        // 后项失败（声明类型与状态登记类型不符）时，前项也不得留下部分绑定。
        let round2 = f.child(&controller);
        let ok_target = f.ref_id();
        let bad_target = f.ref_id();
        assert!(matches!(
            f.coordinator.import_batch_with_states(
                &round2,
                &controller,
                &[ImportSlot::new::<Tracked>(&c2, &ok_target)],
                &[StateImportSlot::new::<Other>(&state, &bad_target)],
            ),
            Err(ScopeError::StateTypeMismatch { .. })
        ));
        assert_eq!(f.refs_len(&round2), 0, "no partial import bindings");
        assert_eq!(f.owner(&d1).unwrap(), root);
        assert_eq!(f.owned_len(&round2), 0);

        // 未初始化的状态不能作为导入来源。
        let uninit = f.uninit_state::<Tracked>(&controller);
        let target = f.ref_id();
        assert!(matches!(
            f.coordinator.import_batch_with_states(
                &round2,
                &controller,
                &[],
                &[StateImportSlot::new::<Tracked>(&uninit, &target)]
            ),
            Err(ScopeError::StateUninitialized { .. })
        ));
        assert_eq!(f.refs_len(&round2), 0);

        // 状态句柄的控制器与 caller 不一致：不得导入。
        let other_controller = f.child(&root);
        let round3 = f.child(&other_controller);
        let target3 = f.ref_id();
        assert!(matches!(
            f.coordinator.import_batch_with_states(
                &round3,
                &other_controller,
                &[],
                &[StateImportSlot::new::<Tracked>(&state, &target3)]
            ),
            Err(ScopeError::StateWrongOwner { .. })
        ));
        assert_eq!(f.refs_len(&round3), 0);
    }

    /// C03：两轮新 owned 值的 Promote：责任转移、无父 Ref 绑定、逐轮导入。
    #[test]
    fn c03_two_rounds_of_new_owned_promote() {
        let mut f = Fixture::new();
        let root = f.root();
        let seed_pos = f.ref_id();
        let d1 = f
            .coordinator
            .register_owned(&root, &seed_pos, f.tracked(1))
            .unwrap();

        // 控制器显式导入 Root 输入，再从自己的本地引用初始化控制状态。
        let controller = f.child(&root);
        let ctrl_seed = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
            )
            .unwrap();
        let state = f.state::<Tracked>(&controller, &ctrl_seed);
        let refs_before = f.refs_len(&controller);

        let round1 = f.child(&controller);
        let r1_in = f.ref_id();
        f.coordinator
            .import_batch_with_states(
                &round1,
                &controller,
                &[],
                &[StateImportSlot::new::<Tracked>(&state, &r1_in)],
            )
            .unwrap();
        let r1_out = f.ref_id();
        let d2 = f
            .coordinator
            .register_owned(&round1, &r1_out, f.tracked(2))
            .unwrap();
        f.coordinator.promote(&round1, &r1_out, &state).unwrap();

        assert_eq!(f.scope_state(&round1), ScopeState::Closed);
        assert_eq!(
            f.owner(&d2).unwrap(),
            controller,
            "source-owned value moved to the controller"
        );
        assert_eq!(f.owner(&d1).unwrap(), root, "seed keeps its owner");
        assert_eq!(
            f.refs_len(&controller),
            refs_before,
            "promote binds no parent ref"
        );
        assert_eq!(f.drops(), 0);

        let round2 = f.child(&controller);
        let r2_in = f.ref_id();
        f.coordinator
            .import_batch_with_states(
                &round2,
                &controller,
                &[],
                &[StateImportSlot::new::<Tracked>(&state, &r2_in)],
            )
            .unwrap();
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&round2, &r2_in)
                .unwrap()
                .num,
            2,
            "round2 sees the promoted state"
        );
        let r2_out = f.ref_id();
        let d3 = f
            .coordinator
            .register_owned(&round2, &r2_out, f.tracked(3))
            .unwrap();
        f.coordinator.promote(&round2, &r2_out, &state).unwrap();
        assert_eq!(f.owner(&d3).unwrap(), controller);
        assert_eq!(f.drops(), 0, "replaced state waits for controlled recycle");
        assert!(f.alive(&d2));

        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(
            f.drops(),
            1,
            "controller-owned replaced state drops exactly once"
        );
        assert!(!f.alive(&d2));
        assert!(f.alive(&d1), "imported seed keeps its owner");
        assert!(f.alive(&d3));
    }

    /// C04：来源原样输出 imported 目标时只更新状态，不转移 owner。
    #[test]
    fn c04_imported_selected_result_only_updates_state() {
        let mut f = Fixture::new();
        let root = f.root();
        let seed_pos = f.ref_id();
        let d1 = f
            .coordinator
            .register_owned(&root, &seed_pos, f.tracked(5))
            .unwrap();
        let controller = f.child(&root);
        let ctrl_seed = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
            )
            .unwrap();
        let state = f.state::<Tracked>(&controller, &ctrl_seed);

        let round = f.child(&controller);
        let in_pos = f.ref_id();
        let out_pos = f.ref_id();
        f.coordinator
            .import_batch_with_states(
                &round,
                &controller,
                &[],
                &[
                    StateImportSlot::new::<Tracked>(&state, &in_pos),
                    StateImportSlot::new::<Tracked>(&state, &out_pos),
                ],
            )
            .unwrap();
        f.coordinator.promote(&round, &out_pos, &state).unwrap();

        assert_eq!(
            f.owner(&d1).unwrap(),
            root,
            "imported target keeps its owner"
        );
        assert_eq!(f.drops(), 0);
        assert!(f.readable_as::<Tracked>(&d1));
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&root, &seed_pos)
                .unwrap()
                .num,
            5
        );
        assert_eq!(f.scope_state(&round), ScopeState::Closed);
    }

    /// C05：连续多轮原样保留同一 imported DataId。
    #[test]
    fn c05_imported_same_data_id_repeats_without_owner_change() {
        let mut f = Fixture::new();
        let root = f.root();
        let seed_pos = f.ref_id();
        let d1 = f
            .coordinator
            .register_owned(&root, &seed_pos, f.tracked(11))
            .unwrap();
        let controller = f.child(&root);
        let ctrl_seed = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
            )
            .unwrap();
        let state = f.state::<Tracked>(&controller, &ctrl_seed);

        for round_index in 0..2 {
            let round = f.child(&controller);
            let in_pos = f.ref_id();
            let out_pos = f.ref_id();
            f.coordinator
                .import_batch_with_states(
                    &round,
                    &controller,
                    &[],
                    &[
                        StateImportSlot::new::<Tracked>(&state, &in_pos),
                        StateImportSlot::new::<Tracked>(&state, &out_pos),
                    ],
                )
                .unwrap();
            f.coordinator.promote(&round, &out_pos, &state).unwrap();
            assert_eq!(
                f.owner(&d1).unwrap(),
                root,
                "round {round_index}: owner stays root"
            );
            assert!(f.alive(&d1));
            assert_eq!(f.drops(), 0);
        }

        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(
            f.drops(),
            0,
            "imported state is never recycled by the controller"
        );
        assert_eq!(f.owner(&d1).unwrap(), root);

        // 后续受控导入仍然成功。
        let round = f.child(&controller);
        let in_pos = f.ref_id();
        f.coordinator
            .import_batch_with_states(
                &round,
                &controller,
                &[],
                &[StateImportSlot::new::<Tracked>(&state, &in_pos)],
            )
            .unwrap();
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&round, &in_pos)
                .unwrap()
                .num,
            11
        );
    }

    /// C06：控制器自有的 same-DataId 提升：不销毁、不重复登记、继续导入与最终 Export。
    #[test]
    fn c06_controller_owned_same_data_id() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let own_pos = f.ref_id();
        let d2 = f
            .coordinator
            .register_owned(&controller, &own_pos, f.tracked(20))
            .unwrap();
        let state = f.state::<Tracked>(&controller, &own_pos);

        for _ in 0..2 {
            let round = f.child(&controller);
            let in_pos = f.ref_id();
            let out_pos = f.ref_id();
            f.coordinator
                .import_batch_with_states(
                    &round,
                    &controller,
                    &[],
                    &[
                        StateImportSlot::new::<Tracked>(&state, &in_pos),
                        StateImportSlot::new::<Tracked>(&state, &out_pos),
                    ],
                )
                .unwrap();
            f.coordinator.promote(&round, &out_pos, &state).unwrap();
            assert_eq!(f.owner(&d2).unwrap(), controller, "no new owner is created");
            assert!(f.alive(&d2));
            assert_eq!(f.drops(), 0);
        }

        // 最终输出位置一次性绑定，再正常 Export 给 caller。
        let final_pos = f.ref_id();
        f.coordinator
            .bind_state_output(&controller, &state, &final_pos)
            .unwrap();
        let root_out = f.ref_id();
        f.finalize_one::<Tracked>(&controller, &final_pos, &root_out)
            .unwrap();
        assert_eq!(f.owner(&d2).unwrap(), root);
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&root, &root_out)
                .unwrap()
                .num,
            20
        );
        assert_eq!(f.drops(), 0, "exported value is owned by the caller");
    }

    /// C07：旧状态回收受存活引用与其它控制状态约束。
    #[test]
    fn c07_recycle_respects_live_references_and_other_states() {
        let mut f = Fixture::new();
        let root = f.root();

        // (a) 尚存活 descendant 的有效 alias 阻止回收。
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);
        let round1 = f.child(&controller);
        let out1 = f.ref_id();
        f.coordinator
            .register_owned(&round1, &out1, f.tracked(1))
            .unwrap();
        f.coordinator.promote(&round1, &out1, &state).unwrap();

        let keeper = f.child(&controller);
        let keeper_pos = f.ref_id();
        f.coordinator
            .import_batch_with_states(
                &keeper,
                &controller,
                &[],
                &[StateImportSlot::new::<Tracked>(&state, &keeper_pos)],
            )
            .unwrap();

        let round2 = f.child(&controller);
        let out2 = f.ref_id();
        let d2 = f
            .coordinator
            .register_owned(&round2, &out2, f.tracked(2))
            .unwrap();
        f.coordinator.promote(&round2, &out2, &state).unwrap();

        // 旧值此时既被 keeper 引用，也被替换前登记为 pending。
        assert_eq!(f.owner(&d2).unwrap(), controller);
        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(f.drops(), 0, "live descendant alias blocks recycling");
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&keeper, &keeper_pos)
                .unwrap()
                .num,
            1
        );

        f.coordinator.abort(&keeper).unwrap();
        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(
            f.drops(),
            1,
            "old state drops exactly once after the alias disappears"
        );
        assert!(f.alive(&d2));

        // (b) 另一个控制状态保留旧值时同样不回收。
        let holder_a = f.uninit_state::<Tracked>(&controller);
        let holder_b = f.uninit_state::<Tracked>(&controller);
        let round3 = f.child(&controller);
        let out3 = f.ref_id();
        let d3 = f
            .coordinator
            .register_owned(&round3, &out3, f.tracked(3))
            .unwrap();
        f.coordinator.promote(&round3, &out3, &holder_a).unwrap();

        let round4 = f.child(&controller);
        let in4 = f.ref_id();
        let out4 = f.ref_id();
        f.coordinator
            .import_batch_with_states(
                &round4,
                &controller,
                &[],
                &[
                    StateImportSlot::new::<Tracked>(&holder_a, &in4),
                    StateImportSlot::new::<Tracked>(&holder_a, &out4),
                ],
            )
            .unwrap();
        f.coordinator.promote(&round4, &out4, &holder_b).unwrap();

        let round5 = f.child(&controller);
        let out5 = f.ref_id();
        f.coordinator
            .register_owned(&round5, &out5, f.tracked(5))
            .unwrap();
        f.coordinator.promote(&round5, &out5, &holder_a).unwrap();

        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(
            f.drops(),
            1,
            "another control state still holds the old value"
        );
        assert!(f.alive(&d3));

        let round6 = f.child(&controller);
        let out6 = f.ref_id();
        f.coordinator
            .register_owned(&round6, &out6, f.tracked(6))
            .unwrap();
        f.coordinator.promote(&round6, &out6, &holder_b).unwrap();
        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(f.drops(), 2, "recycled once all control states moved on");
        assert!(!f.alive(&d3));
    }

    /// C07：控制器自身 alias 与 imported 旧值的回收边界。
    #[test]
    fn c07_recycle_respects_local_alias_and_imported_state() {
        let mut f = Fixture::new();
        let root = f.root();

        // (c) 控制器自身仍有效的本地 alias：不回收，最迟随控制器退出清理。
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);
        let round1 = f.child(&controller);
        let out1 = f.ref_id();
        let d1 = f
            .coordinator
            .register_owned(&round1, &out1, f.tracked(1))
            .unwrap();
        f.coordinator.promote(&round1, &out1, &state).unwrap();
        let final_pos = f.ref_id();
        f.coordinator
            .bind_state_output(&controller, &state, &final_pos)
            .unwrap();

        let round2 = f.child(&controller);
        let out2 = f.ref_id();
        let d2 = f
            .coordinator
            .register_owned(&round2, &out2, f.tracked(2))
            .unwrap();
        f.coordinator.promote(&round2, &out2, &state).unwrap();
        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(
            f.drops(),
            0,
            "controller-local alias must not be deleted for recycling"
        );
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&controller, &final_pos)
                .unwrap()
                .num,
            1
        );

        // 控制器正常退出：pending 兜底清理，未导出值一并销毁。
        let declared: Vec<RefId> = Vec::new();
        let mut slots: Vec<ExportSlot> = Vec::new();
        f.coordinator
            .finalize(&controller, &declared, &mut slots)
            .unwrap();
        assert_eq!(f.drops(), 2);
        assert!(!f.alive(&d1));
        assert!(!f.alive(&d2));

        // (d) imported 旧值从不被控制器回收。
        let seed_pos = f.ref_id();
        let seed = f
            .coordinator
            .register_owned(&root, &seed_pos, f.tracked(9))
            .unwrap();
        let controller2 = f.child(&root);
        let ctrl_seed = f.ref_id();
        f.coordinator
            .import_batch(
                &controller2,
                &root,
                &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
            )
            .unwrap();
        let state2 = f.state::<Tracked>(&controller2, &ctrl_seed);

        let round3 = f.child(&controller2);
        let out3 = f.ref_id();
        f.coordinator
            .register_owned(&round3, &out3, f.tracked(10))
            .unwrap();
        f.coordinator.promote(&round3, &out3, &state2).unwrap();
        f.coordinator.recycle_pending(&controller2).unwrap();
        assert_eq!(
            f.owner(&seed).unwrap(),
            root,
            "imported state keeps its owner"
        );
        assert!(f.alive(&seed));
        assert_eq!(
            f.drops(),
            2,
            "no controller-side recycle of an imported value"
        );

        // imported 值随其 owner 退出而销毁，而不是随控制器。
        f.coordinator.abort(&controller2).unwrap();
        assert_eq!(
            f.drops(),
            3,
            "controller exit cleans its own promoted value"
        );
        let declared: Vec<RefId> = Vec::new();
        let mut slots: Vec<ExportSlot> = Vec::new();
        f.coordinator
            .finalize(&root, &declared, &mut slots)
            .unwrap();
        assert_eq!(f.drops(), 4);
        assert!(!f.alive(&seed));
    }

    /// C08：仅最终输出位置绑定一次；多轮 Promote 不绑定父 Ref。
    #[test]
    fn c08_only_final_output_ref_binding() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);
        let refs_before = f.refs_len(&controller);

        let mut last: Option<DataId> = None;
        for num in [1u32, 2] {
            let round = f.child(&controller);
            let out = f.ref_id();
            let id = f
                .coordinator
                .register_owned(&round, &out, f.tracked(num))
                .unwrap();
            f.coordinator.promote(&round, &out, &state).unwrap();
            last = Some(id);
        }
        assert_eq!(
            f.refs_len(&controller),
            refs_before,
            "no parent Ref binding during rounds"
        );

        let final_pos = f.ref_id();
        f.coordinator
            .bind_state_output(&controller, &state, &final_pos)
            .unwrap();
        assert_eq!(f.refs_len(&controller), refs_before + 1);
        // 同一位置不能再次绑定（不存在解绑／重绑接口）。
        assert!(matches!(
            f.coordinator
                .bind_state_output(&controller, &state, &final_pos),
            Err(ScopeError::RefAlreadyBound { .. })
        ));

        let root_out = f.ref_id();
        f.finalize_one::<Tracked>(&controller, &final_pos, &root_out)
            .unwrap();
        assert_eq!(f.scope_state(&controller), ScopeState::Closed);
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&root, &root_out)
                .unwrap()
                .num,
            2
        );
        let exported = last.expect("two rounds promoted a value");
        assert_eq!(
            f.owner(&exported).unwrap(),
            root,
            "unique owner after export"
        );
        assert_eq!(f.owned_len(&controller), 0);
        // 旧 state 句柄随控制器关闭失效。
        assert!(matches!(
            f.coordinator
                .bind_state_output(&controller, &state, &f.ref_id()),
            Err(ScopeError::ScopeClosed { .. })
        ));
    }

    /// C09：Promote 预检拒绝：状态、责任与两侧内容都不变。
    #[test]
    fn c09_promote_precheck_rejections() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let seed_pos = f.ref_id();
        f.coordinator
            .register_owned(&controller, &seed_pos, f.tracked(1))
            .unwrap();
        let state = f.state::<Tracked>(&controller, &seed_pos);

        // (i) 类型不符
        let round = f.child(&controller);
        let out = f.ref_id();
        f.coordinator
            .register_owned(&round, &out, Other(7))
            .unwrap();
        assert!(matches!(
            f.coordinator.promote(&round, &out, &state),
            Err(ScopeError::TypeMismatch { .. })
        ));
        assert_eq!(f.scope_state(&round), ScopeState::Closed);
        assert_eq!(f.drops(), 0);

        // (ii) 未绑定的输出位置
        let round = f.child(&controller);
        let out = f.ref_id();
        assert!(matches!(
            f.coordinator.promote(&round, &out, &state),
            Err(ScopeError::RefNotBound { .. })
        ));
        assert_eq!(f.scope_state(&round), ScopeState::Closed);

        // (iii) 已失效 target：控制器另一个值导入 Round 后 entry 被销毁。
        let dead_pos = f.ref_id();
        let dead_src = f
            .coordinator
            .register_owned(&controller, &dead_pos, f.tracked(3))
            .unwrap();
        let round = f.child(&controller);
        let out = f.ref_id();
        f.coordinator
            .import_batch(
                &round,
                &controller,
                &[ImportSlot::new::<Tracked>(&dead_pos, &out)],
            )
            .unwrap();
        f.coordinator.container.destroy(&dead_src).unwrap();
        assert!(matches!(
            f.coordinator.promote(&round, &out, &state),
            Err(ScopeError::TargetNotAlive { .. })
        ));
        assert_eq!(f.scope_state(&round), ScopeState::Closed);

        // (iv) sibling-owned：注入一个由兄弟 Scope 负责的 target。
        let sibling = f.child(&controller);
        let sib_pos = f.ref_id();
        let sib_id = f
            .coordinator
            .register_owned(&sibling, &sib_pos, f.tracked(4))
            .unwrap();
        let round = f.child(&controller);
        let out = f.ref_id();
        f.inject_target(&round, &out, RefTarget::Data(sib_id.clone()));
        assert!(matches!(
            f.coordinator.promote(&round, &out, &state),
            Err(ScopeError::IllegalOwner { .. })
        ));
        assert_eq!(f.scope_state(&round), ScopeState::Closed);
        assert!(
            f.alive(&sib_id),
            "rejected promote must not destroy a sibling value"
        );

        // (v) 错误状态句柄：状态属于另一个控制器。
        let other = f.child(&root);
        let other_state = f.uninit_state::<Tracked>(&other);
        let round = f.child(&controller);
        let out = f.ref_id();
        f.coordinator
            .register_owned(&round, &out, f.tracked(5))
            .unwrap();
        assert!(matches!(
            f.coordinator.promote(&round, &out, &other_state),
            Err(ScopeError::StateWrongOwner { .. })
        ));

        // (vi) 非直接父控制器：句柄属于 root，来源是孙 Scope。
        let root_state = f.uninit_state::<Tracked>(&root);
        let round = f.child(&controller);
        let out = f.ref_id();
        let extra = f
            .coordinator
            .register_owned(&round, &out, f.tracked(6))
            .unwrap();
        assert!(matches!(
            f.coordinator.promote(&round, &out, &root_state),
            Err(ScopeError::StateWrongOwner { .. })
        ));
        assert_eq!(f.scope_state(&round), ScopeState::Closed);
        assert!(
            !f.alive(&extra),
            "failure exit cleans the source's own values"
        );
        assert!(matches!(
            f.coordinator
                .bind_state_output(&root, &root_state, &f.ref_id()),
            Err(ScopeError::StateUninitialized { .. })
        ));

        // 所有拒绝都没有改变状态与责任：仍可绑定并读到 seed 值。
        let probe = f.ref_id();
        f.coordinator
            .bind_state_output(&controller, &state, &probe)
            .unwrap();
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&controller, &probe)
                .unwrap()
                .num,
            1
        );
        assert!(f.alive(&sib_id));
    }

    /// C10：正式失败退出保留 ancestor，并优先传播清理诊断。
    #[test]
    fn c10_promote_failure_exit_preserves_ancestor_and_cleanup_diagnostic() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let seed_pos = f.ref_id();
        f.coordinator
            .register_owned(&controller, &seed_pos, f.tracked(1))
            .unwrap();
        let state = f.state::<Tracked>(&controller, &seed_pos);

        let anc_pos = f.ref_id();
        let ancestor = f
            .coordinator
            .register_owned(&root, &anc_pos, f.tracked(50))
            .unwrap();
        let ctrl_anc = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(&anc_pos, &ctrl_anc)],
            )
            .unwrap();

        // 普通预检失败：Round Closed、自有临时值清理，ancestor 与状态保留。
        let round = f.child(&controller);
        let r_in = f.ref_id();
        f.coordinator
            .import_batch(
                &round,
                &controller,
                &[ImportSlot::new::<Tracked>(&ctrl_anc, &r_in)],
            )
            .unwrap();
        let temp = f.ref_id();
        let out = f.ref_id();
        f.coordinator
            .register_owned(&round, &temp, f.tracked(60))
            .unwrap();
        f.coordinator
            .register_owned(&round, &out, Other(61))
            .unwrap();
        assert!(matches!(
            f.coordinator.promote(&round, &out, &state),
            Err(ScopeError::TypeMismatch { .. })
        ));
        assert_eq!(f.scope_state(&round), ScopeState::Closed);
        assert_eq!(f.drops(), 1, "round temporary cleaned exactly once");
        assert!(f.alive(&ancestor));
        assert_eq!(f.owner(&ancestor).unwrap(), root);
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&root, &anc_pos)
                .unwrap()
                .num,
            50
        );
        let probe = f.ref_id();
        f.coordinator
            .bind_state_output(&controller, &state, &probe)
            .unwrap();
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&controller, &probe)
                .unwrap()
                .num,
            1
        );

        // 清理前提破坏：预检无法完成 → 传播清理诊断，不误删、不冒充 Closed。
        let round = f.child(&controller);
        let out = f.ref_id();
        let owned_id = f
            .coordinator
            .register_owned(&round, &out, f.tracked(70))
            .unwrap();
        let broken = f.ref_id();
        let broken_id = f
            .coordinator
            .register_owned(&round, &broken, f.tracked(71))
            .unwrap();
        f.coordinator.container.destroy(&broken_id).unwrap();
        let drops_before = f.drops();
        assert!(matches!(
            f.coordinator.promote(&round, &out, &state),
            Err(ScopeError::Invariant { .. })
        ));
        assert_ne!(f.scope_state(&round), ScopeState::Closed);
        assert_eq!(
            f.drops(),
            drops_before,
            "broken cleanup grants no destruction right"
        );
        assert!(f.alive(&owned_id));
        assert!(f.alive(&ancestor));
    }

    /// C11：Promote／Consume 的来源存活边界。
    #[test]
    fn c11_source_liveness_boundary() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);
        let collector = f.collector::<Tracked>(&controller);

        // Closed 来源：拒绝。
        let closed = f.child(&controller);
        let out = f.ref_id();
        f.coordinator
            .register_owned(&closed, &out, f.tracked(1))
            .unwrap();
        f.coordinator.abort(&closed).unwrap();
        assert!(matches!(
            f.coordinator.promote(&closed, &out, &state),
            Err(ScopeError::ScopeClosed { .. })
        ));
        assert!(matches!(
            f.coordinator.consume_item(&closed, &out, &collector),
            Err(ScopeError::ScopeClosed { .. })
        ));

        // 带活跃 descendant：在冻结／mutation 前拒绝。
        let round = f.child(&controller);
        let out = f.ref_id();
        let id = f
            .coordinator
            .register_owned(&round, &out, f.tracked(2))
            .unwrap();
        let inner = f.child(&round);
        assert!(matches!(
            f.coordinator.promote(&round, &out, &state),
            Err(ScopeError::ActiveDescendants { .. })
        ));
        assert_eq!(
            f.scope_state(&round),
            ScopeState::Active,
            "not frozen before rejection"
        );
        assert_eq!(f.scope_state(&inner), ScopeState::Active);
        assert_eq!(f.owner(&id).unwrap(), round);
        assert!(matches!(
            f.coordinator.consume_item(&round, &out, &collector),
            Err(ScopeError::ActiveDescendants { .. })
        ));
        assert_eq!(f.scope_state(&round), ScopeState::Active);
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
        assert!(matches!(
            f.coordinator
                .finish_collector(&controller, &collector, &f.ref_id()),
            Err(ScopeError::ActiveDescendants { .. })
        ));
        assert_eq!(f.owned_len(&controller), 0);
    }

    /// C12：collector 值只在 Container 建构区；未完成 collector 没有普通 DataId。
    #[test]
    fn c12_collector_physical_storage_is_container_only() {
        fn scope_shape(record: &Scope) {
            let _: &ScopeId = &record.id;
            let _: &Option<ScopeId> = &record.parent;
            let _: &Vec<ScopeId> = &record.children;
            let _: &ScopeState = &record.state;
            let _: &HashMap<RefId, RefTarget> = &record.refs;
            let _: &HashSet<DataId> = &record.owned;
            let _: &u64 = &record.state_next;
        }
        fn registry_shape(registry: &ScopeRegistry) {
            let _: &HashMap<u64, Scope> = &registry.scopes;
            let _: &HashMap<(u64, u64), ControlState> = &registry.states;
            let _: &HashMap<u64, CollectorRecord> = &registry.collectors;
        }
        fn collector_record_shape(record: &CollectorRecord) {
            let _: &CollectorId = &record.id;
            let _: &ScopeId = &record.owner;
            let _: &TypeId = &record.element_type;
            let _: &&'static str = &record.element_name;
        }

        let mut f = Fixture::new();
        let controller = f.child(&f.root());
        let item = f.child(&controller);
        let out = f.ref_id();
        let item_id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        let collector = f.collector::<Tracked>(&controller);
        assert_eq!(item_id.seq(), 0);

        f.coordinator.consume_item(&item, &out, &collector).unwrap();
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
        assert!(!f.alive(&item_id), "consumed item identity is invalid");

        let final_pos = f.ref_id();
        let finished = f
            .coordinator
            .finish_collector(&controller, &collector, &final_pos)
            .unwrap();
        assert_eq!(
            finished.seq(),
            1,
            "the collection DataId is allocated only at completion"
        );
        assert_eq!(
            f.coordinator
                .resolve::<Vec<Tracked>>(&controller, &final_pos)
                .unwrap()
                .len(),
            1
        );

        scope_shape(f.coordinator.registry.lookup(&controller).unwrap());
        registry_shape(&f.coordinator.registry);
        collector_record_shape(&CollectorRecord {
            id: collector,
            owner: controller.clone(),
            element_type: TypeId::of::<Tracked>(),
            element_name: std::any::type_name::<Tracked>(),
        });
    }

    /// C13：collector 身份、责任与完成／清理后的句柄。
    #[test]
    fn c13_collector_identity_and_responsibility() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);

        let tracked_c = f.collector::<Tracked>(&controller);
        let other_c = f.collector::<Other>(&controller);
        assert_ne!(tracked_c, other_c);
        let (name, owner) = f.coordinator.collector_metadata(&tracked_c).unwrap();
        assert_eq!(name, std::any::type_name::<Tracked>());
        assert_eq!(owner, controller);

        // foreign 句柄。
        let mut foreign = Fixture::new();
        let foreign_root = foreign.root();
        let foreign_c = foreign.collector::<Tracked>(&foreign_root);
        assert!(matches!(
            f.coordinator.collector_len(&foreign_c),
            Err(ScopeError::CollectorForeignExecution { .. })
        ));

        // 错误责任 Scope：消费端与完成端的控制器都不是 collector 的 owner。
        let elsewhere = f.child(&root);
        let pos = f.ref_id();
        f.coordinator
            .register_owned(&elsewhere, &pos, f.tracked(1))
            .unwrap();
        assert!(matches!(
            f.coordinator.consume_item(&elsewhere, &pos, &tracked_c),
            Err(ScopeError::CollectorNotOwnedBy { .. })
        ));
        assert_eq!(
            f.scope_state(&elsewhere),
            ScopeState::Closed,
            "failed consume closes its own source only"
        );
        let solo = f.child(&root);
        let wrong_owner = f
            .coordinator
            .finish_collector(&solo, &tracked_c, &f.ref_id())
            .unwrap_err();
        assert!(
            matches!(wrong_owner, ScopeError::CollectorNotOwnedBy { .. }),
            "unexpected diagnostic: {wrong_owner:?}"
        );
        assert_eq!(f.scope_state(&solo), ScopeState::Active);

        // 显式空 collector 完成为新的空 Vec 普通 Data。
        let empty_pos = f.ref_id();
        f.coordinator
            .finish_collector(&controller, &other_c, &empty_pos)
            .unwrap();
        assert_eq!(
            f.coordinator
                .resolve::<Vec<Other>>(&controller, &empty_pos)
                .unwrap()
                .len(),
            0
        );

        // 已完成句柄：不可再追加或重复完成。
        assert!(matches!(
            f.coordinator.collector_len(&other_c),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));
        assert!(matches!(
            f.coordinator
                .finish_collector(&controller, &other_c, &f.ref_id()),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));
        let item = f.child(&controller);
        let item_out = f.ref_id();
        f.coordinator
            .register_owned(&item, &item_out, f.tracked(2))
            .unwrap();
        assert!(matches!(
            f.coordinator.consume_item(&item, &item_out, &other_c),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));

        // 清理后句柄：随控制器退出撤销。
        f.coordinator.abort(&controller).unwrap();
        assert!(matches!(
            f.coordinator.collector_len(&tracked_c),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));
    }

    /// C14：ItemScope 自有输出直接 Consume，无逐项 EachScope 中转。
    #[test]
    fn c14_item_owned_direct_consume() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let collector = f.collector::<Tracked>(&each);
        let item = f.child(&each);
        let out = f.ref_id();
        let item_id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(20))
            .unwrap();

        f.coordinator.consume_item(&item, &out, &collector).unwrap();
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 1);
        assert_eq!(f.drops(), 0, "moved element is not dropped");
        assert!(!f.alive(&item_id));
        assert!(!f.readable_as::<Tracked>(&item_id));
        assert_eq!(f.scope_state(&item), ScopeState::Closed);
        assert_eq!(f.owned_len(&item), 0);
        assert_eq!(f.refs_len(&each), 0);
        assert_eq!(f.owned_len(&each), 0, "no per-item transfer into EachScope");
    }

    /// C15：imported／parent-owned／sibling-owned 输出在取值前拒绝。
    #[test]
    fn c15_imported_and_parent_owned_outputs_rejected_before_moving() {
        let mut f = Fixture::new();
        let root = f.root();
        let shared_pos = f.ref_id();
        let shared = f
            .coordinator
            .register_owned(&root, &shared_pos, f.tracked(1))
            .unwrap();
        let each = f.child(&root);
        let each_pos = f.ref_id();
        f.coordinator
            .import_batch(
                &each,
                &root,
                &[ImportSlot::new::<Tracked>(&shared_pos, &each_pos)],
            )
            .unwrap();
        let collector = f.collector::<Tracked>(&each);

        // (a) 原样重新暴露 shared ancestor 的完整 Data。
        let item = f.child(&each);
        let alias = f.ref_id();
        f.coordinator
            .import_batch(
                &item,
                &each,
                &[ImportSlot::new::<Tracked>(&each_pos, &alias)],
            )
            .unwrap();
        assert!(matches!(
            f.coordinator.consume_item(&item, &alias, &collector),
            Err(ScopeError::IllegalOwner { .. })
        ));
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert!(f.alive(&shared));
        assert_eq!(f.owner(&shared).unwrap(), root);

        // (b) parent(Each) 自己 owned 的 Data。
        let own_pos = f.ref_id();
        let own = f
            .coordinator
            .register_owned(&each, &own_pos, f.tracked(2))
            .unwrap();
        let item2 = f.child(&each);
        let alias2 = f.ref_id();
        f.coordinator
            .import_batch(
                &item2,
                &each,
                &[ImportSlot::new::<Tracked>(&own_pos, &alias2)],
            )
            .unwrap();
        assert!(matches!(
            f.coordinator.consume_item(&item2, &alias2, &collector),
            Err(ScopeError::IllegalOwner { .. })
        ));
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert!(f.alive(&own));

        // (c) sibling-owned：单有 target 存活不授予消费权。
        let item3 = f.child(&each);
        let temp = f.ref_id();
        let temp_id = f
            .coordinator
            .register_owned(&item3, &temp, f.tracked(3))
            .unwrap();
        let item4 = f.child(&each);
        let injected = f.ref_id();
        f.inject_target(&item4, &injected, RefTarget::Data(temp_id.clone()));
        assert!(matches!(
            f.coordinator.consume_item(&item4, &injected, &collector),
            Err(ScopeError::IllegalOwner { .. })
        ));
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert!(f.alive(&temp_id));
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    }

    /// C16：无 owner／重复 owner 的责任破坏诊断，且在移动前拒绝。
    #[test]
    fn c16_missing_and_duplicate_owner_rejections() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let collector = f.collector::<Tracked>(&each);

        // 无 owner：责任记录被撤销，target 仍存活。
        let item = f.child(&each);
        let out = f.ref_id();
        let id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        f.drop_ownership(&item, &id);
        assert!(matches!(
            f.coordinator.consume_item(&item, &out, &collector),
            Err(ScopeError::NoOwner { .. })
        ));
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert!(f.alive(&id), "broken ownership grants no destruction right");

        // 重复 owner：预检发现责任破坏；清理前提校验同样失败，因此优先传播清理诊断。
        let item = f.child(&each);
        let out = f.ref_id();
        let id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(2))
            .unwrap();
        f.add_ownership(&each, &id);
        let error = f
            .coordinator
            .consume_item(&item, &out, &collector)
            .unwrap_err();
        assert!(
            matches!(
                error,
                ScopeError::DuplicateOwner { .. } | ScopeError::Invariant { .. }
            ),
            "duplicate ownership must surface as a responsibility/cleanup diagnostic, got {error:?}"
        );
        assert_eq!(
            f.scope_state(&item),
            ScopeState::Finalizing,
            "broken cleanup must not disguise a damaged scope as Closed"
        );
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
        assert!(f.alive(&id));
    }

    /// C17：Consume 的类型与 target 检查先于移动。
    #[test]
    fn c17_consume_type_and_target_checks() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);

        // (i) 元素类型不符：collector 元素为 Other，item 输出 Tracked。
        let collector = f.collector::<Other>(&each);
        let item = f.child(&each);
        let out = f.ref_id();
        let id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        assert!(matches!(
            f.coordinator.consume_item(&item, &out, &collector),
            Err(ScopeError::TypeMismatch { .. })
        ));
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
        // 拒绝后走正式失败退出：来源自身清理，元素没有进入 collector。
        assert_eq!(f.scope_state(&item), ScopeState::Closed);
        assert!(!f.alive(&id));

        // (ii) 失效 target：imported 值被销毁，来源自身责任集合仍完好。
        let collector = f.collector::<Tracked>(&each);
        let src_pos = f.ref_id();
        let src = f
            .coordinator
            .register_owned(&each, &src_pos, f.tracked(2))
            .unwrap();
        let item = f.child(&each);
        let alias = f.ref_id();
        f.coordinator
            .import_batch(
                &item,
                &each,
                &[ImportSlot::new::<Tracked>(&src_pos, &alias)],
            )
            .unwrap();
        f.coordinator.container.destroy(&src).unwrap();
        assert!(matches!(
            f.coordinator.consume_item(&item, &alias, &collector),
            Err(ScopeError::TargetNotAlive { .. })
        ));
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);

        // (iii) foreign DataId：归属先于类型与移动判定。
        let mut foreign = Fixture::new();
        let foreign_root = foreign.root();
        let foreign_pos = foreign.ref_id();
        let foreign_id = foreign
            .coordinator
            .register_owned(&foreign_root, &foreign_pos, foreign.tracked(3))
            .unwrap();
        let item = f.child(&each);
        let injected = f.ref_id();
        f.inject_target(&item, &injected, RefTarget::Data(foreign_id.clone()));
        let error = f
            .coordinator
            .consume_item(&item, &injected, &collector)
            .unwrap_err();
        assert!(
            matches!(error, ScopeError::Storage { .. }),
            "foreign DataId must be reported as a storage ownership failure, got {error:?}"
        );
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);
    }

    /// C18：descendant 先正常 Export 到 ItemScope，再按同一 Consume 路径收集。
    #[test]
    fn c18_descendant_output_via_export_then_consume() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let collector = f.collector::<Tracked>(&each);

        let item = f.child(&each);
        let descendant = f.child(&item);
        let d_out = f.ref_id();
        let item_out = f.ref_id();
        f.coordinator
            .register_owned(&descendant, &d_out, f.tracked(30))
            .unwrap();
        f.finalize_one::<Tracked>(&descendant, &d_out, &item_out)
            .unwrap();
        assert_eq!(f.scope_state(&descendant), ScopeState::Closed);
        assert_eq!(
            f.owned_len(&item),
            1,
            "exported value is owned by ItemScope"
        );

        f.coordinator
            .consume_item(&item, &item_out, &collector)
            .unwrap();
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
        assert_eq!(f.scope_state(&item), ScopeState::Closed);
        assert_eq!(f.refs_len(&each), 0, "no per-item EachScope Ref binding");
        assert_eq!(f.drops(), 0);
    }

    /// C19：多个 Item 复用同一输出位置与同一 collector；顺序与另一个 collector 不受影响。
    #[test]
    fn c19_multiple_items_share_one_output_ref_and_two_collectors() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let collector = f.collector::<Tracked>(&each);
        let other_collector = f.collector::<Other>(&each);
        let own_pos = f.ref_id();
        let own_id = f
            .coordinator
            .register_owned(&each, &own_pos, Other(99))
            .unwrap();

        // 同一个 body 输出定义位置按 item 重复使用。
        let body_out = f.ref_id();
        for num in [1u32, 2] {
            let item = f.child(&each);
            f.coordinator
                .register_owned(&item, &body_out, f.tracked(num))
                .unwrap();
            f.coordinator
                .consume_item(&item, &body_out, &collector)
                .unwrap();
        }
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 2);
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 2);
        assert_eq!(f.owned_len(&each), 1, "only the pre-existing own value");

        let final_pos = f.ref_id();
        let _vec_id = f
            .coordinator
            .finish_collector(&each, &collector, &final_pos)
            .unwrap();
        let collected = f
            .coordinator
            .resolve::<Vec<Tracked>>(&each, &final_pos)
            .unwrap();
        assert_eq!(
            collected.iter().map(|item| item.num).collect::<Vec<_>>(),
            vec![1, 2],
            "collection order follows item order"
        );
        assert_eq!(f.refs_len(&each), 2, "no per-item growth");
        assert!(
            f.alive(&own_id),
            "the other collector's owner value is untouched"
        );
        assert_eq!(f.coordinator.collector_len(&other_collector).unwrap(), 0);
        assert_eq!(f.drops(), 0);
    }

    /// C20：部分结果与失败 Item 的清理；无正常集合输出、无 detached Scope。
    #[test]
    fn c20_partial_results_failure_cleanup() {
        let mut f = Fixture::new();
        let root = f.root();
        let rules_pos = f.ref_id();
        let rules = f
            .coordinator
            .register_owned(&root, &rules_pos, f.tracked(100))
            .unwrap();
        let each = f.child(&root);
        let each_rules = f.ref_id();
        f.coordinator
            .import_batch(
                &each,
                &root,
                &[ImportSlot::new::<Tracked>(&rules_pos, &each_rules)],
            )
            .unwrap();
        let collector = f.collector::<Tracked>(&each);

        let item1 = f.child(&each);
        let out1 = f.ref_id();
        f.coordinator
            .register_owned(&item1, &out1, f.tracked(1))
            .unwrap();
        f.coordinator
            .consume_item(&item1, &out1, &collector)
            .unwrap();
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);

        // Item2 重新暴露 imported Rules：预检拒绝；失败退出只清理自身。
        let item2 = f.child(&each);
        let temp = f.ref_id();
        f.coordinator
            .register_owned(&item2, &temp, f.tracked(2))
            .unwrap();
        let alias = f.ref_id();
        f.coordinator
            .import_batch(
                &item2,
                &each,
                &[ImportSlot::new::<Tracked>(&each_rules, &alias)],
            )
            .unwrap();
        assert!(matches!(
            f.coordinator.consume_item(&item2, &alias, &collector),
            Err(ScopeError::IllegalOwner { .. })
        ));
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
        assert_eq!(f.drops(), 1, "item2 temporary cleaned exactly once");
        assert!(f.alive(&rules));

        f.coordinator.abort(&each).unwrap();
        assert_eq!(f.drops(), 2, "partial collector result dropped once");
        assert!(f.alive(&rules), "root imported value survives");
        assert_eq!(f.owner(&rules).unwrap(), root);
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&root, &rules_pos)
                .unwrap()
                .num,
            100
        );
        assert_eq!(f.refs_len(&each), 0, "no normal Vec output was produced");
        for scope in [&each, &item1, &item2] {
            assert_eq!(
                f.scope_state(scope),
                ScopeState::Closed,
                "no scope of the aborted subtree is left detached"
            );
        }
    }

    /// C21：完成与最终 Export；空 collector；活跃 descendant 与位置冲突的拒绝。
    #[test]
    fn c21_finish_and_final_export() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let collector = f.collector::<Tracked>(&each);

        let body_out = f.ref_id();
        for num in [1u32, 2] {
            let item = f.child(&each);
            f.coordinator
                .register_owned(&item, &body_out, f.tracked(num))
                .unwrap();
            f.coordinator
                .consume_item(&item, &body_out, &collector)
                .unwrap();
        }

        // 活跃 descendant：完成前拒绝，且无半成品。
        let live_item = f.child(&each);
        assert!(matches!(
            f.coordinator
                .finish_collector(&each, &collector, &f.ref_id()),
            Err(ScopeError::ActiveDescendants { .. })
        ));
        assert_eq!(f.owned_len(&each), 0);
        assert_eq!(f.refs_len(&each), 0);
        f.coordinator.abort(&live_item).unwrap();

        let final_pos = f.ref_id();
        let vec_id = f
            .coordinator
            .finish_collector(&each, &collector, &final_pos)
            .unwrap();
        assert_eq!(f.owner(&vec_id).unwrap(), each);
        assert!(matches!(
            f.coordinator
                .finish_collector(&each, &collector, &f.ref_id()),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));

        // 输出位置冲突：完成前拒绝，collector 保持未完成。
        let empty_collector = f.collector::<Tracked>(&each);
        assert!(matches!(
            f.coordinator
                .finish_collector(&each, &empty_collector, &final_pos),
            Err(ScopeError::RefAlreadyBound { .. })
        ));
        assert_eq!(f.coordinator.collector_len(&empty_collector).unwrap(), 0);

        // 显式空 collector 形成新的空 Vec 普通 Data。
        let empty_pos = f.ref_id();
        let empty_id = f
            .coordinator
            .finish_collector(&each, &empty_collector, &empty_pos)
            .unwrap();
        assert_ne!(empty_id, vec_id);
        assert_eq!(
            f.coordinator
                .resolve::<Vec<Tracked>>(&each, &empty_pos)
                .unwrap()
                .len(),
            0
        );

        // 最终 Export；controller 退出不销毁 caller 接收值。
        let root_out = f.ref_id();
        f.finalize_one::<Vec<Tracked>>(&each, &final_pos, &root_out)
            .unwrap();
        assert_eq!(f.scope_state(&each), ScopeState::Closed);
        assert_eq!(f.owner(&vec_id).unwrap(), root);
        assert_eq!(
            f.coordinator
                .resolve::<Vec<Tracked>>(&root, &root_out)
                .unwrap()
                .iter()
                .map(|item| item.num)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(f.drops(), 0);
        assert!(
            !f.alive(&empty_id),
            "unexported empty Vec is cleaned by controller exit"
        );
    }

    /// C22：完成时 DataId 耗尽：collector 保持未完成，随后 abort 清理部分结果。
    #[test]
    fn c22_data_id_exhaustion_at_finish() {
        let ids = RefIdAllocator::new(RefIdSource::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let mut c = ScopeCoordinator::new(ExecutionIdentity::with_starts(u64::MAX - 2, 0));
        let root = c.root();

        let rules_pos = ids.allocate().unwrap();
        let rules = c
            .register_owned(
                &root,
                &rules_pos,
                Tracked {
                    num: 1,
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        let each = c.create_child(&root).unwrap();
        let each_rules = ids.allocate().unwrap();
        c.import_batch(
            &each,
            &root,
            &[ImportSlot::new::<Tracked>(&rules_pos, &each_rules)],
        )
        .unwrap();
        let collector = c.begin_collector::<Tracked>(&each).unwrap();

        let item = c.create_child(&each).unwrap();
        let out = ids.allocate().unwrap();
        let item_id = c
            .register_owned(
                &item,
                &out,
                Tracked {
                    num: 2,
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        assert_eq!(item_id.seq(), u64::MAX - 1);
        c.consume_item(&item, &out, &collector).unwrap();
        assert_eq!(c.collector_len(&collector).unwrap(), 1);

        let refs_before = c.registry.lookup(&each).unwrap().refs.len();
        let owned_before = c.registry.lookup(&each).unwrap().owned.len();
        let final_pos = ids.allocate().unwrap();
        let error = c
            .finish_collector(&each, &collector, &final_pos)
            .unwrap_err();
        assert!(
            matches!(
                error,
                ScopeError::Storage {
                    source: InternalError::IdSpaceExhausted { .. }
                }
            ),
            "got {error:?}"
        );
        assert_eq!(
            c.collector_len(&collector).unwrap(),
            1,
            "collector stays unfinished"
        );
        let record = c.registry.lookup(&each).unwrap();
        assert_eq!(
            record.refs.len(),
            refs_before,
            "no output ref is bound on exhaustion"
        );
        assert_eq!(
            record.owned.len(),
            owned_before,
            "no ordinary entry on exhaustion"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        c.abort(&each).unwrap();
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "partial result cleaned once"
        );
        assert!(
            c.container.validate(&rules).is_ok(),
            "ancestor value survives"
        );
        assert_eq!(c.resolve::<Tracked>(&root, &rules_pos).unwrap().num, 1);
    }

    /// C23 正例：缩短 borrow 后 Promote／Consume 正常完成（负例见 tests/ui/c23_*.rs）。
    #[test]
    fn c23_shortening_the_borrow_allows_promote_and_consume() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);

        let round = f.child(&controller);
        let out = f.ref_id();
        f.coordinator
            .register_owned(&round, &out, f.tracked(1))
            .unwrap();
        {
            let borrowed = f.coordinator.resolve::<Tracked>(&round, &out).unwrap();
            assert_eq!(borrowed.num, 1);
        }
        f.coordinator.promote(&round, &out, &state).unwrap();

        let each = f.child(&root);
        let collector = f.collector::<Tracked>(&each);
        let item = f.child(&each);
        let item_out = f.ref_id();
        f.coordinator
            .register_owned(&item, &item_out, f.tracked(2))
            .unwrap();
        {
            let borrowed = f.coordinator.resolve::<Tracked>(&item, &item_out).unwrap();
            assert_eq!(borrowed.num, 2);
        }
        f.coordinator
            .consume_item(&item, &item_out, &collector)
            .unwrap();
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    }

    /// C24：三类操作的提交前拒绝快照；清理校验不晚于 commit。
    #[test]
    fn c24_pre_commit_rejection_snapshots() {
        let mut f = Fixture::new();
        let root = f.root();
        let seed_pos = f.ref_id();
        f.coordinator
            .register_owned(&root, &seed_pos, f.tracked(9))
            .unwrap();
        let controller = f.child(&root);
        let ctrl_seed = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(&seed_pos, &ctrl_seed)],
            )
            .unwrap();
        let state = f.state::<Tracked>(&controller, &ctrl_seed);
        let collector = f.collector::<Tracked>(&controller);

        let seed_item = f.child(&controller);
        let seed_item_out = f.ref_id();
        f.coordinator
            .register_owned(&seed_item, &seed_item_out, f.tracked(2))
            .unwrap();
        f.coordinator
            .consume_item(&seed_item, &seed_item_out, &collector)
            .unwrap();

        let snapshot = |f: &Fixture| {
            (
                f.drops(),
                f.refs_len(&controller),
                f.owned_len(&controller),
                f.coordinator.collector_len(&collector).unwrap(),
                f.coordinator.collector_moves(&collector).unwrap(),
                f.scope_state(&controller),
            )
        };

        // Promote：类型不符的 Round 输出来源。
        let round = f.child(&controller);
        let round_out = f.ref_id();
        f.coordinator
            .register_owned(&round, &round_out, Other(3))
            .unwrap();
        let before = snapshot(&f);
        assert!(f.coordinator.promote(&round, &round_out, &state).is_err());
        assert_eq!(
            before,
            snapshot(&f),
            "promote rejection leaves no partial commit"
        );

        // Consume：元素类型不符的 item 输出。
        let item = f.child(&controller);
        let item_out = f.ref_id();
        f.coordinator
            .register_owned(&item, &item_out, Other(4))
            .unwrap();
        let before = snapshot(&f);
        assert!(
            f.coordinator
                .consume_item(&item, &item_out, &collector)
                .is_err()
        );
        assert_eq!(
            before,
            snapshot(&f),
            "consume rejection leaves no partial commit"
        );

        // 完成：输出位置已被占用。
        let before = snapshot(&f);
        assert!(matches!(
            f.coordinator
                .finish_collector(&controller, &collector, &ctrl_seed),
            Err(ScopeError::RefAlreadyBound { .. })
        ));
        assert_eq!(
            before,
            snapshot(&f),
            "finish rejection leaves no partial commit"
        );
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);
    }

    /// C25：控制器退出／abort 清理未完成 collector、pending 与控制状态。
    #[test]
    fn c25_controller_exit_cleans_collector_pending_and_states() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);
        let collector = f.collector::<Tracked>(&controller);

        let item = f.child(&controller);
        let item_out = f.ref_id();
        f.coordinator
            .register_owned(&item, &item_out, f.tracked(1))
            .unwrap();
        f.coordinator
            .consume_item(&item, &item_out, &collector)
            .unwrap();

        let round = f.child(&controller);
        let out = f.ref_id();
        let first = f
            .coordinator
            .register_owned(&round, &out, f.tracked(2))
            .unwrap();
        f.coordinator.promote(&round, &out, &state).unwrap();
        let final_pos = f.ref_id();
        f.coordinator
            .bind_state_output(&controller, &state, &final_pos)
            .unwrap();

        let round2 = f.child(&controller);
        let out2 = f.ref_id();
        f.coordinator
            .register_owned(&round2, &out2, f.tracked(3))
            .unwrap();
        f.coordinator.promote(&round2, &out2, &state).unwrap();
        f.coordinator.recycle_pending(&controller).unwrap();
        assert_eq!(
            f.drops(),
            0,
            "controller-local alias keeps the old state pending"
        );

        f.coordinator.abort(&controller).unwrap();
        assert_eq!(
            f.drops(),
            3,
            "collector result + pending old state + current state"
        );
        assert!(!f.alive(&first));
        assert_eq!(f.scope_state(&controller), ScopeState::Closed);
        assert!(matches!(
            f.coordinator.collector_len(&collector),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));
        assert!(matches!(
            f.coordinator
                .bind_state_output(&controller, &state, &f.ref_id()),
            Err(ScopeError::ScopeClosed { .. })
        ));

        // Container 析构不产生额外 Drop。
        let drops = Arc::clone(&f.drops);
        let coordinator = f.coordinator;
        drop(coordinator);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    /// C26：非 Clone 业务值走完 Promote／Consume／完成／Export，内部面仍是借用式。
    #[test]
    fn c26_non_clone_pipeline_and_internal_surface() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);
        let collector = f.collector::<Tracked>(&controller);

        for num in [1u32, 2] {
            let round = f.child(&controller);
            let out = f.ref_id();
            f.coordinator
                .register_owned(&round, &out, f.tracked(num))
                .unwrap();
            f.coordinator.promote(&round, &out, &state).unwrap();

            let item = f.child(&controller);
            let item_out = f.ref_id();
            f.coordinator
                .register_owned(&item, &item_out, f.tracked(num))
                .unwrap();
            f.coordinator
                .consume_item(&item, &item_out, &collector)
                .unwrap();
        }
        let final_pos = f.ref_id();
        f.coordinator
            .finish_collector(&controller, &collector, &final_pos)
            .unwrap();
        let root_out = f.ref_id();
        f.finalize_one::<Vec<Tracked>>(&controller, &final_pos, &root_out)
            .unwrap();
        assert_eq!(
            f.coordinator
                .resolve::<Vec<Tracked>>(&root, &root_out)
                .unwrap()
                .iter()
                .map(|item| item.num)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        // 内部读取面仍是"返回借用"，没有按值取数的公开入口。
        type BorrowFn<'a> =
            fn(&'a ScopeCoordinator, &ScopeId, &RefId) -> Result<&'a Tracked, ScopeError>;
        let _resolve: BorrowFn<'_> = ScopeCoordinator::resolve::<Tracked>;
    }

    /// C27：CollectorId 与控制状态序号耗尽；清理后不复用、旧句柄不复活。
    #[test]
    fn c27_collector_and_state_sequence_exhaustion() {
        let ids = RefIdAllocator::new(RefIdSource::new());
        let drops = Arc::new(AtomicUsize::new(0));

        // CollectorId 近上限：耗尽后不回绕。
        let mut c = ScopeCoordinator::new(ExecutionIdentity::with_starts_and_collector_start(
            0,
            0,
            u64::MAX - 1,
        ));
        let root = c.root();
        let controller = c.create_child(&root).unwrap();
        let first = c.begin_collector::<Tracked>(&controller).unwrap();
        assert_eq!(first.seq(), u64::MAX - 1);
        let error = c.begin_collector::<Tracked>(&controller).unwrap_err();
        assert!(
            matches!(
                error,
                ScopeError::Storage {
                    source: InternalError::IdSpaceExhausted {
                        kind: IdKind::Collector
                    }
                }
            ),
            "got {error:?}"
        );
        assert_eq!(c.collector_metadata(&first).unwrap().1, controller);

        // 控制状态近上限：耗尽后不回绕；关闭后旧句柄只得到 Closed。
        let mut c2 = ScopeCoordinator::new(ExecutionIdentity::new());
        let root2 = c2.root();
        let scope2 = c2.create_child(&root2).unwrap();
        let pos2 = ids.allocate().unwrap();
        c2.register_owned(
            &scope2,
            &pos2,
            Tracked {
                num: 1,
                drops: Arc::clone(&drops),
            },
        )
        .unwrap();
        c2.with_state_start(&scope2, u64::MAX - 1).unwrap();
        let last = c2.register_state::<Tracked>(&scope2, &pos2).unwrap();
        assert_eq!(last.seq(), u64::MAX - 1);
        let error = c2.register_state::<Tracked>(&scope2, &pos2).unwrap_err();
        assert!(
            matches!(
                error,
                ScopeError::Storage {
                    source: InternalError::IdSpaceExhausted {
                        kind: IdKind::ControlState
                    }
                }
            ),
            "got {error:?}"
        );
        c2.abort(&scope2).unwrap();
        assert!(matches!(
            c2.bind_state_output(&scope2, &last, &ids.allocate().unwrap()),
            Err(ScopeError::ScopeClosed { .. })
        ));

        // 清理后新身份不复用：同一 Execution 内序号继续前进。
        let mut c3 = ScopeCoordinator::new(ExecutionIdentity::new());
        let root3 = c3.root();
        let scope3 = c3.create_child(&root3).unwrap();
        let a = c3.begin_collector::<Tracked>(&scope3).unwrap();
        c3.abort(&scope3).unwrap();
        let scope4 = c3.create_child(&root3).unwrap();
        let b = c3.begin_collector::<Tracked>(&scope4).unwrap();
        assert_eq!(
            (a.seq(), b.seq()),
            (0, 1),
            "collector identities are never reused"
        );
        assert_ne!(a, b);
        assert!(matches!(
            c3.collector_len(&a),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));
    }

    // ---- R7～R9：清理前提与登记／建构值一致性的补证样本 ----

    /// R7：Promote 的清理前提包含来源未完成 collector；提交前拒绝。
    #[test]
    fn r7_promote_cleanup_preflight_covers_unfinished_collectors() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);

        // ancestor：root 的值经控制器导入来源 Round，用于观测误删。
        let anc_pos = f.ref_id();
        let ancestor = f
            .coordinator
            .register_owned(&root, &anc_pos, f.tracked(50))
            .unwrap();
        let ctrl_anc = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(&anc_pos, &ctrl_anc)],
            )
            .unwrap();
        let round = f.child(&controller);
        let round_anc = f.ref_id();
        f.coordinator
            .import_batch(
                &round,
                &controller,
                &[ImportSlot::new::<Tracked>(&ctrl_anc, &round_anc)],
            )
            .unwrap();
        let out = f.ref_id();
        let value = f
            .coordinator
            .register_owned(&round, &out, f.tracked(1))
            .unwrap();
        let broken = f.collector::<Tracked>(&round);
        // test-only 故障：删除物理建构值，保留 registry 登记。
        f.coordinator.container.destroy_collector(&broken).unwrap();

        let before = (
            f.drops(),
            f.refs_len(&controller),
            f.owned_len(&controller),
            f.refs_len(&round),
            f.owned_len(&round),
        );
        let error = f.coordinator.promote(&round, &out, &state).unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "got {error:?}"
        );

        // 提交前拒绝：父状态仍未初始化、两侧 refs／owned 与 Drop 都不变、值仍归来源。
        assert!(matches!(
            f.coordinator
                .bind_state_output(&controller, &state, &f.ref_id()),
            Err(ScopeError::StateUninitialized { .. })
        ));
        assert_eq!(
            before,
            (
                f.drops(),
                f.refs_len(&controller),
                f.owned_len(&controller),
                f.refs_len(&round),
                f.owned_len(&round)
            ),
            "collector precondition failure leaves no partial commit"
        );
        assert_eq!(f.owner(&value).unwrap(), round);
        assert!(f.alive(&value));
        assert!(f.alive(&ancestor), "ancestor value is not destroyed");
        assert_eq!(f.owner(&ancestor).unwrap(), root);
        assert_eq!(
            f.coordinator
                .resolve::<Tracked>(&root, &anc_pos)
                .unwrap()
                .num,
            50
        );
        // 清理自身也失败：来源不冒充 Closed，且故障 collector 的登记不被提前撤销。
        assert_ne!(f.scope_state(&round), ScopeState::Closed);
        assert!(
            f.coordinator.collector_metadata(&broken).is_ok(),
            "registration is revoked only after the building value is actually destroyed"
        );
    }

    /// R7：Consume 的清理前提包含来源未完成 collector；目标 collector 不被触碰。
    #[test]
    fn r7_consume_cleanup_preflight_covers_unfinished_collectors() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let target = f.collector::<Tracked>(&each);

        let item = f.child(&each);
        let out = f.ref_id();
        let item_id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        let broken = f.collector::<Tracked>(&item);
        f.coordinator.container.destroy_collector(&broken).unwrap();

        let drops_before = f.drops();
        let error = f
            .coordinator
            .consume_item(&item, &out, &target)
            .unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "got {error:?}"
        );
        assert_eq!(
            f.coordinator.collector_moves(&target).unwrap(),
            0,
            "target collector is not touched before the preflight passes"
        );
        assert_eq!(f.coordinator.collector_len(&target).unwrap(), 0);
        assert!(f.alive(&item_id), "item value is not moved");
        assert_eq!(f.owned_len(&each), 0);
        assert_eq!(f.drops(), drops_before);
        assert_ne!(f.scope_state(&item), ScopeState::Closed);
        assert!(f.coordinator.collector_metadata(&broken).is_ok());
    }

    /// R8：Consume 在提交前发现 registry 与物理 builder 的元素类型不一致。
    #[test]
    fn r8_consume_rejects_registry_builder_type_mismatch_before_moving() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let collector = f.collector::<Other>(&each);
        // test-only 故障：只改 registry 登记类型，物理建构值仍是 Other。
        {
            let record = f
                .coordinator
                .registry
                .collectors
                .get_mut(&collector.seq())
                .unwrap();
            record.element_type = TypeId::of::<Tracked>();
            record.element_name = std::any::type_name::<Tracked>();
        }

        let item = f.child(&each);
        let out = f.ref_id();
        let item_id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        let error = f
            .coordinator
            .consume_item(&item, &out, &collector)
            .unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "got {error:?}"
        );
        assert_eq!(f.coordinator.collector_moves(&collector).unwrap(), 0);
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 0);

        // 预检失败仍走正式来源失败清理路径：来源自身清理完成 → Closed。
        assert!(!f.alive(&item_id));
        assert_eq!(f.scope_state(&item), ScopeState::Closed);
    }

    /// R8：完成在提交前发现登记／物理不一致，且不消耗普通 DataId、不移除 building。
    #[test]
    fn r8_finish_rejects_registry_builder_type_mismatch_without_consuming() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let collector = f.collector::<Other>(&each);
        {
            let record = f
                .coordinator
                .registry
                .collectors
                .get_mut(&collector.seq())
                .unwrap();
            record.element_type = TypeId::of::<Tracked>();
            record.element_name = std::any::type_name::<Tracked>();
        }

        let position = f.ref_id();
        let error = f
            .coordinator
            .finish_collector(&each, &collector, &position)
            .unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "got {error:?}"
        );
        assert_eq!(
            f.coordinator.collector_len(&collector).unwrap(),
            0,
            "building kept"
        );
        assert_eq!(f.refs_len(&each), 0);
        assert_eq!(f.owned_len(&each), 0);

        // 没有被消耗的普通 DataId：下一个分配仍是首个序号。
        let later = f
            .coordinator
            .register_owned(&each, &position, f.tracked(2))
            .unwrap();
        assert_eq!(
            later.seq(),
            0,
            "rejected finish consumed no ordinary DataId"
        );
    }

    /// R9／C09：Promote 的重复 owner 与 foreign target 拒绝，含整组快照。
    #[test]
    fn c09_promote_rejects_duplicate_owner_and_foreign_target() {
        let mut f = Fixture::new();
        let root = f.root();
        let controller = f.child(&root);
        let state = f.uninit_state::<Tracked>(&controller);

        // 重复 owner。
        let round = f.child(&controller);
        let out = f.ref_id();
        let id = f
            .coordinator
            .register_owned(&round, &out, f.tracked(1))
            .unwrap();
        f.add_ownership(&controller, &id);
        let snapshot = |f: &Fixture| {
            (
                f.drops(),
                f.refs_len(&controller),
                f.owned_len(&controller),
                f.refs_len(&round),
                f.owned_len(&round),
            )
        };
        let before = snapshot(&f);
        let error = f.coordinator.promote(&round, &out, &state).unwrap_err();
        assert!(
            matches!(
                error,
                ScopeError::DuplicateOwner { .. } | ScopeError::Invariant { .. }
            ),
            "got {error:?}"
        );
        assert_eq!(
            before,
            snapshot(&f),
            "duplicate ownership leaves no partial commit"
        );
        assert!(f.alive(&id));
        assert!(matches!(
            f.coordinator
                .bind_state_output(&controller, &state, &f.ref_id()),
            Err(ScopeError::StateUninitialized { .. })
        ));

        // foreign target：注入另一次 Execution 的 DataId。
        let mut foreign = Fixture::new();
        let foreign_root = foreign.root();
        let foreign_pos = foreign.ref_id();
        let foreign_id = foreign
            .coordinator
            .register_owned(&foreign_root, &foreign_pos, foreign.tracked(9))
            .unwrap();
        let round2 = f.child(&controller);
        let injected = f.ref_id();
        f.inject_target(&round2, &injected, RefTarget::Data(foreign_id.clone()));
        let before = (
            f.drops(),
            f.refs_len(&controller),
            f.owned_len(&controller),
            f.owned_len(&round2),
        );
        let error = f
            .coordinator
            .promote(&round2, &injected, &state)
            .unwrap_err();
        assert!(
            matches!(
                error,
                ScopeError::Storage { .. } | ScopeError::Invariant { .. }
            ),
            "got {error:?}"
        );
        assert_eq!(
            before,
            (
                f.drops(),
                f.refs_len(&controller),
                f.owned_len(&controller),
                f.owned_len(&round2)
            ),
            "foreign target leaves no partial commit"
        );
        assert!(matches!(
            f.coordinator
                .bind_state_output(&controller, &state, &f.ref_id()),
            Err(ScopeError::StateUninitialized { .. })
        ));
    }

    /// R9／C25：正常退出清理未完成但有元素的 collector。
    #[test]
    fn c25_normal_exit_cleans_unfinished_collector_with_elements() {
        let mut f = Fixture::new();
        let root = f.root();
        let anc_pos = f.ref_id();
        let ancestor = f
            .coordinator
            .register_owned(&root, &anc_pos, f.tracked(50))
            .unwrap();
        let controller = f.child(&root);
        let ctrl_anc = f.ref_id();
        f.coordinator
            .import_batch(
                &controller,
                &root,
                &[ImportSlot::new::<Tracked>(&anc_pos, &ctrl_anc)],
            )
            .unwrap();
        let collector = f.collector::<Tracked>(&controller);

        let item = f.child(&controller);
        let out = f.ref_id();
        f.coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        f.coordinator.consume_item(&item, &out, &collector).unwrap();
        assert_eq!(f.coordinator.collector_len(&collector).unwrap(), 1);

        // 正常退出：Container 仍存活时部分结果恰好 drop 一次。
        let declared: Vec<RefId> = Vec::new();
        let mut slots: Vec<ExportSlot> = Vec::new();
        f.coordinator
            .finalize(&controller, &declared, &mut slots)
            .unwrap();
        assert_eq!(f.drops(), 1);
        assert_eq!(f.scope_state(&controller), ScopeState::Closed);
        assert!(matches!(
            f.coordinator.collector_len(&collector),
            Err(ScopeError::CollectorNotRegistered { .. })
        ));
        assert!(f.alive(&ancestor), "ancestor value survives");
        assert_eq!(f.owner(&ancestor).unwrap(), root);

        // ancestor 随其 owner 退出销毁：每个值恰好一次。
        let declared: Vec<RefId> = Vec::new();
        let mut slots: Vec<ExportSlot> = Vec::new();
        f.coordinator
            .finalize(&root, &declared, &mut slots)
            .unwrap();
        assert_eq!(f.drops(), 2);

        // Container 析构不产生额外 Drop。
        let drops = Arc::clone(&f.drops);
        let coordinator = f.coordinator;
        drop(coordinator);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    /// R7 收口：登记键与记录身份必须一致；清理不按未经验证的身份删除。
    #[test]
    fn r7_cleanup_rejects_collector_identity_binding_mismatch() {
        let mut f = Fixture::new();
        let root = f.root();

        // Root 的 C0：合法 Consume 一项，形成祖先的部分结果。
        let c0 = f.collector::<Tracked>(&root);
        let item = f.child(&root);
        let out = f.ref_id();
        f.coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        f.coordinator.consume_item(&item, &out, &c0).unwrap();
        assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 1);

        // 子 Scope 的 C1：test-only 只把记录的 id 改成 C0，键与 owner 保留。
        let child = f.child(&root);
        let c1 = f.collector::<Tracked>(&child);
        {
            let record = f
                .coordinator
                .registry
                .collectors
                .get_mut(&c1.seq())
                .unwrap();
            record.id = c0.clone();
        }

        let error = f.coordinator.abort(&child).unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "got {error:?}"
        );

        // 祖先的建构值、Drop、登记与来源状态保持原状。
        assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 1);
        assert_eq!(f.drops(), 0);
        assert!(f.coordinator.collector_metadata(&c0).is_ok());
        assert!(
            f.coordinator.registry.collectors.contains_key(&c1.seq()),
            "the mismatched registration is not revoked by another identity"
        );
        assert_ne!(f.scope_state(&child), ScopeState::Closed);
        // 错配句柄不会重定向到 C0；合法句柄仍指向自己的登记。
        assert!(matches!(
            f.coordinator.collector_len(&c1),
            Err(ScopeError::Invariant { .. })
        ));
        assert_eq!(f.coordinator.collector_metadata(&c0).unwrap().1, root);
    }

    /// R7 收口：Consume 与完成共用同一身份绑定保证。
    #[test]
    fn r7_consume_and_finish_reject_collector_identity_binding_mismatch() {
        let mut f = Fixture::new();
        let root = f.root();
        let each = f.child(&root);
        let c0 = f.collector::<Tracked>(&each);
        let c1 = f.collector::<Tracked>(&each);
        {
            let record = f
                .coordinator
                .registry
                .collectors
                .get_mut(&c1.seq())
                .unwrap();
            record.id = c0.clone();
        }

        // Consume：提交前 Invariant；目标 collector 未被触碰，来源按正式失败清理。
        let item = f.child(&each);
        let out = f.ref_id();
        let item_id = f
            .coordinator
            .register_owned(&item, &out, f.tracked(1))
            .unwrap();
        let error = f.coordinator.consume_item(&item, &out, &c1).unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "got {error:?}"
        );
        assert_eq!(f.coordinator.collector_moves(&c0).unwrap(), 0);
        assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 0);
        assert!(!f.alive(&item_id));
        assert_eq!(f.scope_state(&item), ScopeState::Closed);

        // 完成：提交前 Invariant，且不消耗普通 DataId、不增加 ref／owned。
        let position = f.ref_id();
        let error = f
            .coordinator
            .finish_collector(&each, &c1, &position)
            .unwrap_err();
        assert!(
            matches!(error, ScopeError::Invariant { .. }),
            "got {error:?}"
        );
        assert_eq!(f.refs_len(&each), 0);
        assert_eq!(f.owned_len(&each), 0);
        let later = f
            .coordinator
            .register_owned(&each, &position, f.tracked(2))
            .unwrap();
        assert_eq!(
            later.seq(),
            item_id.seq() + 1,
            "rejected finish consumed no ordinary DataId"
        );
        assert_eq!(f.coordinator.collector_len(&c0).unwrap(), 0);
    }
}
