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
use std::any::{Any, TypeId, type_name, type_name_of_val};
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
/// 业务值。它只在 ItemScope 存活且请求方位于 cap 内时可临时借用。集合来源既可以是
/// 容器里的完整 `Vec<T>`，也可以是另一个受限 item 借用（它的元素自身就是集合），
/// 因此 SPEC §8 的 `Ref<Vec<T>>` 来源可以合法地嵌套；两种来源都只记录定位元数据。
#[derive(Debug, Clone)]
pub(crate) enum RefTarget {
    /// 已存储在 DataContainer 中的完整 Data 实例。
    Data(DataId),
    /// 某个集合内部、有效期不超过 ItemScope 的 item。
    CollectionItem {
        /// 直接集合来源：完整 `Vec<T>` Data，或（嵌套时）另一个受限 item 借用。
        collection: CollectionRoot,
        /// 元素在该来源集合中的下标。
        index: usize,
        /// item 的有效期上限（ItemScope）。
        lifetime_cap: ScopeId,
        /// 封闭的元素访问描述（由 typed `Vec<T>` 工厂创建）。
        access: ItemAccess,
    },
}

/// 一个受限 item 借用的集合来源（真实根数据 + 定位关系）。
///
/// `Data` 是容器中承载真实 `Vec<T>` 的完整 Data；`Item` 递归保存上游 item 目标——它的
/// 元素就是本 item 的直接集合。两种来源都**不拥有业务值、不分配 DataId、不移动或复制
/// 集合**，也不赋予来源任何消费权：嵌套读取只借用外层真实根数据的元素。上游目标随其
/// 所属 Scope 记录一并失效，因此关闭／取消后不会留下悬空引用，逐层 cap 保持原责任链。
#[derive(Debug, Clone)]
pub(crate) enum CollectionRoot {
    /// 容器中的完整 `Vec<T>` Data（顶层 Each 的集合来源）。
    Data(DataId),
    /// 另一个受限 item 借用：它的值本身就是本 item 的直接集合。
    Item(Box<RefTarget>),
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

    /// CollectionItem 的受限借用元数据；完整 Data 返回空。
    fn item(&self) -> Option<(&CollectionRoot, usize, &ScopeId, &ItemAccess)> {
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

    /// CollectionItem 的 lifetime cap；完整 Data 返回空。
    fn item_cap(&self) -> Option<&ScopeId> {
        self.item().map(|(_, _, cap, _)| cap)
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

impl ImportSlot {
    /// 以 `T` 声明 child 本地目标位置的类型。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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

impl ExportSlot {
    /// 以 `T` 声明 child 本地输出位置的类型。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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

/// 受控本地 alias 装配槽位：把同一 Scope 内已绑定位置的 target 复制到该 Scope 的新位置。
///
/// 只建立引用（alias），不移动业务值、不改变 owner，也不产生跨边界能力。
#[derive(Debug, Clone)]
pub(crate) struct AliasSlot {
    source: RefId,
    target: RefId,
    expected: TypeId,
    expected_name: &'static str,
}

impl AliasSlot {
    /// 以 `T` 声明目的位置类型。
    #[allow(dead_code)] // 由定向测试使用；生产路径按登记类型构造
    pub(crate) fn new<T: Any>(source: &RefId, target: RefId) -> Self {
        Self::with_type(source, target, TypeId::of::<T>(), type_name::<T>())
    }

    /// 以运行时类型声明目的位置：Definition 的登记类型决定。
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

/// 成组 Consume 的结果报告：整组独立于单项 [`ConsumeOutcome`]。
///
/// 与 [`ConsumeOutcome`] 同形：整组全部槽通过预检并提交后才报告 [`Self::Consumed`]；
/// 结构拒绝或任一槽预检拒绝走 [`Self::Rejected`]，原始原因与其后的清理失败分别保留，
/// 不使用单项 convenience 的 `cleanup_failure.unwrap_or(primary)` 覆盖原拒绝。
#[derive(Debug)]
pub(crate) enum ConsumeGroupOutcome {
    /// 整组消费成功：全部业务值已移入各自 collector、逐 `DataId` 撤销 Item owned、Item 只关闭一次。
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

/// 成组 Promote 的结果报告：整组独立于单项 [`PromoteOutcome`]。
///
/// 与 [`PromoteOutcome`] 同形：整组全部槽通过预检并提交后才报告 [`Self::Promoted`]；
/// 结构拒绝或任一槽预检拒绝走 [`Self::Rejected`]，原始原因与其后的清理失败分别保留，
/// 不使用单项 convenience 的 `cleanup_failure.unwrap_or(primary)` 覆盖原拒绝。
#[derive(Debug)]
pub(crate) enum PromoteGroupOutcome {
    /// 整组保留成功：全部状态 target 已更新、责任按去重后的 `DataId` 转移、来源只关闭一次。
    Promoted,
    /// 拒绝：`primary` 是原始原因，`cleanup_failure` 是其后的清理失败（若有）。
    Rejected {
        /// 原始拒绝原因。
        primary: ScopeError,
        /// 拒绝后清理来源的诊断（若有）。
        cleanup_failure: Option<ScopeError>,
    },
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

/// 成组 Consume 的单个预检槽：声明位置、完整 Data 目标与目的 collector。
#[derive(Debug)]
struct PreparedConsumeSlot {
    /// 声明输出位置；生产提交段按目标移动，`position` 只由 cfg(test) 的整组快照读取。
    #[allow(dead_code)] // 仅由 cfg(test) 观察路径使用（非 test 构建无消费者）
    position: RefId,
    target: RefTarget,
    collector: CollectorId,
}

/// 成组 Consume 的预检结果。
///
/// 只由 [`ScopeCoordinator::consume_item_group_report`] 在同一次独占操作内生成并提交；
/// 逐槽复用 [`ScopeCoordinator::prepare_consume`] 的完整校验（声明绑定／完整 Data／存活／
/// 元素类型／collector 物理建构值与登记身份／Item-owned／清理前提），并在整组层面补上
/// 重复目标与重复目的 collector 的拒绝。不含任何已移动的值，因此拒绝时不留下半提交。
#[derive(Debug)]
struct PreparedConsumeGroup {
    item: ScopeId,
    slots: Vec<PreparedConsumeSlot>,
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

/// Root 提取的预检结果。
///
/// 只由 [`ScopeCoordinator::prepare_root_extraction`] 生成，并在 `freeze_root` 之后的同一
/// 退出流程内被 `commit_root_extraction` 单次消费。它只含元数据——声明位置、已解析的
/// 物理实例与关闭时的清理集合——不携带业务值、不跨 Execution、不缓存到 Definition，
/// 不能重放。字段私有：外部只能整体传递，不能挑选或改写其中条目。
#[derive(Debug)]
pub(crate) struct PreparedRootExtraction {
    /// 按 Root Signature 声明顺序的（声明位置，物理实例）。
    selections: Vec<(RefId, DataId)>,
    /// 关闭 Root 时需要销毁的剩余 owned（已在预检时整组校验，不含已选实例）。
    close_targets: Vec<DataId>,
}

impl PreparedRootExtraction {
    /// 本次计划要提取的实例数（Unit 为 0）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn take_count(&self) -> usize {
        self.selections.len()
    }
}

/// Scope 协调组件：同一 owner 中的 registry 与唯一 Container。
///
/// 它提供受控的登记、导入、resolve、导出与退出入口，不启动业务调用、不创建 Invocation，
/// 也不提供任意可变 Container 的访问器。后续 ExecutionContext 复用它。
pub(crate) struct ScopeCoordinator {
    registry: ScopeRegistry,
    container: DataContainer,
}

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

    /// 控制状态当前的责任控制器。
    ///
    /// 供 Context 校验"控制提交两端"的权限：不仅是来源 Scope，接收 state 的控制器
    /// 也必须属于当前调用的可见范围。
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

    /// 登记一个已擦除的 owned Data，并把它绑定到本地输出位置。
    ///
    /// 与 [`Self::register_owned`] 同一顺序与拒绝规则：状态与位置占用检查 → Container 插入
    /// → 登记唯一责任与绑定。`type_name` 必须是擦除值真实类型的名字，供后续类型诊断使用。
    /// 供 Root 输入登记使用：业务的静态类型由 `RootInputs` 形状固定，登记边界只保留擦除值。
    pub(crate) fn register_owned_erased(
        &mut self,
        scope: &ScopeId,
        position: &RefId,
        type_name: &'static str,
        value: Box<dyn Any>,
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
            .insert_owned_erased(type_name, value)
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

    /// CollectionItem 借用：cap 有效且请求方在 cap 内 → 集合来源链（真实根数据、逐层
    /// 类型／下标／cap）→ 元素类型 → 责任链。返回的 `&T` 绑定组件的 `&self`，与其他
    /// 借用同寿命上限。
    fn borrow_item<'a, T: Any>(
        &'a self,
        target: &'a RefTarget,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<&'a T, ScopeError> {
        let (collection, index, cap, access) = target
            .item()
            .expect("borrow_item is only called for item targets");
        self.require_in_cap(position, cap, scope)?;
        self.check_item(position, target, TypeId::of::<T>(), type_name::<T>())?;
        let value = self.collection_value(position, collection, cap, access)?;
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

    /// item 目标的共享检查：cap 有效 → 集合来源存活与实际类型（嵌套时逐层向上）→
    /// 元素声明类型 → index 在界 → 责任链（cap 仍能看到来源链的真实根数据）。请求方
    /// 范围由调用方按语义给出诊断（业务读取用 `ItemOutsideCap`，跨 Scope 转移用
    /// `ItemCapEscape`）。
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
        let value = self.collection_value(position, collection, cap, access)?;
        if access.element_type() != expected {
            return Err(ScopeError::TypeMismatch {
                position: position.clone(),
                expected: expected_name,
                actual: access.element_name(),
            });
        }
        self.require_collection_visible(position, collection, cap)?;
        if access.project(value, index).is_none() {
            return Err(ScopeError::ItemIndexOutOfRange {
                position: position.clone(),
                index,
            });
        }
        Ok(())
    }

    /// 定位 item 目标的**直接集合**值：完整 `Vec<T>` 直接借用容器 entry；嵌套来源则
    /// 逐层回溯上游 item 的真实元素，直到真实根 Data。
    ///
    /// 每层都校验来源存活、实际类型与声明的集合类型一致（对真实值 `TypeId` 复核，不靠
    /// 元数据背书）、下标在界，并复核上游 cap 仍有效且包含本层 cap。返回值与组件 `&self`
    /// 同寿命：链上不产生长期 borrow、DataId 或 owner 登记。
    fn collection_value<'a>(
        &'a self,
        position: &RefId,
        collection: &CollectionRoot,
        cap: &ScopeId,
        access: &ItemAccess,
    ) -> Result<&'a dyn Any, ScopeError> {
        match collection {
            CollectionRoot::Data(id) => {
                let value = self.container.borrow_any(id).map_err(|_| {
                    ScopeError::ItemCollectionNotAlive {
                        position: position.clone(),
                        collection: id.clone(),
                    }
                })?;
                self.container
                    .validate_type(id, access.collection_type(), access.collection_name())
                    .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
                Ok(value)
            }
            CollectionRoot::Item(outer) => {
                let (outer_collection, outer_index, outer_cap, outer_access) = outer
                    .item()
                    .expect("a nested collection source is an item target");
                // 上游 cap 仍有效，且本层 cap 落在它之内：原责任链不被拉长。
                self.require_live_cap(position, outer_cap)?;
                self.require_in_cap(position, outer_cap, cap)?;
                let outer_value =
                    self.collection_value(position, outer_collection, outer_cap, outer_access)?;
                let value = outer_access
                    .project(outer_value, outer_index)
                    .ok_or_else(|| ScopeError::ItemIndexOutOfRange {
                        position: position.clone(),
                        index: outer_index,
                    })?;
                // 上游元素的**实际**类型必须就是本层的直接集合类型。此处对回溯投影出的
                // 擦除借用 `value: &dyn Any` 取类型名，`actual` 因此是 `"dyn core::any::Any"`；
                // 它只作诊断，**不是**真实业务集合元素类型名（本层声明的集合类型另见 `expected`）。
                if value.type_id() != access.collection_type() {
                    return Err(ScopeError::TypeMismatch {
                        position: position.clone(),
                        expected: access.collection_name(),
                        actual: type_name_of_val(value),
                    });
                }
                Ok(value)
            }
        }
    }

    /// 来源链的责任链校验：完整集合 Data 必须由 cap 自身或其后代负责；嵌套来源则要求
    /// 链上每一跳都位于 cap 内，并继续校验其真实根数据仍由可达的责任方持有。
    fn require_collection_visible(
        &self,
        position: &RefId,
        collection: &CollectionRoot,
        cap: &ScopeId,
    ) -> Result<(), ScopeError> {
        match collection {
            CollectionRoot::Data(id) => self.require_visible_target(id, cap),
            CollectionRoot::Item(outer) => {
                let (outer_collection, _, outer_cap, _) = outer
                    .item()
                    .expect("a nested collection source is an item target");
                self.require_in_cap(position, outer_cap, cap)?;
                self.require_collection_visible(position, outer_collection, outer_cap)
            }
        }
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

    /// 整组本地 alias 装配：把 Scope 内已绑定位置的 target 复制到同一 Scope 的新位置。
    ///
    /// 逐项预检（来源已绑定 → 存活与类型 → owner 合法 → 目的位置未绑定 → 批内目的
    /// 位置不重复），全部通过后一次提交。它不移动业务值、不改变 owner，也不放宽
    /// cap 或 Ref 单赋值。
    pub(crate) fn bind_local_aliases(
        &mut self,
        scope: &ScopeId,
        aliases: &[AliasSlot],
    ) -> Result<(), ScopeError> {
        let record = self.registry.lookup(scope)?;
        record.require_active()?;
        let mut planned: Vec<(RefId, RefTarget)> = Vec::with_capacity(aliases.len());
        for slot in aliases {
            let target = record
                .refs
                .get(&slot.source)
                .ok_or_else(|| ScopeError::RefNotBound {
                    scope: scope.clone(),
                    position: slot.source.clone(),
                })?;
            match target {
                RefTarget::Data(id) => {
                    self.container
                        .validate_type(id, slot.expected, slot.expected_name)
                        .map_err(|source| ScopeError::from_storage(slot.source.clone(), source))?;
                    let owner = self.owner_of(id)?;
                    if !self.is_ancestor_or_self(&owner, scope) {
                        return Err(ScopeError::IllegalOwner {
                            id: id.clone(),
                            owner,
                            boundary: scope.clone(),
                        });
                    }
                }
                RefTarget::CollectionItem { .. } => {
                    self.check_item(&slot.source, target, slot.expected, slot.expected_name)?;
                    // 请求方 Scope 也必须在 item cap 内；alias 不能把 cap 外来源写成可读绑定。
                    let cap = target.item_cap().expect("item target matched by variant");
                    self.require_in_cap(&slot.source, cap, scope)?;
                }
            }
            if record.refs.contains_key(&slot.target) {
                return Err(ScopeError::RefAlreadyBound {
                    scope: scope.clone(),
                    position: slot.target.clone(),
                });
            }
            if planned.iter().any(|(seen, _)| seen == &slot.target) {
                return Err(ScopeError::DuplicatePosition {
                    position: slot.target.clone(),
                });
            }
            planned.push((slot.target.clone(), target.clone()));
        }
        let record = self.registry.lookup_mut(scope)?;
        for (position, target) in planned {
            record.refs.insert(position, target);
        }
        Ok(())
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
                        let cap = target.item_cap().expect("item target matched by variant");
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
                        let cap = state_target
                            .item_cap()
                            .expect("item target matched by variant");
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
                    let cap = target.item_cap().expect("item target matched by variant");
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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

    /// 成组 Promote 的报告形状：与 [`Self::promote_report`] 同序，把来源 Round 的
    /// **完整声明输出组**一次预检、一次提交到同一控制器的多个控制状态，并在整组提交
    /// 完成后只关闭来源一次。
    ///
    /// 次序：来源 Active 且无活跃 descendant → 结构校验（空组、长度不等、重复目的 state
    /// 均为 typed 拒绝，且在任何状态变更之前完成）→ 冻结 → 观察 `Before` → 逐槽
    /// [`Self::prepare_promote`] → 全部成功后逐槽 [`Self::apply_promote_effects`] → 整组
    /// 只 [`Self::close_scope`] 一次 → 登记提交现场。任一槽预检拒绝：观察 `AfterReject`
    /// （cleanup 之前）→ [`Self::cleanup_subtree`] → `Rejected`。提交段没有 cleanup 分支
    /// （与单项一致）：预检（含清理前提）已全部完成。
    ///
    /// 责任转移以 `DataId` 为单位去重：多个槽共享同一实例时只转移一次（`owned` 集合
    /// 语义本身幂等，提交现场记录额外保留去重后的集合）。整组不绑定父 Definition RefId、
    /// 不提取业务值，也不循环调用单项 `promote`／`commit_promote`。
    pub(crate) fn promote_group_report(
        &mut self,
        source: &ScopeId,
        selected: &[RefId],
        states: &[ControlStateId],
    ) -> PromoteGroupOutcome {
        if let Err(primary) = self.collect_source_early_check(source) {
            return PromoteGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        // 结构校验先于冻结：纯形状错误不改变来源状态，因此没有清理分支。
        if let Err(primary) = self.check_promote_group_shape(selected, states) {
            return PromoteGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        if let Err(primary) = self
            .registry
            .lookup_mut(source)
            .map(|record| record.state = ScopeState::Finalizing)
        {
            return PromoteGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        #[cfg(test)]
        self.record_round_collect_group_probe(
            super::test_support::RoundCollectSnapshotPhase::Before,
            source,
            selected,
            states,
        );
        let mut plans = Vec::with_capacity(selected.len());
        for (position, state) in selected.iter().zip(states) {
            match self.prepare_promote(source, position, state) {
                Ok(plan) => plans.push(plan),
                Err(primary) => {
                    #[cfg(test)]
                    self.record_round_collect_group_probe(
                        super::test_support::RoundCollectSnapshotPhase::AfterReject,
                        source,
                        selected,
                        states,
                    );
                    return PromoteGroupOutcome::Rejected {
                        primary,
                        cleanup_failure: self.cleanup_subtree(source).err(),
                    };
                }
            }
        }
        for plan in &plans {
            self.apply_promote_effects(plan)
                .expect("promote group preflight guarantees infallible effects");
        }
        #[cfg(test)]
        self.record_promote_group_state_probe(&plans);
        if let Err(primary) = self.close_scope(source) {
            return PromoteGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        PromoteGroupOutcome::Promoted
    }

    /// 成组 Promote 的结构校验：整组非空、声明组与目的 state 长度一致、目的控制状态
    /// 逐槽唯一。在任何状态变更之前执行；空组与长度不等用 [`ScopeError::Invariant`]
    /// （与 Context 许可层判定一致），重复目的 state 用
    /// [`ScopeError::DuplicateStateDestination`]。明确不让空组读成 unit Iter 合法。
    fn check_promote_group_shape(
        &self,
        selected: &[RefId],
        states: &[ControlStateId],
    ) -> Result<(), ScopeError> {
        if selected.is_empty() {
            return Err(ScopeError::Invariant {
                violated: "promote group must declare at least one output slot",
            });
        }
        if selected.len() != states.len() {
            return Err(ScopeError::Invariant {
                violated: "promote group selected and destination state counts must match",
            });
        }
        let mut seen: Vec<(u64, u64)> = Vec::with_capacity(states.len());
        for state in states {
            let key = (state.owner().seq(), state.seq());
            if seen.contains(&key) {
                return Err(ScopeError::DuplicateStateDestination {
                    state: state.clone(),
                });
            }
            seen.push(key);
        }
        Ok(())
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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
                    let cap = target.item_cap().expect("item target matched by variant");
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
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn collector_metadata(
        &self,
        collector: &CollectorId,
    ) -> Result<(&'static str, ScopeId), ScopeError> {
        let record = self.lookup_collector(collector)?;
        Ok((record.element_name, record.owner.clone()))
    }

    /// 观测：未完成 collector 已收集的元素数量。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn collector_len(&self, collector: &CollectorId) -> Result<usize, ScopeError> {
        let _ = self.lookup_collector(collector)?;
        self.container
            .collector_len(collector)
            .map_err(|source| ScopeError::Storage { source })
    }

    /// 直接 Consume：把 ItemScope 的一个完整 owned 输出移入其直接 parent 的 collector。
    ///
    /// 不绑定 EachScope 的 Definition RefId，也不建立 collectible 集合：值在 Container
    /// 内部从普通 entry 移入建构值。全部预检（含剩余 owned 清理前提）在冻结与移动前
    /// 完成；Imported／parent-owned／sibling-owned 输出与类型不符都在移动前拒绝。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
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

    /// 成组 Consume 的报告形状：与 [`Self::consume_item_report`] 同序，把当前 Item 的
    /// **完整声明输出组**一次预检、一次提交进对应的 collector 组，并在整组提交完成后
    /// 只关闭 Item 一次。
    ///
    /// 次序严格对齐成组 Consume 纪律：Item 存在／Active／无 live descendant（早检）→
    /// 整组结构校验（非空、长度相等、位置唯一、目的 collector 唯一）→ 冻结（Finalizing）
    /// → 观察 `Before` → 逐槽 [`Self::prepare_consume`]（声明绑定、完整 Data、存活、元素
    /// 类型、collector 物理建构值与登记身份、Item-owned、清理前提）并按 `DataId` 拒绝重复
    /// 目标 → 全部通过后逐槽 [`DataContainer::move_into_collector`]／撤销 Item owned →
    /// 整组只 [`Self::close_scope`] 一次。
    ///
    /// 任一槽预检拒绝：观察 `AfterReject`（cleanup 之前）→ [`Self::cleanup_subtree`] →
    /// `Rejected`；其前的全部槽都不移动、不追加、不撤销 owner、不使 `DataId` 失效。提交段
    /// 在完整预检之后没有可恢复的普通失败（全部前提已逐槽校验），因此不设 rollback 或
    /// 假成功分支。
    pub(crate) fn consume_item_group_report(
        &mut self,
        item: &ScopeId,
        selected: &[RefId],
        collectors: &[CollectorId],
    ) -> ConsumeGroupOutcome {
        if let Err(primary) = self.consume_item_early_check(item) {
            return ConsumeGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        // 结构校验先于冻结：纯形状错误保持 Active 且零副作用，因此没有清理分支。
        if let Err(primary) = self.check_consume_group_shape(selected, collectors) {
            return ConsumeGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        if let Err(primary) = self
            .registry
            .lookup_mut(item)
            .map(|record| record.state = ScopeState::Finalizing)
        {
            return ConsumeGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            };
        }
        #[cfg(test)]
        self.inject_consume_group_cleanup_fault(item, selected);
        #[cfg(test)]
        self.record_consume_group_probe(
            super::test_support::ConsumeGroupSnapshotPhase::Before,
            item,
            selected,
            collectors,
        );

        let mut plans: Vec<PreparedConsumeSlot> = Vec::with_capacity(selected.len());
        let mut seen: Vec<(RefId, DataId)> = Vec::with_capacity(selected.len());
        for (index, (position, collector)) in selected.iter().zip(collectors).enumerate() {
            match self.prepare_consume_slot(index, item, position, collector, &seen) {
                Ok(slot) => {
                    let id = slot
                        .target
                        .data_id()
                        .expect("prepared consume slot targets complete Data")
                        .clone();
                    seen.push((position.clone(), id));
                    plans.push(slot);
                }
                Err(primary) => {
                    #[cfg(test)]
                    self.record_consume_group_probe(
                        super::test_support::ConsumeGroupSnapshotPhase::AfterReject,
                        item,
                        selected,
                        collectors,
                    );
                    return ConsumeGroupOutcome::Rejected {
                        primary,
                        cleanup_failure: self.cleanup_subtree(item).err(),
                    };
                }
            }
        }

        match self.commit_consume_group(PreparedConsumeGroup {
            item: item.clone(),
            slots: plans,
        }) {
            Ok(()) => ConsumeGroupOutcome::Consumed,
            Err(primary) => ConsumeGroupOutcome::Rejected {
                primary,
                cleanup_failure: None,
            },
        }
    }

    /// 成组 Consume 单槽预检：先按 test-only 故障钩子判定"后项预检失败"，再复用单项
    /// [`Self::prepare_consume`] 的完整校验，最后在整组范围内拒绝"同一物理 `DataId` 被
    /// 两个槽消费"。所有判定都不改变任何状态；`seen` 按声明顺序保存此前的 `(位置, 实例)`。
    fn prepare_consume_slot(
        &self,
        index: usize,
        item: &ScopeId,
        position: &RefId,
        collector: &CollectorId,
        seen: &[(RefId, DataId)],
    ) -> Result<PreparedConsumeSlot, ScopeError> {
        let _ = index;
        #[cfg(test)]
        if observe::consume_group_reject_preflight_at(index) {
            return Err(ScopeError::Invariant {
                violated: "test-injected consume group preflight failure",
            });
        }
        let plan = self.prepare_consume(item, position, collector)?;
        if let Some(id) = plan.target.data_id()
            && let Some((first_position, _)) = seen.iter().find(|(_, target)| target == id)
        {
            return Err(ScopeError::DuplicateConsumeTarget {
                first_position: first_position.clone(),
                duplicate_position: position.clone(),
                data_id: id.clone(),
            });
        }
        Ok(PreparedConsumeSlot {
            position: position.clone(),
            target: plan.target,
            collector: plan.collector,
        })
    }

    /// 成组 Consume 的结构校验：整组非空、声明组与目的 collector 长度一致、声明位置与
    /// 目的 collector 逐槽唯一。在任何状态变更之前执行；空组与长度不等用
    /// [`ScopeError::Invariant`]（与 Context 许可层判定一致），重复位置用
    /// [`ScopeError::DuplicatePosition`]，重复目的 collector 用
    /// [`ScopeError::DuplicateCollectorDestination`]。明确不让空组读成 unit Each 合法。
    fn check_consume_group_shape(
        &self,
        selected: &[RefId],
        collectors: &[CollectorId],
    ) -> Result<(), ScopeError> {
        if selected.is_empty() {
            return Err(ScopeError::Invariant {
                violated: "consume group must declare at least one output slot",
            });
        }
        if selected.len() != collectors.len() {
            return Err(ScopeError::Invariant {
                violated: "consume group selected and destination collector counts must match",
            });
        }
        let mut positions: Vec<&RefId> = Vec::with_capacity(selected.len());
        for position in selected {
            if positions.contains(&position) {
                return Err(ScopeError::DuplicatePosition {
                    position: position.clone(),
                });
            }
            positions.push(position);
        }
        let mut seen_collectors: Vec<&CollectorId> = Vec::with_capacity(collectors.len());
        for collector in collectors {
            if seen_collectors.contains(&collector) {
                return Err(ScopeError::DuplicateCollectorDestination {
                    collector: collector.clone(),
                });
            }
            seen_collectors.push(collector);
        }
        Ok(())
    }

    /// 成组 Consume 提交段：逐槽在 Container 内部移动业务值进各自建构区、逐 `DataId`
    /// 撤销 Item owned；全部槽完成后整组只 [`Self::close_scope`] 一次。旧 `DataId` 在移动
    /// 后立即失效，collector 收到每槽恰一个值。
    ///
    /// 提交段没有可恢复的普通失败分支：`prepare_consume_slot` 已逐槽确认目标为 Item 唯一
    /// 负责的完整 Data、元素类型相符、collector 与登记身份一致，因此 `move_into_collector`
    /// 的归属／存活／类型三类拒绝在提交前都已被排除。容器诊断只用于报告不变量破坏。
    fn commit_consume_group(&mut self, plan: PreparedConsumeGroup) -> Result<(), ScopeError> {
        for (index, slot) in plan.slots.iter().enumerate() {
            #[cfg(test)]
            if observe::consume_group_fail_commit_at(index) {
                return Err(ScopeError::Invariant {
                    violated: "test-injected consume group commit failure",
                });
            }
            #[cfg(not(test))]
            let _ = index;
            let data_id = slot
                .target
                .data_id()
                .expect("prepare_consume only plans complete Data targets");
            self.container
                .move_into_collector(&slot.collector, data_id)
                .map_err(|source| ScopeError::Storage { source })?;
            self.registry.lookup_mut(&plan.item)?.owned.remove(data_id);
        }
        #[cfg(test)]
        self.record_consume_group_after_commit(&plan);

        self.close_scope(&plan.item)
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
    /// 集合来源是 `caller`（EachScope）本地已绑定的 `Ref<Vec<T>>`：既可以是完整
    /// `Vec<T>` Data，也可以是合法的受限 item 借用（外层 Each 的 item，其元素自身就是
    /// `Vec<T>`；SPEC §8 接受任意 `Ref<Vec<T>>` 来源）。两条路径都只记录定位元数据：
    /// cap 设为刚建立的 `item_scope`，不分配 DataId、不移动／复制业务值、不增加 owner，
    /// 也不让来源取得任何独立消费权。`index` 必须在实际长度内；目标位置必须尚未绑定。
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
            match bound {
                RefTarget::Data(id) => {
                    self.container
                        .validate_type(id, TypeId::of::<Vec<T>>(), type_name::<Vec<T>>())
                        .map_err(|source_error| {
                            ScopeError::from_storage(source.clone(), source_error)
                        })?;
                    self.require_visible_target(id, caller)?;
                    CollectionRoot::Data(id.clone())
                }
                inner @ RefTarget::CollectionItem { .. } => {
                    // item-backed 集合：上游必须是合法、可读、元素类型为 `Vec<T>` 的受限
                    // 借用。这里只复核并复用其定位元数据，不移动、不复制、不登记 owned。
                    let cap = inner.item_cap().expect("item target matched by variant");
                    self.check_item(source, inner, TypeId::of::<Vec<T>>(), type_name::<Vec<T>>())?;
                    self.require_in_cap(source, cap, caller)?;
                    CollectionRoot::Item(Box::new(inner.clone()))
                }
            }
        };
        let access = ItemAccess::for_collection::<T>();
        let length = {
            let value = self.collection_value(source, &collection, item_scope, &access)?;
            // 实际值是擦除借用 `&dyn Any`；`actual` 取 `type_name_of_val(value)`，即
            // `"dyn core::any::Any"`，仅作诊断，不代表真实业务集合类型名。
            value
                .downcast_ref::<Vec<T>>()
                .ok_or_else(|| ScopeError::TypeMismatch {
                    position: source.clone(),
                    expected: type_name::<Vec<T>>(),
                    actual: type_name_of_val(value),
                })?
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

    // ---- Root owned 提取（V21-10）：冻结 → 整组预检 → 同步提交 → 复用关闭尾段 ----

    /// 冻结 Root：Active → Finalizing，且只允许一次。
    ///
    /// 与 `finalize` 的先决条件一致：仍活跃的 descendant 必须先结束；已 Finalizing 或
    /// Closed 的 Root 不能再次冻结（K17 重入拒绝）。冻结只改变状态，不改 refs／owned／
    /// target／分配序号；失败（含"仍有 descendant"）不改变状态，由调用方进入受控失败清理。
    pub(crate) fn freeze_root(&mut self, root: &ScopeId) -> Result<(), ScopeError> {
        self.registry.lookup(root)?.require_active()?;
        if self.has_live_descendants(root)? {
            return Err(ScopeError::ActiveDescendants {
                scope: root.clone(),
            });
        }
        self.registry.lookup_mut(root)?.state = ScopeState::Finalizing;
        Ok(())
    }

    /// Root 提取的整组预检（只读，任何 take 之前）。
    ///
    /// 判据顺序固定，避免多判据互相遮蔽：
    /// 1. 声明位置已绑定，且是完整 `Data`（`CollectionItem` 在此拒绝，不借用元素）；
    /// 2. 物理实例的身份 → 存活 → 与声明端口类型（复用容器既有诊断与映射）；
    /// 3. 唯一生命周期责任，且责任方就是本 Root；
    /// 4. 声明顺序中全部物理实例互不重复（同一位置重复与不同位置 alias 同一实例都拒绝）；
    /// 5. Root 关闭前提：剩余 owned 与 collector／state 登记的可恢复前提整组校验。
    ///
    /// 只接受 Active 或 Finalizing（后者说明调用方已冻结）；Closed 拒绝。全部检查通过后
    /// 返回只含元数据的计划；任一项失败都不改变 refs／owned／target／分配序号。
    pub(crate) fn prepare_root_extraction(
        &self,
        root: &ScopeId,
        ports: &[super::signature::DeclaredPort],
    ) -> Result<PreparedRootExtraction, ScopeError> {
        let record = self.registry.lookup(root)?;
        match record.state {
            ScopeState::Active | ScopeState::Finalizing => {}
            ScopeState::Closed => {
                return Err(ScopeError::ScopeClosed {
                    scope: root.clone(),
                });
            }
        }
        if self.has_live_descendants(root)? {
            return Err(ScopeError::ActiveDescendants {
                scope: root.clone(),
            });
        }

        let mut selections: Vec<(RefId, DataId)> = Vec::with_capacity(ports.len());
        for port in ports {
            let position = port.position();
            let target = record
                .refs
                .get(position)
                .ok_or_else(|| ScopeError::RefNotBound {
                    scope: root.clone(),
                    position: position.clone(),
                })?;
            // 完整 Data 才可提取；CollectionItem 即使元素类型相同、cap 仍存活也拒绝。
            let id = target.require_data(position)?;
            self.container
                .validate_type(id, port.expected(), port.expected_name())
                .map_err(|source| ScopeError::from_storage(position.clone(), source))?;
            let owner = self.owner_of(id)?;
            if owner != *root {
                return Err(ScopeError::IllegalOwner {
                    id: id.clone(),
                    owner,
                    boundary: root.clone(),
                });
            }
            if let Some((first_ref, _)) = selections.iter().find(|(_, selected)| selected == id) {
                return Err(ScopeError::DuplicateRootDataId {
                    first_ref: first_ref.clone(),
                    duplicate_ref: position.clone(),
                    data_id: id.clone(),
                });
            }
            selections.push((position.clone(), id.clone()));
        }

        // 关闭前提在提取之前整组校验：预检结束时已不再需要重新计算清理集合。
        let close_targets: Vec<DataId> = self
            .cleanup_targets(root)?
            .into_iter()
            .filter(|id| !selections.iter().any(|(_, selected)| selected == id))
            .collect();

        Ok(PreparedRootExtraction {
            selections,
            close_targets,
        })
    }

    /// Root 提取的同步提交：take 全组 owned 值并在同一不可观察边界解除 Root 责任。
    ///
    /// 计划必须来自本次 Root 的 `prepare_root_extraction`，调用前 Root 已冻结（Finalizing）。
    /// 每份值只移动一次；take 与 `RootScope.owned` 移除之间没有可恢复分支（身份与存活已由
    /// 预检证明），因此不会出现"已取走但仍有 owner"或"已解除 owner 但值仍在"的中间状态。
    /// 返回按声明顺序的擦除值，以及关闭 Root 时仍需销毁的剩余 owned（预检时的整组结果）。
    pub(crate) fn commit_root_extraction(
        &mut self,
        root: &ScopeId,
        plan: PreparedRootExtraction,
    ) -> (Vec<Box<dyn Any>>, Vec<DataId>) {
        debug_assert!(
            matches!(
                self.registry
                    .scopes
                    .get(&root.seq())
                    .map(|record| record.state),
                Some(ScopeState::Finalizing)
            ),
            "root extraction commits only after freezing the root"
        );
        let mut values = Vec::with_capacity(plan.selections.len());
        for (_, id) in &plan.selections {
            values.push(self.container.take_verified(id));
            let record = self
                .registry
                .scopes
                .get_mut(&root.seq())
                .expect("root scope record exists until it closes");
            record.owned.remove(id);
        }
        (values, plan.close_targets)
    }

    /// Root 提取后的关闭尾段：复用已验收的整组清理与关闭语义。
    ///
    /// `targets` 必须来自本次预检（`PreparedRootExtraction`），因此关闭阶段不再有可恢复
    /// 的普通失败：本地引用整组失效、剩余 owned 与未完成 collector／state 撤销一次、
    /// 保留 tombstone。已移交 Application 的实例不在 `targets` 中，不会被再次销毁。
    pub(crate) fn close_root_validated(
        &mut self,
        root: &ScopeId,
        targets: Vec<DataId>,
    ) -> Result<(), ScopeError> {
        self.close_validated(root, targets)
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
                    let cap = target.item_cap().expect("item target matched by variant");
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
                let cap = target.item_cap().expect("item target matched by variant");
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

    /// Promote 提交段的**副作用**：更新状态、必要时转移责任、登记待回收旧值。
    ///
    /// 从 [`Self::commit_promote`] 逐字抽出，供单项与成组路径共用；不含关闭来源，也
    /// 不做任何可恢复失败判定。计划里的来源／控制器／状态键都来自本 registry，因此这里
    /// 的查表不会失败。责任转移以 `DataId` 为单位：对同一 `DataId` 重复调用是幂等的
    /// （`owned` 集合的 remove／insert 不产生第二份责任），成组提交可直接对共享实例
    /// 逐槽调用，无需另设去重索引。
    fn apply_promote_effects(&mut self, plan: &PreparedPromote) -> Result<(), ScopeError> {
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
            if let Some(old) = plan.replaced_pending.as_ref()
                && !state.pending.iter().any(|pending| pending == old)
            {
                state.pending.push(old.clone());
            }
        }
        Ok(())
    }

    /// Promote 提交段：更新状态、必要时转移责任、关闭来源、登记待回收旧值。
    ///
    /// 提交段没有可恢复失败分支：预检（含清理前提）已全部完成。
    fn commit_promote(&mut self, plan: PreparedPromote) -> Result<(), ScopeError> {
        self.apply_promote_effects(&plan)?;
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
        #[cfg(test)]
        super::test_support::record_scope_closed(scope.clone());
        if let Some(parent) = parent
            && let Some(parent_record) = self.registry.scopes.get_mut(&parent.seq())
        {
            parent_record.children.retain(|child| child != scope);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/internal/core/scope_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) use observe::{TargetSnapshot, TargetSnapshotPair};

#[cfg(test)]
#[path = "../../tests/internal/core/scope_observe.rs"]
pub(crate) mod observe;
