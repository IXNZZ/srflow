//! Definition 的唯一静态结构与共用验证机制（Engine §3、§10.7～§10.8）。
//!
//! - [`Definition`]：验证完成的静态结构，持有 Body／Step、逻辑位置来源、静态 Core RefId
//!   与 Body 父子关系；不保存 Invocation、ScopeId／DataId、执行次数或业务值；
//! - [`Builder`]：只在构建期可变，登记 Node／Chain／Choose Step、分配 Core RefId、记录 Import 需求；
//! - [`build_definition`]：唯一出口。同步执行 Root 定义闭包，登记 Root 输入的逻辑 Ref，
//!   收集各 Body 声明输出，最后对整棵树统一验证。
//!
//! 验证过程不调用任何业务 Node；失败返回 [`BuildErrorDetail`]，不返回可执行消费的
//! 半成品。每个合法逻辑位置在 Definition 内固定一个 Core RefId；独立的逻辑位置不共用它。

use crate::core::context::ExecutionContext;
use crate::core::engine::data::Data;
use crate::core::engine::error::{BodyError, BuildErrorDetail, ExecFailure, Visibility};
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::{BodyId, DefinitionId, PositionHandle, PositionId};
use crate::core::engine::node::Callable;
use crate::core::engine::shape::{
    InputOps, InputSpec, NodeOutput, OutputOps, PositionDesc, Query, RefShape, RootInput, ShapeOps,
    TypeDesc,
};
use crate::core::identity::{CollectorId, ScopeId};
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};
use crate::core::scope::{ControlStateId, StateImportSlot};
use crate::core::signature::DeclaredPort;
use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

/// Step 的逻辑类别；T01／T02 为 Node 与 Chain，T03 加入 Choose，T04 加入 Retry 与 Iter，
/// T05 加入 Each。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[allow(dead_code)] // 由定向测试与结构检查使用
pub(crate) enum StepKind {
    /// 业务叶子调用。
    Node,
    /// 带独立生命周期边界的子 Body。
    Chain,
    /// 按 `PartialEq` 选择一个分支 Body 执行。
    Choose,
    /// 控制器：按预算重复执行完整 attempt Body，只有显式 Retry 信号触发重试。
    Retry,
    /// 控制器：用 Normal next 推进受控状态，用 IterBreak 返回进入本轮时的 current。
    Iter,
    /// 控制器：按集合元素顺序对每项执行同一 item Body，并按槽聚合输出。
    Each,
}

/// Body 内单个 Step 的只读登记视图与受控执行入口。
pub(crate) trait StepEntry {
    /// Step 类别。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    fn kind(&self) -> StepKind;

    /// 该 Step 使用的输入位置与声明类型；Chain Step 没有显式输入。
    fn inputs(&self) -> &[PositionDesc];

    /// 该 Step 在所属 Body 登记的输出位置。
    fn outputs(&self) -> &[PositionId];

    /// 供诊断的 callable 名称。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    fn callable_name(&self) -> Option<&'static str> {
        None
    }

    /// Chain Step 的子 Body 映射。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    fn chain(&self) -> Option<&ChainStep> {
        None
    }

    /// Each Step 的静态视图。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    fn each(&self) -> Option<&EachStep> {
        None
    }

    /// 登记该 Step 时的定义顺序；用于拒绝前向来源。
    fn order(&self) -> u64;

    /// 在当前 invocation 的 Scope 中顺序执行本 Step。
    fn execute<'a>(
        &'a self,
        definition: &'a Definition<'_>,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecFailure>> + 'a>>;
}

/// Node 调用 Future：输出类型保持具体，借用限定在 `'a` 内。
pub(crate) type NodeCallFuture<'a, N, A, K> =
    Pin<Box<dyn Future<Output = Result<<N as Callable<A, K>>::Output, BodyError>> + 'a>>;

/// Node Step：真实 callable 句柄、声明位置与静态 Core RefId。
pub(crate) struct NodeStep<N, A, K> {
    node: N,
    inputs: Vec<PositionDesc>,
    outputs: Vec<PositionId>,
    #[allow(dead_code)] // 由定向测试与结构检查使用
    name: &'static str,
    order: u64,
    marker: PhantomData<fn(A, K)>,
}

impl<N, A, K> NodeStep<N, A, K> {
    /// 注册的真实 callable 句柄。
    #[allow(dead_code)] // 由定向测试与后续控制器检查使用
    pub(crate) fn callable(&self) -> &N {
        &self.node
    }
}

impl<N, A, K> NodeStep<N, A, K>
where
    N: Callable<A, K>,
    A: RefShape,
    K: 'static,
{
    /// 构造一次真实调用：输出类型保持具体，Query 借用限定在返回的 Future 内。
    pub(crate) fn invoke<'a>(
        &'a self,
        query: Query<<A::Spec as InputSpec>::Borrowed<'a>>,
    ) -> NodeCallFuture<'a, N, A, K> {
        self.node.call(query)
    }
}

impl<N, A, K> StepEntry for NodeStep<N, A, K>
where
    N: Callable<A, K>,
    A: RefShape + ShapeOps,
    A::Spec: InputOps,
    <N::Output as NodeOutput>::Refs: ShapeOps,
    N::Output: OutputOps,
    K: 'static,
{
    fn kind(&self) -> StepKind {
        StepKind::Node
    }

    fn inputs(&self) -> &[PositionDesc] {
        &self.inputs
    }

    fn outputs(&self) -> &[PositionId] {
        &self.outputs
    }

    fn callable_name(&self) -> Option<&'static str> {
        Some(self.name)
    }

    fn order(&self) -> u64 {
        self.order
    }

    fn execute<'a>(
        &'a self,
        definition: &'a Definition<'_>,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecFailure>> + 'a>> {
        Box::pin(crate::core::engine::adapter::invoke_node::<N, A, K>(
            ctx, scope, definition, self,
        ))
    }
}

/// parent 输出槽到 child 出口／祖先 alias 的静态分类。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ChainBinding {
    /// 绑定 child 自身产生的位置（`exits` 的下标）。
    ChildExit(u16),
    /// 只读 alias：绑定祖先位置，不转移数据责任。
    AncestorAlias(PositionId),
    /// 无法解析的声明槽位；只可能出现在必然失败的构建中。
    Unresolved,
}

/// 一个调用边界的输出槽运行期装配信息（Chain 与 Choose 分支共用）。
#[derive(Clone, Debug)]
pub(crate) struct HandoverSlot {
    /// child 本地用于声明出口的位置（每个槽唯一）。
    pub(crate) exit: RefId,
    /// child 本地被 alias 的来源位置（child 产出或导入的祖先位置）。
    pub(crate) source: RefId,
    /// parent（caller）侧被绑定的输出位置。
    pub(crate) output: RefId,
    /// 该槽声明的类型。
    pub(crate) type_desc: TypeDesc,
}

/// Chain Step：child Body、唯一声明出口与 parent 输出槽绑定。
pub(crate) struct ChainStep {
    child: BodyId,
    exits: Vec<PositionId>,
    bindings: Vec<ChainBinding>,
    slots: Vec<HandoverSlot>,
    outputs: Vec<PositionId>,
    order: u64,
}

impl ChainStep {
    /// child Body 的编号。
    pub(crate) fn child(&self) -> BodyId {
        self.child
    }

    /// child 自身产生的位置（按首次出现顺序、去重）；alias 槽位不计入。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn exits(&self) -> &[PositionId] {
        &self.exits
    }

    /// parent 输出槽的静态分类。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn bindings(&self) -> &[ChainBinding] {
        &self.bindings
    }

    /// 每个输出槽的运行期装配信息（出口／来源／parent 位置）。
    pub(crate) fn slots(&self) -> &[HandoverSlot] {
        &self.slots
    }
}

impl StepEntry for ChainStep {
    fn kind(&self) -> StepKind {
        StepKind::Chain
    }

    fn inputs(&self) -> &[PositionDesc] {
        &[]
    }

    fn outputs(&self) -> &[PositionId] {
        &self.outputs
    }

    fn chain(&self) -> Option<&ChainStep> {
        Some(self)
    }

    fn order(&self) -> u64 {
        self.order
    }

    fn execute<'a>(
        &'a self,
        definition: &'a Definition<'_>,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecFailure>> + 'a>> {
        Box::pin(crate::core::engine::boundary::invoke_chain(
            ctx, scope, self, definition,
        ))
    }
}

/// 一个 Choose 分支（case 或 otherwise）的静态装配信息。
///
/// 每个分支有自己的 Body 与本地声明出口；所有分支的第 i 个槽位共享同一个 parent
/// 输出位置，因此运行时只有一个分支的真实装配会写进 parent。
pub(crate) struct ChooseBranch {
    /// 该分支自己的 Body。
    pub(crate) body: BodyId,
    /// 分支自身产生、被声明输出的位置（按首次出现顺序、去重）。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) sources: Vec<PositionId>,
    /// 每个输出槽的分支本地出口、来源与共享 parent 输出位置。
    pub(crate) slots: Vec<HandoverSlot>,
}

/// 一个匹配值与分支的配置对；键是 Definition 配置，不登记为业务输入。
pub(crate) struct ChooseCase<K> {
    pub(crate) key: K,
    pub(crate) branch: ChooseBranch,
}

/// Choose Step：selector 逻辑来源、有序 case 配置、可选 otherwise、共同 parent 输出与顺序。
pub(crate) struct ChooseStep<K> {
    selector: PositionDesc,
    cases: Vec<ChooseCase<K>>,
    otherwise: Option<ChooseBranch>,
    outputs: Vec<PositionId>,
    order: u64,
}

impl<K> ChooseStep<K> {
    /// 构造 Choose Step（构建期收口路径）。
    pub(crate) fn new(
        selector: PositionDesc,
        cases: Vec<ChooseCase<K>>,
        otherwise: Option<ChooseBranch>,
        outputs: Vec<PositionId>,
        order: u64,
    ) -> Self {
        Self {
            selector,
            cases,
            otherwise,
            outputs,
            order,
        }
    }

    /// selector 的逻辑位置与声明类型；同时作为该 Step 的静态输入参与统一验证。
    pub(crate) fn selector(&self) -> PositionDesc {
        self.selector
    }

    /// 按登记顺序的 case 配置。
    pub(crate) fn cases(&self) -> &[ChooseCase<K>] {
        &self.cases
    }

    /// otherwise 分支（如有）。
    pub(crate) fn otherwise(&self) -> Option<&ChooseBranch> {
        self.otherwise.as_ref()
    }
}

impl<K> StepEntry for ChooseStep<K>
where
    K: Data + PartialEq + 'static,
{
    fn kind(&self) -> StepKind {
        StepKind::Choose
    }

    fn inputs(&self) -> &[PositionDesc] {
        std::slice::from_ref(&self.selector)
    }

    fn outputs(&self) -> &[PositionId] {
        &self.outputs
    }

    fn order(&self) -> u64 {
        self.order
    }

    fn execute<'a>(
        &'a self,
        definition: &'a Definition<'_>,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecFailure>> + 'a>> {
        Box::pin(crate::core::engine::boundary::invoke_choose(
            ctx, scope, self, definition,
        ))
    }
}

/// Retry Step：attempt Body、预算与 parent 输出槽绑定（SPEC §10、Engine §6.8）。
///
/// attempt Body 构建一次、多次执行；每次 attempt 在控制器保留的**原始依赖**上重新装配。
pub(crate) struct RetryStep {
    child: BodyId,
    max_retries: usize,
    slots: Vec<HandoverSlot>,
    bridges: Vec<RefId>,
    outputs: Vec<PositionId>,
    order: u64,
}

impl RetryStep {
    /// attempt Body 的编号。
    pub(crate) fn child(&self) -> BodyId {
        self.child
    }

    /// 首次 attempt 之外允许的重试次数；总尝试次数最多 `max_retries + 1`。
    pub(crate) fn max_retries(&self) -> usize {
        self.max_retries
    }

    /// 每个 parent 输出槽的运行期装配信息（attempt 出口／来源／parent 位置）。
    pub(crate) fn slots(&self) -> &[HandoverSlot] {
        &self.slots
    }

    /// controller 本地桥接出口（与 slots 等长）：Round → controller 的整组交付目的位置，
    /// 再由 controller → caller 的第二跳交给 parent 输出位置。
    pub(crate) fn bridges(&self) -> &[RefId] {
        &self.bridges
    }
}

impl StepEntry for RetryStep {
    fn kind(&self) -> StepKind {
        StepKind::Retry
    }

    fn inputs(&self) -> &[PositionDesc] {
        &[]
    }

    fn outputs(&self) -> &[PositionId] {
        &self.outputs
    }

    fn order(&self) -> u64 {
        self.order
    }

    fn execute<'a>(
        &'a self,
        definition: &'a Definition<'_>,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecFailure>> + 'a>> {
        Box::pin(crate::core::engine::boundary::invoke_retry(
            ctx, scope, self, definition,
        ))
    }
}

/// Iter 的逐槽状态登记回调（由 `Flow::iter` 从 `StateShape` 取；不在运行期重建形状）。
pub(crate) type RegisterStatesFn =
    fn(&mut ExecutionContext, &ScopeId, &[RefId]) -> Result<Vec<ControlStateId>, ScopeError>;

/// Iter 的逐槽 StateImport 槽构造回调（controller 状态 → 本轮 formal current 位置）。
pub(crate) type StateImportSlotsFn = fn(&[ControlStateId], &[RefId]) -> Vec<StateImportSlot>;

/// Iter Step：round Body、formal current、预算与 parent 输出槽绑定（SPEC §11、Engine §6.9）。
pub(crate) struct IterStep {
    child: BodyId,
    max_iterations: usize,
    current: Vec<PositionId>,
    initial: Vec<PositionDesc>,
    slots: Vec<HandoverSlot>,
    bridges: Vec<RefId>,
    outputs: Vec<PositionId>,
    order: u64,
    /// 逐槽类型化的状态登记（由 `Flow::iter` 从 `StateShape` 取；不在运行期重建形状）。
    register_states: RegisterStatesFn,
    /// 逐槽类型化的 StateImport 槽构造。
    state_import_slots: StateImportSlotsFn,
}

impl IterStep {
    /// round Body 的编号。
    pub(crate) fn child(&self) -> BodyId {
        self.child
    }

    /// 轮次上限；第一轮计入，`max_iterations` 要求大于零。
    pub(crate) fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    /// formal current 的逻辑位置（owner 是 round Body，词法可见性限于该 round；每轮经 StateImport 绑定）。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn current(&self) -> &[PositionId] {
        &self.current
    }

    /// 初始状态的声明位置（parent 内，按槽位顺序）。
    pub(crate) fn initial(&self) -> &[PositionDesc] {
        &self.initial
    }

    /// 每个 parent 输出槽的运行期装配信息（round 出口／next 来源／parent 位置）。
    pub(crate) fn slots(&self) -> &[HandoverSlot] {
        &self.slots
    }

    /// controller 本地桥接出口（与 slots 等长）；最终交付的两跳映射同 Retry。
    pub(crate) fn bridges(&self) -> &[RefId] {
        &self.bridges
    }

    /// 逐槽登记 controller 初始状态（类型化操作由构建期确定）。
    pub(crate) fn register_states(
        &self,
        ctx: &mut ExecutionContext,
        controller: &ScopeId,
        initial: &[RefId],
    ) -> Result<Vec<ControlStateId>, ScopeError> {
        (self.register_states)(ctx, controller, initial)
    }

    /// 逐槽构造 StateImport 槽：controller 状态 → 本轮 formal current 位置。
    pub(crate) fn state_import_slots(
        &self,
        states: &[ControlStateId],
        targets: &[RefId],
    ) -> Vec<StateImportSlot> {
        (self.state_import_slots)(states, targets)
    }
}

impl StepEntry for IterStep {
    fn kind(&self) -> StepKind {
        StepKind::Iter
    }

    fn inputs(&self) -> &[PositionDesc] {
        &self.initial
    }

    fn outputs(&self) -> &[PositionId] {
        &self.outputs
    }

    fn order(&self) -> u64 {
        self.order
    }

    fn execute<'a>(
        &'a self,
        definition: &'a Definition<'_>,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecFailure>> + 'a>> {
        Box::pin(crate::core::engine::boundary::invoke_iter(
            ctx, scope, self, definition,
        ))
    }
}

/// Each 的逐槽 typed collector 建立回调（由 `Flow::each` 从 `EachCollect` 取；
/// `O` 在实例化期确定，运行期只持有 `CollectorId`）。
pub(crate) type BeginCollectorsFn =
    fn(&mut ExecutionContext, &ScopeId) -> Result<Vec<CollectorId>, ScopeError>;

/// Each 的形式 item 输入绑定回调（由 `Flow::each` 从集合元素类型 `T` 取）。
pub(crate) type BindItemFn =
    fn(&mut ExecutionContext, &ScopeId, &ScopeId, &RefId, &RefId, usize) -> Result<(), ScopeError>;

/// Each 的集合长度读取回调（由 `Flow::each` 从集合元素类型 `T` 取）。
pub(crate) type CollectionLenFn =
    fn(&ExecutionContext, &ScopeId, &RefId) -> Result<usize, ScopeError>;

/// Each 的逐槽 finish 回调（由 `Flow::each` 从 `EachCollect` 取）：全部项成功后把各
/// collector 的建构值变成普通 `Vec<A>` Data 并绑定到 Each 本地最终输出位置。
pub(crate) type FinishCollectorsFn =
    fn(&mut ExecutionContext, &ScopeId, &[CollectorId], &[RefId]) -> Result<(), ScopeError>;

/// Each 一个输出槽的运行期装配信息。
///
/// 每个槽三个位置：`source` 是 item Body 声明的输出（类型 `A`，成组 Consume 的来源）；
/// `local` 是 Each 本地 finish 目标（类型 `Vec<A>`）；`output` 是 caller 可见输出
/// （类型 `Vec<A>`，整组 Export 的目的）。同类型不同逻辑位置各有独立位置，不合并。
#[derive(Clone, Debug)]
pub(crate) struct EachSlot {
    /// item Body 声明输出的静态 Core RefId（类型 `A`）。
    pub(crate) source: RefId,
    /// Each 本地最终 Vec 绑定位置（类型 `Vec<A>`）。
    pub(crate) local: RefId,
    /// caller 输出位置（类型 `Vec<A>`）。
    pub(crate) output: RefId,
    /// 聚合后的声明类型（`Vec<A>`）。
    pub(crate) collected_type: TypeDesc,
}

/// Each Step：集合静态输入、item Body、形式 item 位置与 caller 输出槽（SPEC §8、Engine §6.6）。
///
/// Body 只构建一次、多次执行；形式 item 位置每次 Item 复用同一逻辑位置与 Core RefId，
/// 因此不按 index 动态分配。typed 桥以回调保存，`T`／`O` 不上浮到通用 runner。
pub(crate) struct EachStep {
    item: BodyId,
    collection: PositionDesc,
    item_slot: PositionId,
    slots: Vec<EachSlot>,
    outputs: Vec<PositionId>,
    order: u64,
    begin_collectors: BeginCollectorsFn,
    bind_item: BindItemFn,
    collection_len: CollectionLenFn,
    finish_collectors: FinishCollectorsFn,
}

impl EachStep {
    /// 静态 item Body 的编号（构建一次，运行期多次执行）。
    pub(crate) fn item(&self) -> BodyId {
        self.item
    }

    /// 集合 `Ref<Vec<T>>` 的静态输入位置与类型。
    pub(crate) fn collection(&self) -> PositionDesc {
        self.collection
    }

    /// 形式 item 的逻辑位置（owner 是 item Body；每次 Item 复用）。
    pub(crate) fn item_slot(&self) -> PositionId {
        self.item_slot
    }

    /// 逐输出槽的运行期装配信息（来源／Each 本地／caller 输出）。
    pub(crate) fn slots(&self) -> &[EachSlot] {
        &self.slots
    }

    /// 在 Each Scope 下逐槽建立 typed 空 collector（unit 为零个）。
    pub(crate) fn begin_collectors(
        &self,
        ctx: &mut ExecutionContext,
        each: &ScopeId,
    ) -> Result<Vec<CollectorId>, ScopeError> {
        (self.begin_collectors)(ctx, each)
    }

    /// 把集合第 `index` 个元素作为受限借用绑定到形式 item 位置。
    pub(crate) fn bind_item(
        &self,
        ctx: &mut ExecutionContext,
        item: &ScopeId,
        caller: &ScopeId,
        source: &RefId,
        target: &RefId,
        index: usize,
    ) -> Result<(), ScopeError> {
        (self.bind_item)(ctx, item, caller, source, target, index)
    }

    /// 读取集合 `Ref<Vec<T>>` 的长度（借用立即结束）。
    pub(crate) fn collection_len(
        &self,
        ctx: &ExecutionContext,
        scope: &ScopeId,
        position: &RefId,
    ) -> Result<usize, ScopeError> {
        (self.collection_len)(ctx, scope, position)
    }

    /// 全部项成功后逐槽 finish：建构值成为普通 `Vec<A>` Data 并绑定到 `targets`
    /// （与 `collectors` 逐槽对应，通常取 `slots()[i].local`）。
    pub(crate) fn finish_collectors(
        &self,
        ctx: &mut ExecutionContext,
        each: &ScopeId,
        collectors: &[CollectorId],
        targets: &[RefId],
    ) -> Result<(), ScopeError> {
        (self.finish_collectors)(ctx, each, collectors, targets)
    }
}

impl StepEntry for EachStep {
    fn kind(&self) -> StepKind {
        StepKind::Each
    }

    fn inputs(&self) -> &[PositionDesc] {
        std::slice::from_ref(&self.collection)
    }

    fn outputs(&self) -> &[PositionId] {
        &self.outputs
    }

    fn each(&self) -> Option<&EachStep> {
        Some(self)
    }

    fn order(&self) -> u64 {
        self.order
    }

    fn execute<'a>(
        &'a self,
        definition: &'a Definition<'_>,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecFailure>> + 'a>> {
        Box::pin(crate::core::engine::boundary::invoke_each(
            ctx, scope, self, definition,
        ))
    }
}

/// 一个 Body：有序 Step、声明输出与跨边界 Import 需求。
///
/// Body 嵌套关系（`parent` 链）是 Definition Scope 父子关系的唯一权威来源。
pub(crate) struct Body<'n> {
    id: BodyId,
    parent: Option<BodyId>,
    steps: Vec<Box<dyn StepEntry + 'n>>,
    output: Vec<PositionDesc>,
    output_order: u64,
    imports: Vec<PositionId>,
}

impl<'n> Body<'n> {
    /// Body 编号。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn id(&self) -> BodyId {
        self.id
    }

    /// 父 Body；Root 为 `None`。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn parent(&self) -> Option<BodyId> {
        self.parent
    }

    /// 按定义顺序登记的全部 Step。
    pub(crate) fn steps(&self) -> &[Box<dyn StepEntry + 'n>] {
        &self.steps
    }

    /// 该 Body 声明的输出槽位；空表示显式 unit、已完成零输出。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn output(&self) -> &[PositionDesc] {
        &self.output
    }

    /// 执行本 Body 需要从祖先导入的逻辑位置（含深层捕获的完整中间路径）。
    pub(crate) fn imports(&self) -> &[PositionId] {
        &self.imports
    }
}

/// 位置的生产来源。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PositionSource {
    /// Root 输入的第 N 个声明槽位。
    RootInput { slot: u16 },
    /// 所属 Body 中第 N 个 Step 的 Node 输出槽位。
    NodeOutput { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Chain Step 的 parent 输出槽位。
    ChainOutput { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Chain Step 的 child 声明出口位置。
    ChainExit { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Choose Step 的共享 parent 输出槽位。
    ChooseOutput { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Choose Step 第 M 个分支的声明出口位置。
    ChooseExit { step: usize, branch: u16, slot: u16 },
    /// 所属 Body 中第 N 个 Retry Step attempt Body 的声明出口位置。
    RetryExit { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Retry Step 的 parent 输出槽位。
    RetryOutput { step: usize, slot: u16 },
    /// 所属 round Body 中第 N 个 Iter Step 的 formal current 位置（每轮经 StateImport 绑定，词法可见性限于该 round）。
    IterCurrent { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Iter Step round Body 的声明出口位置（最终交付用）。
    IterExit { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Iter Step 的 parent 输出槽位。
    IterOutput { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Retry Step 的 controller 本地桥接出口（最终交付的第二跳）。
    RetryBridge { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Iter Step 的 controller 本地桥接出口（最终交付的第二跳）。
    IterBridge { step: usize, slot: u16 },
    /// 所属 item Body 中第 N 个 Each Step 的形式 item 位置（每次 Item 复用同一逻辑位置与
    /// Core RefId，运行期绑定不同 CollectionItem；词法可见性限于该 item Body）。
    EachItem { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Each Step 的 Each 本地最终 Vec 位置（finish 绑定用）。
    EachLocal { step: usize, slot: u16 },
    /// 所属 Body 中第 N 个 Each Step 的 caller 输出槽位。
    EachOutput { step: usize, slot: u16 },
}

/// 一个已登记逻辑位置的来源、归属、类型与静态 Core RefId。
pub(crate) struct PositionRecord {
    #[allow(dead_code)] // 由定向测试与结构检查使用（运行期以 core RefId 定位）
    handle: PositionHandle,
    core: RefId,
    owner: BodyId,
    source: PositionSource,
    type_desc: TypeDesc,
    created_order: u64,
}

impl PositionRecord {
    /// 逻辑位置句柄。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn handle(&self) -> PositionHandle {
        self.handle
    }

    /// 该位置固定的静态 Core RefId；每个 Scope 各自绑定一次。
    pub(crate) fn core(&self) -> &RefId {
        &self.core
    }

    /// 拥有该位置的 Body。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn owner(&self) -> BodyId {
        self.owner
    }

    /// 生产来源；位置一旦登记就必有来源，不存在“只分配位置”的中间态。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn source(&self) -> PositionSource {
        self.source
    }

    /// 登记类型。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn type_desc(&self) -> TypeDesc {
        self.type_desc
    }
}

/// 验证完成的 Definition；字段私有，只提供只读访问，没有可绕过后置验证的写入口。
pub(crate) struct Definition<'n> {
    id: DefinitionId,
    bodies: Vec<Body<'n>>,
    positions: Vec<PositionRecord>,
}

impl<'n> Definition<'n> {
    /// 定义身份；与其他 Definition 的 Ref 不能混用。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn id(&self) -> DefinitionId {
        self.id
    }

    /// Root Body。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn root(&self) -> &Body<'n> {
        &self.bodies[BodyId::ROOT.index()]
    }

    /// 按编号取得 Body。
    pub(crate) fn body(&self, id: BodyId) -> &Body<'n> {
        &self.bodies[id.index()]
    }

    /// 全部 Body；下标即 [`BodyId`]。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn bodies(&self) -> &[Body<'n>] {
        &self.bodies
    }

    /// 按句柄取得已登记位置；句柄属于其他 Definition 时为 `None`。
    pub(crate) fn record(&self, handle: PositionHandle) -> Option<&PositionRecord> {
        if handle.definition != self.id {
            return None;
        }
        self.positions.get(handle.position.index())
    }

    /// Root 输入的声明位置（按声明顺序）。
    pub(crate) fn root_inputs(&self) -> impl Iterator<Item = &PositionRecord> {
        self.positions
            .iter()
            .take_while(|record| matches!(record.source, PositionSource::RootInput { .. }))
    }

    /// Root 输出的声明端口（顺序即 Root 输出 Shape 顺序）。
    pub(crate) fn root_output_ports(&self) -> Vec<DeclaredPort> {
        self.root()
            .output
            .iter()
            .map(|desc| {
                DeclaredPort::with_type(
                    self.record(desc.handle)
                        .expect("root output slots are registered positions")
                        .core
                        .clone(),
                    desc.type_desc.id,
                    desc.type_desc.name,
                )
            })
            .collect()
    }

    /// 全部已登记位置。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn positions(&self) -> &[PositionRecord] {
        &self.positions
    }
}

/// Definition 构建器；只在构建期可变，不进入执行消费面。
pub(crate) struct Builder<'n> {
    id: DefinitionId,
    core_source: Arc<RefIdSource>,
    next_position: u32,
    order_cursor: u64,
    bodies: Vec<Body<'n>>,
    positions: Vec<PositionRecord>,
    errors: Vec<BuildErrorDetail>,
}

impl<'n> Builder<'n> {
    // Probe-only entry points over the existing Builder, not a second Definition.
    pub(crate) fn probe_inputs<I: RootInput + InputOps>(
        &mut self,
        root: BodyId,
    ) -> Vec<PositionHandle> {
        <I as InputOps>::type_descs()
            .into_iter()
            .enumerate()
            .map(|(slot, ty)| {
                self.allocate(root, PositionSource::RootInput { slot: slot as u16 }, ty)
            })
            .collect()
    }
    pub(crate) fn probe_missing(&self) -> PositionHandle {
        PositionHandle {
            definition: self.id,
            position: PositionId::new(u32::MAX),
        }
    }
    pub(crate) fn probe_resolve(&self, body: BodyId, ty: TypeDesc) -> Option<PositionHandle> {
        self.positions
            .iter()
            .rev()
            .find(|p| {
                p.type_desc == ty
                    && self.is_ancestor_or_self(p.owner, body)
                    && matches!(
                        p.source,
                        PositionSource::RootInput { .. }
                            | PositionSource::NodeOutput { .. }
                            | PositionSource::ChainOutput { .. }
                            | PositionSource::ChooseOutput { .. }
                            | PositionSource::RetryOutput { .. }
                            | PositionSource::IterCurrent { .. }
                            | PositionSource::IterOutput { .. }
                            | PositionSource::EachItem { .. }
                            | PositionSource::EachOutput { .. }
                    )
            })
            .map(|p| p.handle)
    }
    pub(crate) fn probe_finish(
        mut self,
        output: Vec<PositionDesc>,
    ) -> Result<Definition<'n>, BuildErrorDetail> {
        let order = self.next_order();
        self.bodies[0].output = output;
        self.bodies[0].output_order = order;
        self.finish(BodyId::ROOT)
    }

    pub(crate) fn new() -> Self {
        Self {
            id: DefinitionId::fresh(),
            core_source: RefIdSource::new(),
            next_position: 0,
            order_cursor: 0,
            bodies: Vec::new(),
            positions: Vec::new(),
            errors: Vec::new(),
        }
    }

    pub(crate) fn new_body(&mut self, parent: Option<BodyId>) -> BodyId {
        let id = BodyId::new(self.bodies.len() as u32);
        self.bodies.push(Body {
            id,
            parent,
            steps: Vec::new(),
            output: Vec::new(),
            output_order: 0,
            imports: Vec::new(),
        });
        id
    }

    /// 取得下一个定义顺序值；登记与使用各消耗一个值。
    fn next_order(&mut self) -> u64 {
        let order = self.order_cursor;
        self.order_cursor += 1;
        order
    }

    /// 测试可控入口：当前 Definition 身份。
    #[cfg(test)]
    pub(crate) fn definition_id(&self) -> DefinitionId {
        self.id
    }

    /// 测试可控入口：下一个将分配的位置编号。
    #[cfg(test)]
    pub(crate) fn next_position_id(&self) -> PositionId {
        PositionId::new(self.next_position)
    }

    fn allocate(
        &mut self,
        owner: BodyId,
        source: PositionSource,
        type_desc: TypeDesc,
    ) -> PositionHandle {
        let position = PositionId::new(self.next_position);
        self.next_position += 1;
        let created_order = self.next_order();
        let handle = PositionHandle {
            definition: self.id,
            position,
        };
        match RefIdAllocator::new(Arc::clone(&self.core_source)).allocate() {
            Ok(core) => self.positions.push(PositionRecord {
                handle,
                core,
                owner,
                source,
                type_desc,
                created_order,
            }),
            // checked 分配失败：记录错误且不登记该位置；后续使用会以未登记位置拒绝，
            // 而构建出口仍返回这条明确诊断。
            Err(_) => self.errors.push(BuildErrorDetail::PositionAllocationFailed),
        }
        handle
    }

    /// 取得一个不与任何位置关联的新 Core RefId；耗尽时记录错误并返回 `None`。
    fn fresh_core(&mut self) -> Option<RefId> {
        match RefIdAllocator::new(Arc::clone(&self.core_source)).allocate() {
            Ok(core) => Some(core),
            Err(_) => {
                self.errors.push(BuildErrorDetail::PositionAllocationFailed);
                None
            }
        }
    }

    fn position(&self, handle: PositionHandle) -> Option<&PositionRecord> {
        if handle.definition != self.id {
            return None;
        }
        self.positions.get(handle.position.index())
    }

    fn is_ancestor_or_self(&self, candidate: BodyId, body: BodyId) -> bool {
        let mut current = Some(body);
        while let Some(id) = current {
            if id == candidate {
                return true;
            }
            current = self.bodies[id.index()].parent;
        }
        false
    }

    fn relation(&self, owner: BodyId, user: BodyId) -> Visibility {
        if self.is_ancestor_or_self(user, owner) {
            Visibility::ChildLocal
        } else {
            Visibility::Sibling
        }
    }

    /// 记录一条跨边界依赖：从使用 Body 逐层向上，直到位置所属 Body（不含）为止。
    fn record_import(&mut self, dependent: BodyId, position: PositionId, owner: BodyId) {
        let mut current = Some(dependent);
        while let Some(id) = current {
            if id == owner {
                break;
            }
            let body = &mut self.bodies[id.index()];
            if !body.imports.contains(&position) {
                body.imports.push(position);
            }
            current = body.parent;
        }
    }

    /// 登记 Node Step：登记真实 callable 句柄、输入位置与输出位置。
    pub(crate) fn register_node_step<N, A, K>(
        &mut self,
        body: BodyId,
        node: N,
        input: A,
    ) -> <N::Output as NodeOutput>::Refs
    where
        N: Callable<A, K> + 'n,
        A: RefShape + ShapeOps,
        A::Spec: InputOps,
        <N::Output as NodeOutput>::Refs: ShapeOps,
        N::Output: OutputOps,
    {
        let step_index = self.bodies[body.index()].steps.len();
        let order = self.next_order();
        let inputs = input.positions();
        for desc in &inputs {
            self.record_position_use(body, *desc);
        }
        let mut outputs = Vec::new();
        for (slot, type_desc) in <<N::Output as NodeOutput>::Refs as ShapeOps>::type_descs()
            .into_iter()
            .enumerate()
        {
            let handle = self.allocate(
                body,
                PositionSource::NodeOutput {
                    step: step_index,
                    slot: slot as u16,
                },
                type_desc,
            );
            outputs.push(handle.position);
        }
        self.bodies[body.index()]
            .steps
            .push(Box::new(NodeStep::<N, A, K> {
                node,
                inputs,
                outputs: outputs.clone(),
                name: std::any::type_name::<N>(),
                order,
                marker: PhantomData,
            }));
        <<N::Output as NodeOutput>::Refs as ShapeOps>::from_positions(&mut outputs.into_iter().map(
            |position| PositionHandle {
                definition: self.id,
                position,
            },
        ))
    }

    /// 同步构建 child Body，登记其声明输出并返回 parent 可见的新 Ref。
    ///
    /// 每个 parent 输出槽分配一个 child 本地唯一声明出口与一个 parent 位置；出口在
    /// 运行时 alias 到该槽来源的实际 target，再由整组 Export 交给 parent。
    pub(crate) fn register_chain_outputs(
        &mut self,
        parent: BodyId,
        child: BodyId,
        slots: Vec<PositionDesc>,
    ) -> Vec<PositionHandle> {
        let order = self.next_order();
        {
            let body = &mut self.bodies[child.index()];
            body.output = slots.clone();
            body.output_order = order;
        }
        let step_index = self.bodies[parent.index()].steps.len();
        let mut exits: Vec<PositionId> = Vec::new();
        let mut bindings = Vec::with_capacity(slots.len());
        let mut chain_slots = Vec::with_capacity(slots.len());
        let mut outputs = Vec::with_capacity(slots.len());
        for (slot, desc) in slots.into_iter().enumerate() {
            let mut source = self.position(desc.handle).map(|record| record.core.clone());
            let owner = self.position(desc.handle).map(|record| record.owner);
            let binding = match (source.as_ref(), owner) {
                (Some(_), Some(owner)) if self.is_ancestor_or_self(owner, child) => {
                    if owner == child {
                        if !exits.contains(&desc.handle.position) {
                            exits.push(desc.handle.position);
                        }
                        ChainBinding::ChildExit(
                            exits
                                .iter()
                                .position(|known| *known == desc.handle.position)
                                .expect("exit position just ensured")
                                as u16,
                        )
                    } else {
                        self.record_import(child, desc.handle.position, owner);
                        ChainBinding::AncestorAlias(desc.handle.position)
                    }
                }
                _ => ChainBinding::Unresolved,
            };
            if source.is_none() {
                // 必然失败的构建（伪造／外来位置）：槽位仍保持对齐，诊断由验证阶段给出。
                source = self.fresh_core();
            }
            // child 本地唯一声明出口：alias 到该槽来源的实际 target。
            let exit = self.allocate(
                child,
                PositionSource::ChainExit {
                    step: step_index,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            let output = self.allocate(
                parent,
                PositionSource::ChainOutput {
                    step: step_index,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            // 双重不变量破坏（伪造位置 + 身份序列耗尽）才可能不满足；构建已失败。
            if let (Some(source), Some(exit), Some(output)) = (
                source,
                self.position(exit).map(|record| record.core.clone()),
                self.position(output).map(|record| record.core.clone()),
            ) {
                chain_slots.push(HandoverSlot {
                    exit,
                    source,
                    output,
                    type_desc: desc.type_desc,
                });
            }
            bindings.push(binding);
            outputs.push(output);
        }
        self.bodies[parent.index()].steps.push(Box::new(ChainStep {
            child,
            exits,
            bindings,
            slots: chain_slots,
            outputs: outputs.iter().map(|handle| handle.position).collect(),
            order,
        }));
        outputs
    }

    /// 记录一个逻辑位置在某 Body 的跨边界依赖（Node 输入与 Choose selector 同一规则）。
    pub(crate) fn record_position_use(&mut self, body: BodyId, desc: PositionDesc) {
        let owner = self.position(desc.handle).map(|record| record.owner);
        if let Some(owner) = owner
            && owner != body
            && self.is_ancestor_or_self(owner, body)
        {
            self.record_import(body, desc.handle.position, owner);
        }
    }

    /// 当前 Body 已登记的 Step 数；Choose 的槽位归属与顺序分配用它。
    pub(crate) fn step_count(&self, body: BodyId) -> usize {
        self.bodies[body.index()].steps.len()
    }

    /// 取得下一个定义顺序值（Choose 父 Step 自身使用）。
    pub(crate) fn fresh_order(&mut self) -> u64 {
        self.next_order()
    }

    /// 记录一条配置级构建错误（Choose 空配置、重复 case／otherwise）。
    pub(crate) fn record_config_error(&mut self, detail: BuildErrorDetail) {
        self.errors.push(detail);
    }

    /// 为 Choose 分配共享的 parent 输出槽：每个逻辑槽位一个固定位置。
    pub(crate) fn allocate_choose_outputs(
        &mut self,
        parent: BodyId,
        step: usize,
        type_descs: &[TypeDesc],
    ) -> Vec<PositionHandle> {
        type_descs
            .iter()
            .enumerate()
            .map(|(slot, type_desc)| {
                self.allocate(
                    parent,
                    PositionSource::ChooseOutput {
                        step,
                        slot: slot as u16,
                    },
                    *type_desc,
                )
            })
            .collect()
    }

    /// 登记一个已构建完成的分支：声明其输出，并按槽位装配分支本地出口。
    ///
    /// 来源规则与 Chain 输出一致：分支自身产生的位置成为该槽来源；祖先位置按
    /// 只读 alias 处理并记录逐层 Import；无法解析的声明（伪造／外来位置）保持槽位
    /// 对齐，诊断由统一验证给出。
    pub(crate) fn register_choose_branch(
        &mut self,
        child: BodyId,
        step: usize,
        branch: u16,
        declared: Vec<PositionDesc>,
        outputs: &[PositionHandle],
    ) -> ChooseBranch {
        let order = self.next_order();
        {
            let body = &mut self.bodies[child.index()];
            body.output = declared.clone();
            body.output_order = order;
        }
        let mut sources: Vec<PositionId> = Vec::new();
        let mut slots = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.into_iter().enumerate() {
            let mut source = self.position(desc.handle).map(|record| record.core.clone());
            let owner = self.position(desc.handle).map(|record| record.owner);
            if let Some(owner) = owner
                && self.is_ancestor_or_self(owner, child)
            {
                if owner == child {
                    if !sources.contains(&desc.handle.position) {
                        sources.push(desc.handle.position);
                    }
                } else {
                    self.record_import(child, desc.handle.position, owner);
                }
            }
            if source.is_none() {
                source = self.fresh_core();
            }
            let exit = self.allocate(
                child,
                PositionSource::ChooseExit {
                    step,
                    branch,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            let output = outputs
                .get(slot)
                .and_then(|handle| self.position(*handle))
                .map(|record| record.core.clone());
            // 槽位数量由共同输出 Shape 保证对齐；身份序列耗尽时构建已失败。
            if let (Some(source), Some(exit), Some(output)) = (
                source,
                self.position(exit).map(|record| record.core.clone()),
                output,
            ) {
                slots.push(HandoverSlot {
                    exit,
                    source,
                    output,
                    type_desc: desc.type_desc,
                });
            }
        }
        ChooseBranch {
            body: child,
            sources,
            slots,
        }
    }

    /// 把构建完成的 Choose Step 登记进 parent Body。
    pub(crate) fn push_choose_step<K>(&mut self, parent: BodyId, step: ChooseStep<K>)
    where
        K: Data + PartialEq + 'static,
    {
        self.bodies[parent.index()].steps.push(Box::new(step));
    }

    /// 登记 Retry Step：attempt Body 的声明输出、预算校验与 parent 输出槽。
    ///
    /// 输出来源规则与 Chain 一致：attempt 自身产生的位置成为该槽来源；祖先位置按只读
    /// alias 处理并记录逐层 Import；无法解析的声明保持槽位对齐，由统一验证给出诊断。
    pub(crate) fn register_retry(
        &mut self,
        parent: BodyId,
        child: BodyId,
        declared: Vec<PositionDesc>,
        max_retries: usize,
    ) -> Vec<PositionHandle> {
        let order = self.next_order();
        let step = self.bodies[parent.index()].steps.len();
        if max_retries == usize::MAX {
            self.errors
                .push(BuildErrorDetail::RetryBudgetUnrepresentable {
                    body: parent,
                    step: Some(step),
                });
        }
        {
            let body = &mut self.bodies[child.index()];
            body.output = declared.clone();
            body.output_order = order;
        }
        let mut slots = Vec::with_capacity(declared.len());
        let mut outputs = Vec::with_capacity(declared.len());
        let mut bridges = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.into_iter().enumerate() {
            let mut source = self.position(desc.handle).map(|record| record.core.clone());
            if let Some(owner) = self.position(desc.handle).map(|record| record.owner)
                && self.is_ancestor_or_self(owner, child)
                && owner != child
            {
                self.record_import(child, desc.handle.position, owner);
            }
            if source.is_none() {
                source = self.fresh_core();
            }
            let exit = self.allocate(
                child,
                PositionSource::RetryExit {
                    step,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            let output = self.allocate(
                parent,
                PositionSource::RetryOutput {
                    step,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            let bridge = self.allocate(
                parent,
                PositionSource::RetryBridge {
                    step,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            if let Some(core) = self.position(bridge).map(|record| record.core.clone()) {
                bridges.push(core);
            }
            if let (Some(source), Some(exit), Some(output)) = (
                source,
                self.position(exit).map(|record| record.core.clone()),
                self.position(output).map(|record| record.core.clone()),
            ) {
                slots.push(HandoverSlot {
                    exit,
                    source,
                    output,
                    type_desc: desc.type_desc,
                });
            }
            outputs.push(output);
        }
        self.bodies[parent.index()].steps.push(Box::new(RetryStep {
            child,
            max_retries,
            slots,
            bridges,
            outputs: outputs.iter().map(|handle| handle.position).collect(),
            order,
        }));
        outputs
    }

    /// 为 Iter Step 分配 formal current 位置：每槽一个固定逻辑身份，owner 是 round Body。
    ///
    /// 每轮经 StateImport 把这些位置绑定到控制器状态当前 target；它们不是 parent 中
    /// 永远绑定初始值的 Ref。owner 取 round Body（而非 parent）是静态来源模型的必需部分：
    /// 通用 `validate_use` 因此只允许 round 自身（及其词法后代）使用 current，parent／
    /// sibling／Root 引用会以 `OutOfScope(ChildLocal)` 在 Definition 阶段被拒。
    pub(crate) fn allocate_iter_current(
        &mut self,
        round: BodyId,
        step: usize,
        type_descs: &[TypeDesc],
    ) -> Vec<PositionHandle> {
        type_descs
            .iter()
            .enumerate()
            .map(|(slot, type_desc)| {
                self.allocate(
                    round,
                    PositionSource::IterCurrent {
                        step,
                        slot: slot as u16,
                    },
                    *type_desc,
                )
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)] // 静态登记需逐项显式给出声明组与逐槽类型化操作
    /// 登记 Iter Step：初始状态使用、预算校验、round 声明输出与 parent 输出槽。
    ///
    /// `initial` 是 parent 内声明的初始状态（作为该 Step 的静态输入参与统一验证）；
    /// `declared` 是 round Body 声明的 next 输出（同 StateShape）；每个 parent 输出槽
    /// 同时得到一个 round 本地最终出口（Break 交付 current 时绑定）。
    pub(crate) fn register_iter(
        &mut self,
        parent: BodyId,
        round: BodyId,
        step: usize,
        initial: Vec<PositionDesc>,
        declared: Vec<PositionDesc>,
        current: Vec<PositionHandle>,
        max_iterations: usize,
        register_states: RegisterStatesFn,
        state_import_slots: StateImportSlotsFn,
    ) -> Vec<PositionHandle> {
        // 定义顺序在 round 闭包之后取得：声明输出必须早于该顺序登记（后向来源）。
        let order = self.next_order();
        if max_iterations == 0 {
            self.errors.push(BuildErrorDetail::IterZeroRounds {
                body: parent,
                step: Some(step),
            });
        }
        for desc in &initial {
            self.record_position_use(parent, *desc);
        }
        {
            let body = &mut self.bodies[round.index()];
            body.output = declared.clone();
            body.output_order = order;
        }
        let mut slots = Vec::with_capacity(declared.len());
        let mut outputs = Vec::with_capacity(declared.len());
        let mut bridges = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.into_iter().enumerate() {
            let mut source = self.position(desc.handle).map(|record| record.core.clone());
            if let Some(owner) = self.position(desc.handle).map(|record| record.owner)
                && self.is_ancestor_or_self(owner, round)
                && owner != round
            {
                self.record_import(round, desc.handle.position, owner);
            }
            if source.is_none() {
                source = self.fresh_core();
            }
            let exit = self.allocate(
                round,
                PositionSource::IterExit {
                    step,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            let output = self.allocate(
                parent,
                PositionSource::IterOutput {
                    step,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            let bridge = self.allocate(
                parent,
                PositionSource::IterBridge {
                    step,
                    slot: slot as u16,
                },
                desc.type_desc,
            );
            if let Some(core) = self.position(bridge).map(|record| record.core.clone()) {
                bridges.push(core);
            }
            if let (Some(source), Some(exit), Some(output)) = (
                source,
                self.position(exit).map(|record| record.core.clone()),
                self.position(output).map(|record| record.core.clone()),
            ) {
                slots.push(HandoverSlot {
                    exit,
                    source,
                    output,
                    type_desc: desc.type_desc,
                });
            }
            outputs.push(output);
        }
        self.bodies[parent.index()].steps.push(Box::new(IterStep {
            child: round,
            max_iterations,
            current: current.iter().map(|handle| handle.position).collect(),
            initial,
            slots,
            bridges,
            outputs: outputs.iter().map(|handle| handle.position).collect(),
            order,
            register_states,
            state_import_slots,
        }));
        outputs
    }

    /// 为 Each Step 分配形式 item 位置：owner 是 item Body，每次 Item 复用同一逻辑位置。
    ///
    /// owner 取 item Body（而非 parent）是静态来源模型的必需部分：通用 `validate_use`
    /// 因此只允许 item Body 自身（及其词法后代）使用它；parent／Root／sibling 引用会以
    /// `OutOfScope(ChildLocal)` 在 Definition 阶段被拒（同 Iter formal current）。
    pub(crate) fn allocate_each_item(
        &mut self,
        item: BodyId,
        step: usize,
        type_desc: TypeDesc,
    ) -> PositionHandle {
        self.allocate(item, PositionSource::EachItem { step, slot: 0 }, type_desc)
    }

    #[allow(clippy::too_many_arguments)] // 静态登记需逐项显式给出声明组与逐槽类型化操作
    /// 登记 Each Step：集合静态输入、item Body 声明输出与逐槽 Each-local／caller 输出位置。
    ///
    /// `collection` 作为该 Step 的静态输入参与统一来源／类型／顺序校验；`declared` 是
    /// item Body 声明的逐槽输出（类型 `A`，成组 Consume 的来源）；`collected_descs` 是对应
    /// 聚合类型 `Vec<A>`，用于给 Each-local 与 caller 输出位置定型。祖先 alias 槽记录逐层
    /// Import（同 Retry／Iter）；无法解析的声明保持槽位对齐，由统一验证给出诊断。
    pub(crate) fn register_each(
        &mut self,
        parent: BodyId,
        item: BodyId,
        step: usize,
        collection: PositionDesc,
        item_position: PositionHandle,
        declared: Vec<PositionDesc>,
        collected_descs: Vec<TypeDesc>,
        begin_collectors: BeginCollectorsFn,
        bind_item: BindItemFn,
        collection_len: CollectionLenFn,
        finish_collectors: FinishCollectorsFn,
    ) -> Vec<PositionHandle> {
        let order = self.next_order();
        self.record_position_use(parent, collection);
        {
            let body = &mut self.bodies[item.index()];
            body.output = declared.clone();
            body.output_order = order;
        }
        let mut slots = Vec::with_capacity(declared.len());
        let mut outputs = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.iter().enumerate() {
            let collected_type = collected_descs.get(slot).copied().unwrap_or(desc.type_desc);
            if let Some(owner) = self.position(desc.handle).map(|record| record.owner)
                && self.is_ancestor_or_self(owner, item)
                && owner != item
            {
                self.record_import(item, desc.handle.position, owner);
            }
            let mut source = self.position(desc.handle).map(|record| record.core.clone());
            if source.is_none() {
                // 必然失败的构建（伪造／外来位置）：槽位仍保持对齐，诊断由验证阶段给出。
                source = self.fresh_core();
            }
            let local = self.allocate(
                parent,
                PositionSource::EachLocal {
                    step,
                    slot: slot as u16,
                },
                collected_type,
            );
            let output = self.allocate(
                parent,
                PositionSource::EachOutput {
                    step,
                    slot: slot as u16,
                },
                collected_type,
            );
            if let (Some(source), Some(local), Some(output)) = (
                source,
                self.position(local).map(|record| record.core.clone()),
                self.position(output).map(|record| record.core.clone()),
            ) {
                slots.push(EachSlot {
                    source,
                    local,
                    output,
                    collected_type,
                });
            }
            outputs.push(output);
        }
        self.bodies[parent.index()].steps.push(Box::new(EachStep {
            item,
            collection,
            item_slot: item_position.position,
            slots,
            outputs: outputs.iter().map(|handle| handle.position).collect(),
            order,
            begin_collectors,
            bind_item,
            collection_len,
            finish_collectors,
        }));
        outputs
    }

    /// 校验一次位置使用：定义身份、登记存在性、可用顺序、词法可见性与类型。
    fn validate_use(
        &self,
        body: BodyId,
        step: Option<usize>,
        desc: PositionDesc,
        order: u64,
    ) -> Option<BuildErrorDetail> {
        let handle = desc.handle;
        if handle.definition != self.id {
            return Some(BuildErrorDetail::ForeignDefinition {
                body,
                step,
                position: handle.position,
                expected: self.id,
                found: handle.definition,
            });
        }
        match self.position(handle) {
            None => Some(BuildErrorDetail::UnknownPosition {
                body,
                step,
                position: handle.position,
            }),
            Some(record) => {
                if record.created_order >= order {
                    return Some(BuildErrorDetail::NotYetAvailable {
                        body,
                        step,
                        position: handle.position,
                    });
                }
                if !self.is_ancestor_or_self(record.owner, body) {
                    return Some(BuildErrorDetail::OutOfScope {
                        body,
                        step,
                        position: handle.position,
                        owner: record.owner,
                        relation: self.relation(record.owner, body),
                    });
                }
                if record.type_desc != desc.type_desc {
                    return Some(BuildErrorDetail::TypeMismatch {
                        body,
                        step,
                        position: handle.position,
                        declared: desc.type_desc,
                        registered: record.type_desc,
                    });
                }
                None
            }
        }
    }

    /// 统一验证整棵树并收口；返回首个错误（按 Body 编号与 Step 顺序确定）。
    fn finish(self, root: BodyId) -> Result<Definition<'n>, BuildErrorDetail> {
        if let Some(first) = self.errors.first() {
            return Err(*first);
        }
        let mut error: Option<BuildErrorDetail> = None;
        let mut root_output: Vec<PositionId> = Vec::new();
        for body in &self.bodies {
            for (index, step) in body.steps.iter().enumerate() {
                for desc in step.inputs() {
                    if let Some(found) =
                        self.validate_use(body.id, Some(index), *desc, step.order())
                    {
                        error = error.or(Some(found));
                    }
                }
            }
            for desc in &body.output {
                if let Some(found) = self.validate_use(body.id, None, *desc, body.output_order) {
                    error = error.or(Some(found));
                }
            }
            if body.id == root {
                for desc in &body.output {
                    if root_output.contains(&desc.handle.position) {
                        error = error.or(Some(BuildErrorDetail::DuplicateRootOutput {
                            position: desc.handle.position,
                        }));
                    } else {
                        root_output.push(desc.handle.position);
                    }
                }
            }
        }
        match error {
            Some(found) => Err(found),
            None => Ok(Definition {
                id: self.id,
                bodies: self.bodies,
                positions: self.positions,
            }),
        }
    }
}

/// 唯一 Definition 构建出口。
///
/// 语义：
/// - `In` 确定 Root 输入的类型 Shape，并按声明顺序登记初始逻辑 Ref（不接收或保存
///   本次 owned 输入）；
/// - 同步执行定义闭包，登记 Root 与全部 Chain 的 Body／Step；
/// - 收集 Root 与各 child Body 的声明输出，规范化 Chain 输出映射；
/// - 对整棵 Definition 验证来源、可见性、顺序与类型，最后返回验证完成的结构。
///
/// 失败返回 [`BuildErrorDetail`]，不返回可供执行消费的半成品；任何验证路径都不调用
/// 业务 Node。根闭包同步消费，不要求 `+ 'n`：被登记句柄的寿命由各 Step 的
/// `N: 'n` 约束保证。
#[allow(private_bounds)]
pub(crate) fn build_definition<'n, In, F, Out>(
    definition: F,
) -> Result<Definition<'n>, BuildErrorDetail>
where
    In: RootInput + InputOps,
    In::Refs: ShapeOps + RefShape,
    Out: RefShape + ShapeOps,
    F: FnOnce(&mut Flow<'n>, In::Refs) -> Out,
{
    let builder = Rc::new(RefCell::new(Builder::<'n>::new()));
    let root = builder.borrow_mut().new_body(None);
    debug_assert_eq!(root, BodyId::ROOT);
    let mut handles = Vec::new();
    {
        let mut state = builder.borrow_mut();
        for (slot, type_desc) in <In as InputOps>::type_descs().into_iter().enumerate() {
            handles.push(state.allocate(
                root,
                PositionSource::RootInput { slot: slot as u16 },
                type_desc,
            ));
        }
    }
    let refs = <In::Refs as ShapeOps>::from_positions(&mut handles.into_iter());
    let out = {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        definition(&mut flow, refs)
    };
    {
        let mut state = builder.borrow_mut();
        let output_order = state.next_order();
        let body = &mut state.bodies[root.index()];
        body.output = ShapeOps::positions(out);
        body.output_order = output_order;
    }
    let state = Rc::try_unwrap(builder)
        .ok()
        .expect("definition closures do not retain the internal Flow")
        .into_inner();
    state.finish(root)
}

#[cfg(test)]
mod each_static_tests {
    //! WE-T05 静态层定向测试：Each 的统一 Step 登记、formal item 来源与输出映射。
    //!
    //! 这些测试只做构建期登记与验证，不执行任何业务 Node。

    use super::*;
    use crate::core::engine::data::Data;
    use crate::core::engine::error::{BodyError, BuildErrorDetail, Visibility};
    use crate::core::engine::flow::Flow;
    use crate::core::engine::shape::{Query, Ref};

    struct Item(u32);
    impl Data for Item {}

    struct Score(u32);
    impl Data for Score {}

    async fn score_of(query: Query<&Item>) -> Result<Score, BodyError> {
        Ok(Score(query.get().0))
    }

    async fn double_score(query: Query<&Score>) -> Result<Score, BodyError> {
        Ok(Score(query.get().0 * 2))
    }

    #[test]
    fn t03_each_is_a_unified_ordered_step_with_declared_slots() {
        let definition = build_definition::<Vec<Item>, _, _>(|flow, items| {
            flow.each(items, |item, each| {
                let score = each.then(score_of, item);
                let doubled = each.then(double_score, score);
                (score, doubled)
            })
        })
        .expect("each with a flat tuple body should build");

        let root = definition.root();
        assert_eq!(root.steps().len(), 1);
        assert_eq!(root.steps()[0].kind(), StepKind::Each);
        assert_eq!(
            root.steps()[0].inputs().len(),
            1,
            "the collection is the single static input"
        );
        assert_eq!(root.steps()[0].outputs().len(), 2, "two collected slots");

        let step = root.steps()[0].each().expect("each step view");
        let item_body = definition.body(step.item());
        assert_eq!(item_body.parent(), Some(root.id()));
        assert_eq!(
            item_body.steps().len(),
            2,
            "the full item body is registered"
        );
        assert_eq!(step.slots().len(), 2);

        // formal item: one logical record, owned by the item body, typed as the element.
        let item_records: Vec<_> = definition
            .positions()
            .iter()
            .filter(|record| matches!(record.source(), PositionSource::EachItem { .. }))
            .collect();
        assert_eq!(item_records.len(), 1);
        assert_eq!(item_records[0].owner(), item_body.id());
        assert_eq!(item_records[0].type_desc(), TypeDesc::of::<Item>());
        assert_eq!(item_records[0].handle().position, step.item_slot());

        // collected slots: independent Vec<Score> positions.
        let outputs: Vec<_> = definition
            .positions()
            .iter()
            .filter(|record| matches!(record.source(), PositionSource::EachOutput { .. }))
            .collect();
        assert_eq!(outputs.len(), 2);
        assert!(
            outputs
                .iter()
                .all(|record| record.type_desc() == TypeDesc::of::<Vec<Score>>())
        );
        assert_ne!(outputs[0].core(), outputs[1].core());
        assert!(
            step.slots()
                .iter()
                .all(|slot| slot.collected_type == TypeDesc::of::<Vec<Score>>())
        );
    }

    #[test]
    fn t11_unit_each_registers_no_collector_slots() {
        let definition = build_definition::<Vec<Item>, _, _>(|flow, items| {
            flow.each(items, |item, each| {
                let _score = each.then(score_of, item);
            })
        })
        .expect("unit each should build");

        let root = definition.root();
        assert_eq!(root.steps()[0].kind(), StepKind::Each);
        assert!(root.steps()[0].outputs().is_empty());
        let step = root.steps()[0].each().expect("each step view");
        assert!(step.slots().is_empty());
        assert!(!definition.positions().iter().any(|record| matches!(
            record.source(),
            PositionSource::EachLocal { .. } | PositionSource::EachOutput { .. }
        )));
    }

    #[test]
    fn t04_formal_item_is_child_local_and_rejected_in_the_parent() {
        // Forge a parent use of the item position: the universal validate_use must reject it
        // with OutOfScope(ChildLocal) before any Node runs.
        let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
        let root = builder.borrow_mut().new_body(None);
        let item_body = builder.borrow_mut().new_body(Some(root));
        let item_handle =
            builder
                .borrow_mut()
                .allocate_each_item(item_body, 0, TypeDesc::of::<Item>());
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            let leaked = Ref::<Item>::from_handle(item_handle);
            let _ = flow.then(score_of, leaked);
        }
        let state = Rc::try_unwrap(builder)
            .ok()
            .expect("definition closure does not retain the internal Flow")
            .into_inner();
        let error = match state.finish(root) {
            Ok(_) => panic!("parent use of the formal item must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            BuildErrorDetail::OutOfScope {
                relation: Visibility::ChildLocal,
                owner,
                ..
            } if owner == item_body
        ));
    }
}
