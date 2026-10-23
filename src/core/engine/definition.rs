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
    EachCollect, EachShape, InputOps, InputSpec, NodeOutput, OutputOps, PositionDesc, Query,
    RefShape, RootInput, ShapeOps, TypeDesc,
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

/// Body 的构建用途：普通 Body 声明 Output，直接 Iter Round 声明 Next。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BodyRole {
    /// 普通 Body（Root、Chain child、Choose 分支、Retry attempt、Each item 等）。
    Ordinary,
    /// 直接 Iter Round；只能声明 Next。
    Round,
}

/// Body 声明种类。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum DeclarationKind {
    /// 普通 Output。
    Output,
    /// Iter Round 的 Next。
    Next,
}

/// 声明状态；未声明与显式 unit 必须可区分。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum DeclarationState {
    /// 尚未尝试声明。
    Undeclared,
    /// 已成功声明零位置（显式 unit）。
    Unit,
    /// 已成功声明至少一个位置。
    NonUnit,
    /// 已尝试但失败，或声明种类与 Body 用途不符。
    Failed,
}

/// 一次 Output/Next 声明的元数据。
///
/// `attempted` 在第一次调用声明入口时置位；`declared` 只在该次声明完整成功时为真。
/// 失败尝试保留 `slots`/`order` 供诊断与防覆盖，但不会成为可执行契约。
#[derive(Clone, Debug, Default)]
pub(crate) struct Declaration {
    /// 是否已经尝试过声明（包括失败尝试）。
    pub(crate) attempted: bool,
    /// 是否已成功完成一次完整声明。
    pub(crate) declared: bool,
    /// 首次尝试的种类；用于重复声明诊断。
    pub(crate) kind: Option<DeclarationKind>,
    /// 声明的逐槽位置与类型；失败尝试也保留已解析/已传入的槽位。
    pub(crate) slots: Vec<PositionDesc>,
    /// 声明时点；用于来源可用性与旧收尾检查。
    pub(crate) order: u64,
    /// 首次声明失败的目标原因；用于旧收尾前保留首因。
    pub(crate) first_failure: Option<BuildErrorDetail>,
    /// 该失败是否已经写入 errors（派生失败可能延后到 finish）。
    pub(crate) failure_recorded: bool,
    /// 成功声明之后是否又记录了该 Body 的声明失败（重复/非法后续声明）。
    ///
    /// 收口时据此隔离该 Body 派生的全部父输出（槽位对齐但保持失效），
    /// 同时保留第一份声明快照与错误首因。
    pub(crate) failed: bool,
    /// 第一次成功声明时捕获的 Each typed 能力（声明时点冻结，重复/失败声明不覆盖）。
    pub(crate) each_bridge: Option<EachBridge>,
}

impl Declaration {
    /// 当前声明状态。
    pub(crate) fn state(&self) -> DeclarationState {
        if !self.attempted {
            DeclarationState::Undeclared
        } else if self.declared && self.slots.is_empty() {
            DeclarationState::Unit
        } else if self.declared {
            DeclarationState::NonUnit
        } else {
            DeclarationState::Failed
        }
    }
}

/// 已登记 Choose 分支契约的轻量视图；由 finish 统一比较各分支 Body 的声明状态。
pub(crate) struct ChooseContract {
    /// Choose Step 所在 Body。
    pub(crate) body: BodyId,
    /// Choose Step 在 parent 中的下标。
    pub(crate) step: usize,
    /// 各分支的 Body（按 case 顺序，最后是 otherwise）。
    pub(crate) branches: Vec<BodyId>,
    /// 预分配/登记的 parent 输出槽类型。
    pub(crate) output_types: Vec<TypeDesc>,
}

/// Choose 一个共享输出槽的**真实交接**只读视图（branch/slot → source/exit/output，含类型）。
///
/// `ChooseStep<K>` 是泛型，无法在对象安全的 [`StepEntry`] 上直接取名；因此以非泛型视图
/// 列出实际登记的 `HandoverSlot`，供结构画像比较真实接线（而非仅声明类型/位置数量）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChooseSlotView {
    /// case 序号（otherwise 为最后一个）。
    pub(crate) branch: u16,
    /// 该分支内的槽序号。
    pub(crate) slot: u16,
    /// 该槽在分支 Body 内的来源位置（真实交接起点）。
    pub(crate) source: RefId,
    /// 分支本地声明出口。
    pub(crate) exit: RefId,
    /// 共享父输出位置。
    pub(crate) output: RefId,
    /// 该槽的类型。
    pub(crate) type_desc: TypeDesc,
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

    /// Retry Step 的静态视图。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    fn retry(&self) -> Option<&RetryStep> {
        None
    }

    /// Iter Step 的静态视图。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    fn iter(&self) -> Option<&IterStep> {
        None
    }

    /// Choose 的真实逐槽交接映射（非 Choose Step 返回空表）。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    fn choose_slots(&self) -> Vec<ChooseSlotView> {
        Vec::new()
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

    fn choose_slots(&self) -> Vec<ChooseSlotView> {
        let mut rows = Vec::new();
        for (index, case) in self.cases.iter().enumerate() {
            for (slot, handover) in case.branch.slots.iter().enumerate() {
                rows.push(ChooseSlotView {
                    branch: index as u16,
                    slot: slot as u16,
                    source: handover.source.clone(),
                    exit: handover.exit.clone(),
                    output: handover.output.clone(),
                    type_desc: handover.type_desc,
                });
            }
        }
        if let Some(otherwise) = &self.otherwise {
            for (slot, handover) in otherwise.slots.iter().enumerate() {
                rows.push(ChooseSlotView {
                    branch: self.cases.len() as u16,
                    slot: slot as u16,
                    source: handover.source.clone(),
                    exit: handover.exit.clone(),
                    output: handover.output.clone(),
                    type_desc: handover.type_desc,
                });
            }
        }
        rows
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

    fn retry(&self) -> Option<&RetryStep> {
        Some(self)
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

    fn iter(&self) -> Option<&IterStep> {
        Some(self)
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

/// 声明时捕获的 Each typed 能力（crate 内部元数据，随第一次成功声明冻结）。
///
/// 只保存静态类型与既有 begin／finish 回调，不保存业务值、Collector 实例或第二份执行
/// 计划；运行期 Collector 仍由 EachStep 与调用上下文建立。能力缺失或与冻结槽位不符时，
/// Each 在 Definition 阶段拒绝，不退化为运行期类型查找，也不建假数据槽。
#[derive(Clone, Debug)]
pub(crate) struct EachBridge {
    /// 声明类型 `R` 的逐槽类型（`Ref<A>` 等），用于与冻结声明逐槽核对。
    declared: Vec<TypeDesc>,
    /// 聚合后每槽的声明类型（`Vec<A>` 等，来自 `<R as EachShape>::Collected`）。
    collected: Vec<TypeDesc>,
    /// 建立 typed 空 collector 的既有回调。
    begin: BeginCollectorsFn,
    /// 全部项成功后 finish 成普通 `Vec<A>` 的既有回调。
    finish: FinishCollectorsFn,
}

impl EachBridge {
    /// 从声明类型 `R` 捕获能力（`output`／`output_with` 在有具体声明类型时调用）。
    #[allow(private_bounds)]
    pub(crate) fn of<R>() -> Self
    where
        R: ShapeOps + EachCollect,
        <R as EachShape>::Collected: ShapeOps,
    {
        Self {
            declared: <R as ShapeOps>::type_descs(),
            collected: <<R as EachShape>::Collected as ShapeOps>::type_descs(),
            begin: <R as EachCollect>::begin_collectors,
            finish: <R as EachCollect>::finish_collectors,
        }
    }
}

/// 某 Body 当前 Output 声明的只读视图（冻结槽位与首次尝试/成功/失败标志）。
#[derive(Clone, Debug)]
pub(crate) struct DeclaredOutput {
    /// 冻结的逐槽位置与类型；失败尝试也保留已解析/已传入的槽位。
    pub(crate) slots: Vec<PositionDesc>,
    /// 声明时点。
    pub(crate) order: u64,
    /// 是否已经尝试过声明（含失败尝试）。
    pub(crate) attempted: bool,
    /// 是否已成功完成一次完整声明。
    pub(crate) declared: bool,
    /// 成功声明之后是否又记录了该 Body 的声明失败。
    pub(crate) failed: bool,
}

impl DeclaredOutput {
    /// 声明状态；与内部 `Declaration::state` 同一判据。
    pub(crate) fn state(&self) -> DeclarationState {
        if !self.attempted {
            DeclarationState::Undeclared
        } else if self.declared && self.slots.is_empty() {
            DeclarationState::Unit
        } else if self.declared {
            DeclarationState::NonUnit
        } else {
            DeclarationState::Failed
        }
    }

    /// 是否可作为合法来源：成功声明且其后未记录声明失败。
    pub(crate) fn usable(&self) -> bool {
        self.declared && !self.failed
    }
}

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
    role: BodyRole,
    steps: Vec<Box<dyn StepEntry + 'n>>,
    output: Declaration,
    next: Declaration,
    round_initial: Option<Vec<TypeDesc>>,
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

    /// 该 Body 的构建用途。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn role(&self) -> BodyRole {
        self.role
    }

    /// 该 Body 声明的输出槽位视图；保留既有槽序与语义（空表示零输出）。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn output(&self) -> &[PositionDesc] {
        &self.output.slots
    }

    /// Output 声明的完整元数据。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn output_declaration(&self) -> &Declaration {
        &self.output
    }

    /// Next 声明的完整元数据。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn next_declaration(&self) -> &Declaration {
        &self.next
    }

    /// Round 的初始状态 Shape（仅直接 Round Body 有值）。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn round_initial(&self) -> Option<&[TypeDesc]> {
        self.round_initial.as_deref()
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
    /// 发布到父域的时点；`None` 表示已分配但尚未发布（例如 Choose 预分配输出）。
    published_order: Option<u64>,
    /// 生产登记是否因失败依赖而失效。
    tainted: bool,
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

    /// 发布时点；`None` 表示尚未发布。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn published_order(&self) -> Option<u64> {
        self.published_order
    }

    /// 生产登记是否已失效。
    #[allow(dead_code)] // 由定向测试与结构检查使用
    pub(crate) fn is_tainted(&self) -> bool {
        self.tainted
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
            .slots
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
    /// 失败查询返回的哨兵身份；与当前 Definition 不同，不能被后续位置注册“复活”。
    #[allow(dead_code)] // 由内部查询/下游 T02/T03 使用
    missing_definition: DefinitionId,
    core_source: Arc<RefIdSource>,
    next_position: u32,
    order_cursor: u64,
    bodies: Vec<Body<'n>>,
    positions: Vec<PositionRecord>,
    errors: Vec<BuildErrorDetail>,
    /// Root 期望输出 Shape；`None` 表示本次构建不核对（仅内部测试直接构造 Builder 时）。
    root_expectation: Option<Vec<TypeDesc>>,
    /// Choose 分支契约的轻量视图；由 finish 统一比较。
    choose_contracts: Vec<ChooseContract>,
}

impl<'n> Builder<'n> {
    pub(crate) fn new() -> Self {
        Self {
            id: DefinitionId::fresh(),
            missing_definition: DefinitionId::fresh(),
            core_source: RefIdSource::new(),
            next_position: 0,
            order_cursor: 0,
            bodies: Vec::new(),
            positions: Vec::new(),
            errors: Vec::new(),
            root_expectation: None,
            choose_contracts: Vec::new(),
        }
    }

    pub(crate) fn new_body(&mut self, parent: Option<BodyId>, role: BodyRole) -> BodyId {
        let id = BodyId::new(self.bodies.len() as u32);
        self.bodies.push(Body {
            id,
            parent,
            role,
            steps: Vec::new(),
            output: Declaration::default(),
            next: Declaration::default(),
            round_initial: None,
            imports: Vec::new(),
        });
        id
    }

    /// 当前定义顺序游标；查询与声明读取它，但不消耗。
    #[allow(dead_code)] // 由内部查询/下游 T02/T03 使用
    pub(crate) fn current_order(&self) -> u64 {
        self.order_cursor
    }

    /// 失败查询的哨兵句柄；没有当前 Definition 的合法位置身份。
    #[allow(dead_code)] // 由内部查询/下游 T02/T03 使用
    /// 本次 Definition 内“未登记位置”的替代句柄：身份属于当前 Definition，但下标不会被分配。
    ///
    /// 与 [`Builder::missing_handle`]（跨 Definition 哨兵）不同，声明这样的句柄只会得到
    /// “未登记位置”这一派生拒绝，不会抢先盖掉更早的真实首因。
    pub(crate) fn unresolved_handle(&self) -> PositionHandle {
        PositionHandle {
            definition: self.id,
            position: PositionId::new(u32::MAX),
        }
    }

    pub(crate) fn missing_handle(&self) -> PositionHandle {
        PositionHandle {
            definition: self.missing_definition,
            position: PositionId::new(0),
        }
    }

    /// 设置直接 Round 的初始状态 Shape；由 Flow::iter 在构建 round 前调用。
    pub(crate) fn set_round_initial(&mut self, round: BodyId, types: Vec<TypeDesc>) {
        self.bodies[round.index()].round_initial = Some(types);
    }

    /// 设置 Root 期望输出 Shape；由 Root 构建前端（`build_definition`）与定向测试使用。
    pub(crate) fn set_root_expectation(&mut self, expected: Vec<TypeDesc>) {
        self.root_expectation = Some(expected);
    }

    /// 把位置标记为因失败依赖而失效。
    pub(crate) fn mark_tainted(&mut self, handle: PositionHandle) {
        if let Some(record) = self
            .positions
            .get_mut(handle.position.index())
            .filter(|record| record.handle.definition == handle.definition)
        {
            record.tainted = true;
        }
    }

    /// 把位置发布到父域；使用实际完成登记的时点。
    pub(crate) fn mark_published(&mut self, handle: PositionHandle, order: u64) {
        if let Some(record) = self
            .positions
            .get_mut(handle.position.index())
            .filter(|record| record.handle.definition == handle.definition)
        {
            record.published_order = Some(order);
        }
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

    pub(crate) fn allocate(
        &mut self,
        owner: BodyId,
        source: PositionSource,
        type_desc: TypeDesc,
    ) -> PositionHandle {
        self.allocate_with_publication(owner, source, type_desc, true)
    }

    /// 按编号取得构建中的 Body；定向测试与 finish 共用。
    #[allow(dead_code)] // 由内部测试/下游构建支撑使用
    pub(crate) fn body(&self, id: BodyId) -> &Body<'n> {
        &self.bodies[id.index()]
    }

    /// 已记录的构建错误（按插入顺序）；定向测试用于断言目标路径自身拒绝。
    #[allow(dead_code)] // 由内部测试/下游诊断使用
    pub(crate) fn errors(&self) -> &[BuildErrorDetail] {
        &self.errors
    }

    /// 分配位置；`publish` 为假时保持“已分配但未发布”，供 Choose 父输出预分配使用。
    fn allocate_with_publication(
        &mut self,
        owner: BodyId,
        source: PositionSource,
        type_desc: TypeDesc,
        publish: bool,
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
                published_order: publish.then_some(created_order),
                tainted: false,
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

    pub(crate) fn position(&self, handle: PositionHandle) -> Option<&PositionRecord> {
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

    /// 来源类别是否允许作为业务 Ref。
    fn is_business_source(source: PositionSource) -> bool {
        matches!(
            source,
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
    }

    /// 统一可用性判定：身份、存在、创建/发布时点、失效、类别、可见性与类型。
    ///
    /// resolver、validate_use 与全部 register_* 业务来源入口共用本函数；时间比较均为
    /// 严格早于使用点，后续发布会改变“当前是否可用”，但不会追认较早使用合法。
    pub(crate) fn availability_error(
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
        let record = match self.position(handle) {
            Some(record) => record,
            None => {
                // 当前 Definition 尚未分配的位置是“未来来源”；已越过分配游标却
                // 没有记录才是真正的未知位置（例如 checked 分配失败）。
                return Some(if handle.position.index() == self.next_position as usize {
                    BuildErrorDetail::NotYetAvailable {
                        body,
                        step,
                        position: handle.position,
                    }
                } else {
                    BuildErrorDetail::UnknownPosition {
                        body,
                        step,
                        position: handle.position,
                    }
                });
            }
        };
        if record.created_order >= order {
            return Some(BuildErrorDetail::NotYetAvailable {
                body,
                step,
                position: handle.position,
            });
        }
        if record.tainted {
            return Some(BuildErrorDetail::TaintedSource {
                body,
                step,
                position: handle.position,
            });
        }
        if !matches!(record.published_order, Some(published) if published < order) {
            return Some(BuildErrorDetail::NotPublished {
                body,
                step,
                position: handle.position,
            });
        }
        if !Self::is_business_source(record.source) {
            return Some(BuildErrorDetail::NotBusinessSource {
                body,
                step,
                position: handle.position,
                source: record.source,
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

    /// 未登记位置在前向使用时延后到 finish 分类；登记后仍不存在才判 Unknown。
    fn defer_unregistered(error: BuildErrorDetail, unregistered: bool) -> bool {
        unregistered
            && matches!(
                error,
                BuildErrorDetail::UnknownPosition { .. } | BuildErrorDetail::NotYetAvailable { .. }
            )
    }

    /// 记录一次真实使用：先通过统一判定，再形成逐层 Import。
    ///
    /// 失败时记录并返回目标错误；调用方据此隔离输出，不得继续把失败依赖当作正常来源。
    /// 尚未登记的未来位置不在使用点立即定案，避免把“稍后分配的合法位置”误报成
    /// Unknown；finish 会在全部登记完成后按实际时点给出 NotYetAvailable/Unknown。
    pub(crate) fn record_use_at(
        &mut self,
        body: BodyId,
        step: Option<usize>,
        desc: PositionDesc,
        order: u64,
    ) -> Option<BuildErrorDetail> {
        if let Some(error) = self.availability_error(body, step, desc, order) {
            let unregistered = self.position(desc.handle).is_none();
            if !Self::defer_unregistered(error, unregistered) {
                self.errors.push(error);
            }
            return Some(error);
        }
        let owner = self.position(desc.handle).map(|record| record.owner);
        if let Some(owner) = owner
            && owner != body
        {
            self.record_import(body, desc.handle.position, owner);
        }
        None
    }

    /// 在当前位置解析同类型最近合法业务来源；失败记录 `MissingSource`。
    #[allow(dead_code)] // 由内部 ref_of/自动声明/下游 T02/T03 使用
    pub(crate) fn resolve(
        &mut self,
        body: BodyId,
        type_desc: TypeDesc,
        slot: Option<u16>,
        order: u64,
    ) -> Option<PositionDesc> {
        let found = self
            .positions
            .iter()
            .rev()
            .find(|record| {
                record.type_desc == type_desc
                    && self
                        .availability_error(
                            body,
                            None,
                            PositionDesc {
                                handle: record.handle,
                                type_desc,
                            },
                            order,
                        )
                        .is_none()
            })
            .map(|record| record.handle);
        match found {
            Some(handle) => Some(PositionDesc { handle, type_desc }),
            None => {
                self.errors.push(BuildErrorDetail::MissingSource {
                    body,
                    slot,
                    type_desc,
                });
                None
            }
        }
    }

    /// 登记一次 Output/Next 声明；返回是否完整成功。
    ///
    /// 第一次尝试即占用该 Body 的声明位；失败尝试保留槽位与时点，但后续声明/旧收尾
    /// 不得覆盖或清除。`force_failed` 用于调用方已经记录缺失来源的自动声明。
    pub(crate) fn declare(
        &mut self,
        body: BodyId,
        kind: DeclarationKind,
        slots: Vec<PositionDesc>,
        order: u64,
        force_failed: bool,
    ) -> bool {
        let (attempted, first_kind, role, first_failure, failure_recorded) = {
            let existing = &self.bodies[body.index()];
            let declaration = if existing.output.attempted {
                &existing.output
            } else {
                &existing.next
            };
            (
                existing.output.attempted || existing.next.attempted,
                existing.output.kind.or(existing.next.kind).unwrap_or(kind),
                existing.role,
                declaration.first_failure,
                declaration.failure_recorded,
            )
        };
        if attempted {
            // 首次声明可能因派生失败（taint/前向）未立即写入 errors；旧收尾拒绝时
            // 仍要先保留首因，不能只留下 DuplicateDeclaration。
            if let Some(failure) = first_failure
                && !failure_recorded
            {
                self.errors.push(failure);
                let existing = &mut self.bodies[body.index()];
                let declaration = if existing.output.attempted {
                    &mut existing.output
                } else {
                    &mut existing.next
                };
                declaration.failure_recorded = true;
            }
            self.errors.push(BuildErrorDetail::DuplicateDeclaration {
                body,
                first: first_kind,
                second: kind,
            });
            // 保留第一份声明快照，但把该 Body 标记为声明已失败：收口不得据此发布合法来源。
            let existing = &mut self.bodies[body.index()];
            let declaration = if existing.output.attempted {
                &mut existing.output
            } else {
                &mut existing.next
            };
            declaration.failed = true;
            return false;
        }
        if (role == BodyRole::Ordinary && kind == DeclarationKind::Next)
            || (role == BodyRole::Round && kind == DeclarationKind::Output)
        {
            let error = BuildErrorDetail::DeclarationNotAllowed { body, kind, role };
            self.errors.push(error);
            let target = match kind {
                DeclarationKind::Output => &mut self.bodies[body.index()].output,
                DeclarationKind::Next => &mut self.bodies[body.index()].next,
            };
            target.attempted = true;
            target.declared = false;
            target.kind = Some(kind);
            target.slots = slots;
            target.order = order;
            target.first_failure = Some(error);
            target.failure_recorded = true;
            return false;
        }

        let mut ok = !force_failed;
        // 自动声明已经由 resolver 记录 MissingSource；视为失败已记录。
        let mut failure_recorded = force_failed;
        let mut first_failure = None;
        if !force_failed {
            for desc in &slots {
                if let Some(error) = self.availability_error(body, None, *desc, order) {
                    if first_failure.is_none() {
                        first_failure = Some(error);
                    }
                    let unregistered = self.position(desc.handle).is_none();
                    // tainted 是上游失败派生出的后果；等 finish 先检查更早的输入/来源。
                    // NotPublished 是本次声明自身选择了未发布位置，属于首因，必须保留。
                    let derived = matches!(error, BuildErrorDetail::TaintedSource { .. });
                    if !derived && !Self::defer_unregistered(error, unregistered) {
                        self.errors.push(error);
                        failure_recorded = true;
                    }
                    ok = false;
                }
            }
        }
        if kind == DeclarationKind::Next && !force_failed {
            if slots.is_empty() {
                let error = BuildErrorDetail::NextUnitUnsupported { body };
                self.errors.push(error);
                first_failure.get_or_insert(error);
                failure_recorded = true;
                ok = false;
            } else if let Some(expected) = self.bodies[body.index()].round_initial.clone() {
                if expected.len() != slots.len() {
                    let error = BuildErrorDetail::DeclarationArityMismatch {
                        body,
                        kind,
                        slot: 0,
                        expected: expected.len() as u16,
                        found: slots.len() as u16,
                    };
                    self.errors.push(error);
                    first_failure.get_or_insert(error);
                    failure_recorded = true;
                    ok = false;
                } else {
                    for (slot, (expected, desc)) in expected.iter().zip(slots.iter()).enumerate() {
                        if *expected != desc.type_desc {
                            let error = BuildErrorDetail::DeclarationShapeMismatch {
                                body,
                                kind,
                                slot: slot as u16,
                                expected: *expected,
                                found: Some(desc.type_desc),
                            };
                            self.errors.push(error);
                            first_failure.get_or_insert(error);
                            failure_recorded = true;
                            ok = false;
                        }
                    }
                }
            }
        }
        if ok {
            for desc in &slots {
                let owner = self.position(desc.handle).map(|record| record.owner);
                if let Some(owner) = owner
                    && owner != body
                {
                    self.record_import(body, desc.handle.position, owner);
                }
            }
        }
        let target = match kind {
            DeclarationKind::Output => &mut self.bodies[body.index()].output,
            DeclarationKind::Next => &mut self.bodies[body.index()].next,
        };
        target.attempted = true;
        target.declared = ok;
        target.kind = Some(kind);
        target.slots = slots;
        target.order = order;
        target.first_failure = first_failure;
        target.failure_recorded = failure_recorded;
        ok
    }

    /// 声明普通 Output（低层直接登记；公开路径统一经 `Flow::output`／`output_with`，
    /// 后者在成功声明时同时捕获 typed 能力）。
    #[allow(dead_code)] // 由低层 Builder 定向测试直接使用
    pub(crate) fn declare_output(
        &mut self,
        body: BodyId,
        slots: Vec<PositionDesc>,
        order: u64,
    ) -> bool {
        self.declare(body, DeclarationKind::Output, slots, order, false)
    }

    /// 声明 Iter Round 的 Next。
    pub(crate) fn declare_next(
        &mut self,
        body: BodyId,
        slots: Vec<PositionDesc>,
        order: u64,
    ) -> bool {
        self.declare(body, DeclarationKind::Next, slots, order, false)
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

    /// 自动 Query 的完整输入 Shape 重复类型检查；含重复即记录首对并返回 `false`。
    ///
    /// 只适用于自动 Node 输入：显式连接与 output/next 声明不套用该规则。检查完整 Shape
    /// （非相邻槽同样命中），先于逐槽来源查询，使重复类型成为可辨识首因。
    pub(crate) fn check_auto_query_shape(&mut self, body: BodyId, types: &[TypeDesc]) -> bool {
        for (slot, type_desc) in types.iter().enumerate() {
            if let Some(first) = types[..slot]
                .iter()
                .position(|earlier| earlier == type_desc)
            {
                self.errors.push(BuildErrorDetail::DuplicateQuery {
                    body,
                    type_desc: *type_desc,
                    first_slot: first as u16,
                    second_slot: slot as u16,
                });
                return false;
            }
        }
        true
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
        let mut inputs_ok = true;
        for desc in &inputs {
            if self
                .record_use_at(body, Some(step_index), *desc, order)
                .is_some()
            {
                inputs_ok = false;
            }
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
            if !inputs_ok {
                self.mark_tainted(handle);
            }
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

    /// 读取 child 已有的 Output 声明：返回（冻结槽位、是否可作为合法来源、声明时点）。
    ///
    /// 不补声明、不改声明 order、不重新 resolve；`expected` 为 `Some` 时在此核对期望 Shape，
    /// 不符记录 Definition 诊断并使收口失败。child 自身声明已失败（首因已记录）时不叠加
    /// 二次 MissingDeclaration／Shape 诊断。
    fn consume_child_output(
        &mut self,
        child: BodyId,
        expected: Option<&[TypeDesc]>,
    ) -> (Vec<PositionDesc>, bool, u64) {
        let declaration = self.bodies[child.index()].output.clone();
        let mut ok = declaration.declared && !declaration.failed;
        if let Some(expected) = expected {
            if declaration.state() == DeclarationState::Failed {
                ok = false;
            } else if let Some(error) = self.validate_output_expectation(child, expected) {
                self.errors.push(error);
                ok = false;
            }
        }
        (declaration.slots, ok, declaration.order)
    }

    /// 消费 child 已有的 Output 声明并登记 Chain Step（不补声明、不改声明 order）。
    ///
    /// 每个 parent 输出槽分配一个 child 本地唯一声明出口与一个 parent 位置；出口在
    /// 运行时 alias 到该槽来源的实际 target，再由整组 Export 交给 parent。
    /// 返回（parent 输出槽，收口形状是否成立）。
    pub(crate) fn register_chain_outputs(
        &mut self,
        parent: BodyId,
        child: BodyId,
        expected: Option<&[TypeDesc]>,
    ) -> (Vec<PositionHandle>, bool) {
        let (slots, declared, declare_order) = self.consume_child_output(child, expected);
        let order = self.next_order();
        let step_index = self.bodies[parent.index()].steps.len();
        let mut exits: Vec<PositionId> = Vec::new();
        let mut bindings = Vec::with_capacity(slots.len());
        let mut chain_slots = Vec::with_capacity(slots.len());
        let mut outputs = Vec::with_capacity(slots.len());
        for (slot, desc) in slots.into_iter().enumerate() {
            let source_ok = declared
                && self
                    .availability_error(child, None, desc, declare_order)
                    .is_none();
            let owner = self.position(desc.handle).map(|record| record.owner);
            let binding = if source_ok {
                match owner {
                    Some(owner) if owner == child => {
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
                    }
                    Some(owner) if self.is_ancestor_or_self(owner, child) => {
                        ChainBinding::AncestorAlias(desc.handle.position)
                    }
                    _ => ChainBinding::Unresolved,
                }
            } else {
                ChainBinding::Unresolved
            };
            let mut source = if source_ok {
                self.position(desc.handle).map(|record| record.core.clone())
            } else {
                None
            };
            if source.is_none() {
                // 必然失败的构建：保持槽位对齐；该输出保持失效，不作为正常来源发布。
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
            let exit_core = self.position(exit).map(|record| record.core.clone());
            let output_core = self.position(output).map(|record| record.core.clone());
            let slot_ok = source_ok && source.is_some() && exit_core.is_some();
            if !slot_ok {
                self.mark_tainted(output);
            }
            if let (true, Some(source), Some(exit), Some(output)) =
                (slot_ok, source, exit_core, output_core)
            {
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
        (outputs, declared)
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

    /// 读取某 Body 已冻结的 Output 声明（不补声明、不改时点）。
    pub(crate) fn output_declaration_of(&self, body: BodyId) -> DeclaredOutput {
        let declaration = &self.bodies[body.index()].output;
        DeclaredOutput {
            slots: declaration.slots.clone(),
            order: declaration.order,
            attempted: declaration.attempted,
            declared: declaration.declared,
            failed: declaration.failed,
        }
    }

    /// 读取某 Body 第一次成功声明时捕获的 Each typed 能力（失败/重复声明不产生能力）。
    #[allow(dead_code)] // 供低层定向测试读取 Body 首次冻结的 typed 能力
    pub(crate) fn declared_each_capability(&self, body: BodyId) -> Option<EachBridge> {
        let declaration = &self.bodies[body.index()].output;
        if declaration.declared && !declaration.failed {
            declaration.each_bridge.clone()
        } else {
            None
        }
    }

    /// 记录该 Body 第一次成功声明时捕获的 Each typed 能力。
    ///
    /// 只在成功且未失败的首份声明上生效：重复/失败声明不能覆盖或补写能力。
    pub(crate) fn attach_each_bridge(&mut self, body: BodyId, bridge: EachBridge) {
        let declaration = &mut self.bodies[body.index()].output;
        if declaration.declared && !declaration.failed && declaration.each_bridge.is_none() {
            declaration.each_bridge = Some(bridge);
        }
    }

    /// 为 Choose 分配共享的 parent 输出槽：每个逻辑槽位一个固定位置。
    ///
    /// 这些位置在分支构建期间保持“未发布”；只有 `push_choose_step` 实际完成登记后
    /// 才按新的发布时点公开，避免分支读到自己尚未成立的父输出。
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
                self.allocate_with_publication(
                    parent,
                    PositionSource::ChooseOutput {
                        step,
                        slot: slot as u16,
                    },
                    *type_desc,
                    false,
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
        outputs: &[PositionHandle],
    ) -> ChooseBranch {
        let declaration = self.output_declaration_of(child);
        let order = declaration.order;
        let declared_ok = declaration.usable();
        let declared = declaration.slots;
        let mut sources: Vec<PositionId> = Vec::new();
        let mut slots = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.into_iter().enumerate() {
            let source_ok =
                declared_ok && self.availability_error(child, None, desc, order).is_none();
            let owner = self.position(desc.handle).map(|record| record.owner);
            if source_ok
                && let Some(owner) = owner
                && owner == child
                && !sources.contains(&desc.handle.position)
            {
                sources.push(desc.handle.position);
            }
            let mut source = if source_ok {
                self.position(desc.handle).map(|record| record.core.clone())
            } else {
                None
            };
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
            let output_core = outputs
                .get(slot)
                .and_then(|handle| self.position(*handle))
                .map(|record| record.core.clone());
            let exit_core = self.position(exit).map(|record| record.core.clone());
            let slot_ok = source_ok && source.is_some() && exit_core.is_some();
            if !slot_ok && let Some(handle) = outputs.get(slot) {
                self.mark_tainted(*handle);
            }
            // 槽位数量由共同输出 Shape 保证对齐；身份序列耗尽时构建已失败。
            if let (true, Some(source), Some(exit), Some(output)) =
                (slot_ok, source, exit_core, output_core)
            {
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

    /// 把构建完成的 Choose Step 登记进 parent Body，并在实际 push 时发布父输出。
    ///
    /// 发布时点必须晚于所有分支构建；`ChooseStep.order` 只是 selector／步骤使用点，
    /// 不能复用为输出发布时间。共享输出只有在 selector、配置、全部分支契约与调用方
    /// `O` 期望都合法时才发布；否则全部保持失效（`caller_ok` 由调用方按分支契约与
    /// 期望核对给出，这里再做一次统一契约校验）。`finish` 仍会重复契约校验并给出诊断。
    pub(crate) fn push_choose_step<K>(
        &mut self,
        parent: BodyId,
        step: ChooseStep<K>,
        caller_ok: bool,
    ) -> bool
    where
        K: Data + PartialEq + 'static,
    {
        let publish_order = self.next_order();
        let mut branches =
            Vec::with_capacity(step.cases.len() + usize::from(step.otherwise.is_some()));
        let mut output_types = Vec::with_capacity(step.outputs.len());
        let mut publish = caller_ok;
        for position in &step.outputs {
            let record = self.position(PositionHandle {
                definition: self.id,
                position: *position,
            });
            if let Some(record) = record {
                output_types.push(record.type_desc);
                if record.tainted {
                    publish = false;
                }
            } else {
                publish = false;
            }
        }
        for case in &step.cases {
            branches.push(case.branch.body);
        }
        if let Some(otherwise) = &step.otherwise {
            branches.push(otherwise.body);
        }
        let contract = ChooseContract {
            body: parent,
            step: self.bodies[parent.index()].steps.len(),
            branches,
            output_types,
        };
        if self.validate_choose_contract(&contract).is_some() {
            publish = false;
        }
        self.choose_contracts.push(contract);
        if publish {
            for position in &step.outputs {
                self.mark_published(
                    PositionHandle {
                        definition: self.id,
                        position: *position,
                    },
                    publish_order,
                );
            }
        }
        self.bodies[parent.index()].steps.push(Box::new(step));
        publish
    }

    /// 消费 attempt Body 已有的 Output 声明并登记 Retry Step（不补声明、不改声明 order）。
    ///
    /// 输出来源规则与 Chain 一致：attempt 自身产生的位置成为该槽来源；祖先位置按只读
    /// alias 处理并记录逐层 Import；无法解析的声明保持槽位对齐，由统一验证给出诊断。
    /// 返回（parent 输出槽，收口形状是否成立）。
    pub(crate) fn register_retry(
        &mut self,
        parent: BodyId,
        child: BodyId,
        max_retries: usize,
        expected: Option<&[TypeDesc]>,
    ) -> (Vec<PositionHandle>, bool) {
        let order = self.next_order();
        let step = self.bodies[parent.index()].steps.len();
        if max_retries == usize::MAX {
            self.errors
                .push(BuildErrorDetail::RetryBudgetUnrepresentable {
                    body: parent,
                    step: Some(step),
                });
        }
        let (declared, declared_ok, declare_order) = self.consume_child_output(child, expected);
        let mut slots = Vec::with_capacity(declared.len());
        let mut outputs = Vec::with_capacity(declared.len());
        let mut bridges = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.into_iter().enumerate() {
            let source_ok = declared_ok
                && self
                    .availability_error(child, None, desc, declare_order)
                    .is_none();
            let mut source = if source_ok {
                self.position(desc.handle).map(|record| record.core.clone())
            } else {
                None
            };
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
            let exit_core = self.position(exit).map(|record| record.core.clone());
            let output_core = self.position(output).map(|record| record.core.clone());
            let slot_ok = source_ok && source.is_some() && exit_core.is_some();
            if !slot_ok {
                self.mark_tainted(output);
            }
            if let (true, Some(source), Some(exit), Some(output)) =
                (slot_ok, source, exit_core, output_core)
            {
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
        (outputs, declared_ok)
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
        let next_ok = self.declare_next(round, declared.clone(), order);
        let mut initial_ok = true;
        for desc in &initial {
            if self
                .record_use_at(parent, Some(step), *desc, order)
                .is_some()
            {
                initial_ok = false;
            }
        }
        let mut shape_ok = initial.len() == declared.len();
        if shape_ok {
            for (slot, (expected, declared)) in initial.iter().zip(declared.iter()).enumerate() {
                if expected.type_desc != declared.type_desc {
                    shape_ok = false;
                    self.errors
                        .push(BuildErrorDetail::DeclarationShapeMismatch {
                            body: round,
                            kind: DeclarationKind::Next,
                            slot: slot as u16,
                            expected: expected.type_desc,
                            found: Some(declared.type_desc),
                        });
                }
            }
        } else {
            self.errors
                .push(BuildErrorDetail::DeclarationArityMismatch {
                    body: round,
                    kind: DeclarationKind::Next,
                    slot: 0,
                    expected: initial.len() as u16,
                    found: declared.len() as u16,
                });
        }
        let mut slots = Vec::with_capacity(declared.len());
        let mut outputs = Vec::with_capacity(declared.len());
        let mut bridges = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.into_iter().enumerate() {
            let source_ok = next_ok
                && initial_ok
                && shape_ok
                && self.availability_error(round, None, desc, order).is_none();
            let mut source = if source_ok {
                self.position(desc.handle).map(|record| record.core.clone())
            } else {
                None
            };
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
            let exit_core = self.position(exit).map(|record| record.core.clone());
            let output_core = self.position(output).map(|record| record.core.clone());
            let slot_ok = source_ok && source.is_some() && exit_core.is_some();
            if !slot_ok {
                self.mark_tainted(output);
            }
            if let (true, Some(source), Some(exit), Some(output)) =
                (slot_ok, source, exit_core, output_core)
            {
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
    /// 消费 item Body 已有的 Output 声明与声明时捕获的 typed 能力，登记 Each Step。
    ///
    /// 不补声明、不改声明时点。聚合槽类型来自声明元数据：非空声明缺失 typed 能力时拒绝；
    /// 调用方 `O` 期望与**聚合后**类型比较（先声明状态，再 arity，再逐槽类型）。返回
    /// （caller 可见聚合输出位置，收口是否成立）。
    pub(crate) fn register_each(
        &mut self,
        parent: BodyId,
        item: BodyId,
        step: usize,
        collection: PositionDesc,
        item_position: PositionHandle,
        expected: Option<&[TypeDesc]>,
        bind_item: BindItemFn,
        collection_len: CollectionLenFn,
    ) -> (Vec<PositionHandle>, bool) {
        let declaration = self.output_declaration_of(item);
        let declared_ok = declaration.usable();
        // typed 能力只取该 Body **首次成功声明时冻结**的那一份：调用方不能另行传入一份
        // 与被冻结声明/回调不一致的能力（声明类型、聚合描述与 typed 回调不可拆分）。
        let bridge = self.bodies[item.index()].output.each_bridge.clone();
        let collected_descs: Vec<TypeDesc> = bridge
            .as_ref()
            .map(|bridge| bridge.collected.clone())
            .unwrap_or_default();
        let mut capability_ok = true;
        if declared_ok && bridge.is_none() && !declaration.slots.is_empty() {
            // 声明了非空输出却没有声明时冻结的 typed 能力：不得按 item 槽类型或运行期
            // 类型查找凑出聚合，必须在 Definition 阶段拒绝。
            self.errors
                .push(BuildErrorDetail::MissingEachBridge { body: item });
            capability_ok = false;
        }
        if declared_ok && let Some(bridge) = &bridge {
            // typed 能力必须确属这份冻结声明：槽数、逐槽声明类型与聚合槽数都要一致，
            // 缺失的 collected 槽不得回填 item 槽类型，也不得发布为合法来源。
            let mut bridge_error = None;
            if bridge.declared.len() != declaration.slots.len() {
                bridge_error = Some(BuildErrorDetail::DeclarationArityMismatch {
                    body: item,
                    kind: DeclarationKind::Output,
                    slot: 0,
                    expected: declaration.slots.len() as u16,
                    found: bridge.declared.len() as u16,
                });
            } else if let Some((slot, (declared_type, desc))) = bridge
                .declared
                .iter()
                .zip(declaration.slots.iter())
                .enumerate()
                .find(|(_, (declared_type, desc))| **declared_type != desc.type_desc)
            {
                bridge_error = Some(BuildErrorDetail::DeclarationShapeMismatch {
                    body: item,
                    kind: DeclarationKind::Output,
                    slot: slot as u16,
                    expected: desc.type_desc,
                    found: Some(*declared_type),
                });
            } else if bridge.collected.len() != declaration.slots.len() {
                bridge_error = Some(BuildErrorDetail::DeclarationArityMismatch {
                    body: item,
                    kind: DeclarationKind::Output,
                    slot: 0,
                    expected: declaration.slots.len() as u16,
                    found: bridge.collected.len() as u16,
                });
            }
            if let Some(error) = bridge_error {
                self.errors.push(error);
                capability_ok = false;
            }
        }
        let mut expectation_ok = true;
        if capability_ok
            && declaration.state() != DeclarationState::Failed
            && let Some(expected) = expected
            && let Some(error) = self.validate_declaration_expectation(
                item,
                DeclarationKind::Output,
                declaration.attempted,
                declaration.declared,
                &collected_descs,
                expected,
            )
        {
            self.errors.push(error);
            expectation_ok = false;
        }
        let order = self.next_order();
        let collection_ok = self
            .record_use_at(parent, Some(step), collection, order)
            .is_none();
        let declared = declaration.slots;
        let mut slots = Vec::with_capacity(declared.len());
        let mut outputs = Vec::with_capacity(declared.len());
        for (slot, desc) in declared.iter().enumerate() {
            let collected_type = collected_descs.get(slot).copied().unwrap_or(desc.type_desc);
            let source_ok = declared_ok
                && capability_ok
                && expectation_ok
                && collection_ok
                && self
                    .availability_error(item, None, *desc, declaration.order)
                    .is_none();
            let mut source = if source_ok {
                self.position(desc.handle).map(|record| record.core.clone())
            } else {
                None
            };
            if source.is_none() {
                // 必然失败的构建：保持槽位对齐；该输出保持失效，不作为正常来源发布。
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
            let local_core = self.position(local).map(|record| record.core.clone());
            let output_core = self.position(output).map(|record| record.core.clone());
            let slot_ok = source_ok && source.is_some() && local_core.is_some();
            if !slot_ok {
                self.mark_tainted(output);
            }
            if let (true, Some(source), Some(local), Some(output)) =
                (slot_ok, source, local_core, output_core)
            {
                slots.push(EachSlot {
                    source,
                    local,
                    output,
                    collected_type,
                });
            }
            outputs.push(output);
        }
        // 未声明（无 typed 能力）保持“未声明”语义：收口使用既有 unit 空操作，
        // 零 collector、零业务槽，不建假 Vec。
        let unit_begin: BeginCollectorsFn = <() as EachCollect>::begin_collectors;
        let unit_finish: FinishCollectorsFn = <() as EachCollect>::finish_collectors;
        let (begin_collectors, finish_collectors) = match &bridge {
            Some(bridge) => (bridge.begin, bridge.finish),
            None => (unit_begin, unit_finish),
        };
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
        (outputs, declared_ok && capability_ok && expectation_ok)
    }

    /// 校验一次位置使用；与 resolver/register_* 共用同一 availability 判定。
    fn validate_use(
        &self,
        body: BodyId,
        step: Option<usize>,
        desc: PositionDesc,
        order: u64,
    ) -> Option<BuildErrorDetail> {
        self.availability_error(body, step, desc, order)
    }

    /// 比较 Choose 各分支的声明状态与 Shape。
    pub(crate) fn validate_choose_contract(
        &self,
        contract: &ChooseContract,
    ) -> Option<BuildErrorDetail> {
        let first = *contract.branches.first()?;
        let first_state = self.bodies[first.index()].output.state();
        for (index, branch) in contract.branches.iter().enumerate().skip(1) {
            if self.bodies[branch.index()].output.state() != first_state {
                return Some(BuildErrorDetail::ChooseDeclarationStateMismatch {
                    body: contract.body,
                    step: Some(contract.step),
                    first_branch: 0,
                    second_branch: index as u16,
                });
            }
        }
        match first_state {
            DeclarationState::Failed => Some(BuildErrorDetail::MissingDeclaration {
                body: first,
                kind: DeclarationKind::Output,
            }),
            DeclarationState::Undeclared | DeclarationState::Unit => {
                if contract.output_types.is_empty() {
                    None
                } else {
                    Some(BuildErrorDetail::ChooseArityMismatch {
                        body: contract.body,
                        step: Some(contract.step),
                        branch: 0,
                        expected: 0,
                        found: contract.output_types.len() as u16,
                    })
                }
            }
            DeclarationState::NonUnit => {
                for (branch, body) in contract.branches.iter().enumerate() {
                    let slots = &self.bodies[body.index()].output.slots;
                    if slots.len() != contract.output_types.len() {
                        return Some(BuildErrorDetail::ChooseArityMismatch {
                            body: contract.body,
                            step: Some(contract.step),
                            branch: branch as u16,
                            expected: contract.output_types.len() as u16,
                            found: slots.len() as u16,
                        });
                    }
                    for (slot, (expected, desc)) in
                        contract.output_types.iter().zip(slots.iter()).enumerate()
                    {
                        if *expected != desc.type_desc {
                            return Some(BuildErrorDetail::ChooseSlotTypeMismatch {
                                body: contract.body,
                                step: Some(contract.step),
                                branch: branch as u16,
                                slot: slot as u16,
                                expected: *expected,
                                found: desc.type_desc,
                            });
                        }
                    }
                }
                None
            }
        }
    }

    /// 核对 Root 输出与预期的 owned Shape。
    fn validate_root_output(&self, root: BodyId) -> Option<BuildErrorDetail> {
        let expected = self.root_expectation.as_ref()?;
        self.validate_output_expectation(root, expected)
    }

    /// 按期望 Shape 核对某 Body 的 Output 声明；Root 与 child `_output` 共用同一判据。
    ///
    /// 期望为空（`O = ()`）时只拒绝“已成功声明非空槽”；期望非空时要求已成功声明、arity
    /// 与逐槽类型全部相符。未声明／失败状态是否叠加诊断由调用方决定（child 收口保留首因）。
    fn validate_output_expectation(
        &self,
        body: BodyId,
        expected: &[TypeDesc],
    ) -> Option<BuildErrorDetail> {
        let output = &self.bodies[body.index()].output;
        let found: Vec<TypeDesc> = output.slots.iter().map(|desc| desc.type_desc).collect();
        self.validate_declaration_expectation(
            body,
            DeclarationKind::Output,
            output.attempted,
            output.declared,
            &found,
            expected,
        )
    }

    /// 按期望 Shape 核对一组声明槽（Root、child 收口与 Choose/Each 聚合契约共用同一判据）。
    ///
    /// `declared_ok` 反映该次声明的成功与否；`slots` 对 Choose/Each 是**聚合后**类型，
    /// 因此“期望为空”只拒绝已成功声明的非空槽，与 Root/child 的 unit 语义一致。
    pub(crate) fn validate_declaration_expectation(
        &self,
        body: BodyId,
        kind: DeclarationKind,
        attempted: bool,
        declared: bool,
        found: &[TypeDesc],
        expected: &[TypeDesc],
    ) -> Option<BuildErrorDetail> {
        if expected.is_empty() {
            if attempted && declared && !found.is_empty() {
                return Some(BuildErrorDetail::DeclarationArityMismatch {
                    body,
                    kind,
                    slot: 0,
                    expected: 0,
                    found: found.len() as u16,
                });
            }
            return None;
        }
        if !attempted || !declared {
            return Some(BuildErrorDetail::MissingDeclaration { body, kind });
        }
        if found.len() != expected.len() {
            return Some(BuildErrorDetail::DeclarationArityMismatch {
                body,
                kind,
                slot: 0,
                expected: expected.len() as u16,
                found: found.len() as u16,
            });
        }
        for (slot, (expected, found)) in expected.iter().zip(found.iter()).enumerate() {
            if *expected != *found {
                return Some(BuildErrorDetail::DeclarationShapeMismatch {
                    body,
                    kind,
                    slot: slot as u16,
                    expected: *expected,
                    found: Some(*found),
                });
            }
        }
        None
    }

    /// 核对直接 Round 的 Next 声明与 initial/formal Current Shape。
    fn validate_iter_next(&self, round: BodyId) -> Option<BuildErrorDetail> {
        let body = &self.bodies[round.index()];
        if body.role != BodyRole::Round {
            return None;
        }
        if body.output.attempted {
            return Some(BuildErrorDetail::DeclarationNotAllowed {
                body: round,
                kind: DeclarationKind::Output,
                role: BodyRole::Round,
            });
        }
        if !body.next.attempted || !body.next.declared {
            return Some(BuildErrorDetail::MissingDeclaration {
                body: round,
                kind: DeclarationKind::Next,
            });
        }
        if body.next.slots.is_empty() {
            return Some(BuildErrorDetail::NextUnitUnsupported { body: round });
        }
        for parent in &self.bodies {
            for step in &parent.steps {
                let Some(iter) = step.iter() else { continue };
                if iter.child() != round {
                    continue;
                }
                let initial = iter.initial();
                if body.next.slots.len() != initial.len() {
                    return Some(BuildErrorDetail::DeclarationArityMismatch {
                        body: round,
                        kind: DeclarationKind::Next,
                        slot: 0,
                        expected: initial.len() as u16,
                        found: body.next.slots.len() as u16,
                    });
                }
                for (slot, (desc, expected)) in
                    body.next.slots.iter().zip(initial.iter()).enumerate()
                {
                    if desc.type_desc != expected.type_desc {
                        return Some(BuildErrorDetail::DeclarationShapeMismatch {
                            body: round,
                            kind: DeclarationKind::Next,
                            slot: slot as u16,
                            expected: expected.type_desc,
                            found: Some(desc.type_desc),
                        });
                    }
                }
                return None;
            }
        }
        None
    }

    /// 统一验证整棵树并收口。
    ///
    /// 预记录错误按插入顺序优先；否则按既有 Body/Step 顺序取首个结构错误。
    pub(crate) fn finish(self, root: BodyId) -> Result<Definition<'n>, BuildErrorDetail> {
        if let Some(first) = self.errors.first() {
            return Err(*first);
        }
        let mut error: Option<BuildErrorDetail> = None;
        // 先验证所有 Step 输入，再验证声明槽：声明失败常是输入失败的派生后果，
        // 让更早的真实使用点先给出目标原因。
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
        }
        for body in &self.bodies {
            for desc in &body.output.slots {
                if let Some(found) = self.validate_use(body.id, None, *desc, body.output.order) {
                    error = error.or(Some(found));
                }
            }
            for desc in &body.next.slots {
                if let Some(found) = self.validate_use(body.id, None, *desc, body.next.order) {
                    error = error.or(Some(found));
                }
            }
        }
        for body in &self.bodies {
            if body.role == BodyRole::Round && body.output.attempted {
                error = error.or(Some(BuildErrorDetail::DeclarationNotAllowed {
                    body: body.id,
                    kind: DeclarationKind::Output,
                    role: BodyRole::Round,
                }));
            }
            if body.role == BodyRole::Ordinary && body.next.attempted {
                error = error.or(Some(BuildErrorDetail::DeclarationNotAllowed {
                    body: body.id,
                    kind: DeclarationKind::Next,
                    role: BodyRole::Ordinary,
                }));
            }
        }
        for body in &self.bodies {
            if body.role == BodyRole::Round
                && let Some(found) = self.validate_iter_next(body.id)
            {
                error = error.or(Some(found));
            }
        }
        if let Some(found) = self.validate_root_output(root) {
            error = error.or(Some(found));
        }
        for contract in &self.choose_contracts {
            if let Some(found) = self.validate_choose_contract(contract) {
                error = error.or(Some(found));
            }
        }
        let mut root_output: Vec<PositionId> = Vec::new();
        for desc in &self.bodies[root.index()].output.slots {
            if root_output.contains(&desc.handle.position) {
                error = error.or(Some(BuildErrorDetail::DuplicateRootOutput {
                    position: desc.handle.position,
                }));
            } else {
                root_output.push(desc.handle.position);
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

/// 唯一 Definition 构建出口（T03 Root 前端）。
///
/// 语义：
/// - `In` 确定 Root 输入的类型 Shape，并按声明顺序登记初始逻辑 Ref（不接收或保存
///   本次 owned 输入）；
/// - 用 `Out` 对应 Shape 设置 Root 期望输出（`set_root_expectation`），这是该能力首次
///   接入生产构建路径：调用方期望的 owned Out 在闭包执行前就固定下来；
/// - 同步执行双参数 unit 闭包，登记 Root 与全部 Chain 的 Body／Step；
/// - Root 业务输出**只**来自 Body 内 `output`／`output_with` 声明，不再从闭包返回值补声明
///   （unit 闭包无声明即“未声明”，不会被包装成显式 unit）；
/// - 对整棵 Definition 验证来源、可见性、顺序与类型（含 `validate_root_output` 对声明与
///   期望的核对），最后返回验证完成的结构。
///
/// 失败返回 [`BuildErrorDetail`]，不返回可供执行消费的半成品；任何验证路径都不调用
/// 业务 Node。根闭包同步消费，不要求 `+ 'n`：被登记句柄的寿命由各 Step 的
/// `N: 'n` 约束保证。
#[allow(private_bounds)]
pub(crate) fn build_definition<'n, In, Out, F>(
    definition: F,
) -> Result<Definition<'n>, BuildErrorDetail>
where
    In: RootInput + InputOps,
    In::Refs: ShapeOps,
    Out: RootInput<Owned = Out>,
    Out::Refs: ShapeOps + RefShape<Owned = Out>,
    F: FnOnce(&mut Flow<'n>, In::Refs),
{
    let builder = Rc::new(RefCell::new(Builder::<'n>::new()));
    let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
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
        state.set_root_expectation(<Out::Refs as ShapeOps>::type_descs());
    }
    let refs = <In::Refs as ShapeOps>::from_positions(&mut handles.into_iter());
    {
        // 双参数 unit 闭包：业务输出由 Body 内的 output／output_with 声明。
        let mut flow = Flow::open(Rc::clone(&builder), root);
        definition(&mut flow, refs);
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
        let definition =
            build_definition::<Vec<Item>, (Vec<Score>, Vec<Score>), _>(|flow, items| {
                let out_refs: (Ref<Vec<Score>>, Ref<Vec<Score>>) =
                    flow.each_output(items, |each| {
                        let item = each.ref_of::<Item>();

                        let score = each.then_with_output(score_of, item);
                        let doubled = each.then_with_output(double_score, score);
                        let out_refs = (score, doubled);
                        each.output_with(out_refs);
                    });
                flow.output_with(out_refs);
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
        let definition = build_definition::<Vec<Item>, (), _>(|flow, items| {
            let () = flow.each_output(items, |each| {
                let item = each.ref_of::<Item>();

                let _score = each.then_with_output(score_of, item);
                each.output::<()>();
            });
            flow.output_with(());
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
        let root = builder.borrow_mut().new_body(None, BodyRole::Ordinary);
        let item_body = builder
            .borrow_mut()
            .new_body(Some(root), BodyRole::Ordinary);
        let item_handle =
            builder
                .borrow_mut()
                .allocate_each_item(item_body, 0, TypeDesc::of::<Item>());
        {
            let mut flow = Flow::open(Rc::clone(&builder), root);
            let leaked = Ref::<Item>::from_handle(item_handle);
            let _ = flow.then_with_output(score_of, leaked);
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
