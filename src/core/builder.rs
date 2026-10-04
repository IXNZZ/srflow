//! Definition 构建器、异型 Step 保存与最小顺序驱动。
//!
//! Definition 拥有自己的 `RefId` 来源、声明表（输入位置与此前 Step 的输出位置）和
//! Step 列表；Step 只附加顺序位置，不拥有 Scope，也不缓存某次 Execution 的 `DataId`
//! 或 Prepared runtime output。所有 Node／Orchestrator 都经同一个
//! [`TypedCallBuilder::then`] 接线入口，调用方不写 Marker、不手造 CallSite。
//!
//! 构建期纪律（V21-05 §4.2）：先完成全部可恢复检查（输入归属、位置声明、输出分类、
//! Orchestrator 签名）与可失败预检，再做 0／1／2 输出位置的 checked 整组分配；任何
//! 返回 [`BuildError`] 的路径都不追加 Step、不改变声明表、不消耗本次调用输出序号。
//! 分配成功之后的登记与追加路径不再包含可返回 `BuildError` 的操作。

use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use super::context::{BodyError, ExecutionContext};
use super::data_ref::DataRef;
use super::identity::ScopeId;
use super::node::{AsyncFn0, AsyncFn1, AsyncFn2, Fn0, Fn1, Fn2, Leaf0, Leaf1, Leaf2};
use super::node::{LeafOutput, NodeCall0, NodeCall1, NodeCall2, NodeSite, SharedNode};
use super::orchestrator::{OrchCall, OrchSite, OrchestratorSite, PackFromPorts};
use super::ref_id::{RefId, RefIdAllocator, RefIdSource};
use super::runtime::{RootExecution, RootExit, run_root};
use super::signature::{
    ArcNodeSig, AsyncFnSig, BuildError, Data, DeclaredPort, InputTypes, NodeSig, OrchSig, OutKind,
    SyncFnSig, WireInputs, Wiring,
};

/// 一个已接线的调用点。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) enum CallSite {
    /// 业务叶子：Node 协议或函数适配器，经叶子 site 执行。
    Node(Box<dyn NodeSite>),
    /// 编排体：独立协议，经调用边界建立 child Scope 执行。
    Orchestrator(Box<dyn OrchestratorSite>),
}

/// Definition 中的一个顺序位置。
pub(crate) struct Step {
    site: CallSite,
}

impl Step {
    /// 本次 Step 的调用点。
    pub(crate) fn site(&self) -> &CallSite {
        &self.site
    }
}

/// 接线契约：类型检查完成后把调用点擦除保存。
///
/// 实现分布在各调用对象类别上（函数 item、具体结构体／`Arc` Node、Orchestrator）；
/// 业务侧不实现本 trait，也不接触 `CallSite`。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait BuildSite<M, A0>: IntoCallSite<M, A0> {
    /// 追加 Step 前的额外预检（例如 Orchestrator 内部 Definition 的声明输入）。
    fn precheck(&self, _definition: &Definition, _args: &A0) -> Result<(), BuildError> {
        Ok(())
    }

    /// 构造擦除后的 site；调用时全部检查与整组分配已经完成。
    fn site(self, args: A0, out_positions: &[RefId]) -> CallSite;
}

/// 强类型接线：Marker `M` 决定参数形态与构建输出。
///
/// `BuildOutput` 与 [`Wiring::BuildOutput`] 相等，使 `then` 能在不暴露输出分类的前提下
/// 返回 `DataRef<O>`／`()`／两个位置的 tuple。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait IntoCallSite<M, A0> {
    /// 本次调用的构建输出。
    type BuildOutput;
}

/// 接线入口。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait TypedCallBuilder {
    /// 追加一个调用点：参数形态与输出形态由编译期类型检查，构建期拒绝另行报告。
    fn then<C, M, A0>(&mut self, callable: C, args: A0) -> Result<C::BuildOutput, BuildError>
    where
        C: BuildSite<M, A0>,
        M: Wiring,
        A0: WireInputs,
        C: IntoCallSite<M, A0, BuildOutput = M::BuildOutput>;
}

/// 一个 Definition 的构建状态与执行元数据。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct Definition {
    source: Arc<RefIdSource>,
    allocator: RefIdAllocator,
    inputs: Vec<DeclaredPort>,
    produced: Vec<DeclaredPort>,
    output_ports: Vec<DeclaredPort>,
    steps: Vec<Step>,
}

#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
impl Definition {
    /// 测试构造：以指定来源建立 Definition（用于验证 checked 整组分配耗尽）。
    #[cfg(test)]
    pub(crate) fn new_with_source(source: Arc<RefIdSource>) -> Self {
        Self {
            allocator: RefIdAllocator::new(Arc::clone(&source)),
            source,
            inputs: Vec::new(),
            produced: Vec::new(),
            output_ports: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// 建立一个新的 Definition：新的 `RefId` 来源与唯一序列。
    pub(crate) fn new() -> Self {
        let source = RefIdSource::new();
        Self {
            allocator: RefIdAllocator::new(Arc::clone(&source)),
            source,
            inputs: Vec::new(),
            produced: Vec::new(),
            output_ports: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// 声明一个非空 typed 输入位置。
    pub(crate) fn declare_input<T: 'static>(
        &mut self,
        _name: &'static str,
    ) -> Result<DataRef<T>, BuildError> {
        let position = self.allocator.allocate()?;
        self.inputs.push(DeclaredPort::new::<T>(position.clone()));
        Ok(DataRef::from_position(position))
    }

    /// 声明一个 child-local 输出端口（Orchestrator 内部 Definition 使用）。
    ///
    /// 端口位置可以是此前 Step 已产生的位置（重新暴露 imported Data 同理），也可以由
    /// 编排体在运行时登记；它不是接线来源，除非它同时是已声明输入或 Step 输出。
    pub(crate) fn declare_output_port<T: 'static>(
        &mut self,
        _name: &'static str,
    ) -> Result<DataRef<T>, BuildError> {
        let position = self.allocator.allocate()?;
        self.output_ports
            .push(DeclaredPort::new::<T>(position.clone()));
        Ok(DataRef::from_position(position))
    }

    /// 把已有位置登记为 child-local 输出端口（不分配新位置）。
    ///
    /// 用于两情形：编排体的输出来自某个真实子调用的输出位置，或重新暴露一个完整
    /// imported Data（端口位置即声明输入位置）。位置必须属于本 Definition 的来源。
    pub(crate) fn declare_output_port_for<T: 'static>(
        &mut self,
        position: &DataRef<T>,
        _name: &'static str,
    ) -> Result<(), BuildError> {
        if !position.position().belongs_to(&self.source) {
            return Err(BuildError::ForeignPosition(position.position().clone()));
        }
        if self
            .output_ports
            .iter()
            .any(|port| port.position() == position.position())
        {
            return Err(BuildError::UndeclaredPosition(position.position().clone()));
        }
        self.output_ports
            .push(DeclaredPort::new::<T>(position.position().clone()));
        Ok(())
    }

    /// 已声明输入位置。
    pub(crate) fn inputs(&self) -> &[DeclaredPort] {
        &self.inputs
    }

    /// 声明输出端口。
    pub(crate) fn output_ports(&self) -> &[DeclaredPort] {
        &self.output_ports
    }

    /// 合法接线来源：已声明输入与此前 Step 的输出（按声明顺序）。
    pub(crate) fn declared(&self) -> Vec<&DeclaredPort> {
        self.inputs.iter().chain(self.produced.iter()).collect()
    }

    /// 当前 Step 数量。
    pub(crate) fn step_count(&self) -> usize {
        self.steps.len()
    }

    /// Step 列表（下标即顺序位置）。
    pub(crate) fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// 本次 Definition 已分配的位置数量（测试观测：构建失败不消耗序号）。
    #[cfg(test)]
    pub(crate) fn allocated_probe(&self) -> u64 {
        self.allocator.next_probe()
    }

    /// 输入归属与位置声明检查。
    ///
    /// 只接受当前 Definition 已声明的输入位置或此前 Step 已声明的输出位置；另一条
    /// 来源序列的同类型引用、同一来源但未登记为合法位置的引用都在此拒绝。
    fn check_wiring(&self, position: &RefId) -> Result<(), BuildError> {
        if !position.belongs_to(&self.source) {
            return Err(BuildError::ForeignPosition(position.clone()));
        }
        let declared = self
            .inputs
            .iter()
            .chain(self.produced.iter())
            .any(|port| port.position() == position);
        if !declared {
            return Err(BuildError::UndeclaredPosition(position.clone()));
        }
        Ok(())
    }
}

impl Default for Definition {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
impl TypedCallBuilder for Definition {
    fn then<C, M, A0>(&mut self, callable: C, args: A0) -> Result<C::BuildOutput, BuildError>
    where
        C: BuildSite<M, A0>,
        M: Wiring,
        A0: WireInputs,
        C: IntoCallSite<M, A0, BuildOutput = M::BuildOutput>,
    {
        // 1. 全部输入位置必须属于本 Definition 且已登记为合法来源。
        for position in args.positions() {
            self.check_wiring(position)?;
        }
        // 2. 输出分类预检：普通函数的 unit 输出是构建失败，不产生 DataRef<()>。
        let unit_output = M::output_type() == TypeId::of::<()>();
        if M::ORDINARY_FUNCTION && unit_output {
            return Err(BuildError::UnsupportedFunctionUnitOutput);
        }
        if !M::ORDINARY_FUNCTION && unit_output && M::DECLARED == 1 {
            return Err(BuildError::UnitDataOutputNotSupported);
        }
        // 3. 调用对象类别的额外预检（Orchestrator 内部 Definition 的声明输入与输出分类）。
        callable.precheck(self, &args)?;
        // 4. 整组 checked 分配：失败时一个序号都不消耗。
        let slots = M::allocate(&self.allocator)?;
        let out_positions = M::positions(&slots);
        // 5. 以下路径不可失败：登记声明端口并追加 Step。
        let port_types = M::port_types();
        for (position, (expected_name, expected)) in out_positions.iter().zip(port_types) {
            self.produced.push(DeclaredPort::with_type(
                position.clone(),
                expected,
                expected_name,
            ));
        }
        let site = callable.site(args, &out_positions);
        self.steps.push(Step { site });
        Ok(M::assemble(slots))
    }
}

/// 顺序驱动一个 Definition 的全部 Step（最小 adapter 集成驱动，不是完整 Flow 执行器）。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) async fn run_definition(
    ctx: &mut ExecutionContext,
    definition: &Definition,
) -> Result<(), BodyError> {
    let scope = ctx
        .current_scope()
        .ok_or_else(|| BodyError::new("a definition requires an active caller frame"))?;
    for step in definition.steps() {
        run_site(ctx, &scope, step.site()).await?;
    }
    Ok(())
}

/// 按调用点类别分派：Node 走叶子 site，Orchestrator 走调用边界。
pub(crate) async fn run_site(
    ctx: &mut ExecutionContext,
    scope: &ScopeId,
    site: &CallSite,
) -> Result<(), BodyError> {
    match site {
        CallSite::Node(node) => {
            node.invoke(ctx, scope).await?;
            Ok(())
        }
        CallSite::Orchestrator(orchestrator) => {
            orchestrator.invoke(ctx, scope).await?;
            Ok(())
        }
    }
}

/// Root 驱动：以空声明输出收口，执行一个 Definition 的全部 Step。
///
/// 这是本阶段的最小 adapter 集成入口，不是公开 `Runtime::execute`，也不提供 Root owned
/// take；Root 输入由调用方在进入 body 后登记到 Definition 的声明输入位置。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) async fn run_definition_root(
    execution: RootExecution,
    definition: &Definition,
) -> RootExit {
    run_root(execution, definition, definition_root_body).await
}

/// Root 执行体：Definition 的声明输入位置由调用方预先写入 RootScope。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
async fn definition_root_body(
    ctx: &mut ExecutionContext,
    definition: &Definition,
) -> Result<(), BodyError> {
    run_definition(ctx, definition).await
}

/// Orchestrator 签名预检：接线参数数量、内部 Definition 的声明输入与输出分类必须一致。
///
/// 输入类型由输入 pack（[`PackFromPorts`]）按声明类型还原时核对，因此这里核对数量、
/// pack 构造与输出端口分类；caller 位置的归属与声明由 `then` 的第 1 步负责。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
fn check_orchestrator_inputs<O, I, K>(
    orchestrator: &O,
    positions: &[&RefId],
) -> Result<(), BuildError>
where
    O: OrchCall<I, K> + ?Sized,
    I: 'static + InputTypes,
    K: OutKind,
{
    let inner = orchestrator.definition();
    if inner.inputs().len() != positions.len() {
        return Err(BuildError::InputCountMismatch {
            expected: inner.inputs().len(),
            supplied: positions.len(),
        });
    }
    // 接线 Signature 的输入类型必须与内部 Definition 的声明逐项一致（含后项）。
    let signature = <I as InputTypes>::input_types();
    if signature.len() != inner.inputs().len() {
        return Err(BuildError::InputCountMismatch {
            expected: inner.inputs().len(),
            supplied: signature.len(),
        });
    }
    for (index, ((signature_name, signature_type), port)) in
        signature.iter().zip(inner.inputs()).enumerate()
    {
        if *signature_type != port.expected() {
            return Err(BuildError::SignatureMismatch {
                index,
                expected: signature_name,
                actual: port.expected_name(),
            });
        }
    }
    // 输入 pack 的构造同时核对每项声明类型（`PackFor<I>` 另在类型层绑定 pack 与 I）。
    <O::Pack as PackFromPorts>::from_ports(inner.inputs())?;
    let declared_outputs = inner.output_ports();
    if declared_outputs.len() != K::DECLARED {
        return Err(BuildError::InputCountMismatch {
            expected: K::DECLARED,
            supplied: declared_outputs.len(),
        });
    }
    for (index, (port, (name, expected))) in declared_outputs
        .iter()
        .zip(<K as OutKind>::port_types())
        .enumerate()
    {
        if port.expected() != expected {
            return Err(BuildError::SignatureMismatch {
                index,
                expected: name,
                actual: port.expected_name(),
            });
        }
    }
    Ok(())
}

// ---- 函数 item 的接线实现：普通函数只有数据路径 ----

impl<F: 'static, A: 'static, O: 'static> IntoCallSite<SyncFnSig<(A,), Data<O>>, DataRef<A>> for F
where
    F: for<'x> Fn(&'x A) -> Result<O, BodyError>,
{
    type BuildOutput = DataRef<O>;
}

impl<F: 'static, A: 'static, O: 'static> BuildSite<SyncFnSig<(A,), Data<O>>, DataRef<A>> for F
where
    F: for<'x> Fn(&'x A) -> Result<O, BodyError>,
{
    fn site(self, args: DataRef<A>, out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf1::<Fn1<F, A>, A, Data<O>>::new(
            Fn1(self, PhantomData),
            args.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

impl<F: 'static, A: 'static, B: 'static, O: 'static>
    IntoCallSite<SyncFnSig<(A, B), Data<O>>, (DataRef<A>, DataRef<B>)> for F
where
    F: for<'x, 'y> Fn(&'x A, &'y B) -> Result<O, BodyError>,
{
    type BuildOutput = DataRef<O>;
}

impl<F: 'static, A: 'static, B: 'static, O: 'static>
    BuildSite<SyncFnSig<(A, B), Data<O>>, (DataRef<A>, DataRef<B>)> for F
where
    F: for<'x, 'y> Fn(&'x A, &'y B) -> Result<O, BodyError>,
{
    fn site(self, args: (DataRef<A>, DataRef<B>), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf2::<Fn2<F, A, B>, A, B, Data<O>>::new(
            Fn2(self, PhantomData),
            args.0.position().clone(),
            args.1.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

impl<F: 'static, O: 'static> IntoCallSite<SyncFnSig<(), Data<O>>, ()> for F
where
    F: Fn() -> Result<O, BodyError>,
{
    type BuildOutput = DataRef<O>;
}

impl<F: 'static, O: 'static> BuildSite<SyncFnSig<(), Data<O>>, ()> for F
where
    F: Fn() -> Result<O, BodyError>,
{
    fn site(self, _args: (), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf0::<Fn0<F>, Data<O>>::new(
            Fn0(self),
            out_positions.first().cloned(),
        )))
    }
}

impl<F: 'static, O: 'static> IntoCallSite<AsyncFnSig<(), Data<O>>, ()> for F
where
    F: AsyncFn() -> Result<O, BodyError>,
{
    type BuildOutput = DataRef<O>;
}

impl<F: 'static, O: 'static> BuildSite<AsyncFnSig<(), Data<O>>, ()> for F
where
    F: AsyncFn() -> Result<O, BodyError>,
{
    fn site(self, _args: (), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf0::<AsyncFn0<F>, Data<O>>::new(
            AsyncFn0(self),
            out_positions.first().cloned(),
        )))
    }
}

impl<F: 'static, A: 'static, O: 'static> IntoCallSite<AsyncFnSig<(A,), Data<O>>, DataRef<A>> for F
where
    F: for<'x> AsyncFn(&'x A) -> Result<O, BodyError>,
{
    type BuildOutput = DataRef<O>;
}

impl<F: 'static, A: 'static, O: 'static> BuildSite<AsyncFnSig<(A,), Data<O>>, DataRef<A>> for F
where
    F: for<'x> AsyncFn(&'x A) -> Result<O, BodyError>,
{
    fn site(self, args: DataRef<A>, out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf1::<AsyncFn1<F, A>, A, Data<O>>::new(
            AsyncFn1(self, PhantomData),
            args.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

impl<F: 'static, A: 'static, B: 'static, O: 'static>
    IntoCallSite<AsyncFnSig<(A, B), Data<O>>, (DataRef<A>, DataRef<B>)> for F
where
    F: for<'x, 'y> AsyncFn(&'x A, &'y B) -> Result<O, BodyError>,
{
    type BuildOutput = DataRef<O>;
}

impl<F: 'static, A: 'static, B: 'static, O: 'static>
    BuildSite<AsyncFnSig<(A, B), Data<O>>, (DataRef<A>, DataRef<B>)> for F
where
    F: for<'x, 'y> AsyncFn(&'x A, &'y B) -> Result<O, BodyError>,
{
    fn site(self, args: (DataRef<A>, DataRef<B>), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf2::<AsyncFn2<F, A, B>, A, B, Data<O>>::new(
            AsyncFn2(self, PhantomData),
            args.0.position().clone(),
            args.1.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

// ---- 具体结构体 Node 与 Arc<具体 Node> 的接线实现 ----

impl<N: 'static, A: 'static, K: OutKind> IntoCallSite<NodeSig<(A,), K>, DataRef<A>> for N
where
    N: NodeCall1<A, K>,
    K: LeafOutput,
{
    type BuildOutput = K::BuildOutput;
}

impl<N: 'static, A: 'static, K: OutKind> BuildSite<NodeSig<(A,), K>, DataRef<A>> for N
where
    N: NodeCall1<A, K>,
    K: LeafOutput,
{
    fn site(self, args: DataRef<A>, out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf1::<N, A, K>::new(
            self,
            args.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

impl<N: 'static, A: 'static, B: 'static, K: OutKind>
    IntoCallSite<NodeSig<(A, B), K>, (DataRef<A>, DataRef<B>)> for N
where
    N: NodeCall2<A, B, K>,
    K: LeafOutput,
{
    type BuildOutput = K::BuildOutput;
}

impl<N: 'static, A: 'static, B: 'static, K: OutKind>
    BuildSite<NodeSig<(A, B), K>, (DataRef<A>, DataRef<B>)> for N
where
    N: NodeCall2<A, B, K>,
    K: LeafOutput,
{
    fn site(self, args: (DataRef<A>, DataRef<B>), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf2::<N, A, B, K>::new(
            self,
            args.0.position().clone(),
            args.1.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

impl<N: 'static, K: OutKind> IntoCallSite<NodeSig<(), K>, ()> for N
where
    N: NodeCall0<K>,
    K: LeafOutput,
{
    type BuildOutput = K::BuildOutput;
}

impl<N: 'static, K: OutKind> BuildSite<NodeSig<(), K>, ()> for N
where
    N: NodeCall0<K>,
    K: LeafOutput,
{
    fn site(self, _args: (), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf0::<N, K>::new(
            self,
            out_positions.first().cloned(),
        )))
    }
}

impl<N: 'static, K: OutKind> IntoCallSite<ArcNodeSig<(), K>, ()> for Arc<N>
where
    N: NodeCall0<K>,
    K: LeafOutput,
{
    type BuildOutput = K::BuildOutput;
}

impl<N: 'static, K: OutKind> BuildSite<ArcNodeSig<(), K>, ()> for Arc<N>
where
    N: NodeCall0<K>,
    K: LeafOutput,
{
    fn site(self, _args: (), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf0::<SharedNode<N>, K>::new(
            SharedNode(self),
            out_positions.first().cloned(),
        )))
    }
}

impl<N: 'static, A: 'static, K: OutKind> IntoCallSite<ArcNodeSig<(A,), K>, DataRef<A>> for Arc<N>
where
    N: NodeCall1<A, K>,
    K: LeafOutput,
{
    type BuildOutput = K::BuildOutput;
}

impl<N: 'static, A: 'static, K: OutKind> BuildSite<ArcNodeSig<(A,), K>, DataRef<A>> for Arc<N>
where
    N: NodeCall1<A, K>,
    K: LeafOutput,
{
    fn site(self, args: DataRef<A>, out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf1::<SharedNode<N>, A, K>::new(
            SharedNode(self),
            args.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

impl<N: 'static, A: 'static, B: 'static, K: OutKind>
    IntoCallSite<ArcNodeSig<(A, B), K>, (DataRef<A>, DataRef<B>)> for Arc<N>
where
    N: NodeCall2<A, B, K>,
    K: LeafOutput,
{
    type BuildOutput = K::BuildOutput;
}

impl<N: 'static, A: 'static, B: 'static, K: OutKind>
    BuildSite<ArcNodeSig<(A, B), K>, (DataRef<A>, DataRef<B>)> for Arc<N>
where
    N: NodeCall2<A, B, K>,
    K: LeafOutput,
{
    fn site(self, args: (DataRef<A>, DataRef<B>), out_positions: &[RefId]) -> CallSite {
        CallSite::Node(Box::new(Leaf2::<SharedNode<N>, A, B, K>::new(
            SharedNode(self),
            args.0.position().clone(),
            args.1.position().clone(),
            out_positions.first().cloned(),
        )))
    }
}

// ---- Orchestrator 的接线实现 ----

impl<O: 'static, I: 'static, K: OutKind> IntoCallSite<OrchSig<I, K>, DataRef<I>> for O
where
    O: OrchCall<(I,), K>,
{
    type BuildOutput = K::BuildOutput;
}

impl<O: 'static, I: 'static, K: OutKind> BuildSite<OrchSig<I, K>, DataRef<I>> for O
where
    O: OrchCall<(I,), K>,
{
    fn precheck(&self, _definition: &Definition, args: &DataRef<I>) -> Result<(), BuildError> {
        check_orchestrator_inputs::<O, (I,), K>(self, &[args.position()])
    }

    fn site(self, args: DataRef<I>, out_positions: &[RefId]) -> CallSite {
        CallSite::Orchestrator(Box::new(OrchSite::<O, (I,), K>::new(
            self,
            vec![args.position().clone()],
            out_positions.to_vec(),
        )))
    }
}

impl<O: 'static, I: 'static, J: 'static, K: OutKind>
    IntoCallSite<OrchSig<(I, J), K>, (DataRef<I>, DataRef<J>)> for O
where
    O: OrchCall<(I, J), K>,
{
    type BuildOutput = K::BuildOutput;
}

impl<O: 'static, I: 'static, J: 'static, K: OutKind>
    BuildSite<OrchSig<(I, J), K>, (DataRef<I>, DataRef<J>)> for O
where
    O: OrchCall<(I, J), K>,
{
    fn precheck(
        &self,
        _definition: &Definition,
        args: &(DataRef<I>, DataRef<J>),
    ) -> Result<(), BuildError> {
        check_orchestrator_inputs::<O, (I, J), K>(self, &[args.0.position(), args.1.position()])
    }

    fn site(self, args: (DataRef<I>, DataRef<J>), out_positions: &[RefId]) -> CallSite {
        CallSite::Orchestrator(Box::new(OrchSite::<O, (I, J), K>::new(
            self,
            vec![args.0.position().clone(), args.1.position().clone()],
            out_positions.to_vec(),
        )))
    }
}
