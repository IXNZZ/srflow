//! Orchestrator 协议、child-local 端口与调用边界。
//!
//! Orchestrator 与 Node 使用**独立协议**：它不收到 owned 业务输入，不把业务值移出
//! Container，也不以 `owned I -> O` 假装内部编排。调用边界负责建立直接 child Scope、
//! 整组 Import 到 child 的声明输入位置、运行编排体、按 Signature 整组 Export 并关闭
//! child；caller 输出位置不交给编排体，编排体也不返回已关闭 Scope 的裸 target。
//!
//! 擦除后的校验点（V21-05 §4.5）：边界把输入 pack downcast 为确切的私有
//! [`Targets…`](Targets1) 类型，再逐输入位置验证 Scope 访问关系与容器
//! 归属 → 存活 → 实际 `TypeId`（复用容器 `validate_type`）；失败在业务 body 运行前
//! 进入既有内部错误通道。pack 只携带位置元数据，不含 owned 业务值或长期借用。

use std::any::{Any, TypeId};
use std::marker::PhantomData;

use super::builder::{CallSite, Definition};
use super::context::{BodyError, ExecutionContext, InvocationKind, TerminationKind};
use super::identity::ScopeId;
use super::internal_error::ScopeError;
use super::ref_id::RefId;
use super::scope::{ExportSlot, ImportSlot};
use super::signature::{BuildError, DeclaredPort, InputTypes, NodeFut, OutKind};
use super::signature::{Data, WireInputs};

/// 把 pack 类型与正式输入 Signature 关联：只有匹配的类型 tuple 才成立。
///
/// 与 [`PackFromPorts`] 配对：能被绑定到某个 Signature 的 pack 必然支持从声明端口构造。
pub(crate) trait PackFor<I: 'static>: PackFromPorts {}

impl<A: 'static> PackFor<(A,)> for Targets1<A> {}
impl<A: 'static, B: 'static> PackFor<(A, B)> for Targets2<A, B> {}
impl<I: 'static + InputTypes> PackFor<I> for TargetsN<I> {}

/// 调用语义中的 Scope 角色（只读诊断用标签，不是新的身份种类）。
///
/// 规范没有为 Match／Branch 增加 `InvocationKind`：分支边界仍是
/// [`InvocationKind::Boundary`](super::context::InvocationKind::Boundary)。这里的角色只用于
/// `cfg(test)` 只读记录真实创建点，说明该 child Scope 是由哪一类调用建立的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeRole {
    /// Flow／SubFlow 调用建立的 child Scope。
    Flow,
    /// Match 调用建立的 child Scope（MatchScope）。
    Match,
    /// Match 内部被选 branch 的包装调用建立的 child Scope（BranchScope）。
    Branch,
    /// Each 调用建立的 child Scope（EachScope）。
    Each,
    /// Each 内部每个 item 建立的 child Scope（ItemScope）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    Item,
    /// Loop 调用建立的 child Scope（LoopScope）。
    Loop,
    /// Loop 内部每轮建立的 child Scope（RoundScope）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    Round,
}

/// 业务 Orchestrator 协议：显式声明输入签名 `I`、输出分类 `K` 与输入 pack 类型。
///
/// `definition()` 返回该编排体持有的内部 Definition（真实子调用描述），不是任意 body
/// 闭包；`run` 在当前 Context 与自身 Scope 中组织 child 调用。
pub(crate) trait OrchCall<I: 'static + InputTypes, K: OutKind> {
    /// 输入 pack 类型（[`Targets1`]／[`Targets2`]）。
    ///
    /// `PackFor<I>` 把 pack 与正式输入 Signature 绑定：写错 pack 类型（例如声明
    /// `OrchCall<(u32,), _>` 却给 `Targets1<String>`）在**编译期**就不成立，
    /// 不会等到执行期 Import 才失败。
    type Pack: PackFor<I> + PackFromPorts;

    /// 该编排体在被上层接线调用时使用的 Scope 角色（默认 Flow；Match 覆盖为 Match）。
    ///
    /// 只影响 `cfg(test)` 的创建点记录与诊断标签，不改变调用边界语义。
    const ROLE: ScopeRole = ScopeRole::Flow;

    /// 内部 Definition：声明输入端口、输出端口与真实子调用。
    fn definition(&self) -> &Definition;

    /// 运行编排体；只读写自身 Scope 的声明端口。
    fn run<'a>(&'a self, scope: OrchScope<'a, Self::Pack, K>) -> NodeFut<'a, ()>;
}

/// 由声明端口构造输入 pack；数量或声明类型不匹配时在构建期拒绝。
pub(crate) trait PackFromPorts: 'static {
    /// 按声明输入端口构造 pack。
    fn from_ports(ports: &[DeclaredPort]) -> Result<Self, BuildError>
    where
        Self: Sized;

    /// 校验 pack 自身携带的每个位置：Scope 访问关系与容器归属 → 存活 → 实际 `TypeId`。
    ///
    /// 它校验的是 **pack 里的位置**（业务体真正会读的位置），因此注入损坏 pack 时也会
    /// 被拒绝，不能靠"typed 构建"跳过防御校验。
    fn validate(&self, ctx: &ExecutionContext, child: &ScopeId) -> Result<(), ScopeError>;
}

/// 单输入 pack：只携带 child-local 位置元数据。
pub struct Targets1<A> {
    position: RefId,
    marker: PhantomData<fn() -> A>,
}

/// 双输入 pack：保留顺序与每项类型。
pub struct Targets2<A, B> {
    first: RefId,
    second: RefId,
    marker: PhantomData<fn() -> (A, B)>,
}

/// 三至十六输入 pack：按 Signature 顺序保存位置，并以 `I` 固定各位置类型。
pub struct TargetsN<I> {
    positions: Vec<RefId>,
    marker: PhantomData<fn() -> I>,
}

impl<A: 'static> Targets1<A> {
    /// 只读访问第一个声明输入（类型由 Signature 固定，不猜类型）。
    pub(crate) fn first<'a>(
        &self,
        ctx: &'a ExecutionContext,
        child: &ScopeId,
    ) -> Result<&'a A, ScopeError> {
        ctx.resolve::<A>(child, &self.position)
    }
}

impl<A: 'static, B: 'static> Targets2<A, B> {
    /// 只读访问第一个声明输入。
    pub(crate) fn first<'a>(
        &self,
        ctx: &'a ExecutionContext,
        child: &ScopeId,
    ) -> Result<&'a A, ScopeError> {
        ctx.resolve::<A>(child, &self.first)
    }

    /// 只读访问第二个声明输入；两个位置分别解析，重复 Ref 借用同样合法。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    pub(crate) fn second<'a>(
        &self,
        ctx: &'a ExecutionContext,
        child: &ScopeId,
    ) -> Result<&'a B, ScopeError> {
        ctx.resolve::<B>(child, &self.second)
    }
}

impl<A: 'static> PackFromPorts for Targets1<A> {
    fn from_ports(ports: &[DeclaredPort]) -> Result<Self, BuildError> {
        let [port] = ports else {
            return Err(BuildError::InputCountMismatch {
                expected: 1,
                supplied: ports.len(),
            });
        };
        if port.expected() != TypeId::of::<A>() {
            return Err(BuildError::InputTypeMismatch {
                position: port.position().clone(),
                expected: std::any::type_name::<A>(),
                actual: port.expected_name(),
            });
        }
        Ok(Self {
            position: port.position().clone(),
            marker: PhantomData,
        })
    }

    fn validate(&self, ctx: &ExecutionContext, child: &ScopeId) -> Result<(), ScopeError> {
        ctx.validate_position(
            child,
            &self.position,
            TypeId::of::<A>(),
            std::any::type_name::<A>(),
        )
    }
}

impl<A: 'static, B: 'static> PackFromPorts for Targets2<A, B> {
    fn from_ports(ports: &[DeclaredPort]) -> Result<Self, BuildError> {
        let [first, second] = ports else {
            return Err(BuildError::InputCountMismatch {
                expected: 2,
                supplied: ports.len(),
            });
        };
        if first.expected() != TypeId::of::<A>() {
            return Err(BuildError::InputTypeMismatch {
                position: first.position().clone(),
                expected: std::any::type_name::<A>(),
                actual: first.expected_name(),
            });
        }
        if second.expected() != TypeId::of::<B>() {
            return Err(BuildError::InputTypeMismatch {
                position: second.position().clone(),
                expected: std::any::type_name::<B>(),
                actual: second.expected_name(),
            });
        }
        Ok(Self {
            first: first.position().clone(),
            second: second.position().clone(),
            marker: PhantomData,
        })
    }

    fn validate(&self, ctx: &ExecutionContext, child: &ScopeId) -> Result<(), ScopeError> {
        ctx.validate_position(
            child,
            &self.first,
            TypeId::of::<A>(),
            std::any::type_name::<A>(),
        )?;
        ctx.validate_position(
            child,
            &self.second,
            TypeId::of::<B>(),
            std::any::type_name::<B>(),
        )
    }
}

impl<I: 'static + InputTypes> PackFromPorts for TargetsN<I> {
    fn from_ports(ports: &[DeclaredPort]) -> Result<Self, BuildError> {
        let expected = I::input_types();
        if ports.len() != expected.len() {
            return Err(BuildError::InputCountMismatch {
                expected: expected.len(),
                supplied: ports.len(),
            });
        }
        for (port, (expected_name, expected_type)) in ports.iter().zip(expected) {
            if port.expected() != expected_type {
                return Err(BuildError::InputTypeMismatch {
                    position: port.position().clone(),
                    expected: expected_name,
                    actual: port.expected_name(),
                });
            }
        }
        Ok(Self {
            positions: ports.iter().map(|port| port.position().clone()).collect(),
            marker: PhantomData,
        })
    }

    fn validate(&self, ctx: &ExecutionContext, child: &ScopeId) -> Result<(), ScopeError> {
        for (position, (type_name, type_id)) in self.positions.iter().zip(I::input_types()) {
            ctx.validate_position(child, position, type_id, type_name)?;
        }
        Ok(())
    }
}

/// 编排体内的受控 Scope 视图：只暴露自身 Scope 的声明端口与真实子调用。
pub(crate) struct OrchScope<'a, P, K: OutKind> {
    ctx: &'a mut ExecutionContext,
    child: &'a ScopeId,
    pack: &'a P,
    inner: &'a Definition,
    marker: PhantomData<fn() -> K>,
}

impl<'a, P, K: OutKind> OrchScope<'a, P, K> {
    /// 输入 pack（只携带位置元数据）。
    pub(crate) fn pack(&self) -> &P {
        self.pack
    }

    /// 本编排体自身的 Scope。
    pub(crate) fn child(&self) -> &ScopeId {
        self.child
    }

    /// 只读 Context 视图：供编排体读取自身声明输入与状态，不暴露可变 Container。
    pub(crate) fn ctx_probe(&self) -> &ExecutionContext {
        self.ctx
    }

    /// 顺序执行内部 Definition 的真实子调用（同一双路径 CallSite 递归分派）。
    ///
    /// 这是编排体准备输出的**唯一**方式：新增业务值必须由真实 Node 返回并绑定到本地
    /// 声明位置，编排体视图不提供任意 owned 业务值注入入口。
    pub(crate) async fn run_steps(&mut self) -> Result<(), BodyError> {
        // 与 Root Flow 共用同一顺序主体（`run_definition`）；此处当前 frame 就是 child Scope。
        super::builder::run_definition(self.ctx, self.inner).await
    }

    /// 受控选择执行：只运行**本编排体自身 Definition** 已登记的第 `index` 个可选路径。
    ///
    /// 适用于"一次调用只执行一个 branch"的控制器（Match）。执行面只接受索引：调用点由
    /// 当前执行中的 Definition 自己的私有登记表提供（[`Definition::alternatives`]），
    /// 因此调用者**无法**提交另一张表、外来 `CallSite`、外部 ScopeId 或任意 output target；
    /// 索引越界在任何 child 建立或业务体运行之前被拒绝。
    ///
    /// 它不暴露 `ctx_mut`、可变 Container、owned register／take，也不代替
    /// [`Self::run_steps`] 的顺序语义（调用方不得用它顺序执行全表）。
    pub(crate) async fn run_registered_site(&mut self, index: usize) -> Result<(), BodyError> {
        let Some(site) = self.inner.alternatives().get(index) else {
            return Err(BodyError::from(
                super::internal_error::ScopeError::Invariant {
                    violated: "registered alternative index is out of range",
                },
            ));
        };
        super::builder::run_site(self.ctx, self.child, site).await
    }

    /// 本编排体自身 Definition 已登记的第 `index` 个可选路径（只读；调用前校验用）。
    pub(crate) fn registered_alternative(&self, index: usize) -> Option<&CallSite> {
        self.inner.alternatives().get(index)
    }

    #[cfg(test)]
    /// 测试观测：把自身 Scope 的某个本地位置**预先**绑定到一个测试值（预占 caller 端口样本）。
    ///
    /// 只用于构造"M17 后项 caller 位置冲突"这类真实提交前失败：绑定走真实
    /// `register_owned`，因此冲突仍发生在真实 `finalize` 预检里；它不提供生产绑定入口，
    /// 也不改变清理顺序。
    pub(crate) fn prebind_probe(&mut self, position: &RefId, value: u8) -> Result<(), BodyError> {
        self.ctx
            .register_owned(self.child, position, value)
            .map(|_| ())
            .map_err(BodyError::from)
    }
}

/// Each 调用点专用的受控转交。
///
/// 只对与 Each 形状匹配的 `OrchScope`（`Pack = Sh::Pack`、`K = Data<Vec<O>>`）实现，
/// 并且**以正在执行的 Each 为准**：转交前核对 `scope.inner` 就是该 Each 的 Definition，
/// 包装与最终输出位置也只从该 Each 的登记中取（不接受调用者提供的 runner／ports／target）。
/// 转交在内部建立 collector 并构造 `EachSession`，不把 `&mut ExecutionContext` 交出。
/// 因此另一个 Definition 的 body 即便拿到形状相同的 Scope，也不能借它执行 foreign 包装。
pub(crate) trait EachScopeTransfer<'a, Sh, O: 'static> {
    /// 核对实际 Definition 并建立 collector，把本视图交给该 Each 的受控会话。
    fn begin_each_session(
        self,
        each: &'a super::each::Each<Sh, O>,
    ) -> Result<super::each::EachSession<'a, Sh, O>, BodyError>
    where
        Sh: super::each::EachShape + 'static;
}

impl<'a, Sh, O> EachScopeTransfer<'a, Sh, O> for OrchScope<'a, Sh::Pack, Data<Vec<O>>>
where
    Sh: super::each::EachShapeSpec + 'static,
    O: 'static,
    <<Sh as super::each::EachShape>::Wrapper as super::flow::FlowInputs>::Handles:
        WireInputs + Clone,
{
    fn begin_each_session(
        self,
        each: &'a super::each::Each<Sh, O>,
    ) -> Result<super::each::EachSession<'a, Sh, O>, BodyError> {
        // 本次实际执行的 Definition 必须就是该 Each 自身；否则在 collector／Item／body 之前拒绝。
        if self.inner as *const Definition as *const ()
            != each.raw_definition() as *const Definition as *const ()
        {
            return Err(BodyError::new(
                "each session requires the running definition to be the each orchestrator",
            ));
        }
        each.verify_registration()?;
        let collector = self.ctx.begin_collector::<O>(self.child)?;
        Ok(super::each::EachSession::new(
            self.ctx,
            self.child,
            self.inner,
            self.pack,
            each.registered_wrapper(),
            collector,
            each.final_position().clone(),
        ))
    }
}

/// Loop 调用点专用的受控转交。
///
/// 只对与 Loop 形状匹配的 `OrchScope`（`Pack = Sh::Pack`、`K = Data<Sh::Value>`）实现，
/// 并且**以正在执行的 Loop 为准**：转交前核对 `scope.inner` 就是该 Loop 的 Definition，
/// 包装、唯一声明输出位置与最终输出位置也只从该 Loop 的登记中取。转交在内部构造
/// [`LoopSession`](super::loop_orchestrator::LoopSession)，不把 `&mut ExecutionContext`
/// 交出；因此另一个 Definition 的 body 即便拿到形状相同的 Scope，也不能借它执行 foreign
/// 包装。
pub(crate) trait LoopScopeTransfer<'a, Sh: super::loop_orchestrator::LoopShape> {
    /// 核对实际 Definition 并建立受控 Loop 会话。
    fn begin_loop_session(
        self,
        orchestrator: &'a super::loop_orchestrator::Loop<Sh>,
    ) -> Result<super::loop_orchestrator::LoopSession<'a, Sh>, BodyError>;
}

impl<'a, Sh> LoopScopeTransfer<'a, Sh> for OrchScope<'a, Sh::Pack, Data<Sh::Value>>
where
    Sh: super::loop_orchestrator::LoopShapeSpec + 'static,
    Sh::I: InputTypes,
    <<Sh as super::loop_orchestrator::LoopShape>::Wrapper as super::flow::FlowInputs>::Handles:
        WireInputs + Clone,
{
    fn begin_loop_session(
        self,
        orchestrator: &'a super::loop_orchestrator::Loop<Sh>,
    ) -> Result<super::loop_orchestrator::LoopSession<'a, Sh>, BodyError> {
        // 本次实际执行的 Definition 必须就是该 Loop 自身；否则在 state／Round／body 之前拒绝。
        if self.inner as *const Definition as *const ()
            != orchestrator.raw_definition() as *const Definition as *const ()
        {
            return Err(BodyError::new(
                "loop session requires the running definition to be the loop orchestrator",
            ));
        }
        orchestrator.verify_registration()?;
        Ok(super::loop_orchestrator::LoopSession::new(
            self.ctx,
            self.child,
            self.inner,
            self.pack,
            orchestrator.registered_wrapper(),
            orchestrator.wrapper_output_position().clone(),
            orchestrator.final_position().clone(),
        ))
    }
}

/// 擦除后的 Orchestrator 调用点。
pub(crate) trait OrchestratorSite {
    /// 本次接线使用的 caller 输入位置。
    fn inputs(&self) -> &[RefId];
    /// 本次接线的 caller 输出位置。
    fn outputs(&self) -> &[RefId];
    /// 实际将被调用的 child Definition（只读）。
    ///
    /// 它让调用边界能核对**真实被执行对象**的输入／输出 Signature，而不是任何在别处
    /// 复制的元数据；不提供可变访问，也不能据此取得 Context、owned 值或任意 target。
    fn inner_definition(&self) -> &Definition;
    /// 执行一次编排调用：建立 child、Import、运行编排体、整组 Export 并关闭。
    fn invoke<'a>(
        &'a self,
        ctx: &'a mut ExecutionContext,
        caller: &'a ScopeId,
    ) -> NodeFut<'a, ScopeId>;

    /// 测试注入：替换本次调用的输入 pack（E22 防御校验样本）。
    #[cfg(test)]
    fn inject_pack_probe(&self, _pack: Box<dyn Any>) {}
}

/// Orchestrator 调用点：保存调用对象、caller 位置与本编排体的输入 pack。
pub(crate) struct OrchSite<O, I, K> {
    orchestrator: O,
    caller_inputs: Vec<RefId>,
    caller_outputs: Vec<RefId>,
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    role: ScopeRole,
    #[cfg(test)]
    pack_override: std::cell::RefCell<Option<Box<dyn Any>>>,
    marker: PhantomData<fn() -> (I, K)>,
}

impl<O, I, K> OrchSite<O, I, K> {
    /// 构建调用点：caller 位置、声明输出位置与 Scope 角色已由接线器确定。
    pub(crate) fn new(
        orchestrator: O,
        caller_inputs: Vec<RefId>,
        caller_outputs: Vec<RefId>,
        role: ScopeRole,
    ) -> Self {
        Self {
            orchestrator,
            caller_inputs,
            caller_outputs,
            role,
            #[cfg(test)]
            pack_override: std::cell::RefCell::new(None),
            marker: PhantomData,
        }
    }
}

#[cfg(test)]
/// 测试构造：单输入 pack（E22 注入用）。
pub(crate) fn probe_targets1<A: 'static>(position: RefId) -> Box<dyn Any> {
    Box::new(Targets1::<A> {
        position,
        marker: PhantomData,
    })
}

#[cfg(test)]
/// 测试构造：双输入 pack（E22 注入错误 pack 类型用）。
pub(crate) fn probe_targets2<A: 'static, B: 'static>(first: RefId, second: RefId) -> Box<dyn Any> {
    Box::new(Targets2::<A, B> {
        first,
        second,
        marker: PhantomData,
    })
}

#[cfg(test)]
impl<O, I, K> OrchSite<O, I, K> {
    /// 测试注入：替换本次调用的输入 pack（E22 防御校验样本）。
    pub(crate) fn inject_pack_probe(&self, pack: Box<dyn Any>) {
        *self.pack_override.borrow_mut() = Some(pack);
    }
}

impl<O, I, K> OrchestratorSite for OrchSite<O, I, K>
where
    O: OrchCall<I, K>,
    I: 'static + InputTypes,
    K: OutKind,
{
    fn inputs(&self) -> &[RefId] {
        &self.caller_inputs
    }

    fn outputs(&self) -> &[RefId] {
        &self.caller_outputs
    }

    fn inner_definition(&self) -> &Definition {
        self.orchestrator.definition()
    }

    #[cfg(test)]
    fn inject_pack_probe(&self, pack: Box<dyn Any>) {
        self.inject_pack_probe(pack);
    }

    fn invoke<'a>(
        &'a self,
        ctx: &'a mut ExecutionContext,
        caller: &'a ScopeId,
    ) -> NodeFut<'a, ScopeId> {
        let inner = self.orchestrator.definition();
        let ports = inner.inputs();
        Box::pin(async move {
            // 建立阶段：先做可失败预检（终止、可见范围、caller Active 由 create_child 内部
            // 覆盖），再建立直接 child Scope；失败时不留孤立 Scope 或部分输入绑定。
            let child = ctx.create_child(caller)?;
            // cfg(test) 只读观测：在真实 `create_child` 成功之后、Import 与 body 之前记录
            // 实际 Scope 身份、parent、调用角色与执行域地址，供 Scope 创建证据与唯一执行域
            // 样本核对；不替换 body、不复制提交路径，也不进入非 test 构建。
            #[cfg(test)]
            super::test_support::boundary_creation_record(
                child.clone(),
                caller.clone(),
                self.role,
                ctx.identity_probe(),
                ctx.coordinator_probe(),
                ctx.container_probe(),
            );
            let imports: Vec<ImportSlot> = ports
                .iter()
                .zip(&self.caller_inputs)
                .map(|(port, source)| {
                    ImportSlot::with_type(
                        source,
                        port.position().clone(),
                        port.expected(),
                        port.expected_name(),
                    )
                })
                .collect();
            if let Err(error) = ctx.import_batch(&child, caller, &imports) {
                ctx.terminate(
                    TerminationKind::BodyError,
                    Some(child.clone()),
                    "orchestrator input assembly failed",
                    Some(error.clone()),
                );
                if let Err(cleanup_error) = ctx.abort(&child) {
                    ctx.record_cleanup_failure(child.clone(), cleanup_error);
                }
                return Err(BodyError::from(error));
            }

            let mut guard = ctx
                .enter(InvocationKind::Boundary, &child, true)
                .expect("orchestrator boundary entry after its pre-checks cannot fail");
            let outcome = run_orchestrator_body::<O, I, K>(&mut guard, &child, self, inner).await;
            match outcome {
                Ok(()) => {
                    // cfg(test) 只读观测：提交前记录 child 的本地绑定与责任集合，供整组
                    // Export 失败时的转移／清理证据核对；不改变提交路径。
                    #[cfg(test)]
                    if let Ok((refs, owned)) = guard.snapshot_probe(&child) {
                        super::test_support::export_attempt_record(child.clone(), refs, owned);
                    }
                    let exported = export_orchestrator_outputs(
                        &mut guard,
                        &child,
                        inner,
                        &self.caller_outputs,
                    );
                    match exported {
                        Ok(()) => {
                            guard.release_responsibility(&child);
                            guard.complete();
                            Ok(child)
                        }
                        Err(error) => {
                            guard.failed_with(&error);
                            Err(error)
                        }
                    }
                }
                Err(error) => {
                    guard.failed_with(&error);
                    Err(error)
                }
            }
        })
    }
}

/// Root 装配入口：在真实 RootScope 上直接运行完成态 Orchestrator。
///
/// 与 nested 调用边界不同，它**不**建立 child Scope、不做 Import／Export：
/// - Root 输入由 `Runtime::execute` 用正式 `register_owned` 绑定到 RootScope；
/// - Root 的声明输出由 Root 自身 Step／child 调用边界在 body 运行期间绑定到 RootScope；
/// - 本入口只做擦除后的 pack 防御校验，再按 `OrchCall::run` 运行编排体。
///
/// `OrchScope` 字段保持模块私有：装配与借用只在这里发生，`runtime.rs` 不直接构造它。
pub(crate) async fn run_root_call<'a, O, I, K>(
    ctx: &'a mut ExecutionContext,
    root: &'a O,
    root_scope: &'a ScopeId,
) -> Result<(), BodyError>
where
    O: OrchCall<I, K>,
    I: 'static + InputTypes,
    K: OutKind,
{
    let inner = root.definition();
    let pack: O::Pack = <O::Pack as PackFromPorts>::from_ports(inner.inputs())?;
    {
        // pack 里是本次 RootScope 的声明输入位置：走与 nested 相同的
        // "Scope 访问关系与容器归属 → 存活 → 实际 TypeId" 校验。
        let shared: &ExecutionContext = ctx;
        pack.validate(shared, root_scope)?;
    }
    let scope = OrchScope::<O::Pack, K> {
        ctx,
        child: root_scope,
        pack: &pack,
        inner,
        marker: PhantomData,
    };
    root.run(scope).await
}

/// 编排体运行：先做擦除后的防御校验，再在自身 Scope 中运行 body。
async fn run_orchestrator_body<O, I, K>(
    guard: &mut super::context::InvocationGuard<'_>,
    child: &ScopeId,
    site: &OrchSite<O, I, K>,
    inner: &Definition,
) -> Result<(), BodyError>
where
    O: OrchCall<I, K>,
    I: 'static + InputTypes,
    K: OutKind,
{
    // pack：按已声明 Signature 还原为确切的私有 Targets… 类型。
    #[cfg(test)]
    let injected = site.pack_override.borrow_mut().take();
    #[cfg(test)]
    let pack_box: Box<dyn Any> = match injected {
        Some(pack) => pack,
        None => Box::new(<O::Pack as PackFromPorts>::from_ports(inner.inputs())?),
    };
    #[cfg(not(test))]
    let pack_box: Box<dyn Any> = Box::new(<O::Pack as PackFromPorts>::from_ports(inner.inputs())?);
    let pack = pack_box
        .downcast_ref::<O::Pack>()
        .ok_or_else(|| BodyError::new("orchestrator input pack type mismatch"))?;

    // 逐输入位置验证 Scope 访问关系与容器归属 → 存活 → 实际 TypeId。
    {
        let shared: &ExecutionContext = guard;
        pack.validate(shared, child)?;
    }

    let scope = OrchScope::<O::Pack, K> {
        ctx: guard,
        child,
        pack,
        inner,
        marker: PhantomData,
    };
    site.orchestrator.run(scope).await
}

/// 嵌套 Export 的 cfg(test) 故障：只改操作前元数据，预检／提交仍走生产路径。
///
/// R11-12：目标按**完整 child ScopeId**选择。样本先把执行推进到目标边界已存在的
/// Pending 现场，用 `boundary_creation_snapshot` 取得真实 `ScopeId` 后再安装；命中时
/// 记录 child／caller 完整身份（`ExportFaultHit`），样本必须断言命中，未命中的注入
/// 不能作为证据。
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) enum ExportFault {
    /// 在整组 Export 预检之前，用正式 `register_owned` 预占指定 child 的 caller 第
    /// `index` 个输出位置。
    OccupyCallerSlot {
        /// 目标 child Scope（完整身份）。
        child: ScopeId,
        /// 被预占的 caller 输出位置下标。
        index: usize,
    },
    /// 把指定 child 即将 Export 的 CollectionItem 目标 cap 收紧为 child 自身 Scope：
    /// 目的（caller）必然在 cap 之外 → 触发真实 Export 目的端检查。
    TightenItemCap {
        /// 目标 child Scope（完整身份）。
        child: ScopeId,
    },
}

#[cfg(test)]
thread_local! {
    static EXPORT_FAULT: std::cell::RefCell<Option<ExportFault>> =
        const { std::cell::RefCell::new(None) };
}

/// 安装一次嵌套 Export 故障（只生效一次；未消费前再次安装视为夹具错误）。
#[cfg(test)]
pub(crate) fn install_export_fault(fault: ExportFault) {
    EXPORT_FAULT.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "a previous export fault was never consumed: {slot:?}"
        );
        *slot = Some(fault);
    });
}

/// 取出当前 child 匹配的故障；只按完整 `ScopeId` 与槽位前提判断，不做角色／次数推断。
#[cfg(test)]
fn take_export_fault(child: &ScopeId, slot_count: usize) -> Option<ExportFault> {
    EXPORT_FAULT.with(|slot| {
        let mut borrowed = slot.borrow_mut();
        let matched = match borrowed.as_ref() {
            Some(ExportFault::OccupyCallerSlot {
                child: target,
                index,
            }) => target == child && slot_count > *index,
            Some(ExportFault::TightenItemCap { child: target }) => target == child,
            None => false,
        };
        if matched { borrowed.take() } else { None }
    })
}

/// 整组输出预检与提交：child-local 声明端口 → caller 输出位置。
fn export_orchestrator_outputs(
    guard: &mut super::context::InvocationGuard<'_>,
    child: &ScopeId,
    inner: &Definition,
    caller_outputs: &[RefId],
) -> Result<(), BodyError> {
    let ports = inner.output_ports();
    #[cfg(test)]
    {
        // R11-12：目标按**完整 child ScopeId**选择；命中后记录 child／caller 完整身份，
        // 注入失败（未命中）由样本的 hit 断言捕获，不在这里静默跳过。
        if let Some(fault) = take_export_fault(child, caller_outputs.len()) {
            let caller = guard
                .parent_scope_probe(child)
                .expect("caller lookup for the export fault")
                .expect("export child must have a caller");
            match fault {
                ExportFault::OccupyCallerSlot { index, .. } => {
                    let position = caller_outputs
                        .get(index)
                        .expect("export fault caller slot index");
                    let anchor_position =
                        super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
                            .allocate()
                            .expect("fresh anchor position");
                    let anchor = guard
                        .register_owned(child, &anchor_position, 9u8)
                        .expect("export fault anchor registration");
                    guard.inject_scope_target_probe(
                        &caller,
                        position,
                        super::scope::RefTarget::Data(anchor),
                    );
                    super::test_support::record_export_fault_hit(
                        super::test_support::ExportFaultHit {
                            kind: "occupy-caller-slot",
                            child: child.clone(),
                            caller,
                            index: Some(index),
                        },
                    );
                }
                ExportFault::TightenItemCap { .. } => {
                    // 把 child 中即将 Export 的 CollectionItem 目标 cap 收紧为 child 自身：
                    // 目的（caller）在 cap 之外，必须由真实 Export 目的端检查拒绝。
                    let (refs, _) = guard
                        .snapshot_targets_probe(child)
                        .expect("child refs for the cap fault");
                    let mut tightened = false;
                    for (position, target) in refs {
                        if matches!(target, super::scope::TargetSnapshot::CollectionItem { .. }) {
                            guard.replace_item_cap_probe(child, &position, child);
                            tightened = true;
                        }
                    }
                    assert!(
                        tightened,
                        "tighten-item-cap fault requires a bound CollectionItem in the child"
                    );
                    super::test_support::record_export_fault_hit(
                        super::test_support::ExportFaultHit {
                            kind: "tighten-item-cap",
                            child: child.clone(),
                            caller,
                            index: None,
                        },
                    );
                }
            }
        }
    }
    if ports.len() != caller_outputs.len() {
        return Err(BodyError::new(
            "orchestrator output arity does not match the wiring signature",
        ));
    }
    let declared: Vec<RefId> = ports.iter().map(|port| port.position().clone()).collect();
    let mut slots: Vec<ExportSlot> = ports
        .iter()
        .zip(caller_outputs)
        .map(|(port, caller)| {
            ExportSlot::with_type(
                port.position(),
                caller.clone(),
                port.expected(),
                port.expected_name(),
            )
        })
        .collect();
    guard.finalize(child, &declared, &mut slots)?;
    Ok(())
}
