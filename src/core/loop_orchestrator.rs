//! Loop：Retry／Iter 两种推进策略，在真实 RoundScope 上完成推进与收口。
//!
//! 内部形态（V21-09 交付，尚不是公开 API）：
//!
//! - Loop 自身 Definition 按形状声明输入（Retry 单／双业务输入；Iter 初始状态＋可选
//!   shared），并声明最终 `Data<O>`／`Data<S>` 输出端口。
//! - body 一律登记为**完成态包装 Flow** 内的唯一 Step：包装外层恰一个 Step、声明输出恰
//!   一个位置；该 Step 经既有 CallSite 分派 Node（Round leaf）或 Orchestrator（普通 child
//!   并 Export 到包装声明的 Round-local 输出位置）。包装本身不另建 Scope。
//! - 每轮建立 RoundScope（LoopScope 的直接 child），按策略装配输入：Retry 每轮重新导入
//!   原始输入；Iter 通过 `StateImportSlot` 导入当前控制状态并另行导入 shared。
//! - 本轮正常 Output 的既有字段给出二值控制（[`LoopControl`]）：Continue 时 Retry 丢弃
//!   本轮结果（discard），Iter 把选定 S 经窄许可 Promote 到 current-state；Finish 时把
//!   选定值 Promote 到控制状态，再一次性绑定 Loop 的最终声明输出位置，由上层调用边界
//!   Export 给 caller。同一 `DataId` 的替换不销毁、不新增责任。
//!
//! 失败／取消：body、装配、收口与最终绑定的首次原因都不被后续清理诊断覆盖；错误停止后续
//! 轮次、不返回上一轮状态、不自动技术重试。

use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use super::builder::{BuildSite, Definition, IntoCallSite, TypedCallBuilder};
use super::context::{
    BodyError, ExecutionContext, InvocationGuard, InvocationKind, RoundCollectPermit,
};
use super::data_ref::DataRef;
use super::flow::{Flow, FlowBuilder, FlowInputs};
use super::identity::ScopeId;
use super::internal_error::ScopeError;
use super::orchestrator::{
    LoopScopeTransfer, OrchCall, OrchScope, PackFor, PackFromPorts, ScopeRole, Targets1, Targets2,
};
use super::ref_id::RefId;
use super::scope::{
    ControlStateId, DiscardOutcome, ImportSlot, PromoteOutcome, ScopeState, StateImportSlot,
};
use super::signature::{BuildError, Data, DeclaredPort, InputTypes, NodeFut, WireInputs, Wiring};

// ---------------------------------------------------------------- reader（二值控制）

/// 本轮推进决定：正常 Output 已表达的业务结论，只有两种。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoopDecision {
    /// 继续下一轮。
    Continue,
    /// 结束 Loop，把选定值作为最终结果。
    Finish,
}

/// reader：由正常 Output 的类型自身表达 Continue／Finish。
///
/// 只读取**已计算好的字段**，不取得 Context、DataRef、target、Scope、owned 值或异步 child
/// 调用权；直接字段读取不构成字段级 DataRef，也不产生新的业务 Data。
pub(crate) trait LoopControl: 'static {
    /// 本轮结论。
    fn loop_decision(&self) -> LoopDecision;
}

// ---------------------------------------------------------------- cfg(test) 故障注入

/// Loop 的 cfg(test) 故障：只影响下一次真实 Round 收口，不构成生产能力。
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoopFault {
    /// 收口前在 Round 内登记并立即销毁一份 owned 值：触发"剩余 owned 清理前提"失败。
    CollectCleanupPrecondition,
    /// 收口时先走**通用** `Context::promote`（真实 Round frame 请求 parent state），
    /// 记录其诊断后再走窄许可；用于越权运行期断言。
    GenericPromoteProbe,
    /// body 错误分支：`failed_with` 之后尝试一次普通业务提交与一次受控清理。
    ProbeAfterBodyError,
    /// 回收前把 Loop Scope 置为 Finalizing：触发受控回收的前置拒绝。
    RecycleNotActive,
    /// 最终绑定前预占 Loop 的最终声明输出位置：触发位置已绑定拒绝。
    FinalPositionOccupied,
    /// 登记 Iter 的 current-state 之前，把 Loop 的初始状态输入位置损坏为 item 目标
    /// （错**元素类型**）：触发真实新入口 `register_state` 的类型拒绝。
    CorruptStateInputType,
    /// 同上，但只损坏**下标**（元素类型保持正确），验证另一条判据不被前者遮住。
    CorruptStateInputIndex,
    /// 把 current-state 的 item cap 换成指定 Scope（Closed／cap 外反例）。
    StateCapClosed,
    /// 同上，但换成非祖先 Scope（cap 外）。
    StateCapOutside,
    /// 把许可里的 state 换成另一 Execution 的 state（foreign Execution 反例）。
    ForeignState,
    /// 许可错 round／parent／selected 的逐分支探针（都在提交前拒绝）。
    PermitProbeWrongRound,
    PermitProbeWrongParent,
    PermitProbeWrongSelected,
    /// 最终绑定前把控制状态改回未初始化（最终绑定前置反例）。
    FinalStateUninitialized,
    /// 最终绑定前写入坏 state metadata（声明类型）。
    FinalStateBadMetadata,
    /// 回收前把一个存活旧值绑定为 Loop 的本地 alias：验证 pending 延迟回收。
    DelayedRecycleAlias,
    /// 收口前销毁被选输出本身：`prepare_promote` 的 target 存活判据拒绝，且清理成功后来源
    /// 会真实 Closed（Closed 分支的 guard 处置）。
    PromoteSelectedDestroyed,
    /// 收口前把控制状态的声明类型改成不匹配：`prepare_promote` 的类型判据拒绝。
    PromoteStateTypeMismatch,
    /// 收口前把被选 item 的 cap 换成本次 Round 自身：目的（Loop）不在 cap 内 → 目的侧拒绝。
    PromoteDestCapOutside,
    /// 最终绑定后把 Loop 声明输出位置上的 item cap 换成 caller 的兄弟 Scope：
    /// Export 的**目的**检查（caller 不在 cap 内）拒绝。
    ExportDestCapOutside,
    /// 许可里的 state 换成登记在本次 Round 上的状态：state owner 分支拒绝。
    PermitProbeWrongStateOwner,
    /// 通用 promote 探针的完整前后快照（含 parent refs／owned、state target／pending、序号）。
    GenericPromoteFullProbe,
}

#[cfg(test)]
thread_local! {
    static LOOP_FAULT: std::cell::Cell<Option<LoopFault>> = const { std::cell::Cell::new(None) };
}

/// 安装下一次 Round 收口的故障（只生效一次）。
#[cfg(test)]
pub(crate) fn install_loop_fault(fault: LoopFault) {
    LOOP_FAULT.with(|slot| slot.set(Some(fault)));
}

#[cfg(test)]
thread_local! {
    static FOREIGN_STATE: std::cell::RefCell<Option<ControlStateId>> =
        const { std::cell::RefCell::new(None) };
}

/// 安装另一个 Execution 的 ControlStateId（foreign Execution 反例用）。
#[cfg(test)]
pub(crate) fn install_foreign_state_probe(state: ControlStateId) {
    FOREIGN_STATE.with(|slot| *slot.borrow_mut() = Some(state));
}

#[cfg(test)]
fn foreign_state_probe() -> Option<ControlStateId> {
    FOREIGN_STATE.with(|slot| slot.borrow().clone())
}

#[cfg(test)]
fn take_loop_fault() -> Option<LoopFault> {
    LOOP_FAULT.with(|slot| slot.replace(None))
}

// ---------------------------------------------------------------- 输入形状与策略

/// Loop 的推进策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoopStrategy {
    /// 下一轮仍使用原始输入；完成时保留到最终结果状态。
    Retry,
    /// 继续与完成都保留选定状态到 current-state。
    Iter,
}

/// Retry：单业务输入 `A`，body 产生 `Data<O>`。
pub(crate) struct Retry1<A, O>(PhantomData<fn() -> (A, O)>);
/// Retry：两个业务输入 `(A, B)`，body 产生 `Data<O>`。
#[allow(clippy::type_complexity)] // 形状标记：只承载类型，不承载数据
pub(crate) struct Retry2<A, B, O>(PhantomData<fn() -> (A, B, O)>);
/// Iter：初始状态 `S`，body 产生 `Data<S>`。
pub(crate) struct Iter1<S>(PhantomData<fn() -> S>);
/// Iter：初始状态 `S` ＋ 每轮显式导入的 shared `X`，body 产生 `Data<S>`。
pub(crate) struct Iter2<S, X>(PhantomData<fn() -> (S, X)>);

mod sealed {
    /// 形状标记只由本模块给出，业务侧不能新增 Loop 形状。
    pub trait Sealed {}
}

impl<A: 'static, O: 'static> sealed::Sealed for Retry1<A, O> {}
impl<A: 'static, B: 'static, O: 'static> sealed::Sealed for Retry2<A, B, O> {}
impl<S: 'static> sealed::Sealed for Iter1<S> {}
impl<S: 'static, X: 'static> sealed::Sealed for Iter2<S, X> {}

/// Loop 自身输入声明的形状相关部分。
pub(crate) trait LoopInputs {
    /// 构建方拿到的输入位置句柄。
    type Handles;

    /// 在给定 Definition 上声明 Loop 的输入位置。
    fn declare_loop_inputs(definition: &mut Definition) -> Result<Self::Handles, BuildError>;
}

impl<A: 'static> LoopInputs for (A,) {
    type Handles = DataRef<A>;

    fn declare_loop_inputs(definition: &mut Definition) -> Result<Self::Handles, BuildError> {
        definition.declare_input::<A>("loop input")
    }
}

impl<A: 'static, B: 'static> LoopInputs for (A, B) {
    type Handles = (DataRef<A>, DataRef<B>);

    fn declare_loop_inputs(definition: &mut Definition) -> Result<Self::Handles, BuildError> {
        Ok((
            definition.declare_input::<A>("loop input")?,
            definition.declare_input::<B>("loop input")?,
        ))
    }
}

/// Loop 形状：把 Loop 自身 Signature、输入 pack、包装输入、被推进的值与策略绑在一起。
///
/// `I`／`Wrapper`／`Value` 三者的关系是本任务的类型约束：Retry 的 `I` 与 `Value` 可以不同；
/// Iter 的输出就是关联的 `S`，不能先擦除再在运行时猜测。
pub(crate) trait LoopShape: 'static + sealed::Sealed
where
    <<Self as LoopShape>::Wrapper as FlowInputs>::Handles: WireInputs + Clone,
    <<Self as LoopShape>::Wrapper as FlowInputs>::Pack: PackFor<<Self as LoopShape>::Wrapper>,
{
    /// Loop 自身输入 Signature。
    type I: 'static + InputTypes + LoopInputs;
    /// Loop 输入 pack。
    type Pack: PackFor<Self::I> + PackFromPorts;
    /// 包装 body 的输入 Signature（与 Loop 输入一一对应）。
    type Wrapper: 'static + FlowInputs + InputTypes;
    /// 被推进的值：Retry 为 `O`，Iter 为 `S`。
    type Value: 'static + LoopControl;

    /// 推进策略。
    const STRATEGY: LoopStrategy;

    /// 包装 Definition 的输入位置数。
    fn wrapper_inputs() -> usize;

    /// 包装 Definition 各输入位置的声明类型（按顺序）。
    fn wrapper_input_types() -> Vec<TypeId>;

    /// 把 Loop 的输入／当前状态装配到本轮 Round 的包装输入位置。
    fn bind_round_inputs(
        ctx: &mut ExecutionContext,
        loop_scope: &ScopeId,
        round: &ScopeId,
        loop_inputs: &[DeclaredPort],
        wrapper: &Definition,
        current: Option<&ControlStateId>,
    ) -> Result<(), ScopeError>;
}

/// Retry 装配：每轮重新从 Loop 的原始输入位置导入同一批目标。
fn import_retry_inputs(
    ctx: &mut ExecutionContext,
    loop_scope: &ScopeId,
    round: &ScopeId,
    loop_inputs: &[DeclaredPort],
    wrapper: &Definition,
) -> Result<(), ScopeError> {
    let slots: Vec<ImportSlot> = loop_inputs
        .iter()
        .zip(wrapper.inputs())
        .map(|(source, port)| {
            ImportSlot::with_type(
                source.position(),
                port.position().clone(),
                port.expected(),
                port.expected_name(),
            )
        })
        .collect();
    ctx.import_batch(round, loop_scope, &slots)
}

/// Iter 装配：StateImportSlot 导入当前状态，可选 shared 另行显式导入。
fn import_iter_inputs<S: 'static>(
    ctx: &mut ExecutionContext,
    loop_scope: &ScopeId,
    round: &ScopeId,
    loop_inputs: &[DeclaredPort],
    wrapper: &Definition,
    current: Option<&ControlStateId>,
) -> Result<(), ScopeError> {
    let Some(current) = current else {
        return Err(ScopeError::Invariant {
            violated: "iter round requires the registered current state",
        });
    };
    let state_port = wrapper
        .inputs()
        .first()
        .expect("wrapper declares the state input");
    let from_state = [StateImportSlot::new::<S>(current, state_port.position())];
    let shared: Vec<ImportSlot> = loop_inputs
        .iter()
        .skip(1)
        .zip(wrapper.inputs().iter().skip(1))
        .map(|(source, port)| {
            ImportSlot::with_type(
                source.position(),
                port.position().clone(),
                port.expected(),
                port.expected_name(),
            )
        })
        .collect();
    ctx.import_batch_with_states(round, loop_scope, &shared, &from_state)
}

impl<A: 'static, O: 'static + LoopControl> LoopShape for Retry1<A, O> {
    type I = (A,);
    type Pack = Targets1<A>;
    type Wrapper = (A,);
    type Value = O;
    const STRATEGY: LoopStrategy = LoopStrategy::Retry;

    fn wrapper_inputs() -> usize {
        1
    }

    fn wrapper_input_types() -> Vec<TypeId> {
        vec![TypeId::of::<A>()]
    }

    fn bind_round_inputs(
        ctx: &mut ExecutionContext,
        loop_scope: &ScopeId,
        round: &ScopeId,
        loop_inputs: &[DeclaredPort],
        wrapper: &Definition,
        _current: Option<&ControlStateId>,
    ) -> Result<(), ScopeError> {
        import_retry_inputs(ctx, loop_scope, round, loop_inputs, wrapper)
    }
}

impl<A: 'static, B: 'static, O: 'static + LoopControl> LoopShape for Retry2<A, B, O> {
    type I = (A, B);
    type Pack = Targets2<A, B>;
    type Wrapper = (A, B);
    type Value = O;
    const STRATEGY: LoopStrategy = LoopStrategy::Retry;

    fn wrapper_inputs() -> usize {
        2
    }

    fn wrapper_input_types() -> Vec<TypeId> {
        vec![TypeId::of::<A>(), TypeId::of::<B>()]
    }

    fn bind_round_inputs(
        ctx: &mut ExecutionContext,
        loop_scope: &ScopeId,
        round: &ScopeId,
        loop_inputs: &[DeclaredPort],
        wrapper: &Definition,
        _current: Option<&ControlStateId>,
    ) -> Result<(), ScopeError> {
        import_retry_inputs(ctx, loop_scope, round, loop_inputs, wrapper)
    }
}

impl<S: 'static + LoopControl> LoopShape for Iter1<S> {
    type I = (S,);
    type Pack = Targets1<S>;
    type Wrapper = (S,);
    type Value = S;
    const STRATEGY: LoopStrategy = LoopStrategy::Iter;

    fn wrapper_inputs() -> usize {
        1
    }

    fn wrapper_input_types() -> Vec<TypeId> {
        vec![TypeId::of::<S>()]
    }

    fn bind_round_inputs(
        ctx: &mut ExecutionContext,
        loop_scope: &ScopeId,
        round: &ScopeId,
        loop_inputs: &[DeclaredPort],
        wrapper: &Definition,
        current: Option<&ControlStateId>,
    ) -> Result<(), ScopeError> {
        import_iter_inputs::<S>(ctx, loop_scope, round, loop_inputs, wrapper, current)
    }
}

impl<S: 'static + LoopControl, X: 'static> LoopShape for Iter2<S, X> {
    type I = (S, X);
    type Pack = Targets2<S, X>;
    type Wrapper = (S, X);
    type Value = S;
    const STRATEGY: LoopStrategy = LoopStrategy::Iter;

    fn wrapper_inputs() -> usize {
        2
    }

    fn wrapper_input_types() -> Vec<TypeId> {
        vec![TypeId::of::<S>(), TypeId::of::<X>()]
    }

    fn bind_round_inputs(
        ctx: &mut ExecutionContext,
        loop_scope: &ScopeId,
        round: &ScopeId,
        loop_inputs: &[DeclaredPort],
        wrapper: &Definition,
        current: Option<&ControlStateId>,
    ) -> Result<(), ScopeError> {
        import_iter_inputs::<S>(ctx, loop_scope, round, loop_inputs, wrapper, current)
    }
}

// ---------------------------------------------------------------- 完成态

/// 完成态 Loop：不可变 Definition、登记的包装 Flow 与登记元数据。
pub(crate) struct Loop<Sh: LoopShape> {
    definition: Arc<Definition>,
    /// 登记的 body 包装：Node 与 Orchestrator body 都作为其唯一 Step。
    wrapper: Flow<Sh::Wrapper, Data<Sh::Value>>,
    /// 登记时捕获的包装 Definition 对象身份。
    wrapper_identity: *const (),
    /// 登记时捕获的包装声明输入端口。
    wrapper_inputs: Vec<DeclaredPort>,
    /// 登记时捕获的包装声明输出端口。
    wrapper_outputs: Vec<DeclaredPort>,
    /// 登记时捕获的包装 Step 调用点身份。
    wrapper_step_identity: *const (),
    /// Loop 最终声明输出位置。
    final_position: RefId,
    marker: PhantomData<fn() -> Sh>,
}

impl<Sh: LoopShape> Clone for Loop<Sh> {
    /// `Clone` 只复制完成态句柄：共享同一不可变 Definition 与登记包装，**不复制任何
    /// 业务 Data 或运行状态**；同一 Loop 可复用于多个调用位置。
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
            wrapper: self.wrapper.clone(),
            wrapper_identity: self.wrapper_identity,
            wrapper_inputs: self.wrapper_inputs.clone(),
            wrapper_outputs: self.wrapper_outputs.clone(),
            wrapper_step_identity: self.wrapper_step_identity,
            final_position: self.final_position.clone(),
            marker: PhantomData,
        }
    }
}

impl<Sh: LoopShape> std::fmt::Debug for Loop<Sh> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 只报告定义规模：完成态不携带业务值，也不要求形状类型实现 `Debug`。
        formatter
            .debug_struct("Loop")
            .field("strategy", &Sh::STRATEGY)
            .field("steps", &self.wrapper.definition().steps().len())
            .field("outputs", &self.wrapper.definition().output_ports().len())
            .finish()
    }
}

impl<Sh: LoopShape> Loop<Sh> {
    /// 登记的包装 Definition（只读；供身份核对与观察）。
    pub(crate) fn wrapper_definition(&self) -> &Definition {
        self.wrapper.definition()
    }

    /// 登记的包装 Flow（只读；只由 Loop 专用转交使用）。
    pub(crate) fn registered_wrapper(&self) -> &Flow<Sh::Wrapper, Data<Sh::Value>> {
        &self.wrapper
    }

    /// 登记包装的唯一声明输出位置：本轮 selected 只能取该位置。
    pub(crate) fn wrapper_output_position(&self) -> &RefId {
        self.wrapper_outputs
            .first()
            .expect("registered loop wrapper declares exactly one output")
            .position()
    }

    /// Loop 最终输出位置（只读观察用）。
    pub(crate) fn final_position(&self) -> &RefId {
        &self.final_position
    }

    /// 执行前核对：实际包装对象与登记身份／声明一致。
    ///
    /// 保证运行期不会执行"同类型的另一个包装"，也不接受陈旧或外来 ports／Step 表；篡改
    /// 只能经 test-only 元数据故障构造。
    pub(crate) fn verify_registration(&self) -> Result<(), BodyError> {
        let live = self.wrapper.definition();
        if live as *const Definition as *const () != self.wrapper_identity {
            return Err(BodyError::new(
                "registered loop body is not the registered wrapper definition",
            ));
        }
        let inputs = live.inputs();
        if inputs.len() != self.wrapper_inputs.len()
            || inputs
                .iter()
                .zip(&self.wrapper_inputs)
                .any(|(live, recorded)| {
                    live.position() != recorded.position() || live.expected() != recorded.expected()
                })
        {
            return Err(BodyError::new(
                "registered loop body inputs do not match the registered wrapper",
            ));
        }
        if inputs.len() != Sh::wrapper_inputs()
            || inputs
                .iter()
                .zip(Sh::wrapper_input_types())
                .any(|(port, expected)| port.expected() != expected)
        {
            return Err(BodyError::new(
                "registered loop body inputs do not match the loop shape",
            ));
        }
        let outputs = live.output_ports();
        if outputs.len() != 1 {
            return Err(BodyError::new(
                "registered loop body must declare exactly one output",
            ));
        }
        if outputs.len() != self.wrapper_outputs.len()
            || outputs
                .iter()
                .zip(&self.wrapper_outputs)
                .any(|(live, recorded)| {
                    live.position() != recorded.position() || live.expected() != recorded.expected()
                })
        {
            return Err(BodyError::new(
                "registered loop body outputs do not match the registered wrapper",
            ));
        }
        let steps = live.steps();
        if steps.len() != 1
            || steps[0].site() as *const _ as *const () != self.wrapper_step_identity
        {
            return Err(BodyError::new(
                "registered loop body step is not the registered wrapper step",
            ));
        }
        Ok(())
    }
}

impl<Sh: LoopShape> OrchCall<Sh::I, Data<Sh::Value>> for Loop<Sh>
where
    Sh::I: InputTypes,
{
    type Pack = Sh::Pack;
    const ROLE: ScopeRole = ScopeRole::Loop;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(&'a self, scope: OrchScope<'a, Self::Pack, Data<Sh::Value>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            // 受控转交：核对本次实际 Definition 就是本 Loop，并核对登记包装后在内部
            // 建立会话；不把可变 Context、包装或最终位置交给调用方。
            let session = LoopScopeTransfer::begin_loop_session(scope, self)?;
            run_loop(session).await
        })
    }
}

// ---------------------------------------------------------------- 运行主体

/// 一轮的结果：继续下一轮，或已准备最终输出。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundStep {
    Continue,
    Finish,
}

/// Loop 的运行主体：登记控制状态后按轮推进，直到某轮给出 Finish。
async fn run_loop<Sh: LoopShape>(mut session: LoopSession<'_, Sh>) -> Result<(), BodyError> {
    let state = session.register_control_state()?;
    loop {
        match session.run_round(&state).await? {
            RoundStep::Continue => {}
            RoundStep::Finish => return Ok(()),
        }
    }
}

/// 受控 Loop 会话：绑定本 Loop Definition、LoopScope、登记包装与控制状态。
///
/// 只暴露 `register_control_state` 与 `run_round`；轮次循环与停止决定留在 Loop 实现内，
/// 不向调用者暴露 `ctx_mut`／Container／owned 注入／任意 target／任意 state 提交入口。
pub(crate) struct LoopSession<'a, Sh: LoopShape> {
    ctx: &'a mut ExecutionContext,
    /// Loop 调用的 Scope（控制状态 owner 与 Round 的直接 parent）。
    loop_scope: &'a ScopeId,
    /// Loop 自身 Definition（声明输入位置）。
    inner: &'a Definition,
    pack: &'a Sh::Pack,
    wrapper: &'a Flow<Sh::Wrapper, Data<Sh::Value>>,
    /// 登记包装的唯一声明输出位置。
    wrapper_output: RefId,
    /// Loop 最终声明输出位置。
    final_position: RefId,
    /// 运行期轮次计数：只用于 cfg(test) 记录，不进入完成态、不进入任何业务判断。
    #[cfg(test)]
    rounds: u32,
    marker: PhantomData<fn() -> Sh>,
}

impl<'a, Sh: LoopShape> LoopSession<'a, Sh> {
    /// 受控构造：只由 `OrchScope` 的 Loop 专用转交调用（调用方拿不到可变 Context）。
    #[allow(clippy::too_many_arguments)] // 内部构造：Context／Scope／Definition／pack／登记包装
    pub(crate) fn new(
        ctx: &'a mut ExecutionContext,
        loop_scope: &'a ScopeId,
        inner: &'a Definition,
        pack: &'a Sh::Pack,
        wrapper: &'a Flow<Sh::Wrapper, Data<Sh::Value>>,
        wrapper_output: RefId,
        final_position: RefId,
    ) -> Self {
        Self {
            ctx,
            loop_scope,
            inner,
            pack,
            wrapper,
            wrapper_output,
            final_position,
            #[cfg(test)]
            rounds: 0,
            marker: PhantomData,
        }
    }

    /// 登记控制状态：Iter 从 Loop 的初始状态输入位置登记 current-state；Retry 登记
    /// 尚未初始化的最终结果状态（首次合法 Promote 即初始化）。
    fn register_control_state(&mut self) -> Result<ControlStateId, BodyError> {
        let loop_scope = self.loop_scope.clone();
        #[cfg(test)]
        super::test_support::record(&format!("loop-scope:{}", loop_scope.seq()));
        let state = match Sh::STRATEGY {
            LoopStrategy::Retry => self
                .ctx
                .register_uninitialized_state::<Sh::Value>(&loop_scope)
                .map_err(BodyError::from),
            LoopStrategy::Iter => {
                let input = self
                    .inner
                    .inputs()
                    .first()
                    .expect("iter loop declares the initial state input")
                    .position()
                    .clone();
                #[cfg(test)]
                if let Some(fault) = take_loop_fault() {
                    match fault {
                        LoopFault::CorruptStateInputType => {
                            // 只损坏元素声明类型（下标保持合法）：类型判据必须独立拒绝。
                            let access = super::scope::ItemAccess::for_collection::<u8>();
                            self.ctx
                                .corrupt_item_access_probe(&loop_scope, &input, access, None)
                                .expect("corrupt state input type probe");
                        }
                        LoopFault::CorruptStateInputIndex => {
                            // 只损坏下标（元素类型保持正确）：下标判据必须独立拒绝。
                            let access = super::scope::ItemAccess::for_collection::<Sh::Value>();
                            self.ctx
                                .corrupt_item_access_probe(
                                    &loop_scope,
                                    &input,
                                    access,
                                    Some(usize::MAX),
                                )
                                .expect("corrupt state input index probe");
                        }
                        other => install_loop_fault(other),
                    }
                }
                self.ctx
                    .register_state::<Sh::Value>(&loop_scope, &input)
                    .map_err(BodyError::from)
            }
        };
        #[cfg(test)]
        if let Ok(state) = state.as_ref() {
            // 身份包含 Execution identity：跨 Execution 的同序号必须可区分。
            super::test_support::record(&format!("loop-state:{state}"));
        }
        state
    }

    /// 运行一轮：建立 RoundScope → 装配输入 → 驱动登记包装的唯一 Step → 读二值控制 →
    /// 收口（Promote／discard）。
    async fn run_round(&mut self, state: &ControlStateId) -> Result<RoundStep, BodyError> {
        let loop_scope = self.loop_scope.clone();
        let wrapper = self.wrapper.definition();
        #[cfg(test)]
        {
            self.rounds += 1;
            super::test_support::record(&format!("loop-round:{}", self.rounds));
        }
        let round = self
            .ctx
            .create_child(&loop_scope)
            .map_err(BodyError::from)?;
        #[cfg(test)]
        super::test_support::boundary_creation_record(
            round.clone(),
            loop_scope.clone(),
            ScopeRole::Round,
            self.ctx.identity_probe(),
            self.ctx.coordinator_probe(),
            self.ctx.container_probe(),
        );

        // 输入装配发生在 guard 之前：失败必须收回已建立的 Round，不能留下 detached child；
        // 清理失败独立记录，不覆盖装配的首错。
        let current = match Sh::STRATEGY {
            LoopStrategy::Iter => Some(state),
            LoopStrategy::Retry => None,
        };
        if let Err(primary) = Sh::bind_round_inputs(
            self.ctx,
            &loop_scope,
            &round,
            self.inner.inputs(),
            wrapper,
            current,
        ) {
            let cleanup_failure = self.ctx.abort(&round).err();
            if let Some(cleanup) = cleanup_failure {
                self.ctx.record_cleanup_failure(round.clone(), cleanup);
            }
            return Err(BodyError::from(primary));
        }

        // 许可只由本会话的 Round 边界使用：构造点唯一，且不经过任何调用者参数。
        let permit = RoundCollectPermit {
            loop_scope: loop_scope.clone(),
            state: state.clone(),
            round: round.clone(),
            wrapper_output: self.wrapper_output.clone(),
        };

        let mut guard = self
            .ctx
            .enter(InvocationKind::Boundary, &round, true)
            .expect("round boundary entry after its pre-checks cannot fail");
        if let Err(error) = run_wrapper_step(&mut guard, &round, wrapper).await {
            #[cfg(test)]
            let probe_after = take_loop_fault() == Some(LoopFault::ProbeAfterBodyError);
            guard.failed_with(&error);
            #[cfg(test)]
            if probe_after {
                // 终止已写入：普通业务提交必须被拒绝，受控清理仍允许。
                let fresh = super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
                    .allocate()
                    .expect("probe position");
                let business = self.ctx.register_owned::<u8>(&loop_scope, &fresh, 7u8);
                super::test_support::record(&format!("post-fail-business:{business:?}"));
                // 第二个独立入口：普通 commit（finalize 空声明）也必须被拒绝。
                let commit = self
                    .ctx
                    .finalize(&loop_scope, &[], &mut Vec::new())
                    .map_err(|error| format!("{error:?}"));
                super::test_support::record(&format!("post-fail-commit:{commit:?}"));
                let cleanup = self.ctx.abort(&round);
                super::test_support::record(&format!("post-fail-cleanup:{cleanup:?}"));
            }
            return Err(error);
        }
        #[cfg(test)]
        {
            match take_loop_fault() {
                Some(LoopFault::CollectCleanupPrecondition) => {
                    // 保持被选输出有效，但 Round 的 owned 里出现一个已不存在的 entry。
                    let position =
                        super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
                            .allocate()
                            .expect("fault position");
                    if let Ok(id) = guard.register_owned::<u8>(&round, &position, 7u8) {
                        guard.destroy_probe(&id);
                    }
                }
                Some(LoopFault::GenericPromoteFullProbe) => {
                    guard.record_round_collect_before_probe(
                        super::test_support::RoundCollectOperation::Promote,
                        &round,
                        Some(&self.wrapper_output),
                        Some(state),
                    );
                    let outcome = guard.promote(&round, &self.wrapper_output, state);
                    super::test_support::record(&format!("generic-promote:{outcome:?}"));
                    super::test_support::record_generic_promote_probe(outcome.err());
                    guard.record_round_collect_after_reject_probe(
                        super::test_support::RoundCollectOperation::Promote,
                        &round,
                        Some(&self.wrapper_output),
                        Some(state),
                    );
                }
                Some(LoopFault::GenericPromoteProbe) => {
                    let before = guard.snapshot_probe(&round).ok();
                    let before_state = guard.state(&loop_scope).ok();
                    let outcome = guard.promote(&round, &self.wrapper_output, state);
                    super::test_support::record(&format!("generic-promote:{outcome:?}"));
                    let after = guard.snapshot_probe(&round).ok();
                    let after_state = guard.state(&loop_scope).ok();
                    super::test_support::record(&format!(
                        "generic-promote-stable:{}",
                        before == after
                    ));
                    super::test_support::record(&format!(
                        "generic-promote-unfrozen:{}",
                        before_state == after_state && before_state == Some(ScopeState::Active)
                    ));
                }
                Some(fault @ LoopFault::PermitProbeWrongRound)
                | Some(fault @ LoopFault::PermitProbeWrongParent)
                | Some(fault @ LoopFault::PermitProbeWrongSelected)
                | Some(fault @ LoopFault::PermitProbeWrongStateOwner)
                | Some(fault @ LoopFault::ForeignState) => {
                    let bad = match fault {
                        LoopFault::PermitProbeWrongRound => RoundCollectPermit {
                            round: loop_scope.clone(),
                            ..permit.clone()
                        },
                        LoopFault::PermitProbeWrongParent => RoundCollectPermit {
                            loop_scope: round.clone(),
                            ..permit.clone()
                        },
                        LoopFault::PermitProbeWrongStateOwner => {
                            let other = guard
                                .register_uninitialized_state::<u8>(&round)
                                .expect("round state probe");
                            RoundCollectPermit {
                                state: other,
                                ..permit.clone()
                            }
                        }
                        LoopFault::ForeignState => {
                            let foreign = foreign_state_probe().expect("foreign state installed");
                            RoundCollectPermit {
                                state: foreign,
                                ..permit.clone()
                            }
                        }
                        _ => permit.clone(),
                    };
                    let selected = if fault == LoopFault::PermitProbeWrongSelected {
                        wrapper.inputs()[0].position().clone()
                    } else {
                        self.wrapper_output.clone()
                    };
                    let outcome = guard.promote_in_round_boundary(&round, &selected, &bad);
                    super::test_support::record(&format!("permit-probe:{outcome:?}"));
                    let structured = match &outcome {
                        PromoteOutcome::Rejected { primary, .. } => Some(primary.clone()),
                        PromoteOutcome::Promoted => None,
                    };
                    super::test_support::record_generic_promote_probe(structured);
                    let snapshot = guard.snapshot_probe(&round).ok();
                    super::test_support::record(&format!(
                        "permit-probe-round-state:{:?}",
                        guard.state(&round).ok()
                    ));
                    super::test_support::record(&format!(
                        "permit-probe-round-refs:{}",
                        snapshot
                            .as_ref()
                            .map(|(refs, _)| refs.len())
                            .unwrap_or(usize::MAX)
                    ));
                }
                other => {
                    // 本段只消费自己的故障：其余故障留到后面的真实故障点再取。
                    if let Some(fault) = other {
                        install_loop_fault(fault);
                    }
                }
            }
        }

        // 读取本轮正常 Output 已表达的二值控制：结果借用在本块内结束，之后才可变收口。
        let decision = match guard.resolve::<Sh::Value>(&round, &self.wrapper_output) {
            Ok(value) => value.loop_decision(),
            Err(error) => {
                let error = BodyError::from(error);
                guard.failed_with(&error);
                return Err(error);
            }
        };

        #[cfg(test)]
        {
            match take_loop_fault() {
                Some(LoopFault::StateCapClosed) => {
                    // 把**被选位置**的 item cap 换成一个已 Closed 的正式 Scope：Promote 预检
                    // 必须在提交前拒绝（Closed 的 cap 不可能是请求方的祖先）。
                    let closed = guard.create_child(&round).expect("cap probe scope");
                    guard.abort(&closed).expect("close cap probe scope");
                    guard
                        .corrupt_item_cap_probe(&round, &self.wrapper_output, closed)
                        .expect("corrupt selected cap");
                }
                Some(LoopFault::StateCapOutside) => {
                    // 保留到最终绑定前处理（Promote 会用新 target 覆盖旧 cap）。
                    install_loop_fault(LoopFault::StateCapOutside);
                }
                Some(LoopFault::PromoteSelectedDestroyed) => {
                    // 真实销毁被选输出本身：Promote 预检的 target 存活判据必须拒绝。
                    if let Ok(Some(id)) = guard.target_data_id_probe(&round, &self.wrapper_output) {
                        guard.destroy_probe(&id);
                    }
                }
                Some(LoopFault::PromoteDestCapOutside) => {
                    guard
                        .corrupt_item_cap_probe(&round, &self.wrapper_output, round.clone())
                        .expect("corrupt destination cap");
                }
                Some(LoopFault::PromoteStateTypeMismatch) => {
                    guard
                        .corrupt_state_type_probe(state, std::any::TypeId::of::<u8>(), "u8")
                        .expect("corrupt state type");
                }
                Some(fault) => install_loop_fault(fault),
                None => {}
            }
        }
        let retain = !matches!(
            (Sh::STRATEGY, decision),
            (LoopStrategy::Retry, LoopDecision::Continue)
        );
        if retain {
            // 继续（Iter）与完成都保留选定状态到控制状态：同一 `DataId` 不销毁、不新增责任。
            match guard.promote_in_round_boundary(&round, &self.wrapper_output, &permit) {
                PromoteOutcome::Promoted => {
                    guard.release_responsibility(&round);
                    guard.complete();
                    #[cfg(test)]
                    super::test_support::record("loop-collect:promoted");
                    if decision == LoopDecision::Finish {
                        let loop_scope = self.loop_scope.clone();
                        #[cfg(test)]
                        if let Some(fault) = take_loop_fault() {
                            match fault {
                                LoopFault::FinalStateUninitialized => {
                                    self.ctx
                                        .uninitialize_state_probe(state)
                                        .expect("uninitialize state probe");
                                }
                                LoopFault::StateCapOutside => {
                                    let sibling = self
                                        .ctx
                                        .create_child(&loop_scope)
                                        .expect("cap probe sibling");
                                    self.ctx
                                        .corrupt_state_cap_probe(state, sibling)
                                        .expect("corrupt state cap");
                                }
                                LoopFault::FinalStateBadMetadata => {
                                    self.ctx
                                        .corrupt_state_type_probe(
                                            state,
                                            std::any::TypeId::of::<u8>(),
                                            "u8",
                                        )
                                        .expect("corrupt state type probe");
                                }
                                other => install_loop_fault(other),
                            }
                        }
                        #[cfg(test)]
                        if let Some(fault) = take_loop_fault() {
                            if fault == LoopFault::FinalPositionOccupied {
                                // 位置已绑定即可触发拒绝；值类型不参与该前置检查。此处当前
                                // frame 已是 Loop（Round 已关闭），走的正是合法 Loop 入口。
                                self.ctx
                                    .register_owned::<u8>(&loop_scope, &self.final_position, 7u8)
                                    .expect("occupy final position");
                            } else {
                                install_loop_fault(fault);
                            }
                        }
                        #[cfg(test)]
                        self.ctx.record_final_bind_before_probe(
                            &loop_scope,
                            state,
                            &self.final_position,
                        );
                        match self
                            .ctx
                            .bind_state_output(&loop_scope, state, &self.final_position)
                        {
                            Ok(()) => {}
                            Err(error) => {
                                #[cfg(test)]
                                self.ctx.record_final_bind_after_reject_probe(
                                    &loop_scope,
                                    state,
                                    &self.final_position,
                                );
                                return Err(BodyError::from(error));
                            }
                        }
                        #[cfg(test)]
                        if let Some(fault) = take_loop_fault() {
                            if fault == LoopFault::ExportDestCapOutside {
                                // cap 收紧为**实际 LoopScope**：来源（=LoopScope）仍在 cap 内、
                                // cap 存活，但 caller（LoopScope 的 parent）在 cap 外 —— 只有
                                // Export 的**目的端**检查能拒绝它。
                                let out = self.final_position.clone();
                                self.ctx
                                    .corrupt_item_cap_probe(&loop_scope, &out, loop_scope.clone())
                                    .expect("corrupt export destination cap");
                                #[cfg(test)]
                                super::test_support::record("export-cap-fault-applied");
                            } else {
                                install_loop_fault(fault);
                            }
                        }
                        #[cfg(test)]
                        super::test_support::record("loop-finished");
                        return Ok(RoundStep::Finish);
                    }
                    self.recycle_after_round()?;
                    Ok(RoundStep::Continue)
                }
                PromoteOutcome::Rejected {
                    primary,
                    cleanup_failure,
                } => Self::fail_round(guard, &round, primary, cleanup_failure),
            }
        } else {
            // Retry Continue：本轮结果随 Round 结束丢弃，不初始化最终结果状态。
            match guard.discard_in_round_boundary(&round, &permit) {
                DiscardOutcome::Discarded => {
                    guard.release_responsibility(&round);
                    guard.complete();
                    #[cfg(test)]
                    super::test_support::record("loop-collect:discarded");
                    self.recycle_after_round()?;
                    Ok(RoundStep::Continue)
                }
                DiscardOutcome::Rejected {
                    primary,
                    cleanup_failure,
                } => Self::fail_round(guard, &round, primary, cleanup_failure),
            }
        }
    }

    /// 收口拒绝的守卫处置：已关闭→释放责任但以原错终止；未关闭→保留责任由 guard 清理。
    ///
    /// 原错与清理错独立保存；返回原错，不把收口拒绝记成功、不继续下一轮。
    fn fail_round(
        mut guard: InvocationGuard<'_>,
        round: &ScopeId,
        primary: ScopeError,
        cleanup_failure: Option<ScopeError>,
    ) -> Result<RoundStep, BodyError> {
        let closed = guard
            .state(round)
            .map(|state| state == ScopeState::Closed)
            .unwrap_or(false);
        if let Some(cleanup) = cleanup_failure.as_ref() {
            guard.record_cleanup_failure(round.clone(), cleanup.clone());
        }
        #[cfg(test)]
        {
            super::test_support::record(if closed {
                "round-guard:closed"
            } else {
                "round-guard:open"
            });
            super::test_support::record(&format!("round-primary:{primary:?}"));
            super::test_support::record(&format!("round-failure-scope:{}", round.seq()));
            if cleanup_failure.is_some() {
                super::test_support::record("round-cleanup-failure");
            }
            if let Some(saved) = guard.cleanup_report() {
                super::test_support::record(&format!("round-cleanup-report:{saved:?}"));
            }
        }
        if closed {
            guard.release_responsibility(round);
        }
        #[cfg(test)]
        let _ = &mut guard;
        let error = BodyError::from(primary);
        guard.failed_with(&error);
        Err(error)
    }

    /// Round 已关闭后的受控回收：只有仍由本 Loop 负责、无存活引用、无其它控制状态保留的
    /// 旧状态才会被销毁；不制造"无人引用"。
    fn recycle_after_round(&mut self) -> Result<(), BodyError> {
        let loop_scope = self.loop_scope.clone();
        #[cfg(test)]
        if let Some(fault) = take_loop_fault() {
            if fault == LoopFault::RecycleNotActive {
                self.ctx.set_finalizing_probe(&loop_scope)?;
            } else {
                install_loop_fault(fault);
            }
        }
        #[cfg(test)]
        {
            // 延迟回收：把仍存活的待回收旧值绑定为 Loop 的本地 alias，回收条件因此不满足。
            if let Some(fault) = take_loop_fault() {
                if fault == LoopFault::DelayedRecycleAlias {
                    match self
                        .ctx
                        .pending_probe(&loop_scope)
                        .and_then(|pending| pending.into_iter().next())
                    {
                        Some(old) => {
                            let alias_position = super::ref_id::RefIdAllocator::new(
                                super::ref_id::RefIdSource::new(),
                            )
                            .allocate()
                            .expect("alias position");
                            self.ctx
                                .bind_alias_probe(&loop_scope, &alias_position, &old)?;
                            super::test_support::record(&format!("delayed-alias:{:?}", old));
                        }
                        // 还没有待回收旧值：保留故障到下一个真实回收点。
                        None => install_loop_fault(fault),
                    }
                } else {
                    install_loop_fault(fault);
                }
            }
        }
        self.ctx
            .recycle_pending(&loop_scope)
            .map_err(BodyError::from)
    }
}

/// 在 RoundScope 中驱动登记包装的**唯一** Step：Node 走 Leaf、Orchestrator 走 child 边界。
async fn run_wrapper_step(
    guard: &mut InvocationGuard<'_>,
    round: &ScopeId,
    wrapper: &Definition,
) -> Result<(), BodyError> {
    let step = wrapper
        .steps()
        .first()
        .expect("registered loop wrapper declares exactly one step");
    super::builder::run_site(guard, round, step.site()).await
}

// ---------------------------------------------------------------- 构建态

/// Loop 构建态：声明输入／输出、登记 body 包装，但**不实现** Orchestrator 协议。
pub(crate) struct LoopBuilder<Sh: LoopShape> {
    definition: Definition,
    final_position: RefId,
    wrapper_builder: Option<FlowBuilder<Sh::Wrapper>>,
    wrapper_handles: Option<<Sh::Wrapper as FlowInputs>::Handles>,
    produced: Option<DataRef<Sh::Value>>,
    /// 测试注入：被替换的包装端口／Step 身份快照（登记核对样本）。
    #[cfg(test)]
    tampered_outputs: Option<Vec<DeclaredPort>>,
    #[cfg(test)]
    tampered_inputs: Option<Vec<DeclaredPort>>,
    #[cfg(test)]
    tampered_step_identity: Option<*const ()>,
    marker: PhantomData<fn() -> Sh>,
}

impl<Sh: LoopShape> LoopBuilder<Sh> {
    /// 建立 Loop：声明形状输入与最终输出端口，并开始包装 Flow。
    pub(crate) fn start() -> Result<Self, BuildError> {
        let mut definition = Definition::new();
        <Sh::I as LoopInputs>::declare_loop_inputs(&mut definition)?;
        let final_position = definition.declare_output_port::<Sh::Value>("loop output")?;
        let (wrapper_builder, wrapper_handles) = FlowBuilder::<Sh::Wrapper>::start()?;
        Ok(Self {
            definition,
            final_position: final_position.position().clone(),
            wrapper_builder: Some(wrapper_builder),
            wrapper_handles: Some(wrapper_handles),
            produced: None,
            #[cfg(test)]
            tampered_outputs: None,
            #[cfg(test)]
            tampered_inputs: None,
            #[cfg(test)]
            tampered_step_identity: None,
            marker: PhantomData,
        })
    }

    /// 登记 body：作为包装 Definition 内**唯一** Step，输出必须是单份 `Data<Value>`。
    ///
    /// 函数／结构体 Node／`Arc<具体 Node>`／完成态 Flow 都走既有 typed 接线：构建期由
    /// `BuildSite::precheck` 核对 Orchestrator 声明，静态类型由 Marker 绑定。
    ///
    /// 原子性：重复登记在追加 Step／分配 Ref 之前拒绝；可恢复的接线拒绝保留既有构建态与
    /// 第一登记。
    pub(crate) fn then_body<C, M>(&mut self, body: C) -> Result<(), BuildError>
    where
        C: BuildSite<M, <Sh::Wrapper as FlowInputs>::Handles>,
        M: Wiring<BuildOutput = DataRef<Sh::Value>>,
        C: IntoCallSite<M, <Sh::Wrapper as FlowInputs>::Handles, BuildOutput = DataRef<Sh::Value>>,
    {
        if self.produced.is_some() {
            return Err(BuildError::SecondLoopBody);
        }
        let handles = self
            .wrapper_handles
            .clone()
            .expect("loop builder handles stay alive until finish");
        let wrapper = self
            .wrapper_builder
            .as_mut()
            .expect("loop builder stays open until finish");
        let produced: DataRef<Sh::Value> =
            TypedCallBuilder::then::<C, M, _>(wrapper, body, handles)?;
        self.produced = Some(produced);
        Ok(())
    }

    /// 完成：包装 finish 后形成不可变完成态 Loop，并捕获登记身份与声明端口。
    pub(crate) fn finish(mut self) -> Result<Loop<Sh>, BuildError> {
        let produced = self.produced.take().ok_or(BuildError::LoopBodyMissing)?;
        let wrapper = self
            .wrapper_builder
            .take()
            .expect("loop builder is still open")
            .finish::<Data<Sh::Value>, _>(produced)?;
        let wrapper_definition = wrapper.definition();
        if wrapper_definition.steps().len() != 1 || wrapper_definition.output_ports().len() != 1 {
            return Err(BuildError::LoopWrapperShape);
        }
        let wrapper_identity = wrapper_definition as *const Definition as *const ();
        let wrapper_step_identity = wrapper_definition
            .steps()
            .first()
            .expect("loop wrapper has exactly one registered body step")
            .site() as *const _ as *const ();
        #[cfg(test)]
        let wrapper_inputs = self
            .tampered_inputs
            .take()
            .unwrap_or_else(|| wrapper_definition.inputs().to_vec());
        #[cfg(not(test))]
        let wrapper_inputs = wrapper_definition.inputs().to_vec();
        #[cfg(test)]
        let wrapper_outputs = self
            .tampered_outputs
            .take()
            .unwrap_or_else(|| wrapper_definition.output_ports().to_vec());
        #[cfg(not(test))]
        let wrapper_outputs = wrapper_definition.output_ports().to_vec();
        #[cfg(test)]
        let wrapper_step_identity = self
            .tampered_step_identity
            .take()
            .unwrap_or(wrapper_step_identity);
        #[allow(clippy::arc_with_non_send_sync)]
        // 单线程、非 Send 执行模型：只共享不可变 Definition
        let definition = Arc::new(self.definition);
        Ok(Loop {
            definition,
            wrapper,
            wrapper_identity,
            wrapper_inputs,
            wrapper_outputs,
            wrapper_step_identity,
            final_position: self.final_position,
            marker: PhantomData,
        })
    }
}

#[cfg(test)]
impl<Sh: LoopShape> LoopBuilder<Sh> {
    /// 测试观测：包装 Definition 当前 Step 数量（构建失败不留 Step）。
    pub(crate) fn wrapper_step_count_probe(&self) -> Option<usize> {
        self.wrapper_builder
            .as_ref()
            .map(|builder| builder.step_count_probe())
    }

    /// 测试观测：Loop 自身 Definition 已分配的位置数量。
    pub(crate) fn allocated_probe(&self) -> u64 {
        self.definition.allocated_probe()
    }

    /// 测试注入：把登记的包装输入端口快照替换为外来 ports。
    pub(crate) fn tamper_wrapper_inputs_probe(&mut self, ports: Vec<DeclaredPort>) {
        self.tampered_inputs = Some(ports);
    }

    /// 测试注入：把登记的包装输出端口快照替换为外来 ports。
    pub(crate) fn tamper_wrapper_outputs_probe(&mut self, ports: Vec<DeclaredPort>) {
        self.tampered_outputs = Some(ports);
    }

    /// 测试注入：把登记的包装 Step 身份替换为外来调用点。
    pub(crate) fn tamper_wrapper_step_probe(&mut self, identity: *const ()) {
        self.tampered_step_identity = Some(identity);
    }
}

#[cfg(test)]
impl<Sh: LoopShape> LoopSession<'_, Sh> {
    /// 编译期字段见证：字段类型变为其它类型时本函数不再编译。
    #[allow(dead_code)]
    fn session_field_witness(&self) {
        let _: &ExecutionContext = self.ctx;
        let _: &ScopeId = self.loop_scope;
        let _: &Definition = self.inner;
        let _: &Sh::Pack = self.pack;
        let _: &Flow<Sh::Wrapper, Data<Sh::Value>> = self.wrapper;
        let _: &RefId = &self.wrapper_output;
        let _: &RefId = &self.final_position;
        let _: PhantomData<fn() -> Sh> = self.marker;
    }
}
