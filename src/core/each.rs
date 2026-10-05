//! Each：按 `Vec<T>` 顺序逐项调用同一个 body，并把每项**新产生的 owned 输出**直接
//! Consume 进 collector。
//!
//! 内部形态（V21-08 交付，尚不是公开 API）：
//!
//! - Each 自身 Definition 声明集合输入（`Vec<T>`）与可选 shared 输入，以及最终
//!   `Vec<O>` 输出端口。
//! - body 一律登记为**完成态包装 Flow**（`Flow<(T,), Data<O>>` 或 `Flow<(T,S), Data<O>>`）
//!   内的唯一 Step。这样两类 body（Node／Orchestrator）都拥有真实声明的输入／输出端口，
//!   身份与声明从该包装 Definition 捕获，不需要 `NodeSite` 自省。
//! - 执行时按索引顺序建立 ItemScope（EachScope 的直接 child），在 ItemScope 中绑定
//!   `CollectionItem` 目标与 shared alias，然后用**顺序 Step 驱动**执行包装 Definition；
//!   包装本身不另建 Scope，也不走固定 Export 收口。
//! - 每项成功后把包装的声明输出位置（ItemScope 的完整 owned Data）直接 Consume 进
//!   EachScope 的 collector；全部项完成后 `finish_collector` 绑定最终 `Vec<O>` 输出，
//!   再由上层调用边界 Export 给 caller。
//!
//! 失败／取消：body、装配、Consume、finish 与最终 Export 的首次原因都不被后续清理诊断
//! 覆盖；错误停止后续项、不返回部分集合。

use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use super::builder::{BuildSite, Definition, IntoCallSite, TypedCallBuilder};
use super::context::ItemConsumePermit;
use super::context::{BodyError, ExecutionContext, InvocationGuard, InvocationKind};
use super::data_ref::DataRef;
use super::flow::{Flow, FlowBuilder, FlowInputs};
use super::identity::{CollectorId, ScopeId};
use super::internal_error::ScopeError;
use super::orchestrator::{
    EachScopeTransfer, OrchCall, OrchScope, PackFor, PackFromPorts, ScopeRole, Targets1, Targets2,
};
use super::ref_id::RefId;
use super::signature::{Data, DeclaredPort, InputTypes, NodeFut, WireInputs, Wiring};

// ---------------------------------------------------------------- 输入形状

/// 无 shared 的 Each 形状标记：输入 `(Vec<T>,)`，body 输入 `(T,)`。
pub(crate) struct EachOnly<T>(PhantomData<fn() -> T>);

/// 带一个 shared Data 的 Each 形状标记：输入 `(Vec<T>, S)`，body 输入 `(T, S)`。
pub(crate) struct EachShared<T, S>(PhantomData<fn() -> (T, S)>);

/// 无 shared 时包装 Definition 的私有 shared 占位类型。
pub(crate) struct NoShared;

mod sealed {
    /// 形状标记只由本模块给出，业务侧不能新增 Each 形状。
    pub trait Sealed {}
}

impl<T: 'static> sealed::Sealed for EachOnly<T> {}
impl<T: 'static, S: 'static> sealed::Sealed for EachShared<T, S> {}

/// Each 输入形状：把 Each 自身 Signature、输入 pack、集合元素、shared 与包装 body
/// 形状绑在一起，供 `OrchCall` 与构建器共用。
pub(crate) trait EachShape: 'static + sealed::Sealed
where
    <<Self as EachShape>::Wrapper as FlowInputs>::Handles: WireInputs + Clone,
    <<Self as EachShape>::Wrapper as FlowInputs>::Pack: PackFor<<Self as EachShape>::Wrapper>,
{
    /// Each 自身输入 Signature（`(Vec<T>,)` 或 `(Vec<T>, S)`）。
    type I: 'static + InputTypes + EachInputs;
    /// Each 输入 pack（集合 + 可选 shared）。
    type Pack: PackFor<Self::I> + PackFromPorts;
    /// 集合元素类型 `T`。
    type Element: 'static;
    /// shared 类型（无 shared 时为 [`NoShared`]）。
    type Shared: 'static;
    /// 包装 body 的输入 Signature（`(T,)` 或 `(T, S)`）。
    type Wrapper: 'static + FlowInputs + InputTypes;

    /// 包装 Definition 的输入位置数。
    fn wrapper_inputs() -> usize;

    /// 包装 Definition 各输入位置的声明类型（按顺序）。
    fn wrapper_input_types() -> Vec<TypeId>;

    /// 从 Each 输入 pack 解析集合 `&Vec<T>`（共享重借用，调用方负责尽快结束）。
    fn collection<'a>(
        pack: &Self::Pack,
        ctx: &'a ExecutionContext,
        scope: &ScopeId,
    ) -> Result<&'a Vec<Self::Element>, ScopeError>;

    /// 从 Each 输入 pack 解析可选 shared（无 shared 时返回空）。
    fn shared<'a>(
        pack: &Self::Pack,
        ctx: &'a ExecutionContext,
        scope: &ScopeId,
    ) -> Result<Option<&'a Self::Shared>, ScopeError>;

    /// 在 ItemScope 中绑定包装输入：item 目标 +（若有）shared alias。
    fn bind_item_inputs(
        ctx: &mut ExecutionContext,
        each_scope: &ScopeId,
        item_scope: &ScopeId,
        each: &Definition,
        wrapper: &Definition,
        index: usize,
    ) -> Result<(), ScopeError>;
}

impl<T: 'static> EachShape for EachOnly<T> {
    type I = (Vec<T>,);
    type Pack = Targets1<Vec<T>>;
    type Element = T;
    type Shared = NoShared;
    type Wrapper = (T,);

    fn wrapper_inputs() -> usize {
        1
    }

    fn wrapper_input_types() -> Vec<TypeId> {
        vec![TypeId::of::<T>()]
    }

    fn collection<'a>(
        pack: &Self::Pack,
        ctx: &'a ExecutionContext,
        scope: &ScopeId,
    ) -> Result<&'a Vec<T>, ScopeError> {
        pack.first(ctx, scope)
    }

    fn shared<'a>(
        _pack: &Self::Pack,
        _ctx: &'a ExecutionContext,
        _scope: &ScopeId,
    ) -> Result<Option<&'a NoShared>, ScopeError> {
        Ok(None)
    }

    fn bind_item_inputs(
        ctx: &mut ExecutionContext,
        each_scope: &ScopeId,
        item_scope: &ScopeId,
        each: &Definition,
        wrapper: &Definition,
        index: usize,
    ) -> Result<(), ScopeError> {
        let collection_source = each
            .inputs()
            .first()
            .expect("each declares the collection input")
            .position()
            .clone();
        let item_port = wrapper
            .inputs()
            .first()
            .expect("wrapper declares the item input")
            .position()
            .clone();
        ctx.bind_item_input::<T>(
            item_scope,
            each_scope,
            &collection_source,
            &item_port,
            index,
        )
    }
}

impl<T: 'static, S: 'static> EachShape for EachShared<T, S> {
    type I = (Vec<T>, S);
    type Pack = Targets2<Vec<T>, S>;
    type Element = T;
    type Shared = S;
    type Wrapper = (T, S);

    fn wrapper_inputs() -> usize {
        2
    }

    fn wrapper_input_types() -> Vec<TypeId> {
        vec![TypeId::of::<T>(), TypeId::of::<S>()]
    }

    fn collection<'a>(
        pack: &Self::Pack,
        ctx: &'a ExecutionContext,
        scope: &ScopeId,
    ) -> Result<&'a Vec<T>, ScopeError> {
        pack.first(ctx, scope)
    }

    fn shared<'a>(
        pack: &Self::Pack,
        ctx: &'a ExecutionContext,
        scope: &ScopeId,
    ) -> Result<Option<&'a S>, ScopeError> {
        pack.second(ctx, scope).map(Some)
    }

    fn bind_item_inputs(
        ctx: &mut ExecutionContext,
        each_scope: &ScopeId,
        item_scope: &ScopeId,
        each: &Definition,
        wrapper: &Definition,
        index: usize,
    ) -> Result<(), ScopeError> {
        let collection_source = each
            .inputs()
            .first()
            .expect("each declares the collection input")
            .position()
            .clone();
        let item_port = wrapper
            .inputs()
            .first()
            .expect("wrapper declares the item input")
            .position()
            .clone();
        ctx.bind_item_input::<T>(
            item_scope,
            each_scope,
            &collection_source,
            &item_port,
            index,
        )?;
        // shared 是显式 Import 的普通 alias：来源是 EachScope 的 shared 输入位置。
        let shared_source = each
            .inputs()
            .get(1)
            .expect("each declares the shared input")
            .position()
            .clone();
        let shared_port = wrapper
            .inputs()
            .get(1)
            .expect("wrapper declares the shared input")
            .position()
            .clone();
        let slot = super::scope::ImportSlot::with_type(
            &shared_source,
            shared_port,
            TypeId::of::<S>(),
            std::any::type_name::<S>(),
        );
        ctx.import_batch(item_scope, each_scope, &[slot])
    }
}

/// Each 自身输入的声明（形状相关）。
pub(crate) trait EachInputs {
    /// 构建方拿到的输入位置句柄。
    type Handles;

    /// 在给定 Definition 上声明 Each 的输入位置（集合 + 可选 shared）。
    fn declare_each_inputs(definition: &mut Definition)
    -> Result<(), super::signature::BuildError>;
}

impl<T: 'static> EachInputs for (Vec<T>,) {
    type Handles = DataRef<Vec<T>>;

    fn declare_each_inputs(
        definition: &mut Definition,
    ) -> Result<(), super::signature::BuildError> {
        definition.declare_input::<Vec<T>>("collection")?;
        Ok(())
    }
}

impl<T: 'static, S: 'static> EachInputs for (Vec<T>, S) {
    type Handles = (DataRef<Vec<T>>, DataRef<S>);

    fn declare_each_inputs(
        definition: &mut Definition,
    ) -> Result<(), super::signature::BuildError> {
        definition.declare_input::<Vec<T>>("collection")?;
        definition.declare_input::<S>("shared")?;
        Ok(())
    }
}

// ---------------------------------------------------------------- cfg(test) 故障注入

/// item 输入元数据故障（只影响下一次 Item 绑定后的输入校验）。
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ItemMetadataFault {
    /// 元素声明类型与实际集合不符。
    ElementType,
    /// 下标越界。
    Index,
}

#[cfg(test)]
thread_local! {
    static ITEM_METADATA_FAULT: std::cell::Cell<Option<ItemMetadataFault>> =
        const { std::cell::Cell::new(None) };
}

/// 安装下一次 Item 绑定后的元数据故障（只生效一次）。
#[cfg(test)]
pub(crate) fn install_item_metadata_fault(fault: ItemMetadataFault) {
    ITEM_METADATA_FAULT.with(|slot| slot.set(Some(fault)));
}

#[cfg(test)]
fn take_item_metadata_fault() -> Option<ItemMetadataFault> {
    ITEM_METADATA_FAULT.with(|slot| slot.replace(None))
}

/// Consume 前的故障模式（只影响下一次收口）。
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreConsumeFault {
    /// 销毁被选输出本身（该输出随之失效）。
    Selected,
    /// 销毁 Item 的另一份 owned 值，保持被选输出有效：触发"清理前提失败"。
    OtherOwned,
}

#[cfg(test)]
thread_local! {
    static PRE_CONSUME_FAULT: std::cell::Cell<Option<PreConsumeFault>> =
        const { std::cell::Cell::new(None) };
}

/// finish 前置故障：经由**真实 Each finish 路径**触发 `finish_collector` 的四类拒绝。
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinishFault {
    /// controller 不是 Active。
    NonActive,
    /// 最终输出位置已被绑定。
    AlreadyBound,
    /// 控制器仍有活的 descendant。
    LiveDescendant,
    /// collector owner 与传入 controller 不一致。
    OwnerMismatch,
}

#[cfg(test)]
thread_local! {
    static FINISH_FAULT: std::cell::Cell<Option<FinishFault>> = const { std::cell::Cell::new(None) };
}

/// 安装下一次 finish 的前置故障（只生效一次）。
#[cfg(test)]
pub(crate) fn install_finish_fault(fault: FinishFault) {
    FINISH_FAULT.with(|slot| slot.set(Some(fault)));
}

#[cfg(test)]
fn take_finish_fault() -> Option<FinishFault> {
    FINISH_FAULT.with(|slot| slot.replace(None))
}

/// 安装下一次 Consume 前的故障（只生效一次）。
#[cfg(test)]
pub(crate) fn install_pre_consume_fault(fault: PreConsumeFault) {
    PRE_CONSUME_FAULT.with(|slot| slot.set(Some(fault)));
}

#[cfg(test)]
fn take_pre_consume_fault() -> Option<PreConsumeFault> {
    PRE_CONSUME_FAULT.with(|slot| slot.replace(None))
}

// ---------------------------------------------------------------- 完成态

/// 完成态 Each：不可变 Definition、登记的包装 Flow 与登记元数据。
pub(crate) struct Each<Sh: EachShape, O: 'static> {
    definition: Arc<Definition>,
    /// 登记的 body 包装：Node 与 Orchestrator body 都作为其唯一 Step。
    wrapper: Flow<Sh::Wrapper, Data<O>>,
    /// 登记时捕获的包装 Definition 对象身份。
    wrapper_identity: *const (),
    /// 登记时捕获的包装声明端口（输入 + 输出）。
    wrapper_inputs: Vec<DeclaredPort>,
    wrapper_outputs: Vec<DeclaredPort>,
    /// 登记时捕获的包装 Step 调用点身份（同类型外来 body 替换在此被拒绝）。
    wrapper_step_identity: *const (),
    /// Each 最终 `Vec<O>` 输出位置。
    final_position: RefId,
    marker: PhantomData<fn() -> (Sh, O)>,
}

impl<Sh: EachShape, O: 'static> Each<Sh, O> {
    /// 登记的包装 Definition（只读；供身份核对与观察）。
    pub(crate) fn wrapper_definition(&self) -> &Definition {
        self.wrapper.definition()
    }

    /// 登记的包装 Flow（只读；只由 Each 专用转交使用）。
    pub(crate) fn registered_wrapper(&self) -> &Flow<Sh::Wrapper, Data<O>> {
        &self.wrapper
    }

    /// 最终输出位置（只读观察用）。
    pub(crate) fn final_position(&self) -> &RefId {
        &self.final_position
    }

    /// 执行前核对：实际包装对象与登记身份／声明一致。
    ///
    /// 这保证运行期不会执行"同类型的另一个包装"，也不接受陈旧或外来 ports 表；篡改
    /// 只能经 test-only 元数据故障构造。
    pub(crate) fn verify_registration(&self) -> Result<(), BodyError> {
        let live = self.wrapper.definition();
        if live as *const Definition as *const () != self.wrapper_identity {
            return Err(BodyError::new(
                "registered each body is not the registered wrapper definition",
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
                "registered each body inputs do not match the registered wrapper",
            ));
        }
        if inputs.len() != Sh::wrapper_inputs()
            || inputs
                .iter()
                .zip(Sh::wrapper_input_types())
                .any(|(port, expected)| port.expected() != expected)
        {
            return Err(BodyError::new(
                "registered each body inputs do not match the each shape",
            ));
        }
        let steps = live.steps();
        if steps.len() != 1
            || steps[0].site() as *const _ as *const () != self.wrapper_step_identity
        {
            return Err(BodyError::new(
                "registered each body step is not the registered wrapper step",
            ));
        }
        let outputs = live.output_ports();
        if outputs.len() != self.wrapper_outputs.len()
            || outputs
                .iter()
                .zip(&self.wrapper_outputs)
                .any(|(live, recorded)| {
                    live.position() != recorded.position() || live.expected() != recorded.expected()
                })
        {
            return Err(BodyError::new(
                "registered each body outputs do not match the registered wrapper",
            ));
        }
        Ok(())
    }
}

impl<Sh: EachShape, O: 'static> OrchCall<Sh::I, Data<Vec<O>>> for Each<Sh, O>
where
    Sh::I: InputTypes,
{
    type Pack = Sh::Pack;
    const ROLE: ScopeRole = ScopeRole::Each;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(&'a self, scope: OrchScope<'a, Self::Pack, Data<Vec<O>>>) -> NodeFut<'a, ()> {
        Box::pin(async move {
            // 受控转交：核对本次实际 Definition 就是本 Each，并在内部建立 collector 与会话；
            // 不把可变 Context、包装或最终位置交给调用方。
            let session: EachSession<'_, Sh, O> =
                EachScopeTransfer::begin_each_session(scope, self)?;
            run_each(session).await
        })
    }
}

/// Each 的运行主体：逐项建立 ItemScope → 驱动包装 Step → 直接 Consume；全部完成后 finish。
async fn run_each<Sh: EachShape, O: 'static>(
    mut session: EachSession<'_, Sh, O>,
) -> Result<(), BodyError> {
    #[cfg(test)]
    super::test_support::record(&format!(
        "wrapper-allocated:{}",
        session.wrapper.definition().allocated_probe()
    ));
    let count = session.item_count()?;
    for index in 0..count {
        session.run_item(index).await?;
    }
    session.finish()?;
    #[cfg(test)]
    super::test_support::record(&format!(
        "wrapper-allocated-end:{}",
        session.wrapper.definition().allocated_probe()
    ));
    Ok(())
}

/// 受控 Each 会话：绑定当前 Each Definition、登记包装与自身 Scope。
///
/// 只暴露 `item_count`、`run_item`、`finish` 三个入口；索引循环与停止决定留在 Each
/// 实现内，不向调用者暴露 `ctx_mut`／Container／owned 注入／任意 target。
pub(crate) struct EachSession<'a, Sh: EachShape, O: 'static> {
    ctx: &'a mut ExecutionContext,
    /// Each 调用的 Scope（collector owner 与 Item 的直接 parent）。
    each: &'a ScopeId,
    /// Each 自身 Definition。
    inner: &'a Definition,
    pack: &'a Sh::Pack,
    wrapper: &'a Flow<Sh::Wrapper, Data<O>>,
    collector: CollectorId,
    final_position: RefId,
    marker: PhantomData<fn() -> (Sh, O)>,
}

impl<'a, Sh: EachShape, O: 'static> EachSession<'a, Sh, O> {
    /// 受控构造：只由 `OrchScope` 的 Each 专用转交调用（调用方拿不到可变 Context）。
    #[allow(clippy::too_many_arguments)] // 内部构造：Context／Scope／Definition／pack／登记包装
    pub(crate) fn new(
        ctx: &'a mut ExecutionContext,
        each: &'a ScopeId,
        inner: &'a Definition,
        pack: &'a Sh::Pack,
        wrapper: &'a Flow<Sh::Wrapper, Data<O>>,
        collector: CollectorId,
        final_position: RefId,
    ) -> Self {
        Self {
            ctx,
            each,
            inner,
            pack,
            wrapper,
            collector,
            final_position,
            marker: PhantomData,
        }
    }

    /// 集合长度：共享重借用在此结束，之后才建立 ItemScope。
    fn item_count(&self) -> Result<usize, BodyError> {
        let shared: &ExecutionContext = self.ctx;
        let values = Sh::collection(self.pack, shared, self.each).map_err(BodyError::from)?;
        Ok(values.len())
    }

    /// 运行第 `index` 项：创建 ItemScope → 绑定 item／shared → 驱动包装 Step → 直接 Consume。
    async fn run_item(&mut self, index: usize) -> Result<(), BodyError> {
        let each_scope = self.each.clone();
        let wrapper = self.wrapper.definition();
        let item_scope = self
            .ctx
            .create_child(&each_scope)
            .map_err(BodyError::from)?;
        #[cfg(test)]
        super::test_support::boundary_creation_record(
            item_scope.clone(),
            each_scope.clone(),
            ScopeRole::Item,
            self.ctx.identity_probe(),
            self.ctx.coordinator_probe(),
            self.ctx.container_probe(),
        );
        if let Err(primary) = Sh::bind_item_inputs(
            self.ctx,
            &each_scope,
            &item_scope,
            self.inner,
            wrapper,
            index,
        ) {
            // 尚未进入 guard：必须收回已建立的 child，不能留下 detached Item。
            let cleanup_failure = self.ctx.abort(&item_scope).err();
            if let Some(cleanup) = cleanup_failure {
                self.ctx.record_cleanup_failure(item_scope.clone(), cleanup);
            }
            return Err(BodyError::from(primary));
        }

        #[cfg(test)]
        if let Some(fault) = take_item_metadata_fault() {
            // 保持输入位置已绑定，只损坏 CollectionItem 元数据：真实 pack／Node 输入校验
            // 必须在 body 之前拒绝。
            let item_port = wrapper
                .inputs()
                .first()
                .expect("wrapper declares the item input")
                .position()
                .clone();
            let access = match fault {
                ItemMetadataFault::ElementType => super::scope::ItemAccess::for_collection::<u8>(),
                ItemMetadataFault::Index => {
                    super::scope::ItemAccess::for_collection::<<Sh as EachShape>::Element>()
                }
            };
            let index = match fault {
                ItemMetadataFault::Index => Some(usize::MAX),
                ItemMetadataFault::ElementType => None,
            };
            self.ctx
                .corrupt_item_access_probe(&item_scope, &item_port, access, index)?;
        }

        // 许可只由本会话的 Item 边界使用：构造点唯一，且不经过任何调用者参数。
        let permit = ItemConsumePermit {
            each: each_scope.clone(),
            collector: self.collector.clone(),
        };
        let selected = wrapper
            .output_ports()
            .first()
            .expect("wrapper declares exactly one owned output")
            .position()
            .clone();

        let mut guard = self
            .ctx
            .enter(InvocationKind::Boundary, &item_scope, true)
            .expect("item boundary entry after its pre-checks cannot fail");
        if let Err(error) = run_wrapper_steps(&mut guard, &item_scope, wrapper).await {
            guard.failed_with(&error);
            return Err(error);
        }
        #[cfg(test)]
        {
            if let Ok(moves) = guard.collector_moves_probe(&self.collector) {
                super::test_support::record(&format!("collector-before:{moves}"));
            }
            match take_pre_consume_fault() {
                Some(PreConsumeFault::Selected) => {
                    if let Ok(Some(id)) = guard.target_data_id_probe(&item_scope, &selected) {
                        guard.destroy_probe(&id);
                    }
                }
                Some(PreConsumeFault::OtherOwned) => {
                    // 在 ItemScope 内登记一份 test-only owned 值并立刻销毁它：被选输出保持
                    // 有效，但 Item 的 owned 集合里出现一个已不存在的 entry，使"剩余 owned
                    // 的清理前提"在 prepare 段失败。
                    let position =
                        super::ref_id::RefIdAllocator::new(super::ref_id::RefIdSource::new())
                            .allocate()
                            .expect("fresh item-local position");
                    if let Ok(id) = guard.register_owned::<u8>(&item_scope, &position, 7u8) {
                        guard.destroy_probe(&id);
                    }
                }
                None => {}
            }
        }
        match guard.consume_in_item_boundary(&item_scope, &selected, &permit) {
            super::scope::ConsumeOutcome::Consumed => {
                #[cfg(test)]
                {
                    if let Ok(moves) = guard.collector_moves_probe(&self.collector) {
                        super::test_support::record(&format!("collector-after:{moves}"));
                    }
                    // EachScope 的控制元数据不随 item 增长：refs 保持声明输入数 + 输出位置，
                    // ordinary owned 在 finish 之前始终为空。
                    if let Ok((refs, owned)) = guard.snapshot_targets_probe(&each_scope) {
                        super::test_support::record(&format!("each-refs:{}", refs.len()));
                        super::test_support::record(&format!("each-owned:{}", owned.len()));
                    }
                }
                guard.release_responsibility(&item_scope);
                guard.complete();
                Ok(())
            }
            super::scope::ConsumeOutcome::Rejected {
                primary,
                cleanup_failure,
            } => {
                // 只读核对真实状态：内部 Consume 可能已经关闭并清理了 Item。
                let closed = guard
                    .state(&item_scope)
                    .map(|state| state == super::scope::ScopeState::Closed)
                    .unwrap_or(false);
                if let Some(cleanup) = cleanup_failure.as_ref() {
                    // 先把清理诊断登记到 Context（首次诊断不被覆盖），随后只读回读它。
                    guard.record_cleanup_failure(item_scope.clone(), cleanup.clone());
                }
                #[cfg(test)]
                {
                    super::test_support::record(if closed {
                        "item-guard:closed"
                    } else {
                        "item-guard:open"
                    });
                    super::test_support::record(&format!("item-primary:{primary:?}"));
                    // 首错的**定位**：本次失败 frame 使用的 Scope（就是该 ItemScope）。
                    super::test_support::record(&format!(
                        "item-failure-scope:{}",
                        item_scope.seq()
                    ));
                    if cleanup_failure.is_some() {
                        super::test_support::record("consume-cleanup-failure");
                    }
                    if let Some(saved) = guard.cleanup_report() {
                        super::test_support::record(&format!("item-cleanup-report:{saved:?}"));
                    }
                }
                if closed {
                    guard.release_responsibility(&item_scope);
                }
                let error = BodyError::from(primary);
                guard.failed_with(&error);
                Err(error)
            }
        }
    }

    /// 全部项完成（含空集合）：一次 finish collector 并绑定最终 `Vec<O>` 输出位置。
    fn finish(&mut self) -> Result<(), BodyError> {
        let each_scope = self.each.clone();
        #[cfg(test)]
        let controller = match take_finish_fault() {
            Some(FinishFault::NonActive) => {
                self.ctx.set_finalizing_probe(&each_scope)?;
                each_scope.clone()
            }
            Some(FinishFault::AlreadyBound) => {
                // 先在最终输出位置做一次真实绑定，触发位置已绑定前置。
                self.ctx
                    .register_owned::<Vec<O>>(&each_scope, &self.final_position, Vec::new())?;
                each_scope.clone()
            }
            Some(FinishFault::LiveDescendant) => {
                // 真实建立仍存活的 descendant，触发"有活 descendant"前置。
                self.ctx.create_child(&each_scope)?;
                each_scope.clone()
            }
            Some(FinishFault::OwnerMismatch) => self.ctx.create_child(&each_scope)?,
            None => each_scope.clone(),
        };
        #[cfg(not(test))]
        let controller = each_scope.clone();
        #[cfg(test)]
        {
            // 基线取在**故障注入之后**：四类故障统一比较相等，任何额外 `DataId` 消耗都会被检出。
            let state = self.ctx.snapshot_targets_probe(&each_scope).ok();
            super::test_support::record(&format!("finish-before-state:{state:?}"));
            super::test_support::record(&format!(
                "finish-before-next-id:{:?}",
                self.ctx.next_data_id_probe()
            ));
        }
        let outcome = self
            .ctx
            .finish_collector(&controller, &self.collector, &self.final_position);
        match outcome {
            Ok(_) => {
                #[cfg(test)]
                if let Ok((refs, owned)) = self.ctx.snapshot_targets_probe(&each_scope) {
                    // 全部项完成后才出现一次最终绑定：refs 增加恰好一个输出位置，owned
                    // 增加一份完整 `Vec<O>` Data。
                    super::test_support::record(&format!("each-finish-refs:{}", refs.len()));
                    super::test_support::record(&format!("each-finish-owned:{}", owned.len()));
                }
                Ok(())
            }
            Err(error) => {
                #[cfg(test)]
                {
                    // 拒绝前无部分提交：记录 EachScope 的完整状态（身份级）、collector 状态
                    // 与下一个 DataId 序号，供测试与 finish 操作前的记录逐项比较。
                    let state = self.ctx.snapshot_targets_probe(&each_scope).ok();
                    super::test_support::record(&format!("finish-reject-state:{state:?}"));
                    if let Ok((refs, owned)) = self.ctx.snapshot_targets_probe(&each_scope) {
                        super::test_support::record(&format!("finish-reject-refs:{}", refs.len()));
                        super::test_support::record(&format!(
                            "finish-reject-owned:{}",
                            owned.len()
                        ));
                    }
                    if let Ok(moves) = self.ctx.collector_moves_probe(&self.collector) {
                        super::test_support::record(&format!("finish-reject-moves:{moves}"));
                    }
                    super::test_support::record(&format!(
                        "finish-reject-next-id:{:?}",
                        self.ctx.next_data_id_probe()
                    ));
                }
                Err(BodyError::from(error))
            }
        }
    }
}

/// 在 ItemScope 中顺序驱动包装 Definition 的 Step（包装本身不另建 Scope）。
async fn run_wrapper_steps(
    guard: &mut InvocationGuard<'_>,
    item_scope: &ScopeId,
    wrapper: &Definition,
) -> Result<(), BodyError> {
    for step in wrapper.steps() {
        super::builder::run_site(guard, item_scope, step.site()).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------- 构建态

/// Each 构建态：声明输入／输出、登记 body 包装，但**不实现** Orchestrator 协议。
pub(crate) struct EachBuilder<Sh: EachShape, O: 'static> {
    definition: Definition,
    final_position: RefId,
    wrapper_builder: Option<FlowBuilder<Sh::Wrapper>>,
    wrapper_handles: Option<<Sh::Wrapper as FlowInputs>::Handles>,
    produced: Option<DataRef<O>>,
    /// 测试注入：被替换的包装端口快照（H08 防御校验样本）。
    #[cfg(test)]
    tampered_outputs: Option<Vec<DeclaredPort>>,
    marker: PhantomData<fn() -> (Sh, O)>,
}

impl<Sh: EachShape, O: 'static> EachBuilder<Sh, O> {
    /// 建立 Each：声明集合（+ 可选 shared）输入与最终 `Vec<O>` 输出端口，并开始包装 Flow。
    pub(crate) fn start() -> Result<Self, super::signature::BuildError> {
        let mut definition = Definition::new();
        <Sh::I as EachInputs>::declare_each_inputs(&mut definition)?;
        let final_position = definition.declare_output_port::<Vec<O>>("each output")?;
        let (wrapper_builder, wrapper_handles) = FlowBuilder::<Sh::Wrapper>::start()?;
        Ok(Self {
            definition,
            final_position: final_position.position().clone(),
            wrapper_builder: Some(wrapper_builder),
            wrapper_handles: Some(wrapper_handles),
            produced: None,
            #[cfg(test)]
            tampered_outputs: None,
            marker: PhantomData,
        })
    }

    /// 登记 body：作为包装 Definition 内**唯一** Step，输出必须是单份 `Data<O>`。
    ///
    /// 函数／结构体 Node／`Arc<具体 Node>`／完成态 Flow 都走既有 typed 接线：构建期由
    /// `BuildSite::precheck` 核对 Orchestrator 声明，静态类型由 Marker 绑定。
    ///
    /// 原子性：重复登记在追加 Step／分配 Ref 之前以 [`BuildError::SecondEachBody`] 拒绝；
    /// 可恢复的接线拒绝（例如普通函数 unit 输出）保留既有构建态与第一登记，不丢 wrapper。
    pub(crate) fn then_body<C, M>(&mut self, body: C) -> Result<(), super::signature::BuildError>
    where
        C: BuildSite<M, <Sh::Wrapper as FlowInputs>::Handles>,
        M: Wiring<BuildOutput = DataRef<O>>,
        C: IntoCallSite<M, <Sh::Wrapper as FlowInputs>::Handles, BuildOutput = DataRef<O>>,
    {
        if self.produced.is_some() {
            return Err(super::signature::BuildError::SecondEachBody);
        }
        let handles = self
            .wrapper_handles
            .clone()
            .expect("each builder handles stay alive until finish");
        let wrapper = self
            .wrapper_builder
            .as_mut()
            .expect("each builder stays open until finish");
        let produced: DataRef<O> = TypedCallBuilder::then::<C, M, _>(wrapper, body, handles)?;
        self.produced = Some(produced);
        Ok(())
    }

    /// 完成：包装 finish 后形成不可变完成态 Each，并捕获登记身份与声明端口。
    pub(crate) fn finish(mut self) -> Result<Each<Sh, O>, super::signature::BuildError> {
        let produced = self
            .produced
            .take()
            .ok_or(super::signature::BuildError::EachBodyMissing)?;
        let wrapper = self
            .wrapper_builder
            .take()
            .expect("each builder is still open")
            .finish::<Data<O>, _>(produced)?;
        let wrapper_identity = wrapper.definition() as *const Definition as *const ();
        let wrapper_step_identity = wrapper
            .definition()
            .steps()
            .first()
            .expect("each wrapper has exactly one registered body step")
            .site() as *const _ as *const ();
        let wrapper_inputs = wrapper.definition().inputs().to_vec();
        #[cfg(test)]
        let wrapper_outputs = self
            .tampered_outputs
            .take()
            .unwrap_or_else(|| wrapper.definition().output_ports().to_vec());
        #[cfg(not(test))]
        let wrapper_outputs = wrapper.definition().output_ports().to_vec();
        #[allow(clippy::arc_with_non_send_sync)]
        // 单线程、非 Send 执行模型：只共享不可变 Definition
        let definition = Arc::new(self.definition);
        Ok(Each {
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
impl<Sh: EachShape, O: 'static> EachBuilder<Sh, O> {
    /// 测试观测：包装 Definition 当前 Step 数量（构建失败不留 Step）。
    pub(crate) fn wrapper_step_count_probe(&self) -> Option<usize> {
        self.wrapper_builder
            .as_ref()
            .map(|builder| builder.step_count_probe())
    }

    /// 测试观测：Each 自身 Definition 已分配的位置数量。
    pub(crate) fn allocated_probe(&self) -> u64 {
        self.definition.allocated_probe()
    }

    /// 测试注入：把登记的包装端口快照替换为外来 ports（H08 防御校验样本）。
    pub(crate) fn tamper_wrapper_outputs_probe(&mut self, ports: Vec<DeclaredPort>) {
        self.tampered_outputs = Some(ports);
    }
}

#[cfg(test)]
impl<Sh: EachShape, O: 'static> EachSession<'_, Sh, O> {
    /// 编译期字段见证：字段类型变为其他类型时本函数不再编译（H10）。
    #[allow(dead_code)]
    fn session_field_witness(&self) {
        let _: &ExecutionContext = self.ctx;
        let _: &ScopeId = self.each;
        let _: &Definition = self.inner;
        let _: &Sh::Pack = self.pack;
        let _: &Flow<Sh::Wrapper, Data<O>> = self.wrapper;
        let _: &CollectorId = &self.collector;
        let _: &RefId = &self.final_position;
        let _: PhantomData<fn() -> (Sh, O)> = self.marker;
    }
}
