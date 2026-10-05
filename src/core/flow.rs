//! 完整 Flow Definition：输入／输出 Signature、完成态与顺序执行主体。
//!
//! 与 V21-05 的任意 Orchestrator 测试结构体不同，这里的 [`Flow`] 是**完成态**：
//! 它把构建态 [`FlowBuilder`] 校验过的、不可变的 [`Definition`] 封装起来，并实现
//! [`OrchCall`]，因此可以接入同一 typed `then`、作为 Root Flow 顺序执行，或作为
//! 父 Flow 的 child（SubFlow）。
//!
//! 关键边界：
//! - **输入**：只有单／双非空位置两种形状（[`FlowInputs`]），没有 `()` 输入；零输入
//!   Node 仍可接入非空 Flow。输入位置在构建初始化时分配，句柄即 `DataRef`。
//! - **输出**：三种完成选择——`()`（显式 unit，不占 Ref 或 DataId）、一个 `DataRef<O>`
//!   （`Data<O>`）、两个 `DataRef` 的 tuple（`Out2<O1, O2>`）。未完成 Builder 没有
//!   Orchestrator 协议，不能被执行或组合。
//! - **完成校验**：先整组校验（来源归属 → 已声明输入／已产出位置 → 位置唯一 → 非 unit
//!   业务类型），再声明输出端口；失败不产生 Flow、不分配或消耗 Ref 序号、不留半份输出声明。
//! - **顺序主体**：Root Flow 与 SubFlow 共用 [`crate::core::builder::run_definition`] 的
//!   顺序 Step 循环；输入装配与输出结束边界分别由 Root 驱动与 [`crate::core::orchestrator::OrchSite`]
//!   提供。Flow body 不做 caller 绑定，也不创建／关闭自己的 FlowScope。
//! - **复用**：完成态只共享不可变 Definition（`Arc`），句柄 `Clone` 不复制业务 Data；
//!   同一 Flow 定义可重复执行、可在多个调用位置复用。

use std::marker::PhantomData;
use std::sync::Arc;

use super::builder::{BuildSite, Definition, IntoCallSite, TypedCallBuilder};
use super::data_ref::DataRef;
use super::orchestrator::{OrchCall, OrchScope, PackFor, PackFromPorts, Targets1, Targets2};
use super::signature::{
    BuildError, Data, DeclaredPort, InputTypes, NodeFut, Out2, OutKind, Unit, WireInputs, Wiring,
};

#[allow(dead_code)]
// V21-06 交付的内部能力：当前消费者是 V21-06 验收样本；公开入口由 V21-10 接续
/// Flow 输入 Signature：单／双非空位置。
///
/// `Pack` 与该 Signature 绑定（[`PackFor`]），因此一个 `Flow` 用作 child 时，调用边界的
/// 输入 pack 与声明输入类型在**编译期**就一致；`Handles` 是构建方拿到的位置句柄。
pub(crate) trait FlowInputs: 'static {
    /// 与输入 Signature 绑定的 pack 类型。
    type Pack: PackFromPorts;
    /// 构建方拿到的输入位置句柄。
    type Handles;

    /// 在给定 Definition 上声明本形状的全部输入位置。
    fn declare(definition: &mut Definition) -> Result<Self::Handles, BuildError>;
}

impl<A: 'static> FlowInputs for (A,) {
    type Pack = Targets1<A>;
    type Handles = DataRef<A>;

    fn declare(definition: &mut Definition) -> Result<Self::Handles, BuildError> {
        definition.declare_input::<A>("input")
    }
}

impl<A: 'static, B: 'static> FlowInputs for (A, B) {
    type Pack = Targets2<A, B>;
    type Handles = (DataRef<A>, DataRef<B>);

    fn declare(definition: &mut Definition) -> Result<Self::Handles, BuildError> {
        Ok((
            definition.declare_input::<A>("input")?,
            definition.declare_input::<B>("input")?,
        ))
    }
}

#[allow(dead_code)] // V21-06 交付的内部能力：当前消费者是 V21-06 验收样本
/// 完成操作的 typed 输出选择 → 输出分类。
///
/// 加入此处空实现之外的 None 不能成立：三种选择各自只有一个实现，`K` 由选择类型唯一确定，
/// 调用方不写 Marker，也不依赖"关联类型反推"。
pub(crate) trait FlowOutput<K: OutKind> {
    /// 本次选择要声明的 child-local 输出端口（按选择顺序）。
    fn ports(self) -> Vec<DeclaredPort>;
}

impl FlowOutput<Unit> for () {
    fn ports(self) -> Vec<DeclaredPort> {
        // 显式 unit：不分配位置、不产生 unit Data 或 Ref。
        Vec::new()
    }
}

impl<O: 'static> FlowOutput<Data<O>> for DataRef<O> {
    fn ports(self) -> Vec<DeclaredPort> {
        vec![DeclaredPort::new::<O>(self.position().clone())]
    }
}

impl<O1: 'static, O2: 'static> FlowOutput<Out2<O1, O2>> for (DataRef<O1>, DataRef<O2>) {
    fn ports(self) -> Vec<DeclaredPort> {
        vec![
            DeclaredPort::new::<O1>(self.0.position().clone()),
            DeclaredPort::new::<O2>(self.1.position().clone()),
        ]
    }
}

#[allow(dead_code)] // V21-06 交付的内部能力：当前消费者是 V21-06 验收样本
/// Flow 构建态：声明输入、按顺序追加 Step，但**不实现** Orchestrator 协议。
///
/// 未完成的 Builder 既不能被执行，也不能作为 child 接入 `then`；只有 [`Self::finish`]
/// 产出 [`Flow`] 之后才成立。
pub(crate) struct FlowBuilder<I: FlowInputs> {
    definition: Definition,
    marker: PhantomData<fn() -> I>,
}

#[allow(dead_code)] // V21-06 交付的内部能力：当前消费者是 V21-06 验收样本
impl<I: FlowInputs> FlowBuilder<I> {
    /// 建立 Flow：声明本形状的输入位置，并返回构建方使用的句柄。
    pub(crate) fn start() -> Result<(Self, I::Handles), BuildError> {
        let mut definition = Definition::new();
        let handles = I::declare(&mut definition)?;
        Ok((
            Self {
                definition,
                marker: PhantomData,
            },
            handles,
        ))
    }

    /// 完成 Flow：整组校验选择，再声明输出端口并形成不可变完成态。
    ///
    /// 失败时消费式 Builder 已经结束：不产生 Flow、不追加 Step、不分配或消耗 Ref 序号、
    /// 不留下半份输出声明；失败后是否继续复用该 Builder 不属于本阶段保证。
    #[allow(clippy::arc_with_non_send_sync)] // 单线程、非 Send 执行模型：只共享不可变定义句柄
    pub(crate) fn finish<K: OutKind, C: FlowOutput<K>>(
        self,
        choice: C,
    ) -> Result<Flow<I, K>, BuildError> {
        let ports = choice.ports();
        let mut definition = self.definition;
        // 1. 整组校验：任何一项失败都不写入输出端口。
        definition.check_finish_outputs(&ports)?;
        // 2. 提交：声明全部输出端口（此后没有可失败操作）。
        definition.declare_finish_outputs(ports);
        Ok(Flow {
            definition: Arc::new(definition),
            marker: PhantomData,
        })
    }
}

#[cfg(test)]
impl<I: FlowInputs> FlowBuilder<I> {
    /// 测试观测：本次构建已分配的位置数量（完成失败不消耗序号）。
    pub(crate) fn allocated_probe(&self) -> u64 {
        self.definition.allocated_probe()
    }

    /// 测试观测：构建态 Definition（只读）。
    pub(crate) fn definition_probe(&self) -> &Definition {
        &self.definition
    }
}

#[cfg(test)]
impl<I: FlowInputs> FlowBuilder<I> {
    /// 测试观测：构建态 Definition 当前 Step 数量。
    pub(crate) fn step_count_probe(&self) -> usize {
        self.definition.step_count()
    }
}

impl<I: FlowInputs> TypedCallBuilder for FlowBuilder<I> {
    fn then<C, M, A0>(&mut self, callable: C, args: A0) -> Result<C::BuildOutput, BuildError>
    where
        C: BuildSite<M, A0>,
        M: Wiring,
        A0: WireInputs,
        C: IntoCallSite<M, A0, BuildOutput = M::BuildOutput>,
    {
        self.definition.then(callable, args)
    }
}

#[allow(dead_code)] // V21-06 交付的内部能力：子 Flow 复用与 Root 驱动由后续任务接入
/// 完成态 Flow：封装已验证、不可变的 Definition，实现 Orchestrator 协议。
///
/// 不保存任何某次 Execution 的 ScopeId／DataId／输入借用／Prepared 输出／可变业务状态
/// 或 Context；`Clone` 只复制定义句柄（共享同一个 `Arc<Definition>`），不复制业务 Data。
pub(crate) struct Flow<I, K> {
    definition: Arc<Definition>,
    marker: PhantomData<fn() -> (I, K)>,
}

impl<I, K> std::fmt::Debug for Flow<I, K> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 只报告定义规模：完成态不携带业务值，也不要求调用方类型实现 `Debug`。
        formatter
            .debug_struct("Flow")
            .field("steps", &self.definition.step_count())
            .field("outputs", &self.definition.output_ports().len())
            .finish()
    }
}

impl<I, K> Clone for Flow<I, K> {
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
            marker: PhantomData,
        }
    }
}

impl<I, K> OrchCall<I, K> for Flow<I, K>
where
    I: FlowInputs + InputTypes,
    K: OutKind,
    I::Pack: PackFor<I>,
{
    type Pack = I::Pack;

    fn definition(&self) -> &Definition {
        &self.definition
    }

    fn run<'a>(&'a self, mut scope: OrchScope<'a, Self::Pack, K>) -> NodeFut<'a, ()> {
        // 顺序主体：与 Root Flow 共用 run_definition 的 Step 循环；child Scope、
        // 输入校验与输出 Export 都由已经进入的调用边界负责。
        Box::pin(async move {
            scope.run_steps().await?;
            Ok(())
        })
    }
}
