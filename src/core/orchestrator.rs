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

use super::builder::{Definition, run_site};
use super::context::{BodyError, ExecutionContext, InvocationKind, TerminationKind};
use super::identity::ScopeId;
use super::internal_error::ScopeError;
use super::ref_id::RefId;
use super::scope::{ExportSlot, ImportSlot};
use super::signature::{BuildError, DeclaredPort, InputTypes, NodeFut, OutKind};

/// 把 pack 类型与正式输入 Signature 关联：只有匹配的类型 tuple 才成立。
pub(crate) trait PackFor<I: 'static> {}

impl PackFor<()> for Targets0 {}
impl<A: 'static> PackFor<(A,)> for Targets1<A> {}
impl<A: 'static, B: 'static> PackFor<(A, B)> for Targets2<A, B> {}

/// 业务 Orchestrator 协议：显式声明输入签名 `I`、输出分类 `K` 与输入 pack 类型。
///
/// `definition()` 返回该编排体持有的内部 Definition（真实子调用描述），不是任意 body
/// 闭包；`run` 在当前 Context 与自身 Scope 中组织 child 调用。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait OrchCall<I: 'static + InputTypes, K: OutKind> {
    /// 输入 pack 类型（[`Targets0`]／[`Targets1`]／[`Targets2`]）。
    ///
    /// `PackFor<I>` 把 pack 与正式输入 Signature 绑定：写错 pack 类型（例如声明
    /// `OrchCall<(u32,), _>` 却给 `Targets1<String>`）在**编译期**就不成立，
    /// 不会等到执行期 Import 才失败。
    type Pack: PackFor<I> + PackFromPorts;

    /// 内部 Definition：声明输入端口、输出端口与真实子调用。
    fn definition(&self) -> &Definition;

    /// 运行编排体；只读写自身 Scope 的声明端口。
    fn run<'a>(&'a self, scope: OrchScope<'a, Self::Pack, K>) -> NodeFut<'a, ()>;
}

/// 由声明端口构造输入 pack；数量或声明类型不匹配时在构建期拒绝。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
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

/// 零输入 pack。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct Targets0;

/// 单输入 pack：只携带 child-local 位置元数据。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct Targets1<A> {
    position: RefId,
    marker: PhantomData<fn() -> A>,
}

/// 双输入 pack：保留顺序与每项类型。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct Targets2<A, B> {
    first: RefId,
    second: RefId,
    marker: PhantomData<fn() -> (A, B)>,
}

#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
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

#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
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
    pub(crate) fn second<'a>(
        &self,
        ctx: &'a ExecutionContext,
        child: &ScopeId,
    ) -> Result<&'a B, ScopeError> {
        ctx.resolve::<B>(child, &self.second)
    }
}

impl PackFromPorts for Targets0 {
    fn from_ports(ports: &[DeclaredPort]) -> Result<Self, BuildError> {
        if ports.is_empty() {
            Ok(Self)
        } else {
            Err(BuildError::InputCountMismatch {
                expected: 0,
                supplied: ports.len(),
            })
        }
    }

    fn validate(&self, _ctx: &ExecutionContext, _child: &ScopeId) -> Result<(), ScopeError> {
        Ok(())
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

/// 编排体内的受控 Scope 视图：只暴露自身 Scope 的声明端口与真实子调用。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct OrchScope<'a, P, K: OutKind> {
    ctx: &'a mut ExecutionContext,
    child: &'a ScopeId,
    pack: &'a P,
    inner: &'a Definition,
    marker: PhantomData<fn() -> K>,
}

#[allow(dead_code)] // 编排体视图由 V21-05 的真实执行样本驱动
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
        for step in self.inner.steps() {
            run_site(self.ctx, self.child, step.site()).await?;
        }
        Ok(())
    }
}

/// 擦除后的 Orchestrator 调用点。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait OrchestratorSite {
    /// 本次接线使用的 caller 输入位置。
    fn inputs(&self) -> &[RefId];
    /// 本次接线的 caller 输出位置。
    fn outputs(&self) -> &[RefId];
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
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct OrchSite<O, I, K> {
    orchestrator: O,
    caller_inputs: Vec<RefId>,
    caller_outputs: Vec<RefId>,
    #[cfg(test)]
    pack_override: std::cell::RefCell<Option<Box<dyn Any>>>,
    marker: PhantomData<fn() -> (I, K)>,
}

#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
impl<O, I, K> OrchSite<O, I, K> {
    /// 构建调用点：caller 位置与声明输出位置已由接线器确定。
    pub(crate) fn new(
        orchestrator: O,
        caller_inputs: Vec<RefId>,
        caller_outputs: Vec<RefId>,
    ) -> Self {
        Self {
            orchestrator,
            caller_inputs,
            caller_outputs,
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

/// 编排体运行：先做擦除后的防御校验，再在自身 Scope 中运行 body。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
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

/// 整组输出预检与提交：child-local 声明端口 → caller 输出位置。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
fn export_orchestrator_outputs(
    guard: &mut super::context::InvocationGuard<'_>,
    child: &ScopeId,
    inner: &Definition,
    caller_outputs: &[RefId],
) -> Result<(), BodyError> {
    let ports = inner.output_ports();
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
