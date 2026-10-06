//! 业务 Node 协议、函数适配器与叶子调用 site。
//!
//! 业务 Node 是业务叶子：它只接收已有 Data 的只读借用与自身配置，返回新的 owned
//! Data 或 `()`；它不接触 `Context`、`Scope`、`DataRef`、`RefTarget` 或内部端口对象。
//!
//! 两条适配路径（V21-05 §4.1）：
//! - **函数 item**：普通同步函数以 `Fn` 约束、异步函数以 `AsyncFn` 约束适配，输出只在
//!   Future 完成后固定（Future 本身仍可借用输入）；两者都由本模块的适配器接入协议。
//! - **结构体／`Arc<具体 Node>`**：以"单一短生命周期 + boxed Future"的协议方法调用：
//!   `fn call<'a>(&'a self, a: &'a A) -> NodeFut<'a, O>`，self 与输入重借用缩短到同一
//!   调用生命周期，await 完成或 Future 丢弃后才登记输出。
//!
//! 普通函数路径只支持产生 Data；`O = ()` 的实例化在构建预检中被
//! [`BuildError::UnsupportedFunctionUnitOutput`](super::signature::BuildError::UnsupportedFunctionUnitOutput)
//! 拒绝，不在此处引入第二个 unit 候选 impl（那会造成 `E0283`／`E0284` 歧义）。

use std::marker::PhantomData;
use std::sync::Arc;

use super::context::{
    BodyError, ExecutionContext, InvocationGuard, InvocationKind, TerminationKind,
};
use super::identity::{DataId, ScopeId};
use super::ref_id::RefId;
use super::signature::{Data, NodeFut, OutKind, OutputKind, Unit};

// ---- 业务协议：结构体 Node 与 Arc<具体 Node> ----

/// 零输入 Node 协议。
///
/// 由具体结构体 Node 实现；`Arc<具体 Node>` 会自动经共享包装接入同一协议。
pub trait NodeCall0<K: OutputKind> {
    /// 执行本次调用；返回值是新的 owned Data 或 `()`。
    fn call<'a>(&'a self) -> NodeFut<'a, K::Output>;
}

/// 单输入 Node 协议：`self` 与输入共享同一个短调用生命周期。
pub trait NodeCall1<A: 'static, K: OutputKind> {
    /// 执行本次调用；输入借用与 `self` 借用随 Future 结束。
    fn call<'a>(&'a self, a: &'a A) -> NodeFut<'a, K::Output>;
}

/// 双输入 Node 协议：两个输入与 `self` 共享同一个短调用生命周期。
pub trait NodeCall2<A: 'static, B: 'static, K: OutputKind> {
    /// 执行本次调用；两个输入借用与 `self` 借用随 Future 结束。
    fn call<'a>(&'a self, a: &'a A, b: &'a B) -> NodeFut<'a, K::Output>;
}

/// `Arc<具体 Node>` 的持有包装：把共享句柄委托给具体 Node 的协议实现。
///
/// 它只包装句柄、不复制 Node、也不转成函数指针；定义可以存储同一 `Arc` 的多个 clone。
pub(crate) struct SharedNode<N>(pub(crate) Arc<N>);

impl<N, K: OutKind> NodeCall0<K> for SharedNode<N>
where
    N: NodeCall0<K>,
{
    fn call<'a>(&'a self) -> NodeFut<'a, K::Output> {
        (*self.0).call()
    }
}

impl<N, A: 'static, K: OutKind> NodeCall1<A, K> for SharedNode<N>
where
    N: NodeCall1<A, K>,
{
    fn call<'a>(&'a self, a: &'a A) -> NodeFut<'a, K::Output> {
        (*self.0).call(a)
    }
}

impl<N, A: 'static, B: 'static, K: OutKind> NodeCall2<A, B, K> for SharedNode<N>
where
    N: NodeCall2<A, B, K>,
{
    fn call<'a>(&'a self, a: &'a A, b: &'a B) -> NodeFut<'a, K::Output> {
        (*self.0).call(a, b)
    }
}

// ---- 函数 item 适配器：把普通函数接到同一协议 ----

/// 普通同步函数适配器（0／1／2 参数）。
pub(crate) struct Fn0<F>(pub(crate) F);
/// 单输入同步函数适配器。
pub(crate) struct Fn1<F, A>(pub(crate) F, pub(crate) PhantomData<fn(&A)>);
/// 双输入同步函数适配器。
pub(crate) struct Fn2<F, A, B>(pub(crate) F, pub(crate) PhantomData<fn(&A, &B)>);

/// 普通异步函数适配器（0／1／2 参数）。
pub(crate) struct AsyncFn0<F>(pub(crate) F);
/// 单输入异步函数适配器。
pub(crate) struct AsyncFn1<F, A>(pub(crate) F, pub(crate) PhantomData<fn(&A)>);
/// 双输入异步函数适配器。
pub(crate) struct AsyncFn2<F, A, B>(pub(crate) F, pub(crate) PhantomData<fn(&A, &B)>);

impl<F, O: 'static> NodeCall0<Data<O>> for Fn0<F>
where
    F: Fn() -> Result<O, BodyError>,
{
    fn call<'a>(&'a self) -> NodeFut<'a, O> {
        Box::pin(std::future::ready((self.0)()))
    }
}

impl<F, A: 'static, O: 'static> NodeCall1<A, Data<O>> for Fn1<F, A>
where
    F: for<'x> Fn(&'x A) -> Result<O, BodyError>,
{
    fn call<'a>(&'a self, a: &'a A) -> NodeFut<'a, O> {
        Box::pin(std::future::ready((self.0)(a)))
    }
}

impl<F, A: 'static, B: 'static, O: 'static> NodeCall2<A, B, Data<O>> for Fn2<F, A, B>
where
    F: for<'x, 'y> Fn(&'x A, &'y B) -> Result<O, BodyError>,
{
    fn call<'a>(&'a self, a: &'a A, b: &'a B) -> NodeFut<'a, O> {
        Box::pin(std::future::ready((self.0)(a, b)))
    }
}

impl<F, O: 'static> NodeCall0<Data<O>> for AsyncFn0<F>
where
    F: AsyncFn() -> Result<O, BodyError>,
{
    fn call<'a>(&'a self) -> NodeFut<'a, O> {
        Box::pin(async move { (self.0)().await })
    }
}

impl<F, A: 'static, O: 'static> NodeCall1<A, Data<O>> for AsyncFn1<F, A>
where
    F: for<'x> AsyncFn(&'x A) -> Result<O, BodyError>,
{
    fn call<'a>(&'a self, a: &'a A) -> NodeFut<'a, O> {
        Box::pin(async move { (self.0)(a).await })
    }
}

impl<F, A: 'static, B: 'static, O: 'static> NodeCall2<A, B, Data<O>> for AsyncFn2<F, A, B>
where
    F: for<'x, 'y> AsyncFn(&'x A, &'y B) -> Result<O, BodyError>,
{
    fn call<'a>(&'a self, a: &'a A, b: &'a B) -> NodeFut<'a, O> {
        Box::pin(async move { (self.0)(a, b).await })
    }
}

// ---- 叶子输出登记 ----

/// 叶子输出登记：只有"一份 Data"与"显式 unit"两种分类构成叶子 site。
pub(crate) trait LeafOutput: OutKind {
    /// 在输入借用结束后登记输出。
    fn store(
        guard: &mut InvocationGuard<'_>,
        scope: &ScopeId,
        position: Option<&RefId>,
        value: Self::Output,
    ) -> Result<Option<DataId>, BodyError>;
}

impl<O: 'static> LeafOutput for Data<O> {
    fn store(
        guard: &mut InvocationGuard<'_>,
        scope: &ScopeId,
        position: Option<&RefId>,
        value: O,
    ) -> Result<Option<DataId>, BodyError> {
        let position = position.ok_or_else(|| {
            BodyError::new("a data leaf output requires a declared output position")
        })?;
        Ok(Some(guard.register_owned(scope, position, value)?))
    }
}

impl LeafOutput for Unit {
    fn store(
        _guard: &mut InvocationGuard<'_>,
        _scope: &ScopeId,
        _position: Option<&RefId>,
        _value: (),
    ) -> Result<Option<DataId>, BodyError> {
        // unit 输出不产生 DataId、target、绑定或正常 Data 序号消耗。
        Ok(None)
    }
}

// ---- 擦除后的 Node site ----

/// 擦除后的 Node 调用 site：业务 Node 只出现在 `Leaf…` 的私有字段里。
pub(crate) trait NodeSite {
    /// 本次接线的逻辑输入位置（按声明顺序）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn inputs(&self) -> &[RefId];
    /// 本次调用的声明输出位置（unit 为空）。
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn outputs(&self) -> &[RefId];
    /// 执行一次叶子调用：进入沿用 caller Scope 的 Leaf Invocation。
    fn invoke<'a>(
        &'a self,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> NodeFut<'a, Option<DataId>>;
}

/// 零输入叶子 site。
pub(crate) struct Leaf0<H, K> {
    node: H,
    output: Option<RefId>,
    marker: PhantomData<fn() -> K>,
}

/// 单输入叶子 site。
pub(crate) struct Leaf1<H, A, K> {
    node: H,
    input: RefId,
    output: Option<RefId>,
    marker: PhantomData<fn(&A) -> K>,
}

/// 双输入叶子 site。
pub(crate) struct Leaf2<H, A, B, K> {
    node: H,
    inputs: [RefId; 2],
    output: Option<RefId>,
    marker: PhantomData<fn(&A, &B) -> K>,
}

impl<H, K> Leaf0<H, K> {
    /// 构建零输入叶子 site。
    pub(crate) fn new(node: H, output: Option<RefId>) -> Self {
        Self {
            node,
            output,
            marker: PhantomData,
        }
    }
}

impl<H, A, K> Leaf1<H, A, K> {
    /// 构建单输入叶子 site。
    pub(crate) fn new(node: H, input: RefId, output: Option<RefId>) -> Self {
        Self {
            node,
            input,
            output,
            marker: PhantomData,
        }
    }
}

impl<H, A, B, K> Leaf2<H, A, B, K> {
    /// 构建双输入叶子 site。
    pub(crate) fn new(node: H, first: RefId, second: RefId, output: Option<RefId>) -> Self {
        Self {
            node,
            inputs: [first, second],
            output,
            marker: PhantomData,
        }
    }
}

impl<H, K> NodeSite for Leaf0<H, K>
where
    H: NodeCall0<K>,
    K: LeafOutput,
{
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn inputs(&self) -> &[RefId] {
        &[]
    }

    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn outputs(&self) -> &[RefId] {
        self.output.as_slice()
    }

    fn invoke<'a>(
        &'a self,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> NodeFut<'a, Option<DataId>> {
        Box::pin(async move {
            // 调用前预检：当前 Scope 与输出可绑定性。已知输出冲突时业务体不运行。
            if let Some(position) = self.output.as_ref() {
                // 建立阶段失败：按已验收的建立失败终止机制保存执行错误、原 Scope 诊断与
                // 本次调用使用的 Scope；调用方即使捕获错误也不能继续普通业务。
                if let Err(error) = ctx.precheck_output(scope, position) {
                    ctx.terminate(
                        TerminationKind::BodyError,
                        Some(scope.clone()),
                        "leaf output precheck failed",
                        Some(error.clone()),
                    );
                    return Err(BodyError::from(error));
                }
            }
            let mut guard = ctx.enter(InvocationKind::Leaf, scope, false)?;
            // 共享重借用：调用与输入／self 借用限定在本块内结束，之后才可变登记输出。
            let outcome = {
                let _shared: &ExecutionContext = &guard;
                self.node.call().await
            };
            match outcome {
                Ok(value) => match K::store(&mut guard, scope, self.output.as_ref(), value) {
                    Ok(stored) => {
                        guard.complete();
                        Ok(stored)
                    }
                    // 业务已结束的可失败登记走显式执行错误退出，保留原 Scope 诊断。
                    Err(error) => {
                        guard.failed_with(&error);
                        Err(error)
                    }
                },
                Err(error) => {
                    guard.failed_with(&error);
                    Err(error)
                }
            }
        })
    }
}

impl<H, A, K> NodeSite for Leaf1<H, A, K>
where
    H: NodeCall1<A, K>,
    A: 'static,
    K: LeafOutput,
{
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn inputs(&self) -> &[RefId] {
        std::slice::from_ref(&self.input)
    }

    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn outputs(&self) -> &[RefId] {
        self.output.as_slice()
    }

    fn invoke<'a>(
        &'a self,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> NodeFut<'a, Option<DataId>> {
        Box::pin(async move {
            if let Some(position) = self.output.as_ref() {
                // 建立阶段失败：按已验收的建立失败终止机制保存执行错误、原 Scope 诊断与
                // 本次调用使用的 Scope；调用方即使捕获错误也不能继续普通业务。
                if let Err(error) = ctx.precheck_output(scope, position) {
                    ctx.terminate(
                        TerminationKind::BodyError,
                        Some(scope.clone()),
                        "leaf output precheck failed",
                        Some(error.clone()),
                    );
                    return Err(BodyError::from(error));
                }
            }
            let mut guard = ctx.enter(InvocationKind::Leaf, scope, false)?;
            let outcome = {
                let shared: &ExecutionContext = &guard;
                match shared.resolve::<A>(scope, &self.input) {
                    Ok(borrowed) => self.node.call(borrowed).await,
                    Err(error) => Err(BodyError::from(error)),
                }
            };
            match outcome {
                Ok(value) => match K::store(&mut guard, scope, self.output.as_ref(), value) {
                    Ok(stored) => {
                        guard.complete();
                        Ok(stored)
                    }
                    Err(error) => {
                        guard.failed_with(&error);
                        Err(error)
                    }
                },
                Err(error) => {
                    guard.failed_with(&error);
                    Err(error)
                }
            }
        })
    }
}

impl<H, A, B, K> NodeSite for Leaf2<H, A, B, K>
where
    H: NodeCall2<A, B, K>,
    A: 'static,
    B: 'static,
    K: LeafOutput,
{
    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn inputs(&self) -> &[RefId] {
        &self.inputs
    }

    #[allow(dead_code)] // 仅由 cfg(test) 验收与观察路径使用（非 test 构建无消费者）
    fn outputs(&self) -> &[RefId] {
        self.output.as_slice()
    }

    fn invoke<'a>(
        &'a self,
        ctx: &'a mut ExecutionContext,
        scope: &'a ScopeId,
    ) -> NodeFut<'a, Option<DataId>> {
        Box::pin(async move {
            if let Some(position) = self.output.as_ref() {
                // 建立阶段失败：按已验收的建立失败终止机制保存执行错误、原 Scope 诊断与
                // 本次调用使用的 Scope；调用方即使捕获错误也不能继续普通业务。
                if let Err(error) = ctx.precheck_output(scope, position) {
                    ctx.terminate(
                        TerminationKind::BodyError,
                        Some(scope.clone()),
                        "leaf output precheck failed",
                        Some(error.clone()),
                    );
                    return Err(BodyError::from(error));
                }
            }
            let mut guard = ctx.enter(InvocationKind::Leaf, scope, false)?;
            let outcome = {
                // 双输入先完整解析再调用：后项缺失或类型不符时业务 body 不运行。
                let shared: &ExecutionContext = &guard;
                match (
                    shared.resolve::<A>(scope, &self.inputs[0]),
                    shared.resolve::<B>(scope, &self.inputs[1]),
                ) {
                    (Ok(first), Ok(second)) => self.node.call(first, second).await,
                    (Err(error), _) | (_, Err(error)) => Err(BodyError::from(error)),
                }
            };
            match outcome {
                Ok(value) => match K::store(&mut guard, scope, self.output.as_ref(), value) {
                    Ok(stored) => {
                        guard.complete();
                        Ok(stored)
                    }
                    Err(error) => {
                        guard.failed_with(&error);
                        Err(error)
                    }
                },
                Err(error) => {
                    guard.failed_with(&error);
                    Err(error)
                }
            }
        })
    }
}
