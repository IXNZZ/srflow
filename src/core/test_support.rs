//! V21-06 起的共享测试支持：poll／gate 驱动、业务事件与 child Scope 观测、Root 输入登记。
//!
//! 只在本 crate 的 `#[cfg(test)]` 构建下存在；不提供生产调用、业务 Data 注入或长期借用
//! 存储。V21-05 与 V21-06 的样本共用这里的实现，避免复制第二份 gate／pending 机制：
//! - [`drive`]／[`drive_pinned`]／[`advance_to_pending`]：手动 poll（`Waker::noop`），
//!   每次 Pending 先释放挂起点；
//! - [`install_gate`]／[`release_gate`]／[`gate_wait`]：线程局部挂起点；
//! - [`record`]／[`take_events`]／[`take_shared_events`]：业务见证与 V21-04 既有
//!   `context::creation_counts` 事件合入同一序列；
//! - child Scope 观测：窄函数读写线程局部列表，不公开可任意修改的集合；
//! - [`RootInput`]／[`root_input`]：只经真实 `register_owned` 把应用层夹具值登记到
//!   RootScope 的声明输入位置。
//!
//! 每个样本在起点重置自己使用的 gate／事件／child 观测，保持线程局部隔离；业务值、
//! Node／Orchestrator 夹具与业务调用计数留在各自测试模块。

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll, Waker};

use super::context::ExecutionContext;
use super::identity::ScopeId;
use super::ref_id::RefId;

// ---- 业务事件（与 Context 事件同序列） ----

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// 记录一个可观察事件（顺序即证据）。
///
/// 同时写入 V21-04 既有的 `context::creation_counts` 事件日志：业务 Drop 见证因此与
/// guard 清理／frame 退出事件处在同一条可比较序列上（frame 次序证据）。
pub(crate) fn record(event: &str) {
    EVENTS.with(|events| events.borrow_mut().push(event.to_string()));
    super::context::creation_counts::record_event(event);
}

/// 取走 Context 侧共享日志（含 guard 清理与 frame 退出事件）。
pub(crate) fn take_shared_events() -> Vec<String> {
    super::context::creation_counts::take_events()
}

/// 取走当前线程的业务事件序列。
pub(crate) fn take_events() -> Vec<String> {
    EVENTS.with(|events| std::mem::take(&mut *events.borrow_mut()))
}

// ---- 线程局部挂起点 ----

thread_local! {
    static PENDING_GATE: RefCell<Option<Rc<Cell<bool>>>> = const { RefCell::new(None) };
}

/// 安装一个挂起点；异步业务体在 await 前调用 [`gate_wait`]。
pub(crate) fn install_gate() {
    PENDING_GATE.with(|gate| *gate.borrow_mut() = Some(Rc::new(Cell::new(false))));
}

/// 释放已安装的挂起点。
pub(crate) fn release_gate() {
    PENDING_GATE.with(|gate| {
        if let Some(open) = gate.borrow().as_ref() {
            open.set(true);
        }
    });
}

/// 在挂起点上等待：安装时先 Pending，释放后下一次 poll 返回。
pub(crate) async fn gate_wait() {
    let open = PENDING_GATE.with(|gate| gate.borrow().clone());
    if let Some(open) = open {
        std::future::poll_fn(|_| {
            if open.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

// ---- Future 驱动 ----

/// 推进到 Ready；每次 Pending 先释放挂起点。
pub(crate) fn drive<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
                release_gate();
            }
        }
    }
}

/// 推进一个已 boxed 的 Future 到 Ready；每次 Pending 先释放挂起点。
pub(crate) fn drive_pinned<F: Future + ?Sized>(mut boxed: Pin<Box<F>>) -> F::Output {
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match boxed.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
                release_gate();
            }
        }
    }
}

/// 推进到第 `stops` 次 Pending 后停下，返回仍持有 Future 本体的 Box。
///
/// 丢弃这个 Box 才是"丢弃 Future 本体"；只丢一个 `Pin<&mut F>` 或引用不算。
pub(crate) fn advance_to_pending<F: Future>(future: F, stops: usize) -> Pin<Box<F>> {
    let mut boxed = Box::pin(future);
    for stop in 1..=stops {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match boxed.as_mut().poll(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!("future completed before pending stop {stop}"),
        }
    }
    boxed
}

// ---- child Scope 观测（窄接口） ----

thread_local! {
    static CHILD_SCOPES: RefCell<Vec<ScopeId>> = const { RefCell::new(Vec::new()) };
}

/// 清空本次样本记录的 child Scope。
pub(crate) fn child_scope_reset() {
    CHILD_SCOPES.with(|scopes| scopes.borrow_mut().clear());
}

/// 记录一个被观察到的直接 child Scope（由样本中的编排体调用）。
pub(crate) fn child_scope_record(child: ScopeId) {
    CHILD_SCOPES.with(|scopes| scopes.borrow_mut().push(child));
}

/// 当前记录的 child Scope 快照（按记录顺序）。
pub(crate) fn child_scope_snapshot() -> Vec<ScopeId> {
    CHILD_SCOPES.with(|scopes| scopes.borrow().clone())
}

// ---- 真实调用边界的 child Scope 观测（cfg(test) 只读元数据） ----

thread_local! {
    static BOUNDARY_CHILD_SCOPES: RefCell<Vec<ScopeId>> = const { RefCell::new(Vec::new()) };
    static BOUNDARY_ADDRESSES: RefCell<Vec<(ScopeId, *const (), *const (), *const ())>> =
        const { RefCell::new(Vec::new()) };
    #[allow(clippy::type_complexity)]
    static EXPORT_ATTEMPTS: RefCell<Vec<(ScopeId, Vec<(RefId, crate::core::identity::DataId)>, Vec<crate::core::identity::DataId>)>> =
        const { RefCell::new(Vec::new()) };
}

/// 清空由真实调用边界自动记录的 child Scope。
pub(crate) fn boundary_child_scope_reset() {
    BOUNDARY_CHILD_SCOPES.with(|scopes| scopes.borrow_mut().clear());
}

/// 真实 `OrchSite` 调用边界建立 child 后记录其身份（仅元数据）。
pub(crate) fn boundary_child_scope_record(child: ScopeId) {
    BOUNDARY_CHILD_SCOPES.with(|scopes| scopes.borrow_mut().push(child));
}

/// 由真实调用边界记录的 child Scope 快照（按建立顺序）。
pub(crate) fn boundary_child_scope_snapshot() -> Vec<ScopeId> {
    BOUNDARY_CHILD_SCOPES.with(|scopes| scopes.borrow().clone())
}

/// 记录一次真实调用边界的执行域地址（身份／Coordinator／Container）。
pub(crate) fn boundary_address_record(
    child: ScopeId,
    identity: *const (),
    coordinator: *const (),
    container: *const (),
) {
    BOUNDARY_ADDRESSES.with(|entries| {
        entries
            .borrow_mut()
            .push((child.clone(), identity, coordinator, container))
    });
    boundary_child_scope_record(child);
}

/// 由真实调用边界记录的执行域地址快照。
#[allow(clippy::type_complexity)]
pub(crate) fn boundary_address_snapshot() -> Vec<(ScopeId, *const (), *const (), *const ())> {
    BOUNDARY_ADDRESSES.with(|entries| entries.borrow().clone())
}

/// 清空真实调用边界记录的执行域地址。
pub(crate) fn boundary_address_reset() {
    BOUNDARY_ADDRESSES.with(|entries| entries.borrow_mut().clear());
    EXPORT_ATTEMPTS.with(|entries| entries.borrow_mut().clear());
}

/// 真实调用边界在整组 Export 预检失败时记录 child 的本地绑定与责任集合（只读元数据）。
pub(crate) fn export_attempt_record(
    child: ScopeId,
    refs: Vec<(RefId, crate::core::identity::DataId)>,
    owned: Vec<crate::core::identity::DataId>,
) {
    EXPORT_ATTEMPTS.with(|entries| entries.borrow_mut().push((child, refs, owned)));
}

/// 取走 Export 提交前记录的快照（失败时用于比较）。
#[allow(clippy::type_complexity)]
pub(crate) fn export_attempt_snapshot() -> Vec<(
    ScopeId,
    Vec<(RefId, crate::core::identity::DataId)>,
    Vec<crate::core::identity::DataId>,
)> {
    EXPORT_ATTEMPTS.with(|entries| std::mem::take(&mut *entries.borrow_mut()))
}

// ---- Root 输入登记 ----

/// 一个 Root 输入：声明位置 + 把值登记到该位置。
pub(crate) type RootInput = (RefId, Box<dyn FnOnce(&mut ExecutionContext, &RefId)>);

/// 把 `value` 登记到声明输入位置的便捷构造。
///
/// 只代表测试驱动中的 Application 交接：经真实 `register_owned` 进入 RootScope 的
/// 声明输入位置，不提供生产 Data 注入入口。
pub(crate) fn root_input<T: 'static>(
    position: &super::data_ref::DataRef<T>,
    value: T,
) -> RootInput {
    let position = position.position().clone();
    (
        position,
        Box::new(move |ctx: &mut ExecutionContext, position: &RefId| {
            ctx.register_owned(&ctx.root_scope(), position, value)
                .expect("root input");
        }),
    )
}
