use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};

use super::creation_counts;
use super::{BodyError, ExecutionContext, InvocationKind, TerminationKind, invoke_leaf};

use crate::core::internal_error::ScopeError;
use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};
use crate::core::runtime::RootExecution;

use crate::core::scope::{ExportSlot, ImportSlot, ScopeState, StateImportSlot};

/// 非 Clone 业务值，带 Drop 观测。
struct Tracked {
    num: u32,
    drops: Arc<AtomicUsize>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        creation_counts::record_event(&format!("value:{}", self.num));
    }
}

/// 记录"内层 Future 析构"事件的探针：随 async 状态机一起析构。
struct Probe(&'static str);

impl Drop for Probe {
    fn drop(&mut self) {
        creation_counts::record_event(self.0);
    }
}

/// 可控挂起点：关闭时返回真实 Pending，`release` 后下一个 poll 返回 Ready。
///
/// 不使用 `wake_by_ref` 自旋；测试通过手动 poll 驱动，无需 executor。
#[derive(Clone)]
struct Gate {
    open: Rc<Cell<bool>>,
}

impl Gate {
    fn closed() -> Self {
        Self {
            open: Rc::new(Cell::new(false)),
        }
    }

    fn release(&self) {
        self.open.set(true);
    }

    async fn wait(&self) {
        std::future::poll_fn(|_| {
            if self.open.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

/// 手动推进直到 Ready；每次 Pending 后调用 `on_pending`（用于释放挂起点）。
fn drive_to_ready<F: Future>(future: F, mut on_pending: impl FnMut(usize)) -> F::Output {
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
                on_pending(pending);
            }
        }
    }
}

/// 推进到第 `stops` 次 Pending 后停下，返回仍持有 Future 本体的 Box。
///
/// 丢弃该 Box 才是"丢弃 Future 本体"；只丢一个 `Pin<&mut F>` 或引用不算。
fn advance_to_pending<F: Future>(future: F, stops: usize) -> Pin<Box<F>> {
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

/// 诊断观察：经协调组件读取业务值，不经 Context 业务入口。
///
/// 终止后的观察只能用这一条（§4.8 允许纯诊断观察，业务入口已拒绝）。
fn observe_num(
    ctx: &ExecutionContext,
    scope: &crate::core::identity::ScopeId,
    position: &RefId,
) -> Result<u32, ScopeError> {
    Ok(ctx.coordinator.resolve::<Tracked>(scope, position)?.num)
}

/// 诊断观察：一个 DataId 的唯一责任 Scope。
fn observe_owner(
    ctx: &ExecutionContext,
    id: &crate::core::identity::DataId,
) -> Result<crate::core::identity::ScopeId, ScopeError> {
    ctx.coordinator.owner_probe(id)
}

/// 以给定 Drop 观测句柄构造业务值。
fn tracked_num(num: u32, drops: &Arc<AtomicUsize>) -> Tracked {
    Tracked {
        num,
        drops: Arc::clone(drops),
    }
}

struct Fixture {
    execution: RootExecution,
    ids: RefIdAllocator,
    drops: Arc<AtomicUsize>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            execution: RootExecution::start(),
            ids: RefIdAllocator::new(RefIdSource::new()),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn context_mut(&mut self) -> &mut ExecutionContext {
        self.execution.context_mut()
    }

    fn take_execution(self) -> RootExecution {
        self.execution
    }

    fn ref_id(&self) -> RefId {
        self.ids.allocate().unwrap()
    }

    fn tracked(&self, num: u32) -> Tracked {
        Tracked {
            num,
            drops: Arc::clone(&self.drops),
        }
    }

    fn drops(&self) -> usize {
        self.drops.load(Ordering::SeqCst)
    }

    fn root(&self) -> crate::core::identity::ScopeId {
        self.execution.context().root_scope()
    }

    /// Root 输入夹具：经真实 `register_owned` 登记 Application 交接的 owned 输入。
    fn root_input(&mut self, value: Tracked) -> (RefId, crate::core::identity::DataId) {
        let position = self.ref_id();
        let root = self.root();
        let id = self
            .execution
            .context_mut()
            .register_owned(&root, &position, value)
            .unwrap();
        (position, id)
    }

    fn spare_value(&self, num: u32) -> Tracked {
        self.tracked(num)
    }
}

// ---- 叶子／编排执行体（只接收 &T 或 Context，不接收 caller 输出位置） ----

struct LeafConfig {
    gate: Gate,
}

async fn leaf_double(input: &Tracked, config: LeafConfig) -> Tracked {
    config.gate.wait().await;
    Tracked {
        num: input.num * 2,
        drops: Arc::clone(&input.drops),
    }
}

#[test]
fn d01_root_creation_is_unique_and_shared() {
    let before = creation_counts::snapshot();
    let mut first = RootExecution::start();
    let second = RootExecution::start();
    let after = creation_counts::snapshot();
    assert_eq!(
        (after.0 - before.0, after.1 - before.1, after.2 - before.2),
        (2, 2, 2),
        "each root execution creates exactly one context, coordinator and container"
    );

    // 两个同时存活的 Root：身份与三项地址互不相同。
    let identity_first = first.context().identity_probe();
    let coordinator_first = first.context().coordinator_probe();
    let container_first = first.context().container_probe();
    assert_ne!(identity_first, second.context().identity_probe());
    assert_ne!(coordinator_first, second.context().coordinator_probe());
    assert_ne!(container_first, second.context().container_probe());

    // 同一 Execution 内 child／grandchild 共享身份与存储，且不新建任何一份。
    let root = first.context().root_scope();
    {
        let guard = first
            .context_mut()
            .enter(InvocationKind::Root, &root, true)
            .unwrap();
        let mut guard = guard;
        let child = guard.create_child(&root).unwrap();
        let grandchild = guard.create_child(&child).unwrap();
        assert!(grandchild != child);
        guard.complete();
    }
    assert_eq!(
        creation_counts::snapshot(),
        after,
        "nested scopes create no new pieces"
    );
    assert_eq!(identity_first, first.context().identity_probe());
    assert_eq!(coordinator_first, first.context().coordinator_probe());
    assert_eq!(container_first, first.context().container_probe());
}

#[test]
fn d03_invocation_is_not_scope() {
    struct Config {
        root: crate::core::identity::ScopeId,
        input: RefId,
        leaf_output: RefId,
        gate: Gate,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        assert_eq!(ctx.frame_depth(), 1);
        // 叶子沿用 RootScope：不新增 Scope，也不承担 RootScope 的销毁责任。
        invoke_leaf(
            ctx,
            &root,
            &config.input,
            &config.leaf_output,
            LeafConfig {
                gate: config.gate.clone(),
            },
            leaf_double,
        )
        .await?;
        assert_eq!(ctx.frame_depth(), 1, "leaf frame restored to its caller");
        assert_eq!(ctx.current_scope().as_ref(), Some(&root));
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input_pos, input_id) = f.root_input(f.tracked(2));
    let leaf_output = f.ref_id();
    let gate = Gate::closed();
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(
            execution,
            Config {
                root: root.clone(),
                input: input_pos,
                leaf_output: leaf_output.clone(),
                gate: gate.clone(),
            },
            body,
        ),
        |pending| {
            if pending >= 1 {
                gate.release();
            }
        },
    );
    assert!(exit.terminated().is_none());
    let _ = (input_id, leaf_output);
}

#[test]
fn d04_frame_stack_restores_and_orders() {
    struct ChildConfig {
        root: crate::core::identity::ScopeId,
        child_input: RefId,
        leaf_output: RefId,
        leaf_output_again: RefId,
        declared_output: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
    }
    struct Config {
        root: crate::core::identity::ScopeId,
        input: RefId,
        child_input: RefId,
        leaf_output: RefId,
        leaf_output_again: RefId,
        boundary_output: RefId,
        root_export: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
    }

    async fn child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ChildConfig,
    ) -> Result<(), BodyError> {
        assert_eq!(
            ctx.frame_depth(),
            2,
            "boundary frame sits on the root frame"
        );
        assert_eq!(
            ctx.current_frame().unwrap().kind(),
            InvocationKind::Boundary
        );
        assert_eq!(ctx.current_scope().as_ref(), Some(child));

        // 边界内两次顺序叶子调用：每次结束后都回到本边界 frame。
        invoke_leaf(
            ctx,
            child,
            &config.child_input,
            &config.leaf_output,
            LeafConfig {
                gate: config.gate.clone(),
            },
            leaf_double,
        )
        .await?;
        assert_eq!(ctx.frame_depth(), 2);
        // 顺序第二次调用：输出位置必须另取，单赋值不可重绑。
        invoke_leaf(
            ctx,
            child,
            &config.child_input,
            &config.leaf_output_again,
            LeafConfig {
                gate: config.gate.clone(),
            },
            leaf_double,
        )
        .await?;
        assert_eq!(ctx.frame_depth(), 2);

        // 编排执行体可建立 Item／Round 等 descendant 作用域，由本边界退出覆盖。
        let extra = ctx.create_child(child)?;
        assert_eq!(ctx.parent_of(&extra)?.as_ref(), Some(child));
        ctx.abort(&extra)?;

        let value = {
            let gate = config.gate.clone();
            gate.wait().await;
            tracked_num(3, &config.drops)
        };
        ctx.register_owned(child, &config.declared_output, value)?;
        let _ = &config.root;
        Ok(())
    }

    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        assert_eq!(ctx.frame_depth(), 1);
        assert_eq!(ctx.current_frame().unwrap().kind(), InvocationKind::Root);

        let mut outputs = vec![ExportSlot::new::<Tracked>(
            &config.boundary_output,
            &config.root_export,
        )];
        let declared = vec![config.boundary_output.clone()];
        let imports = [ImportSlot::new::<Tracked>(
            &config.input,
            &config.child_input,
        )];
        crate::core::context::observe::invoke_boundary(
            ctx,
            &imports,
            &declared,
            &mut outputs,
            ChildConfig {
                root: root.clone(),
                child_input: config.child_input.clone(),
                leaf_output: config.leaf_output.clone(),
                leaf_output_again: config.leaf_output_again.clone(),
                declared_output: config.boundary_output.clone(),
                gate: config.gate.clone(),
                drops: Arc::clone(&config.drops),
            },
            child_body,
        )
        .await?;

        assert_eq!(
            ctx.frame_depth(),
            1,
            "caller frame restored after the boundary"
        );
        assert_eq!(ctx.current_scope().as_ref(), Some(&root));
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input_pos, _) = f.root_input(f.tracked(2));
    let child_input = f.ref_id();
    let leaf_output = f.ref_id();
    let leaf_output_again = f.ref_id();
    let boundary_output = f.ref_id();
    let root_export = f.ref_id();
    let gate = Gate::closed();
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(
            execution,
            Config {
                root,
                input: input_pos,
                child_input,
                leaf_output,
                leaf_output_again,
                boundary_output,
                root_export,
                gate: gate.clone(),
                drops,
            },
            body,
        ),
        |pending| {
            if pending >= 1 {
                gate.release();
            }
        },
    );
    assert!(
        exit.terminated().is_none(),
        "unexpected termination: {:?}",
        exit.terminated()
    );
    assert!(exit.close_error().is_none());
}

/// 另一类型，用于多输出边界与类型不符。
struct Other(u32);

#[test]
fn d05_explicit_input_isolation_and_rejections() {
    struct Config {
        root: crate::core::identity::ScopeId,
        root_local: RefId,
        child_local: RefId,
        state_local: RefId,
        mixed_local: RefId,
        mixed_state: RefId,
        not_imported: RefId,
        illegal_position: RefId,
        foreign_scope: crate::core::identity::ScopeId,
        sibling_position: RefId,
        root_id: crate::core::identity::DataId,
        drops: Arc<AtomicUsize>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        // sibling-owned 值：由本调用建立（Root 的直接 child）并登记。
        let sibling = ctx.create_child(&root)?;
        let sibling_owned = ctx.register_owned(
            &sibling,
            &config.sibling_position,
            tracked_num(99, &config.drops),
        )?;
        // 导入不复制业务值、不改变 owner。
        assert_eq!(observe_owner(ctx, &config.root_id)?, root);
        assert_eq!(observe_owner(ctx, &sibling_owned)?, sibling);

        // 显式导入后才可解析；未导入位置不搜索 ancestor.refs。
        let s1 = ctx.create_child(&root)?;
        ctx.import_batch(
            &s1,
            &root,
            &[ImportSlot::new::<Tracked>(
                &config.root_local,
                &config.child_local,
            )],
        )?;
        assert_eq!(ctx.resolve::<Tracked>(&s1, &config.child_local)?.num, 11);
        assert!(matches!(
            ctx.resolve::<Tracked>(&s1, &config.not_imported),
            Err(ScopeError::RefNotBound { .. })
        ));
        // foreign 与 Closed 在返回借用前拒绝。
        assert!(matches!(
            ctx.resolve::<Tracked>(&config.foreign_scope, &config.child_local),
            Err(ScopeError::ForeignExecution { .. })
        ));
        let closed = ctx.create_child(&root)?;
        ctx.abort(&closed)?;
        assert!(matches!(
            ctx.resolve::<Tracked>(&closed, &config.child_local),
            Err(ScopeError::ScopeClosed { .. })
        ));
        // 非法 owner（sibling-owned）：单有 target 存活不授予读取权。
        let s2 = ctx.create_child(&root)?;
        ctx.coordinator.inject_target_probe(
            &s2,
            &config.illegal_position,
            crate::core::scope::RefTarget::Data(sibling_owned.clone()),
        );
        assert!(matches!(
            ctx.resolve::<Tracked>(&s2, &config.illegal_position),
            Err(ScopeError::IllegalOwner { .. })
        ));

        // 混合 local + 状态来源的整组导入。
        let controller = ctx.create_child(&root)?;
        ctx.import_batch(
            &controller,
            &root,
            &[ImportSlot::new::<Tracked>(
                &config.root_local,
                &config.state_local,
            )],
        )?;
        let state = ctx.register_state::<Tracked>(&controller, &config.state_local)?;
        let target = ctx.create_child(&controller)?;
        ctx.import_batch_with_states(
            &target,
            &controller,
            &[ImportSlot::new::<Tracked>(
                &config.state_local,
                &config.mixed_local,
            )],
            &[StateImportSlot::new::<Tracked>(&state, &config.mixed_state)],
        )?;
        assert_eq!(
            ctx.resolve::<Tracked>(&target, &config.mixed_local)?.num,
            11
        );
        assert_eq!(
            ctx.resolve::<Tracked>(&target, &config.mixed_state)?.num,
            11
        );

        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            0,
            "imports neither clone nor drop input data"
        );
        // 本调用建立的 Scope 必须由自己关闭，否则 Root 收口会被活跃 descendant 拒绝。
        ctx.abort(&target)?;
        ctx.abort(&controller)?;
        ctx.abort(&s1)?;
        ctx.abort(&s2)?;
        ctx.abort(&sibling)?;
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (root_local, root_id) = f.root_input(f.tracked(11));
    let child_local = f.ref_id();
    let state_local = f.ref_id();
    let mixed_local = f.ref_id();
    let mixed_state = f.ref_id();
    let not_imported = f.ref_id();
    let illegal_position = f.ref_id();
    // 另一次 Execution 的 ScopeId。
    let foreign_execution = RootExecution::start();
    let foreign_scope = foreign_execution.context().root_scope();

    let config = Config {
        root: root.clone(),
        root_local,
        child_local,
        state_local,
        mixed_local,
        mixed_state,
        not_imported,
        illegal_position,
        foreign_scope,
        sibling_position: f.ref_id(),
        root_id,
        drops: Arc::clone(&f.drops),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
    assert!(exit.close_error().is_none());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "the sibling value and the root input each drop exactly once at scope exit"
    );
}

#[test]
fn d06_leaf_await_registers_output_once_and_unit_leaves_allocate_nothing() {
    let _ = Other(0);
    async fn unit_leaf(_input: &Tracked, gate: Gate) {
        gate.wait().await;
    }
    async fn body(ctx: &mut ExecutionContext, config: D06Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let id = invoke_leaf(
            ctx,
            &root,
            &config.input,
            &config.output,
            LeafConfig {
                gate: config.gate.clone(),
            },
            leaf_double,
        )
        .await?;
        assert_eq!(ctx.resolve::<Tracked>(&root, &config.output)?.num, 4);
        assert_eq!(ctx.frame_depth(), 1, "leaf frame restored");
        // 无输出叶子：不登记 Data，也不产生新的 DataId。
        crate::core::context::observe::invoke_leaf_unit(
            ctx,
            &root,
            &config.input,
            config.gate.clone(),
            unit_leaf,
        )
        .await?;
        let probe = ctx.register_owned(&root, &config.probe, tracked_num(1, &config.drops))?;
        assert_eq!(probe.seq(), id.seq() + 1, "unit output allocated no DataId");
        // 输入保留原 owner；输出恰好登记一次。
        assert_eq!(observe_owner(ctx, &config.input_id)?, root);
        assert_eq!(observe_num(ctx, &root, &config.output)?, 4);
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input_pos, input_id) = f.root_input(f.tracked(2));
    let output = f.ref_id();
    let probe_pos = f.ref_id();
    let gate = Gate::closed();
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let config = D06Config {
        root: root.clone(),
        input: input_pos,
        input_id: input_id.clone(),
        output: output.clone(),
        probe: probe_pos,
        gate: gate.clone(),
        drops,
    };
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |pending| {
            if pending >= 1 {
                gate.release();
            }
        },
    );
    assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
    assert!(exit.close_error().is_none());
    let _ = (&input_id, &root);
}

struct D06Config {
    root: crate::core::identity::ScopeId,
    input: RefId,
    input_id: crate::core::identity::DataId,
    output: RefId,
    probe: RefId,
    gate: Gate,
    drops: Arc<AtomicUsize>,
}

/// 手动推进到 Ready，同时保留 Future 本体（用于"Ready 后丢弃"对照）。
fn poll_to_ready_keeping<F: Future>(
    future: F,
    mut on_pending: impl FnMut(usize),
) -> (Pin<Box<F>>, F::Output) {
    let mut boxed = Box::pin(future);
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = TaskContext::from_waker(waker);
        match boxed.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return (boxed, output),
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
                on_pending(pending);
            }
        }
    }
}

struct D07Config {
    root: crate::core::identity::ScopeId,
    first: RefId,
    second: RefId,
    export_a: RefId,
    export_b: RefId,
    export_a2: RefId,
    export_b2: RefId,
    gate: Gate,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

#[test]
fn d07_boundary_exports_two_outputs_atomically() {
    struct ChildConfig {
        first: RefId,
        second: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ChildConfig,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        let value = {
            let gate = config.gate.clone();
            gate.wait().await;
            tracked_num(1, &config.drops)
        };
        ctx.register_owned(child, &config.first, value)?;
        ctx.register_owned(child, &config.second, Other(2))?;
        Ok(())
    }
    async fn body(ctx: &mut ExecutionContext, config: D07Config) -> Result<(), BodyError> {
        let root = config.root.clone();

        // 正常：两项声明输出由调用边界整组提交。
        let mut outputs = vec![
            ExportSlot::new::<Tracked>(&config.first, &config.export_a),
            ExportSlot::new::<Other>(&config.second, &config.export_b),
        ];
        let declared = vec![config.first.clone(), config.second.clone()];
        crate::core::context::observe::invoke_boundary(
            ctx,
            &[],
            &declared,
            &mut outputs,
            ChildConfig {
                first: config.first.clone(),
                second: config.second.clone(),
                gate: config.gate.clone(),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&config.slot),
            },
            child_body,
        )
        .await?;
        assert_eq!(observe_num(ctx, &root, &config.export_a)?, 1);
        assert_eq!(ctx.resolve::<Other>(&root, &config.export_b)?.0, 2);

        // 失败：后项类型不符 → 无部分输出，child 临时值恰一次清理。
        let mut outputs_bad = vec![
            ExportSlot::new::<Tracked>(&config.first, &config.export_a2),
            ExportSlot::new::<Tracked>(&config.second, &config.export_b2),
        ];
        let slot = Rc::new(Cell::new(None));
        let failure = crate::core::context::observe::invoke_boundary(
            ctx,
            &[],
            &declared,
            &mut outputs_bad,
            ChildConfig {
                first: config.first.clone(),
                second: config.second.clone(),
                gate: config.gate.clone(),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&slot),
            },
            child_body,
        )
        .await
        .unwrap_err();
        assert!(
            failure.scope_error().is_some(),
            "second output type mismatch"
        );
        let faulty = slot.take().expect("child scope recorded");
        assert_eq!(
            ctx.state(&faulty)?,
            ScopeState::Closed,
            "failed boundary closes its scope"
        );
        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            1,
            "only the failed boundary's own temporary is dropped so far"
        );
        assert!(ctx.parent_of(&root)?.is_none());
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (_input_pos, _) = f.root_input(f.tracked(5));
    let gate = Gate::closed();
    let config = D07Config {
        root: root.clone(),
        first: f.ref_id(),
        second: f.ref_id(),
        export_a: f.ref_id(),
        export_b: f.ref_id(),
        export_a2: f.ref_id(),
        export_b2: f.ref_id(),
        gate: gate.clone(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |pending| {
            if pending >= 1 {
                gate.release();
            }
        },
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    assert!(
        exit.cleanup_events() >= 1,
        "failing boundary guard cleaned its scope"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        3,
        "failed temporary + the value exported before the failure + the root input, each once"
    );
}

struct ControllerConfig {
    rules: RefId,
    rules_id: crate::core::identity::DataId,
    item_out: RefId,
    round_out: RefId,
    expose_a: RefId,
    expose_b: RefId,
    vec_out: RefId,
    drops: Arc<AtomicUsize>,
}

async fn controller_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: ControllerConfig,
) -> Result<(), BodyError> {
    // 控制器输入仍是 root 负责的 imported 值。
    let collector = ctx.begin_collector::<Tracked>(child)?;

    // D09：ItemScope（控制器的直接 child）直接 Consume。
    let item = ctx.create_child(child)?;
    ctx.register_owned(&item, &config.item_out, tracked_num(7, &config.drops))?;
    ctx.consume_item(&item, &config.item_out, &collector)?;
    assert_eq!(ctx.state(&item)?, ScopeState::Closed);

    // imported 输出拒绝：ItemScope 重新暴露 imported Rules 没有可转移责任。
    let item2 = ctx.create_child(child)?;
    ctx.import_batch(
        &item2,
        child,
        &[ImportSlot::new::<Tracked>(&config.rules, &config.item_out)],
    )?;
    assert!(matches!(
        ctx.consume_item(&item2, &config.item_out, &collector),
        Err(ScopeError::IllegalOwner { .. })
    ));
    assert_eq!(
        ctx.coordinator.refs_len_probe(child)?,
        1,
        "no per-item parent ref is bound on the controller"
    );

    // D08：两轮 Promote；第二轮原样保留同一 DataId。
    let state = ctx.register_state::<Tracked>(child, &config.rules)?;
    let round = ctx.create_child(child)?;
    ctx.register_owned(&round, &config.round_out, tracked_num(8, &config.drops))?;
    ctx.promote(&round, &config.round_out, &state)?;
    assert_eq!(ctx.state(&round)?, ScopeState::Closed);

    let round2 = ctx.create_child(child)?;
    ctx.import_batch_with_states(
        &round2,
        child,
        &[],
        &[
            StateImportSlot::new::<Tracked>(&state, &config.expose_a),
            StateImportSlot::new::<Tracked>(&state, &config.expose_b),
        ],
    )?;
    ctx.promote(&round2, &config.expose_b, &state)?;
    assert_eq!(
        config.drops.load(Ordering::SeqCst),
        0,
        "same-DataId promotion destroys nothing"
    );
    ctx.recycle_pending(child)?;
    assert_eq!(
        observe_owner(ctx, &config.rules_id)?,
        ctx.coordinator.owner_probe(&config.rules_id)?,
        "imported initial value keeps its owner"
    );
    assert_eq!(observe_num(ctx, child, &config.rules)?, 11);

    // 完成 collector：新的普通 Vec 绑定控制器最终输出位置。
    ctx.finish_collector(child, &collector, &config.vec_out)?;
    assert_eq!(
        ctx.resolve::<Vec<Tracked>>(child, &config.vec_out)?.len(),
        1
    );
    Ok(())
}

#[test]
fn d08_promote_and_d09_consume_through_context() {
    struct Config {
        root: crate::core::identity::ScopeId,
        rules: RefId,
        rules_id: crate::core::identity::DataId,
        controller_rules: RefId,
        controller_rules_id: crate::core::identity::DataId,
        item_out: RefId,
        round_out: RefId,
        expose_a: RefId,
        expose_b: RefId,
        vec_out: RefId,
        root_export: RefId,
        drops: Arc<AtomicUsize>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let mut outputs = vec![ExportSlot::new::<Vec<Tracked>>(
            &config.vec_out,
            &config.root_export,
        )];
        let declared = vec![config.vec_out.clone()];
        let imports = [ImportSlot::new::<Tracked>(
            &config.rules,
            &config.controller_rules,
        )];
        let child = crate::core::context::observe::invoke_boundary(
            ctx,
            &imports,
            &declared,
            &mut outputs,
            ControllerConfig {
                rules: config.controller_rules.clone(),
                rules_id: config.controller_rules_id.clone(),
                item_out: config.item_out.clone(),
                round_out: config.round_out.clone(),
                expose_a: config.expose_a.clone(),
                expose_b: config.expose_b.clone(),
                vec_out: config.vec_out.clone(),
                drops: Arc::clone(&config.drops),
            },
            controller_body,
        )
        .await?;
        assert_eq!(
            ctx.state(&child)?,
            ScopeState::Closed,
            "exported boundary closed"
        );
        assert_eq!(observe_owner(ctx, &config.rules_id)?, root);
        assert_eq!(
            ctx.resolve::<Vec<Tracked>>(&root, &config.root_export)?
                .len(),
            1
        );
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (rules, rules_id) = f.root_input(f.tracked(11));
    let controller_rules = f.ref_id();
    let item_out = f.ref_id();
    let round_out = f.ref_id();
    let expose_a = f.ref_id();
    let expose_b = f.ref_id();
    let vec_out = f.ref_id();
    let root_export = f.ref_id();
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let config = Config {
        root: root.clone(),
        rules: rules.clone(),
        rules_id: rules_id.clone(),
        controller_rules,
        controller_rules_id: rules_id.clone(),
        item_out,
        round_out,
        expose_a,
        expose_b,
        vec_out,
        root_export,
        drops: Arc::clone(&drops),
    };
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert!(exit.terminated().is_none(), "{:?}", exit.terminated());
    assert!(exit.close_error().is_none());
    // Rules 由 root 负责，未被子边界回收；controller 的自有值随后各一次析构。
    assert_eq!(
        drops.load(Ordering::SeqCst),
        3,
        "controller state + collected element (exported then dropped with the root) + root input"
    );
    let _ = (root, rules);
}

struct ErrorConfig {
    input: RefId,
    temp: RefId,
    leaf_out: RefId,
    gate: Gate,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

async fn failing_leaf(_input: &Tracked, gate: Gate) -> Result<Tracked, BodyError> {
    gate.wait().await;
    Err(BodyError::new("leaf body failed"))
}

async fn error_child_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: ErrorConfig,
) -> Result<(), BodyError> {
    config.slot.set(Some(child.clone()));
    ctx.register_owned(child, &config.temp, tracked_num(3, &config.drops))?;

    let result = crate::core::context::observe::invoke_fallible_leaf(
        ctx,
        child,
        &config.input,
        &config.leaf_out,
        config.gate.clone(),
        failing_leaf,
    )
    .await;
    assert!(result.is_err());
    // D11：沿用 caller Scope 的叶子 guard 不清理 caller 的绑定与责任。
    assert_eq!(
        ctx.state(child)?,
        ScopeState::Active,
        "leaf guard must not clean the caller scope"
    );
    assert_eq!(
        observe_num(ctx, child, &config.temp)?,
        3,
        "caller-owned temporary is untouched by the leaf failure"
    );
    Err(BodyError::new("boundary propagates the leaf failure"))
}

#[test]
fn d10_error_exit_and_d11_leaf_guard_keeps_caller_scope() {
    async fn body(ctx: &mut ExecutionContext, config: D10Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let failure = crate::core::context::observe::invoke_boundary(
            ctx,
            &[ImportSlot::new::<Tracked>(
                &config.input,
                &config.child_input,
            )],
            &[],
            &mut outputs,
            ErrorConfig {
                input: config.child_input.clone(),
                temp: config.temp.clone(),
                leaf_out: config.leaf_out.clone(),
                gate: config.gate.clone(),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&config.slot),
            },
            error_child_body,
        )
        .await;
        let failure = failure.expect_err("boundary must fail");
        assert_eq!(failure.note(), "boundary propagates the leaf failure");

        let child = config.slot.take().expect("child scope recorded");
        assert_eq!(
            ctx.state(&child)?,
            ScopeState::Closed,
            "error exit clean closed the child"
        );
        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            1,
            "the child's own temporary drops exactly once"
        );
        // ancestor 保留且可读（纯诊断观察，业务入口已终止）。
        assert_eq!(observe_num(ctx, &root, &config.input)?, 3);
        assert_eq!(observe_owner(ctx, &config.input_id)?, root);
        assert_eq!(
            ctx.termination().unwrap().kind(),
            TerminationKind::BodyError
        );
        assert!(
            ctx.cleanup_failure().is_none(),
            "this failure cleaned successfully"
        );
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input, input_id) = f.root_input(f.tracked(3));
    let broken_pos = f.ref_id();
    let config = D10Config {
        root: root.clone(),
        input,
        input_id: input_id.clone(),
        child_input: f.ref_id(),
        temp: f.ref_id(),
        leaf_out: f.ref_id(),
        broken: broken_pos,
        probe: f.ref_id(),
        gate: Gate::closed(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let gate = config.gate.clone();
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |pending| {
            if pending >= 1 {
                gate.release();
            }
        },
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    assert!(exit.cleanup_events() >= 1);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "child temporary + root input, each exactly once"
    );
    let _ = (root, input_id);
}

struct D10Config {
    root: crate::core::identity::ScopeId,
    input: RefId,
    input_id: crate::core::identity::DataId,
    child_input: RefId,
    temp: RefId,
    leaf_out: RefId,
    broken: RefId,
    probe: RefId,
    gate: Gate,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

#[test]
fn d12_error_with_cleanup_failure_keeps_both_diagnostics() {
    async fn child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ErrorConfig,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(1, &config.drops))?;
        let broken = ctx.register_owned(child, &config.leaf_out, tracked_num(2, &config.drops))?;
        // test-only 故障：销毁 entry，责任记录保留 → guard 清理前提失败。
        ctx.coordinator.destroy_probe(&broken);
        Err(BodyError::new(
            "body fails with a broken cleanup precondition",
        ))
    }
    async fn body(ctx: &mut ExecutionContext, config: D10Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let failure = crate::core::context::observe::invoke_boundary(
            ctx,
            &[ImportSlot::new::<Tracked>(
                &config.input,
                &config.child_input,
            )],
            &[],
            &mut outputs,
            ErrorConfig {
                input: config.child_input.clone(),
                temp: config.temp.clone(),
                leaf_out: config.broken.clone(),
                gate: config.gate.clone(),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&config.slot),
            },
            child_body,
        )
        .await
        .expect_err("boundary must fail");
        let _ = failure;

        // 两类诊断并存且互不覆盖。
        assert_eq!(
            ctx.termination().unwrap().kind(),
            TerminationKind::BodyError
        );
        let cleanup = ctx
            .cleanup_failure()
            .expect("cleanup failure is recorded separately");
        let child = config.slot.take().expect("child scope recorded");
        assert_eq!(cleanup.scope(), &child);
        assert_ne!(
            ctx.state(&child)?,
            ScopeState::Closed,
            "a scope with a failed cleanup is not disguised as closed"
        );
        // 终止后普通业务与提交入口拒绝；受控失败清理仍允许。
        assert!(matches!(
            ctx.register_owned(&root, &config.probe, tracked_num(9, &config.drops)),
            Err(ScopeError::Terminated {
                kind: TerminationKind::BodyError
            })
        ));
        assert!(matches!(
            ctx.resolve::<Tracked>(&root, &config.input),
            Err(ScopeError::Terminated { .. })
        ));
        assert!(matches!(
            ctx.finalize(&root, &[], &mut Vec::new()),
            Err(ScopeError::Terminated { .. })
        ));
        assert!(
            ctx.abort(&child).is_err(),
            "broken cleanup still reports its diagnostic"
        );
        assert_eq!(
            observe_num(ctx, &root, &config.input)?,
            3,
            "ancestor preserved"
        );
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input, input_id) = f.root_input(f.tracked(3));
    let broken_pos = f.ref_id();
    let config = D10Config {
        root: root.clone(),
        input,
        input_id,
        child_input: f.ref_id(),
        temp: f.ref_id(),
        leaf_out: f.ref_id(),
        broken: broken_pos,
        probe: f.ref_id(),
        gate: Gate::closed(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        4,
        "destroyed entry + surviving temporary + root input + the armed probe value"
    );
    let _ = root;
}

#[test]
fn d13_unpolled_and_ready_drops_do_not_recidivate() {
    // (a) 未 poll 就丢弃：body 不启动，不建立 Scope，也不清理。
    let started = Rc::new(Cell::new(false));
    let execution = RootExecution::start();
    {
        let future =
            crate::core::runtime::observe::run_root(execution, Rc::clone(&started), unreached_body);
        drop(future);
    }
    assert!(!started.get(), "an unpolled future never runs its body");

    // (b) 正常 Ready 后丢弃本体：不再重复清理。
    let mut f = Fixture::new();
    let root = f.root();
    let (input, _) = f.root_input(f.tracked(4));
    let config = D13Config {
        root: root.clone(),
        input,
        output: f.ref_id(),
        gate: Gate::closed(),
    };
    let gate = config.gate.clone();
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let (boxed, exit) = poll_to_ready_keeping(
        crate::core::runtime::observe::run_root(execution, config, ok_body),
        |pending| {
            if pending >= 1 {
                gate.release();
            }
        },
    );
    assert!(exit.terminated().is_none());
    assert_eq!(
        exit.cleanup_events(),
        0,
        "successful root closes by finalization"
    );
    drop(boxed);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "input + leaf output drop exactly once at context teardown"
    );

    // (c) 错误 Ready 后丢弃本体：不改写成取消，也不重复清理。
    let mut f = Fixture::new();
    let root = f.root();
    let (input, _) = f.root_input(f.tracked(6));
    let config = D13Config {
        root: root.clone(),
        input,
        output: f.ref_id(),
        gate: Gate::closed(),
    };
    let gate = config.gate.clone();
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let (boxed, exit) = poll_to_ready_keeping(
        crate::core::runtime::observe::run_root(execution, config, fail_body),
        |pending| {
            if pending >= 1 {
                gate.release();
            }
        },
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    let cleanup_events = exit.cleanup_events();
    assert_eq!(
        cleanup_events, 2,
        "leaf guard exit plus the root guard's controlled failure exit"
    );
    drop(boxed);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "only the root input remains; no second cleanup and no double drop"
    );
}

struct D13Config {
    root: crate::core::identity::ScopeId,
    input: RefId,
    output: RefId,
    gate: Gate,
}

async fn unreached_body(
    _ctx: &mut ExecutionContext,
    started: Rc<Cell<bool>>,
) -> Result<(), BodyError> {
    started.set(true);
    Ok(())
}

async fn ok_body(ctx: &mut ExecutionContext, config: D13Config) -> Result<(), BodyError> {
    let root = config.root.clone();
    invoke_leaf(
        ctx,
        &root,
        &config.input,
        &config.output,
        LeafConfig {
            gate: config.gate.clone(),
        },
        leaf_double,
    )
    .await?;
    Ok(())
}

async fn fail_body(ctx: &mut ExecutionContext, config: D13Config) -> Result<(), BodyError> {
    let root = config.root.clone();
    let _ = crate::core::context::observe::invoke_fallible_leaf(
        ctx,
        &root,
        &config.input,
        &config.output,
        config.gate.clone(),
        failing_leaf,
    )
    .await;
    Ok(())
}

struct CancelConfig {
    root: crate::core::identity::ScopeId,
    input: RefId,
    child_input: RefId,
    temp: RefId,
    child_output: RefId,
    root_export: RefId,
    gate: Gate,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

async fn cancel_child_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: ChildCancelConfig,
) -> Result<(), BodyError> {
    config.slot.set(Some(child.clone()));
    ctx.register_owned(child, &config.temp, tracked_num(5, &config.drops))?;
    config.gate.wait().await;
    Ok(())
}

struct ChildCancelConfig {
    temp: RefId,
    gate: Gate,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

struct CounterLeaf {
    runs: Rc<Cell<usize>>,
}

async fn counting_unit_leaf(_input: &Tracked, config: CounterLeaf) {
    config.runs.set(config.runs.get() + 1);
}

struct CounterChild {
    runs: Rc<Cell<usize>>,
}

async fn counting_child(
    _ctx: &mut ExecutionContext,
    _child: &crate::core::identity::ScopeId,
    config: CounterChild,
) -> Result<(), BodyError> {
    config.runs.set(config.runs.get() + 1);
    Ok(())
}

#[test]
fn d14_cancel_after_scope_creation_cleans_child_only() {
    async fn body(ctx: &mut ExecutionContext, config: CancelConfig) -> Result<(), BodyError> {
        let root = config.root.clone();
        let mut outputs = vec![ExportSlot::new::<Tracked>(
            &config.child_output,
            &config.root_export,
        )];
        let declared = vec![config.child_output.clone()];
        {
            let imports = [ImportSlot::new::<Tracked>(
                &config.input,
                &config.child_input,
            )];
            let future = crate::core::context::observe::invoke_boundary(
                ctx,
                &imports,
                &declared,
                &mut outputs,
                ChildCancelConfig {
                    temp: config.temp.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                cancel_child_body,
            );
            // 推到真实 Pending 后丢弃持有本体的 Box。
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }

        let child = config.slot.take().expect("child scope recorded");
        assert_eq!(
            ctx.state(&child)?,
            ScopeState::Closed,
            "cancelled boundary closed its scope"
        );
        assert!(ctx.is_terminated());
        assert_eq!(
            ctx.termination().unwrap().kind(),
            TerminationKind::Cancelled
        );
        // R13：诊断上报只返回副本，终止状态不可被活跃调用清除。
        assert!(ctx.termination_report().is_some());
        assert!(
            ctx.is_terminated(),
            "reporting diagnostics must not clear the termination state"
        );
        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            1,
            "guard cleanup dropped the child's own temporary once"
        );
        assert_eq!(
            observe_num(ctx, &root, &config.input)?,
            3,
            "ancestor survives"
        );
        assert_eq!(ctx.frame_depth(), 1, "child frame removed by the guard");
        assert_eq!(
            ctx.coordinator.refs_len_probe(&root)?,
            1,
            "no output binding was committed"
        );

        // D20：取消后的调用边界。
        assert!(matches!(
            ctx.register_owned(&root, &config.child_output, tracked_num(9, &config.drops)),
            Err(ScopeError::Terminated {
                kind: TerminationKind::Cancelled
            })
        ));
        assert!(matches!(
            ctx.resolve::<Tracked>(&root, &config.input),
            Err(ScopeError::Terminated { .. })
        ));
        assert!(matches!(
            ctx.finalize(&root, &[], &mut Vec::new()),
            Err(ScopeError::Terminated { .. })
        ));
        assert!(
            ctx.abort(&child).is_ok(),
            "controlled failure cleanup is still allowed"
        );
        assert_eq!(ctx.frame_depth(), 1, "no new invocation was created");

        // D20：真正发起叶子／边界调用，执行体计数必须为 0，且不新增 frame／Scope。
        let leaf_runs = Rc::new(Cell::new(0));
        let leaf_result = crate::core::context::observe::invoke_leaf_unit(
            ctx,
            &root,
            &config.input,
            CounterLeaf {
                runs: Rc::clone(&leaf_runs),
            },
            counting_unit_leaf,
        )
        .await;
        assert!(matches!(
            leaf_result.as_ref().unwrap_err().scope_error(),
            Some(ScopeError::Terminated { .. })
        ));
        assert_eq!(
            leaf_runs.get(),
            0,
            "no business body ran after cancellation"
        );

        let child_runs = Rc::new(Cell::new(0));
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let boundary_result = crate::core::context::observe::invoke_boundary(
            ctx,
            &[],
            &[],
            &mut outputs,
            CounterChild {
                runs: Rc::clone(&child_runs),
            },
            counting_child,
        )
        .await;
        assert!(matches!(
            boundary_result.as_ref().unwrap_err().scope_error(),
            Some(ScopeError::Terminated { .. })
        ));
        assert_eq!(
            child_runs.get(),
            0,
            "no boundary body ran after cancellation"
        );
        assert_eq!(ctx.frame_depth(), 1);
        assert_eq!(
            ctx.coordinator.refs_len_probe(&root)?,
            1,
            "no new binding was committed"
        );
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input, _) = f.root_input(f.tracked(3));
    let config = CancelConfig {
        root: root.clone(),
        input,
        child_input: f.ref_id(),
        temp: f.ref_id(),
        child_output: f.ref_id(),
        root_export: f.ref_id(),
        gate: Gate::closed(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
    assert!(exit.cleanup_events() >= 1);
}

#[test]
fn d15_cancel_while_input_borrow_is_pending() {
    struct LeafCfg;
    struct Config {
        root: crate::core::identity::ScopeId,
        input: RefId,
        child_input: RefId,
        temp: RefId,
        leaf_out: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn holding_leaf(_input: &Tracked, gate: Gate) -> Tracked {
        gate.wait().await;
        Tracked {
            num: 0,
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }
    async fn child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: CancellingChild,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(4, &config.drops))?;
        // 输入借用跨真实 Pending：叶子持 &Tracked 等待挂起点。
        let _ = invoke_leaf(
            ctx,
            child,
            &config.child_input,
            &config.leaf_out,
            config.gate.clone(),
            holding_leaf,
        )
        .await;
        Ok(())
    }
    struct CancellingChild {
        child_input: RefId,
        temp: RefId,
        leaf_out: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let mut outputs: Vec<ExportSlot> = Vec::new();
        {
            let imports = [ImportSlot::new::<Tracked>(
                &config.input,
                &config.child_input,
            )];
            let future = crate::core::context::observe::invoke_boundary(
                ctx,
                &imports,
                &[],
                &mut outputs,
                CancellingChild {
                    child_input: config.child_input.clone(),
                    temp: config.temp.clone(),
                    leaf_out: config.leaf_out.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                child_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }

        let child = config.slot.take().expect("child scope recorded");
        // 借用已随内层 Future 析构结束：随后还能可变借用 Context（清理入口）。
        assert!(
            ctx.abort(&child).is_ok(),
            "the input borrow ended before cleanup"
        );
        assert_eq!(ctx.state(&child)?, ScopeState::Closed);
        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            1,
            "the child's own temporary drops exactly once"
        );
        assert_eq!(
            observe_num(ctx, &root, &config.input)?,
            3,
            "ancestor borrow source survives"
        );
        assert_eq!(ctx.frame_depth(), 1);
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input, _) = f.root_input(f.tracked(3));
    let config = Config {
        root: root.clone(),
        input,
        child_input: f.ref_id(),
        temp: f.ref_id(),
        leaf_out: f.ref_id(),
        gate: Gate::closed(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
    let _ = LeafCfg;
}

#[test]
fn d16_multilevel_drop_cleans_every_level() {
    struct InnerCfg {
        temp: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn inner_body(
        ctx: &mut ExecutionContext,
        inner: &crate::core::identity::ScopeId,
        config: InnerCfg,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(inner.clone()));
        ctx.register_owned(inner, &config.temp, tracked_num(1, &config.drops))?;
        config.gate.wait().await;
        Ok(())
    }
    struct OuterCfg {
        inner: InnerCfg,
        outer_temp: RefId,
        outer_slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        drops: Arc<AtomicUsize>,
    }
    async fn outer_body(
        ctx: &mut ExecutionContext,
        outer: &crate::core::identity::ScopeId,
        config: OuterCfg,
    ) -> Result<(), BodyError> {
        config.outer_slot.set(Some(outer.clone()));
        ctx.register_owned(outer, &config.outer_temp, tracked_num(2, &config.drops))?;
        let mut inner_outputs: Vec<ExportSlot> = Vec::new();
        let _ = crate::core::context::observe::invoke_boundary(
            ctx,
            &[],
            &[],
            &mut inner_outputs,
            config.inner,
            inner_body,
        )
        .await?;
        Ok(())
    }
    struct Config {
        inner: InnerCfg,
        outer_temp: RefId,
        outer_slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
        drops: Arc<AtomicUsize>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let inner_slot_recorded = Rc::clone(&config.inner.slot);
        {
            let outer_slot = Rc::clone(&config.outer_slot);
            let drops = Arc::clone(&config.drops);
            let inner_slot = Rc::clone(&config.inner.slot);
            let inner = config.inner;
            let outer_temp = config.outer_temp;
            let future = crate::core::context::observe::invoke_boundary(
                ctx,
                &[],
                &[],
                &mut outputs,
                OuterCfg {
                    inner,
                    outer_temp,
                    outer_slot,
                    drops,
                },
                outer_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
            let _ = inner_slot;
        }

        assert_eq!(ctx.frame_depth(), 1, "all nested frames removed");
        let outer = config.outer_slot.take().expect("outer recorded");
        let inner = inner_slot_recorded.take().expect("inner recorded");
        assert_eq!(ctx.state(&outer)?, ScopeState::Closed);
        assert_eq!(ctx.state(&inner)?, ScopeState::Closed, "no detached child");
        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            2,
            "each level's own temporary dropped exactly once"
        );
        assert_eq!(
            ctx.termination().unwrap().kind(),
            TerminationKind::Cancelled
        );
        Ok(())
    }

    let f = Fixture::new();
    let root = f.root();
    let _ = root;
    let config = Config {
        inner: InnerCfg {
            temp: f.ref_id(),
            gate: Gate::closed(),
            drops: Arc::clone(&f.drops),
            slot: Rc::new(Cell::new(None)),
        },
        outer_temp: f.ref_id(),
        outer_slot: Rc::new(Cell::new(None)),
        drops: Arc::clone(&f.drops),
    };
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
}

#[test]
fn d17_cancel_with_partial_results_and_promoted_state() {
    struct ChildCfg {
        rules: RefId,
        item_out: RefId,
        round_out: RefId,
        round_out_again: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ChildCfg,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        let collector = ctx.begin_collector::<Tracked>(child)?;
        let item = ctx.create_child(child)?;
        ctx.register_owned(&item, &config.item_out, tracked_num(7, &config.drops))?;
        ctx.consume_item(&item, &config.item_out, &collector)?;
        let state = ctx.register_state::<Tracked>(child, &config.rules)?;
        let round = ctx.create_child(child)?;
        ctx.register_owned(&round, &config.round_out, tracked_num(8, &config.drops))?;
        ctx.promote(&round, &config.round_out, &state)?;
        // 第二次替换：上一轮 controller-owned 状态成为 pending 回收责任。
        let round2 = ctx.create_child(child)?;
        ctx.register_owned(
            &round2,
            &config.round_out_again,
            tracked_num(10, &config.drops),
        )?;
        ctx.promote(&round2, &config.round_out_again, &state)?;
        config.gate.wait().await;
        Ok(())
    }
    struct Config {
        root: crate::core::identity::ScopeId,
        rules: RefId,
        child_rules: RefId,
        rules_id: crate::core::identity::DataId,
        item_out: RefId,
        round_out: RefId,
        round_out_again: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let mut outputs: Vec<ExportSlot> = Vec::new();
        {
            let imports = [ImportSlot::new::<Tracked>(
                &config.rules,
                &config.child_rules,
            )];
            let future = crate::core::context::observe::invoke_boundary(
                ctx,
                &imports,
                &[],
                &mut outputs,
                ChildCfg {
                    rules: config.child_rules.clone(),
                    item_out: config.item_out.clone(),
                    round_out: config.round_out.clone(),
                    round_out_again: config.round_out_again.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                child_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }
        let child = config.slot.take().expect("child recorded");
        assert_eq!(ctx.state(&child)?, ScopeState::Closed);
        assert_eq!(
            config.drops.load(Ordering::SeqCst),
            3,
            "collector partial result, pending state and current state each drop once"
        );
        // imported 初值仍由 root 负责，且没有正常集合输出。
        assert_eq!(observe_num(ctx, &root, &config.rules)?, 11);
        assert_eq!(observe_owner(ctx, &config.rules_id)?, root);
        assert_eq!(
            ctx.coordinator.refs_len_probe(&root)?,
            1,
            "no Vec output was bound"
        );
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (rules, rules_id) = f.root_input(f.tracked(11));
    let config = Config {
        root: root.clone(),
        rules: rules.clone(),
        child_rules: f.ref_id(),
        rules_id: rules_id.clone(),
        item_out: f.ref_id(),
        round_out: f.ref_id(),
        round_out_again: f.ref_id(),
        gate: Gate::closed(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        4,
        "collector partial + pending state + current state + root input, each once"
    );
    let _ = rules;
}

#[test]
fn d18_root_future_owner_drop_is_a_controlled_exit() {
    struct Config {
        temp: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ChildCancelConfig,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        ctx.register_owned(child, &config.temp, tracked_num(1, &config.drops))?;
        config.gate.wait().await;
        Ok(())
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = ctx.root_scope();
        let mut outputs: Vec<ExportSlot> = Vec::new();
        let _ = crate::core::context::observe::invoke_boundary(
            ctx,
            &[],
            &[],
            &mut outputs,
            ChildCancelConfig {
                temp: config.temp.clone(),
                gate: config.gate.clone(),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&config.slot),
            },
            child_body,
        )
        .await;
        let _ = root;
        Ok(())
    }

    let before_cleanups = creation_counts::guard_cleanups();
    let mut f = Fixture::new();
    let (input, input_id) = f.root_input(f.tracked(42));
    let config = Config {
        temp: f.ref_id(),
        gate: Gate::closed(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let other = Fixture::new();
    let execution = f.take_execution();
    let boxed = advance_to_pending(
        crate::core::runtime::observe::run_root(execution, config, body),
        1,
    );
    // 丢弃拥有 Context 的 Root Future 本体。
    drop(boxed);

    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "child temporary (guard cleanup) + root input (context teardown), each once"
    );
    assert!(
        creation_counts::guard_cleanups() > before_cleanups,
        "the cancel path ran through guard cleanup, not only natural container drop"
    );
    assert_eq!(other.drops(), 0, "another execution is unaffected");
    let _ = (input, input_id);
}

#[test]
fn d19_drop_cleanup_failure_stays_observable() {
    struct Config {
        root: crate::core::identity::ScopeId,
        input: RefId,
        child_input: RefId,
        temp: RefId,
        other_collector: RefId,
        other_value: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn child_body(
        ctx: &mut ExecutionContext,
        child: &crate::core::identity::ScopeId,
        config: ChildCancelConfig2,
    ) -> Result<(), BodyError> {
        config.slot.set(Some(child.clone()));
        let broken = ctx.register_owned(child, &config.broken, tracked_num(1, &config.drops))?;
        ctx.register_owned(child, &config.temp, tracked_num(2, &config.drops))?;
        ctx.coordinator.destroy_probe(&broken);
        config.gate.wait().await;
        Ok(())
    }
    struct ChildCancelConfig2 {
        broken: RefId,
        temp: RefId,
        gate: Gate,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let mut outputs: Vec<ExportSlot> = Vec::new();
        {
            let imports = [ImportSlot::new::<Tracked>(
                &config.input,
                &config.child_input,
            )];
            let future = crate::core::context::observe::invoke_boundary(
                ctx,
                &imports,
                &[],
                &mut outputs,
                ChildCancelConfig2 {
                    broken: config.other_value.clone(),
                    temp: config.temp.clone(),
                    gate: config.gate.clone(),
                    drops: Arc::clone(&config.drops),
                    slot: Rc::clone(&config.slot),
                },
                child_body,
            );
            let boxed = advance_to_pending(future, 1);
            drop(boxed);
        }

        let child = config.slot.take().expect("child recorded");
        assert_eq!(
            ctx.termination().unwrap().kind(),
            TerminationKind::Cancelled
        );
        let cleanup = ctx
            .cleanup_failure()
            .expect("cancellation cleanup failure recorded");
        assert_eq!(cleanup.scope(), &child);
        assert_ne!(
            ctx.state(&child)?,
            ScopeState::Closed,
            "broken scope is not disguised"
        );
        // ancestor 与另一份 collector 内容不受影响。
        assert_eq!(observe_num(ctx, &root, &config.input)?, 3);
        assert_eq!(
            ctx.coordinator
                .resolve::<Tracked>(&root, &config.other_collector)?
                .num,
            5
        );
        assert!(matches!(
            ctx.register_owned(
                &root,
                &config.other_collector,
                tracked_num(9, &config.drops)
            ),
            Err(ScopeError::Terminated { .. })
        ));
        let _ = config.other_value;
        Ok(())
    }

    let mut f = Fixture::new();
    let root = f.root();
    let (input, _) = f.root_input(f.tracked(3));
    let other_pos = f.ref_id();
    let other_value = f.spare_value(5);
    f.context_mut()
        .register_owned(&root, &other_pos, other_value)
        .unwrap();
    let config = Config {
        root: root.clone(),
        input,
        child_input: f.ref_id(),
        temp: f.ref_id(),
        other_collector: other_pos.clone(),
        other_value: f.ref_id(),
        gate: Gate::closed(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
    assert!(
        drops.load(Ordering::SeqCst) >= 4,
        "destroyed entry, surviving temporary, root input and sibling value all accounted"
    );
    let _ = root;
}

// ---- R7～R11：复审反例的永久回归 ----

struct R7Config {
    root: crate::core::identity::ScopeId,
    root_input: RefId,
    unbound_source: RefId,
    child_input: RefId,
    runs: Rc<Cell<usize>>,
    drops: Arc<AtomicUsize>,
}

struct R7ChildConfig {
    runs: Rc<Cell<usize>>,
}

async fn r7_child_body(
    _ctx: &mut ExecutionContext,
    _child: &crate::core::identity::ScopeId,
    config: R7ChildConfig,
) -> Result<(), BodyError> {
    config.runs.set(config.runs.get() + 1);
    Ok(())
}

async fn r7_body(ctx: &mut ExecutionContext, config: R7Config) -> Result<(), BodyError> {
    let root = config.root.clone();
    let refs_before = ctx.coordinator.refs_len_probe(&root)?;
    let imports = [ImportSlot::new::<Tracked>(
        &config.unbound_source,
        &config.child_input,
    )];
    let mut outputs: Vec<ExportSlot> = Vec::new();
    let error = crate::core::context::observe::invoke_boundary(
        ctx,
        &imports,
        &[],
        &mut outputs,
        R7ChildConfig {
            runs: Rc::clone(&config.runs),
        },
        r7_child_body,
    )
    .await
    .expect_err("caller position is not bound");
    assert!(
        matches!(error.scope_error(), Some(ScopeError::RefNotBound { .. })),
        "the original import diagnostic is returned: {error:?}"
    );

    // 首次执行失败记录：定位与原始诊断都保存，不被清理结果覆盖。
    assert!(
        ctx.is_terminated(),
        "establishment failure terminates the execution"
    );
    let termination = ctx.termination().expect("termination recorded");
    assert_eq!(termination.kind(), TerminationKind::BodyError);
    assert_eq!(termination.note(), "boundary input assembly failed");
    assert!(
        matches!(
            termination.scope_error(),
            Some(ScopeError::RefNotBound { .. })
        ),
        "the first failure keeps the original Scope error"
    );
    assert!(
        ctx.cleanup_failure().is_none(),
        "this path has no ownable child content, so cleanup succeeds"
    );

    // 无部分 Import；child body 未运行；随后新业务与正常 commit 全部被拒绝。
    assert_eq!(ctx.coordinator.refs_len_probe(&root)?, refs_before);
    assert_eq!(config.runs.get(), 0, "the child body never ran");
    assert!(matches!(
        ctx.register_owned(&root, &config.child_input, tracked_num(1, &config.drops)),
        Err(ScopeError::Terminated { .. })
    ));
    assert!(matches!(
        ctx.resolve::<Tracked>(&root, &config.root_input),
        Err(ScopeError::Terminated { .. })
    ));
    assert!(matches!(
        ctx.finalize(&root, &[], &mut Vec::new()),
        Err(ScopeError::Terminated { .. })
    ));
    assert_eq!(
        ctx.frame_depth(),
        1,
        "no boundary frame survived the failure"
    );
    Ok(())
}

#[test]
fn r7_import_failure_records_first_failure_and_blocks_new_work() {
    let mut f = Fixture::new();
    let root = f.root();
    let (input, _) = f.root_input(f.tracked(1));
    let config = R7Config {
        root: root.clone(),
        root_input: input.clone(),
        unbound_source: f.ref_id(),
        child_input: f.ref_id(),
        runs: Rc::new(Cell::new(0)),
        drops: Arc::clone(&f.drops),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, r7_body),
        |_| {},
    );
    assert_eq!(
        exit.terminated(),
        Some(TerminationKind::BodyError),
        "the root goes through the controlled failure exit"
    );
    assert!(
        exit.close_error().is_none(),
        "no normal close was attempted"
    );
    assert_eq!(
        exit.termination_note(),
        Some("boundary input assembly failed"),
        "the first failure note survives the root close"
    );
    assert!(
        exit.cleanup_events() >= 1,
        "the root guard cleaned its subtree"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "root input plus the value handed to the rejected registration, each exactly once"
    );
    let _ = (input, root);
}

struct R7TypeConfig {
    root: crate::core::identity::ScopeId,
    source: RefId,
    first_target: RefId,
    second_target: RefId,
    runs: Rc<Cell<usize>>,
    drops: Arc<AtomicUsize>,
}

async fn r7_type_body(ctx: &mut ExecutionContext, config: R7TypeConfig) -> Result<(), BodyError> {
    let root = config.root.clone();
    let imports = [
        ImportSlot::new::<Tracked>(&config.source, &config.first_target),
        // 第二项声明 `Other`，实际是 `Tracked`：整组必须一起失败。
        ImportSlot::new::<Other>(&config.source, &config.second_target),
    ];
    let mut outputs: Vec<ExportSlot> = Vec::new();
    let error = crate::core::context::observe::invoke_boundary(
        ctx,
        &imports,
        &[],
        &mut outputs,
        R7ChildConfig {
            runs: Rc::clone(&config.runs),
        },
        r7_child_body,
    )
    .await
    .expect_err("the second slot declares the wrong type");
    assert!(matches!(
        error.scope_error(),
        Some(ScopeError::TypeMismatch { .. })
    ));
    assert_eq!(
        ctx.coordinator.refs_len_probe(&root)?,
        1,
        "no partial import binding is committed (group is all-or-nothing)"
    );
    assert_eq!(config.runs.get(), 0);
    assert_eq!(ctx.frame_depth(), 1);
    assert!(ctx.is_terminated());
    assert!(matches!(
        ctx.register_owned(&root, &config.second_target, tracked_num(1, &config.drops)),
        Err(ScopeError::Terminated { .. })
    ));
    Ok(())
}

#[test]
fn r7_second_slot_type_failure_leaves_no_partial_bindings() {
    let mut f = Fixture::new();
    let root = f.root();
    let (source, _) = f.root_input(f.tracked(1));
    let config = R7TypeConfig {
        root: root.clone(),
        source,
        first_target: f.ref_id(),
        second_target: f.ref_id(),
        runs: Rc::new(Cell::new(0)),
        drops: Arc::clone(&f.drops),
    };
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, r7_type_body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    assert!(exit.cleanup_events() >= 1);
    let _ = root;
}

struct R8Config {
    root: crate::core::identity::ScopeId,
    root_pos: RefId,
    root_id: crate::core::identity::DataId,
    child_pos: RefId,
    forbidden_write: RefId,
}

async fn r8_body(ctx: &mut ExecutionContext, config: R8Config) -> Result<(), BodyError> {
    let root = config.root.clone();
    // caller（Root）的位置属于当前调用自己，可读。
    assert_eq!(
        *ctx.coordinator.resolve::<u32>(&root, &config.root_pos)?,
        41u32
    );
    let mut outputs: Vec<ExportSlot> = Vec::new();
    let imports = [ImportSlot::new::<u32>(&config.root_pos, &config.child_pos)];
    crate::core::context::observe::invoke_boundary(
        ctx,
        &imports,
        &[],
        &mut outputs,
        R8ChildConfig {
            root: root.clone(),
            root_pos: config.root_pos.clone(),
            child_pos: config.child_pos.clone(),
            forbidden_write: config.forbidden_write.clone(),
        },
        r8_child_body,
    )
    .await
    .expect("the boundary itself succeeds; the child's attempts are what must fail");
    // 越权尝试没有留下任何绑定：Root 只保留最初的输入位置。
    assert_eq!(ctx.coordinator.refs_len_probe(&root)?, 1);
    let _ = &config.root_id;
    Ok(())
}

struct R8ChildConfig {
    root: crate::core::identity::ScopeId,
    root_pos: RefId,
    child_pos: RefId,
    forbidden_write: RefId,
}

async fn r8_child_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: R8ChildConfig,
) -> Result<(), BodyError> {
    assert_eq!(ctx.current_scope().as_ref(), Some(child));
    // 读 caller／ancestor 的本地位置被拒绝。
    assert!(matches!(
        ctx.resolve::<u32>(&config.root, &config.root_pos),
        Err(ScopeError::OutsideInvocation { .. })
    ));
    // 向 caller 直接登记输出同样被拒绝。
    assert!(matches!(
        ctx.register_owned(&config.root, &config.forbidden_write, 42u32),
        Err(ScopeError::OutsideInvocation { .. })
    ));
    // child 自己的（已导入）位置仍然可用。
    assert_eq!(*ctx.resolve::<u32>(child, &config.child_pos)?, 41u32);
    Ok(())
}

#[test]
fn r8_child_cannot_touch_caller_scope_through_context() {
    let mut f = Fixture::new();
    let root = f.root();
    let root_pos = f.ref_id();
    let root_id = f
        .context_mut()
        .register_owned(&root, &root_pos, 41u32)
        .unwrap();
    // child 自己的输入位置（边界导入）
    let child_pos = f.ref_id();
    let forbidden_write = f.ref_id();
    let execution = f.take_execution();
    let config = R8Config {
        root: root.clone(),
        root_pos: root_pos.clone(),
        root_id: root_id.clone(),
        child_pos,
        forbidden_write,
    };
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, r8_body),
        |_| {},
    );
    assert!(
        exit.terminated().is_none(),
        "terminated={:?} body={:?} scope={:?}",
        exit.terminated(),
        exit.body_error().map(|error| error.note()),
        exit.body_error()
            .and_then(|error| error.scope_error().map(|scope| scope.to_string()))
    );
    assert!(exit.close_error().is_none(), "{:?}", exit.close_error());
}

struct R9Config {
    root: crate::core::identity::ScopeId,
    live_child: RefId,
    temp: RefId,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

async fn r9_body(ctx: &mut ExecutionContext, config: R9Config) -> Result<(), BodyError> {
    let root = config.root.clone();
    // body 建立一个直接 child 与自有临时值后直接返回 Ok，未退出 child。
    let child = ctx.create_child(&root)?;
    config.slot.set(Some(child.clone()));
    ctx.register_owned(&child, &config.temp, tracked_num(1, &config.drops))?;
    let _ = config.live_child;
    Ok(())
}

#[test]
fn r9_root_close_failure_enters_controlled_exit() {
    let mut f = Fixture::new();
    let root = f.root();
    let (_, _) = f.root_input(f.tracked(1));
    let config = R9Config {
        root: root.clone(),
        live_child: f.ref_id(),
        temp: f.ref_id(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, r9_body),
        |_| {},
    );

    assert_eq!(
        exit.terminated(),
        Some(TerminationKind::BodyError),
        "a failed root close is a controlled failure exit, not a quiet close_error"
    );
    assert!(
        matches!(
            exit.close_error(),
            Some(ScopeError::ActiveDescendants { .. })
        ),
        "the close diagnostic is preserved: {:?}",
        exit.close_error()
    );
    assert_eq!(exit.cleanup_scope(), None, "cleanup itself succeeded");
    assert!(
        exit.cleanup_events() >= 1,
        "the guard cleaned the root subtree"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "the live child's temporary and the root input each drop exactly once"
    );
}

struct SwallowConfig {
    temp: RefId,
    drops: Arc<AtomicUsize>,
    gate: Gate,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

async fn swallow_body(ctx: &mut ExecutionContext, config: SwallowConfig) -> Result<(), BodyError> {
    let mut outputs: Vec<ExportSlot> = Vec::new();
    {
        let future = crate::core::context::observe::invoke_boundary(
            ctx,
            &[],
            &[],
            &mut outputs,
            SwallowChild {
                temp: config.temp.clone(),
                drops: Arc::clone(&config.drops),
                gate: config.gate.clone(),
                slot: Rc::clone(&config.slot),
            },
            swallow_child_body,
        );
        let boxed = advance_to_pending(future, 1);
        drop(boxed);
    }
    // 外层接住取消并返回 Ok：Root 收口仍必须走受控失败退出。
    Ok(())
}

struct SwallowChild {
    temp: RefId,
    drops: Arc<AtomicUsize>,
    gate: Gate,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

async fn swallow_child_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: SwallowChild,
) -> Result<(), BodyError> {
    config.slot.set(Some(child.clone()));
    ctx.register_owned(child, &config.temp, tracked_num(1, &config.drops))?;
    config.gate.wait().await;
    Ok(())
}

#[test]
fn r9_swallowed_cancel_still_enters_controlled_root_exit() {
    let mut f = Fixture::new();
    let (_, _) = f.root_input(f.tracked(1));
    let config = SwallowConfig {
        temp: f.ref_id(),
        drops: Arc::clone(&f.drops),
        gate: Gate::closed(),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, swallow_body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
    assert!(
        exit.close_error().is_none(),
        "no normal close was attempted after cancellation"
    );
    assert!(
        exit.cleanup_events() >= 2,
        "child guard and root guard both cleaned"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "child temporary and root input each drop exactly once"
    );
}

struct R10Config {
    temp: RefId,
    drops: Arc<AtomicUsize>,
}

async fn r10_body(ctx: &mut ExecutionContext, config: R10Config) -> Result<(), BodyError> {
    let child = ctx.create_child(&ctx.root_scope())?;
    ctx.register_owned(&child, &config.temp, tracked_num(1, &config.drops))?;
    Err(BodyError::new("original detailed execution failure"))
}

#[test]
fn r10_root_exit_keeps_original_diagnostic() {
    let f = Fixture::new();
    let config = R10Config {
        temp: f.ref_id(),
        drops: Arc::clone(&f.drops),
    };
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, r10_body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    assert_eq!(
        exit.termination_note(),
        Some("original detailed execution failure"),
        "the original failure note is preserved"
    );
    assert_eq!(
        exit.body_error().map(|error| error.note()),
        Some("original detailed execution failure"),
        "the body error object is returned, not dropped"
    );
    assert!(exit.cleanup_error().is_none());
}

#[test]
fn r11_closed_or_finalizing_scope_cannot_be_entered() {
    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let context = execution.context_mut();
    context.abort(&root).unwrap();
    assert!(matches!(
        context.enter(InvocationKind::Root, &root, true),
        Err(ScopeError::ScopeClosed { .. }) | Err(ScopeError::ScopeNotActive { .. })
    ));

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    let context = execution.context_mut();
    let guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let mut guard = guard;
    let child = guard.create_child(&root).unwrap();
    drop(guard);
    // child 仍存活，但 Root 已因 guard 取消而进入清理/关闭路径。
    let state = context.state(&root).unwrap();
    assert_eq!(
        state,
        ScopeState::Closed,
        "the cancelled root guard cleaned the subtree"
    );
    assert_eq!(
        context.state(&child).unwrap(),
        ScopeState::Closed,
        "the cancelled guard cleaned the whole subtree"
    );
    // 已关闭的 Scope 与已终止的 Execution 都不能再进入调用。
    assert!(matches!(
        context.enter(InvocationKind::Boundary, &child, true),
        Err(ScopeError::ScopeClosed { .. })
            | Err(ScopeError::ScopeNotActive { .. })
            | Err(ScopeError::OutsideInvocation { .. })
            | Err(ScopeError::Terminated { .. })
    ));
}

// ---- R12：补齐的验收证据 ----

// ---- R12：更多补齐证据（D08／D09、D15～D18、D19、D20） ----

// ---- D15／D16／D18：析构先后关系的事件证据 ----

struct OrderingConfig {
    temp: RefId,
    leaf_out: RefId,
    input: RefId,
    root_input: RefId,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

struct OrderingChild {
    temp: RefId,
    leaf_out: RefId,
    input: RefId,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

/// D07：caller 输出位置冲突，且失败时 owner 没有部分转移。
struct D07ConflictChild {
    declared: RefId,
    id_slot: Rc<Cell<Option<crate::core::identity::DataId>>>,
    drops: Arc<AtomicUsize>,
    slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

async fn conflict_child_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: D07ConflictChild,
) -> Result<(), BodyError> {
    config.slot.set(Some(child.clone()));
    let id = ctx.register_owned(child, &config.declared, tracked_num(3, &config.drops))?;
    // 出口尚未提交：本值仍由 child 唯一负责（没有部分转移）。
    assert_eq!(observe_owner(ctx, &id)?, child.clone());
    assert!(ctx.coordinator.alive_probe(&id));
    config.id_slot.set(Some(id));
    Ok(())
}

#[test]
fn d07_caller_position_conflict_leaves_no_partial_transfer() {
    struct Config {
        root: crate::core::identity::ScopeId,
        caller_taken: RefId,
        child_declared: RefId,
        drops: Arc<AtomicUsize>,
        id_slot: Rc<Cell<Option<crate::core::identity::DataId>>>,
        scope_slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        // 预先占用 caller 输出位置，制造真实绑定冲突。
        ctx.register_owned(&root, &config.caller_taken, tracked_num(1, &config.drops))?;
        let refs_before = ctx.coordinator.refs_len_probe(&root)?;

        let mut outputs = vec![ExportSlot::new::<Tracked>(
            &config.child_declared,
            &config.caller_taken,
        )];
        let declared = vec![config.child_declared.clone()];
        let failure = crate::core::context::observe::invoke_boundary(
            ctx,
            &[],
            &declared,
            &mut outputs,
            D07ConflictChild {
                declared: config.child_declared.clone(),
                id_slot: Rc::clone(&config.id_slot),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&config.scope_slot),
            },
            conflict_child_body,
        )
        .await
        .expect_err("caller output position is already bound");
        assert!(
            matches!(
                failure.scope_error(),
                Some(ScopeError::RefAlreadyBound { .. })
            ),
            "got {failure:?}"
        );
        assert_eq!(
            ctx.coordinator.refs_len_probe(&root)?,
            refs_before,
            "no caller binding was added and none was removed"
        );
        let child = config.scope_slot.take().expect("child recorded");
        assert_eq!(ctx.state(&child)?, ScopeState::Closed);
        let value = config.id_slot.take().expect("child value recorded");
        assert!(
            !ctx.coordinator.alive_probe(&value),
            "the rejected child value was cleaned with its scope"
        );
        Ok(())
    }

    let f = Fixture::new();
    let root = f.root();
    let config = Config {
        root: root.clone(),
        caller_taken: f.ref_id(),
        child_declared: f.ref_id(),
        drops: Arc::clone(&f.drops),
        id_slot: Rc::new(Cell::new(None)),
        scope_slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "the rejected child value and the pre-bound caller value drop once each"
    );
}

/// D15／D16／D18：借用见证 + 清理／frame／存储析构的准确事件序列。
struct BorrowWitness<'a>(&'a Tracked);

impl Drop for BorrowWitness<'_> {
    fn drop(&mut self) {
        // 在析构见证里读取输入：证明借用结束时数据仍存活、清理尚未销毁它。
        let observed = self.0.num;
        creation_counts::record_event(&format!("borrow-witness-drop:{observed}"));
    }
}

async fn witness_leaf(input: &Tracked, _config: LeafConfig) -> Tracked {
    let witness = BorrowWitness(input);
    let probe = Probe("inner-future-drop");
    std::future::poll_fn(|_| Poll::<()>::Pending).await;
    let _ = (&witness, &probe);
    unreachable!()
}

async fn witness_child_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: OrderingChild,
) -> Result<(), BodyError> {
    config.slot.set(Some(child.clone()));
    ctx.register_owned(child, &config.temp, tracked_num(7, &config.drops))?;
    let _ = invoke_leaf(
        ctx,
        child,
        &config.input,
        &config.leaf_out,
        LeafConfig {
            gate: Gate::closed(),
        },
        witness_leaf,
    )
    .await;
    Ok(())
}

async fn witness_outer(
    ctx: &mut ExecutionContext,
    config: OrderingConfig,
) -> Result<(), BodyError> {
    let mut outputs: Vec<ExportSlot> = Vec::new();
    let imports = [ImportSlot::new::<Tracked>(
        &config.input,
        &config.root_input,
    )];
    {
        let future = crate::core::context::observe::invoke_boundary(
            ctx,
            &imports,
            &[],
            &mut outputs,
            OrderingChild {
                temp: config.temp.clone(),
                leaf_out: config.leaf_out.clone(),
                input: config.root_input.clone(),
                drops: Arc::clone(&config.drops),
                slot: Rc::clone(&config.slot),
            },
            witness_child_body,
        );
        let boxed = advance_to_pending(future, 1);
        drop(boxed);
    }
    Ok(())
}

#[test]
fn d15_d16_d18_full_destruction_sequence_is_observed() {
    let _ = creation_counts::take_events();
    let mut f = Fixture::new();
    let (root_input, _) = f.root_input(f.tracked(9));
    let config = OrderingConfig {
        temp: f.ref_id(),
        leaf_out: f.ref_id(),
        input: root_input.clone(),
        root_input: f.ref_id(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, witness_outer),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));

    let events = creation_counts::take_events();
    let pos = |needle: &str| {
        events
            .iter()
            .position(|event| event == needle)
            .unwrap_or_else(|| panic!("event {needle} missing in {events:?}"))
    };
    let inner = pos("inner-future-drop");
    let witness = pos("borrow-witness-drop:9");
    let leaf_start = pos("cleanup-start:none");
    let leaf_end = pos("cleanup-end:none");
    let leaf_frame = pos("frame-exit:leaf:1");
    let child_start = pos("cleanup-start:1");
    let child_end = pos("cleanup-end:1");
    let child_frame = pos("frame-exit:boundary:1");
    let root_start = pos("cleanup-start:0");
    let root_end = pos("cleanup-end:0");
    let root_frame = pos("frame-exit:root:0");
    let child_value = pos("value:7");
    let root_value = pos("value:9");
    let context_drop = pos("context-drop");
    let container_drop = pos("container-drop");

    assert!(
        inner < witness,
        "inner future locals drop before the borrow witness: {events:?}"
    );
    assert!(
        witness < leaf_start,
        "the input borrow ends before any cleanup begins: {events:?}"
    );
    assert!(
        leaf_end < child_start,
        "the leaf frame finishes before the boundary guard cleans: {events:?}"
    );
    assert!(
        leaf_frame < child_start,
        "the leaf frame exits before the boundary cleanup starts: {events:?}"
    );
    assert!(
        child_value > child_start && child_value < child_end,
        "the child temporary is destroyed inside the boundary guard cleanup: {events:?}"
    );
    assert!(
        child_end < child_frame,
        "the boundary frame exits only after its cleanup finished: {events:?}"
    );
    assert!(
        root_value > root_start && root_value < root_end,
        "the root input is destroyed inside the root guard cleanup: {events:?}"
    );
    assert!(
        root_end < root_frame,
        "the root frame exits after its cleanup"
    );
    assert!(
        child_frame < root_frame,
        "the inner frame exits before the root frame"
    );
    assert!(
        root_frame < context_drop,
        "the root frame exits before the context drop"
    );
    assert!(
        context_drop < container_drop,
        "storage drops with the context fields"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "child temporary and root input"
    );
}

// ---- 第三次复审：R10／R11 与 R12 剩余证据 ----

struct R10CancelConfig {
    input: RefId,
    child_input: RefId,
    leaf_out: RefId,
    drops: Arc<AtomicUsize>,
    scope_seen: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

struct R10Child {
    input: RefId,
    leaf_out: RefId,
    drops: Arc<AtomicUsize>,
    scope_seen: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
}

async fn r10_holding_leaf(_input: &Tracked, _config: LeafConfig) -> Tracked {
    let _probe = Probe("r10-inner-future-drop");
    std::future::poll_fn(|_| Poll::<()>::Pending).await;
    unreachable!()
}

async fn r10_child_body(
    ctx: &mut ExecutionContext,
    child: &crate::core::identity::ScopeId,
    config: R10Child,
) -> Result<(), BodyError> {
    config.scope_seen.set(Some(child.clone()));
    // leaf 沿用 child 的 Scope 且不承担其销毁责任：取消定位必须来自 frame。
    let _ = invoke_leaf(
        ctx,
        child,
        &config.input,
        &config.leaf_out,
        LeafConfig {
            gate: Gate::closed(),
        },
        r10_holding_leaf,
    )
    .await;
    let _ = &config.drops;
    Ok(())
}

async fn r10_outer(ctx: &mut ExecutionContext, config: R10CancelConfig) -> Result<(), BodyError> {
    let mut outputs: Vec<ExportSlot> = Vec::new();
    let imports = [ImportSlot::new::<Tracked>(
        &config.input,
        &config.child_input,
    )];
    {
        let future = crate::core::context::observe::invoke_boundary(
            ctx,
            &imports,
            &[],
            &mut outputs,
            R10Child {
                input: config.child_input.clone(),
                leaf_out: config.leaf_out.clone(),
                drops: Arc::clone(&config.drops),
                scope_seen: Rc::clone(&config.scope_seen),
            },
            r10_child_body,
        );
        let boxed = advance_to_pending(future, 1);
        drop(boxed);
    }
    Ok(())
}

#[test]
fn r10_leaf_cancellation_keeps_the_frame_scope_location() {
    let mut f = Fixture::new();
    let (input, _) = f.root_input(f.tracked(1));
    let config = R10CancelConfig {
        input: input.clone(),
        child_input: f.ref_id(),
        leaf_out: f.ref_id(),
        drops: Arc::clone(&f.drops),
        scope_seen: Rc::new(Cell::new(None)),
    };
    let scope_seen = Rc::clone(&config.scope_seen);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, r10_outer),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::Cancelled));
    assert_eq!(exit.termination_note(), Some("pending future dropped"));
    assert_eq!(
        exit.termination_scope().cloned(),
        scope_seen.take(),
        "the cancellation location is the frame scope actually used, not the (empty) responsibility set"
    );
    assert!(
        exit.termination_scope_error().is_none(),
        "cancellation carries no scope error of its own"
    );
}

#[test]
fn r11_broken_frame_relation_is_recorded_not_silently_truncated() {
    struct Config {
        temp: RefId,
        drops: Arc<AtomicUsize>,
        slot: Rc<Cell<Option<crate::core::identity::ScopeId>>>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let child = ctx.create_child(&ctx.root_scope())?;
        config.slot.set(Some(child.clone()));
        ctx.register_owned(&child, &config.temp, tracked_num(1, &config.drops))?;
        // test-only 元数据故障：把当前 Boundary frame 的 parent 破坏成 None。
        let guard = ctx.enter(InvocationKind::Boundary, &child, true).unwrap();
        guard.context.frames[1].parent = None;
        drop(guard);

        // 异常关系：记录 Invariant 并保留现场（不清理、不 truncate）。
        assert_eq!(
            ctx.frame_depth(),
            2,
            "the broken frame is preserved for diagnosis"
        );
        assert_eq!(
            ctx.termination().map(|termination| termination.kind()),
            Some(TerminationKind::BodyError)
        );
        assert_eq!(
            ctx.termination().map(|termination| termination.note()),
            Some("a nested invocation frame must point at its immediate caller frame")
        );
        assert_ne!(
            ctx.state(&child)?,
            ScopeState::Closed,
            "an abnormal frame relation does not clean its scope as if it were a normal cancel"
        );
        let _ = ctx.abort(&child);
        Ok(())
    }

    let f = Fixture::new();
    let config = Config {
        temp: f.ref_id(),
        drops: Arc::clone(&f.drops),
        slot: Rc::new(Cell::new(None)),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let _exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the child temporary drops exactly once"
    );
}

/// D07：使用真实 finalize 入口验证后项失败时首项没有部分转移。
#[test]
fn d07_finalize_failure_leaves_no_partial_owner_transfer() {
    struct Config {
        root: crate::core::identity::ScopeId,
        child_first: RefId,
        child_second: RefId,
        caller_out_a: RefId,
        caller_out_b: RefId,
        drops: Arc<AtomicUsize>,
    }
    async fn body(ctx: &mut ExecutionContext, config: Config) -> Result<(), BodyError> {
        let root = config.root.clone();
        let child = ctx.create_child(&root)?;
        let first =
            ctx.register_owned(&child, &config.child_first, tracked_num(1, &config.drops))?;
        let second = ctx.register_owned(&child, &config.child_second, Other(2))?;
        // 提交前：两项都由 child 唯一负责，caller 未拥有它们。
        assert_eq!(observe_owner(ctx, &first)?, child.clone());
        assert_eq!(observe_owner(ctx, &second)?, child.clone());
        let caller_refs = ctx.coordinator.refs_len_probe(&root)?;
        let caller_owned = ctx.coordinator.owned_len_probe(&root)?;

        // 后项声明类型不符：整组拒绝。
        let declared = vec![config.child_first.clone(), config.child_second.clone()];
        let mut outputs = vec![
            ExportSlot::new::<Tracked>(&config.child_first, &config.caller_out_a),
            ExportSlot::new::<Tracked>(&config.child_second, &config.caller_out_b),
        ];
        let error = ctx.finalize(&child, &declared, &mut outputs).unwrap_err();
        assert!(
            matches!(error, ScopeError::TypeMismatch { .. }),
            "got {error:?}"
        );

        // caller 的 refs／owned 都没有部分改变；两项都没有被转移，随 child 清理析构。
        assert_eq!(ctx.coordinator.refs_len_probe(&root)?, caller_refs);
        assert_eq!(ctx.coordinator.owned_len_probe(&root)?, caller_owned);
        assert!(
            ctx.coordinator.owner_probe(&first).is_err(),
            "no transfer to the caller"
        );
        assert!(!ctx.coordinator.alive_probe(&first));
        assert!(!ctx.coordinator.alive_probe(&second));
        assert_eq!(ctx.state(&child)?, ScopeState::Closed);
        // 真实边界会把该失败传播出去，由受控退出收口。
        Err(error.into())
    }

    let f = Fixture::new();
    let root = f.root();
    let config = Config {
        root: root.clone(),
        child_first: f.ref_id(),
        child_second: f.ref_id(),
        caller_out_a: f.ref_id(),
        caller_out_b: f.ref_id(),
        drops: Arc::clone(&f.drops),
    };
    let drops = Arc::clone(&f.drops);
    let execution = f.take_execution();
    let exit = drive_to_ready(
        crate::core::runtime::observe::run_root(execution, config, body),
        |_| {},
    );
    assert_eq!(exit.terminated(), Some(TerminationKind::BodyError));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "only the counted temporary"
    );
    let _ = root;
}

// ---- R12 剩余：D16 两层 Boundary 取消、D18 Root 本体 drop ----

// ---- 第四次复审：R11 的 Scope／调用类别关系反例固化 ----

// ---- 成组 Consume 的窄许可（WE-T05 §5.1） ----

#[test]
fn g10_group_consume_in_item_boundary_moves_all_slots_and_closes_item_once() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let root = f.root();
    let p0 = f.ref_id();
    let p1 = f.ref_id();
    let drops = Arc::clone(&f.drops);
    {
        let ctx = f.context_mut();
        let mut root_guard = ctx.enter(InvocationKind::Root, &root, true).unwrap();
        let each = root_guard.create_child(&root).unwrap();
        let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
        let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
        let mut each_guard = root_guard
            .enter(InvocationKind::Boundary, &each, true)
            .unwrap();
        let item = each_guard.create_child(&each).unwrap();
        each_guard
            .register_owned(&item, &p0, tracked_num(1, &drops))
            .unwrap();
        each_guard
            .register_owned(&item, &p1, tracked_num(2, &drops))
            .unwrap();
        let mut item_guard = each_guard
            .enter(InvocationKind::Boundary, &item, true)
            .unwrap();
        let permit = super::ItemGroupPermit {
            item: item.clone(),
            each: each.clone(),
            group: vec![p0.clone(), p1.clone()],
            collectors: vec![c0.clone(), c1.clone()],
        };
        let outcome =
            item_guard.consume_group_in_item_boundary(&item, &[p0.clone(), p1.clone()], &permit);
        assert!(
            matches!(outcome, super::ConsumeGroupOutcome::Consumed),
            "got {outcome:?}"
        );
        assert_eq!(item_guard.coordinator.collector_moves(&c0).unwrap(), 1);
        assert_eq!(item_guard.coordinator.collector_moves(&c1).unwrap(), 1);
        assert_eq!(item_guard.coordinator.collector_len(&c0).unwrap(), 1);
        assert_eq!(item_guard.coordinator.collector_len(&c1).unwrap(), 1);
        assert_eq!(
            item_guard.state(&item).unwrap(),
            ScopeState::Closed,
            "Item 只关闭一次"
        );
        assert_eq!(
            item_guard.state(&each).unwrap(),
            ScopeState::Active,
            "collector owner 仍存活"
        );
        item_guard.release_responsibility(&item);
        item_guard.complete();
        // each／root 由各自 guard 的清理收口（销毁未完成 collector）。
        drop(each_guard);
        root_guard.complete();
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "两个业务值随 collector 各析构一次"
    );
}

#[test]
fn g11_group_consume_permit_rejects_mismatch_before_any_move() {
    crate::core::test_support::reset_observations();
    let mut f = Fixture::new();
    let root = f.root();
    let p0 = f.ref_id();
    let p1 = f.ref_id();
    let drops = Arc::clone(&f.drops);
    {
        let ctx = f.context_mut();
        let mut root_guard = ctx.enter(InvocationKind::Root, &root, true).unwrap();
        let each = root_guard.create_child(&root).unwrap();
        let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
        let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
        // 另一个 Each（外来 collector owner）。
        let foreign_each = root_guard.create_child(&root).unwrap();
        let foreign = root_guard
            .begin_collector::<Tracked>(&foreign_each)
            .unwrap();
        let mut each_guard = root_guard
            .enter(InvocationKind::Boundary, &each, true)
            .unwrap();
        let item = each_guard.create_child(&each).unwrap();
        each_guard
            .register_owned(&item, &p0, tracked_num(1, &drops))
            .unwrap();
        each_guard
            .register_owned(&item, &p1, tracked_num(2, &drops))
            .unwrap();
        let mut item_guard = each_guard
            .enter(InvocationKind::Boundary, &item, true)
            .unwrap();

        let permit = super::ItemGroupPermit {
            item: item.clone(),
            each: each.clone(),
            group: vec![p0.clone(), p1.clone()],
            collectors: vec![c0.clone(), c1.clone()],
        };
        // (1) 声明组被裁剪（selected 少一槽）。
        match item_guard.consume_group_in_item_boundary(&item, std::slice::from_ref(&p0), &permit) {
            super::ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
                matches!(&primary, ScopeError::Invariant { violated }
                        if *violated == "group item collection length does not match the declared group"),
                "got {primary:?}"
            ),
            other => panic!("expected rejection, got {other:?}"),
        }
        // (2) 换项：selected 位置与声明组不一致（长度相同）。
        let swapped = vec![p1.clone(), p0.clone()];
        match item_guard.consume_group_in_item_boundary(&item, &swapped, &permit) {
            super::ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
                matches!(&primary, ScopeError::Invariant { violated }
                        if *violated == "group item collection slot does not match the declared group position"),
                "got {primary:?}"
            ),
            other => panic!("expected rejection, got {other:?}"),
        }
        // (3) 外来 collector（owner 不是许可 Each）。
        let foreign_permit = super::ItemGroupPermit {
            item: item.clone(),
            each: each.clone(),
            group: vec![p0.clone(), p1.clone()],
            collectors: vec![c0.clone(), foreign.clone()],
        };
        match item_guard.consume_group_in_item_boundary(
            &item,
            &[p0.clone(), p1.clone()],
            &foreign_permit,
        ) {
            super::ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
                matches!(&primary, ScopeError::Invariant { violated }
                        if *violated == "group item permit collector is not owned by the registered each scope"),
                "got {primary:?}"
            ),
            other => panic!("expected rejection, got {other:?}"),
        }

        // 冻结前的许可拒绝：Item 保持 Active、零物理移动。
        assert_eq!(item_guard.state(&item).unwrap(), ScopeState::Active);
        assert_eq!(item_guard.coordinator.collector_moves(&c0).unwrap(), 0);
        assert_eq!(item_guard.coordinator.collector_moves(&c1).unwrap(), 0);
        assert_eq!(item_guard.coordinator.owned_len_probe(&item).unwrap(), 2);

        item_guard.release_responsibility(&item);
        item_guard.complete();
        drop(each_guard);
        root_guard.complete();
    }
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}
