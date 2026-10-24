//! WE-T04 定向样本（E 矩阵补缺）。
use crate::Data;
use crate::core::context::{ExecutionContext, InvocationGuard, InvocationKind};
use crate::core::engine::definition::{Definition, PositionSource, build_definition};
use crate::core::engine::error::{
    BodyError, BuildErrorDetail, ExecFailure, FrameworkDiagnostic, RunError, Visibility,
};
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::PositionHandle;
use crate::core::engine::observe::{self, ControllerRoundRecord, FrameRecord};
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use crate::core::identity::{DataId, ExecutionIdentity, ScopeId};
use crate::core::ref_id::RefId;
use crate::core::scope::{ScopeState, TargetSnapshot};
use crate::core::test_support::{
    ExportPreCleanupSnapshot, ExportSnapshotPhase, RootSnapshotPhase, closed_scope_reset,
    closed_scope_snapshot, promote_group_states_reset, reset_observations, root_snapshots_reset,
    round_collect_group_reset, take_export_pre_cleanup, take_log_snapshot,
    take_promote_group_states, take_root_snapshots, take_termination_saved,
};
use futures::executor::block_on;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

macro_rules! reset_all {
    () => {{
        observe::reset();
        reset_observations();
        round_collect_group_reset();
        promote_group_states_reset();
        take_root_snapshots();
        root_snapshots_reset();
        closed_scope_reset();
        CALLS.with(|slot| slot.set(0));
    }};
}

thread_local! {
    /// 业务 Node 的调用计数（按需读取，证明实际执行／重启）。
    static CALLS: Cell<u32> = const { Cell::new(0) };
}

fn calls() -> u32 {
    CALLS.with(Cell::get)
}

/// 逐实例 Drop 与产生事件日志。
#[derive(Data)]
struct Tracked {
    value: u32,
    tag: &'static str,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.log.borrow_mut().push(format!("drop:{}", self.tag));
    }
}

/// 祖先依赖：被深层普通捕获（E06），全程不改 owner。
#[derive(Data)]
struct Anchor {
    value: u32,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for Anchor {
    fn drop(&mut self) {
        self.log.borrow_mut().push(String::from("drop:anchor"));
    }
}

#[derive(Data)]
struct Prompt(u32);

#[derive(Data)]
struct Report(u32);

/// 借用视图的裸业务值（E33：非 `Clone`／非 `Copy`／非 `Send`）。
#[derive(Data)]
struct Opaque {
    cell: Rc<RefCell<u32>>,
}

fn frames() -> Vec<FrameRecord> {
    observe::take_frames()
}

fn rounds() -> Vec<ControllerRoundRecord> {
    observe::take_controller_rounds()
}

fn root_scope_from(frames: &[FrameRecord]) -> ScopeId {
    frames
        .iter()
        .find(|record| record.kind == "root" && record.phase == "enter")
        .expect("root frame entered")
        .scope
        .clone()
}

fn controller_of(records: &[ControllerRoundRecord], kind: &str) -> ScopeId {
    records
        .iter()
        .find(|record| record.kind == kind && record.at == "controller-created")
        .unwrap_or_else(|| panic!("{kind} controller created"))
        .controller
        .clone()
}

// ================================================================ E05：frame／caller／责任

/// 迭代推进一格（E05 状态）。
async fn bump_ctx(query: Query<&Tracked>) -> Result<Tracked, BodyError> {
    let current = query.get();
    current.log.borrow_mut().push(String::from("run:ctx"));
    Ok(Tracked {
        value: current.value + 1,
        tag: "next",
        log: Rc::clone(&current.log),
    })
}

/// 第二轮起发出 IterBreak（保证至少一轮 Normal）。
async fn break_after_first(query: Query<&Tracked>) -> Result<(), BodyError> {
    if query.get().value >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

async fn consume_ctx(query: Query<&Tracked>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// E05：Retry → Iter —— 真实 controller／Round frame 的进出、caller 映射与各轮 Scope 不同。
#[test]
fn e05_controllers_expose_real_frames_callers_and_distinct_rounds() {
    reset_all!();
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, state: Ref<Tracked>| {
            let () = flow.retry_output(1, |attempt| {
                let inner = attempt.iter_output(state, 3, |round| {
                    let current = round.current::<Tracked>();
                    let next = round.then_with_output(&bump_ctx, current);
                    round.then_with_output(&break_after_first, current);
                    round.next_with(next);
                });
                let _: () = attempt.then_with_output(&consume_ctx, inner);
                attempt.output::<()>();
            });
            flow.output_with(());
        },
        Tracked {
            value: 0,
            tag: "seed",
            log: Rc::clone(&log),
        },
    ));
    assert_eq!(outcome.expect("the iter handles the break"), ());

    let frames = frames();
    let root = root_scope_from(&frames);
    // Root frame 成对；业务 Node 在各自 Round 的真实 Scope 内进出 leaf frame（Retry／Iter
    // 控制器不额外记录 boundary frame，其责任由控制器观察点与 leaf frame 共同锚定）。
    assert_eq!(
        frames
            .iter()
            .filter(|record| record.kind == "root")
            .map(|record| record.phase)
            .collect::<Vec<_>>(),
        ["enter", "exit"]
    );
    let leaf_enter: Vec<ScopeId> = frames
        .iter()
        .filter(|record| record.kind == "leaf" && record.phase == "enter")
        .map(|record| record.scope.clone())
        .collect();
    let leaf_exit: Vec<ScopeId> = frames
        .iter()
        .filter(|record| record.kind == "leaf" && record.phase == "exit")
        .map(|record| record.scope.clone())
        .collect();
    assert_eq!(leaf_enter, leaf_exit, "leaf frames are paired");
    assert!(
        frames
            .iter()
            .all(|record| record.scope != root || record.kind == "root")
    );

    let rounds = rounds();
    let retry_controller = controller_of(&rounds, "retry");
    let iter_controller = controller_of(&rounds, "iter");
    assert_ne!(retry_controller, iter_controller);
    // caller 映射：Retry 的 caller 是 Root；Iter 的 caller 是 Retry 的当前 Round。
    let retry_created = rounds
        .iter()
        .find(|record| record.kind == "retry" && record.at == "controller-created")
        .unwrap();
    assert_eq!(retry_created.caller, root);
    assert_eq!(retry_created.controller, retry_controller);
    let iter_created = rounds
        .iter()
        .find(|record| record.kind == "iter" && record.at == "controller-created")
        .unwrap();
    assert_eq!(iter_created.controller, iter_controller);
    let retry_round = rounds
        .iter()
        .find(|record| record.kind == "retry" && record.at == "round-created")
        .unwrap()
        .round
        .clone()
        .unwrap();
    assert_eq!(
        iter_created.caller, retry_round,
        "the iter controller's caller is the enclosing retry round"
    );
    assert_ne!(iter_controller, retry_round);

    // 各轮 Scope 不同且单调。
    let iter_rounds: Vec<ScopeId> = rounds
        .iter()
        .filter(|record| record.kind == "iter" && record.at == "round-created")
        .filter_map(|record| record.round.clone())
        .collect();
    assert_eq!(iter_rounds.len(), 2);
    assert_ne!(iter_rounds[0], iter_rounds[1]);
    assert!(iter_rounds[0].seq() < iter_rounds[1].seq());

    // 每个业务 Node 的 leaf frame 都落在对应的 Round Scope 上：Iter 两轮各有一轮 Node，
    // Retry round 内还有内层 Iter 之外的 consume Node（unit 输出）。
    let mut expected_leaf_scopes = iter_rounds.clone();
    expected_leaf_scopes.push(retry_round.clone());
    assert!(
        leaf_enter
            .iter()
            .all(|scope| expected_leaf_scopes.contains(scope)),
        "leaf scopes are the real round scopes: {leaf_enter:?}"
    );
    assert!(iter_rounds.iter().all(|round| leaf_enter.contains(round)));
    assert!(leaf_enter.contains(&retry_round));

    // state owner：成组 Promote 的 state 归属 iter controller。
    let promotes = take_promote_group_states();
    assert_eq!(promotes.len(), 1, "one normal promote in the iter");
    assert_eq!(promotes[0].controller, iter_controller);
    assert_eq!(promotes[0].source, iter_rounds[0]);
    assert_eq!(promotes[0].states[0].0, iter_controller);
    reset_all!();
}

// ================================================================ E06：深层 ordinary＋StateImport

/// 同时读取普通捕获的祖先 Anchor 与 formal current（StateImport），并推进 state。
async fn combine(query: Query<(&Tracked, &Anchor)>) -> Result<Tracked, BodyError> {
    let (current, anchor) = query.get();
    current
        .log
        .borrow_mut()
        .push(format!("combine:{}+{}", current.value, anchor.value));
    Ok(Tracked {
        value: current.value + anchor.value,
        tag: "next",
        log: Rc::clone(&current.log),
    })
}

/// E06：Root → Chain → Iter —— 深层 ordinary 捕获与 StateImport 在直接 caller 装配。
#[test]
fn e06_deep_ordinary_capture_and_state_import_meet_at_the_direct_caller() {
    reset_all!();
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Tracked, _>(
        |flow: &mut Flow<'_>, (state, anchor): (Ref<Tracked>, Ref<Anchor>)| {
            let out_refs: Ref<Tracked> = flow.chain_output(|sub| {
                let out_refs = sub.iter_output(state, 3, |round| {
                    let current = round.current::<Tracked>();
                    let next = round.then_with_output(&combine, (current, anchor));
                    round.then_with_output(&break_after_first, current);
                    round.next_with(next);
                });
                sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        (
            Tracked {
                value: 0,
                tag: "seed",
                log: Rc::clone(&log),
            },
            Anchor {
                value: 1,
                log: Rc::clone(&log),
            },
        ),
    ));
    let delivered = outcome.expect("the chain delivers the iter output");
    // combine 读到了二者：第一轮 value = 0 + 1 = 1，第二轮 Break 交付进入本轮的 current(1)。
    assert_eq!(delivered.value, 1);
    let events = log.borrow().clone();
    assert!(
        events.contains(&String::from("combine:0+1")),
        "round 1 read the formal current and the captured ancestor: {events:?}"
    );
    assert!(
        events.contains(&String::from("combine:1+1")),
        "round 2 still reads both: {events:?}"
    );

    let frames = frames();
    let root = root_scope_from(&frames);
    let chain_rounds = rounds();
    let iter_controller = controller_of(&chain_rounds, "iter");
    let iter_round = chain_rounds
        .iter()
        .find(|record| record.kind == "iter" && record.at == "round-created")
        .unwrap()
        .round
        .clone()
        .unwrap();
    // Iter controller 的直接 caller 是 Chain 的 child Body（不是 Root），证明在直接 caller 装配。
    let iter_created = chain_rounds
        .iter()
        .find(|record| record.kind == "iter" && record.at == "controller-created")
        .unwrap();
    assert_ne!(iter_created.caller, root);
    assert_eq!(iter_created.controller, iter_controller);
    assert!(iter_round.seq() > iter_controller.seq());

    // Chain Import：child=Chain 的调用 Scope，caller=Root；两个位置（state 与 anchor）各绑定一次。
    let imports = observe::take_chain_imports();
    assert_eq!(imports.len(), 1, "one chain import batch");
    let import = &imports[0];
    assert_eq!(import.caller, root);
    assert_ne!(import.child, root);
    assert_eq!(
        import.pairs.len(),
        2,
        "state + ancestor are both imported: {:?}",
        import.pairs
    );
    assert_ne!(
        import.pairs[0].0, import.pairs[1].0,
        "distinct core positions"
    );
    assert!(import.pairs.iter().all(|(source, target)| source == target));

    // 原 owner 不变：祖先 Anchor 是 Root 自有输入，循环从不接管它；它在 Root 关闭时才 Drop。
    assert!(
        events.contains(&String::from("drop:anchor")),
        "the anchor is released when Root closes, not by the loop: {events:?}"
    );
    let snapshots = take_root_snapshots();
    let after_commit = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit)
        .expect("after-commit root snapshot");
    assert!(
        !after_commit.root_owned.is_empty(),
        "root still owns its inputs at extraction: {:?}",
        after_commit.root_owned
    );
    reset_all!();
}

// ================================================================ E08：Retry 全输出交付

async fn generate(query: Query<&Prompt>) -> Result<Report, BodyError> {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(Report(query.get().0 + 1))
}

/// E08：Retry 混合输出（自产 Report ＋ 祖先 alias）经两跳 Export 正式交付。
#[test]
fn e08_retry_mixed_output_delivers_self_and_ancestor_alias() {
    reset_all!();
    let runtime = Runtime::new();
    let (report, prompt) = block_on(runtime.execute::<_, (Report, Prompt), _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: (Ref<Report>, Ref<Prompt>) = flow.retry_output(1, |attempt| {
                let report = attempt.then_with_output(&generate, prompt);
                let out_refs = (report, prompt);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(41),
    ))
    .expect("mixed retry output is delivered");
    assert_eq!((report.0, prompt.0), (42, 41));
    assert_eq!(calls(), 1, "single attempt");

    let deliveries = observe::take_controller_deliveries();
    assert_eq!(deliveries.len(), 2, "two hops: {deliveries:?}");
    assert_eq!(deliveries[0].hop, "round-to-controller");
    assert_eq!(deliveries[1].hop, "controller-to-caller");
    assert!(deliveries.iter().all(|record| record.kind == "retry"));
    assert!(
        deliveries.iter().all(|record| record.pairs.len() == 2),
        "both slots"
    );
    // 来源／目的 Scope 链：Round → controller → caller。
    assert_ne!(deliveries[0].from, deliveries[0].to);
    assert_eq!(
        deliveries[0].to, deliveries[1].from,
        "the controller is both the first hop's destination and the second hop's source"
    );
    assert_ne!(deliveries[1].from, deliveries[1].to);
    reset_all!();
}

/// E08：Retry 重复 target 输出（两个槽指向同一自产 DataId）仍整组交付。
#[test]
fn e08_retry_duplicate_target_output_is_delivered_as_a_group() {
    reset_all!();
    let runtime = Runtime::new();
    let report = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let pair: (Ref<Report>, Ref<Report>) = flow.retry_output(1, |attempt| {
                let report = attempt.then_with_output(&generate, prompt);
                let out_refs = (report, report);
                attempt.output_with(out_refs);
            });
            let out_refs = pair.0;
            flow.output_with(out_refs);
        },
        Prompt(9),
    ))
    .expect("selecting one of a duplicated pair is legal");
    assert_eq!(report.0, 10);

    let deliveries = observe::take_controller_deliveries();
    assert_eq!(deliveries.len(), 2);
    assert!(deliveries.iter().all(|record| record.pairs.len() == 2));
    reset_all!();
}

// ================================================================ E28：Root 零部分 take

/// E28：Retry 的重复 target 输出整组带到 Root 时，Root 预检拒绝且 take 次数为零。
#[test]
fn e28_duplicate_root_targets_are_rejected_with_zero_takes() {
    reset_all!();
    crate::core::test_support::take_log_reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Report, Report), _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: (Ref<Report>, Ref<Report>) = flow.retry_output(1, |attempt| {
                let report = attempt.then_with_output(&generate, prompt);
                let out_refs = (report, report);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(7),
    ));
    let error = outcome.err().expect("duplicate root targets are rejected");
    match &error {
        RunError::Runtime(runtime) => {
            assert!(
                runtime
                    .note()
                    .contains("root extraction preflight rejected"),
                "actual note: {}",
                runtime.note()
            );
        }
        other => panic!("expected Runtime preflight rejection, got {other}"),
    }
    // 业务确实执行过（不是构建期拒绝），但零 take。
    assert_eq!(calls(), 1, "the attempt ran before root extraction");

    let takes = crate::core::test_support::take_log_snapshot();
    assert!(takes.is_empty(), "no partial take on rejection: {takes:?}");
    let snapshots = take_root_snapshots();
    let rejected = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::PreflightRejected)
        .expect("preflight rejection snapshot");
    assert_eq!(rejected.planned_takes, 0);
    assert!(
        !rejected.root_owned.is_empty(),
        "root still owns the produced value"
    );
    reset_all!();
}

// ================================================================ E28：最终交付出口的后项拒绝
//
// 任务书同行的 T04 对应要求：「最终 Export 后项（late）拒绝 —— 精确目的位置诊断、caller 全组
// refs／owned／owner／alive／取号前后相等；Each 产物失败清理、不半导出」，并明确不得用 Root
// 重复 DataId 预检代替（后者见上一条 `e28_duplicate_root_targets_are_rejected_with_zero_takes`，
// 两者在本文件分开归档）。
//
// 可达性结论（逐行核对，见交付报告）：Retry／Iter 交付出口的目的位置由
// `definition.rs::register_retry`／`register_iter` 以 `RetryBridge`／`RetryOutput`／`IterBridge`／
// `IterOutput` **新分配**（下方 `definition_positions` 逐项核对），公开 `Flow` 构造无法预先把它
// 绑定到 caller；类型判据也由构建期静态类型保证。因此该后项占位拒绝**没有生产可达样本**，且既有
// `install_export_occupy_fault`（钩子与调用点都属 `boundary.rs`）没有接到 `deliver_round`／
// `export_group`。下列样本是本任务登记的**可达性替代**：占位只由样本自己在 caller Scope 用**正式**
// `register_owned` 施加（与既有 Chain／Choose 占位样本 `b16_*`／`c19_*` 同一做法，不新增生产或
// 测试钩子、不改 `boundary.rs`）；驱动与拒绝全部由真实主链完成：
// `StepEntry::execute → boundary::invoke_retry／invoke_iter → deliver_round（第一跳）／
// export_group（第二跳）→ InvocationGuard::finalize → ScopeCoordinator::complete_exit／prepare_export`。

/// 样本自带的合法占位值（与 `install_export_occupy_fault` 的夹具同型：不参与业务类型检查）。
const OCCUPANT: u8 = 0;

/// caller 侧完整快照：refs（target-aware）＋ owned（逐项 owner／alive）＋下一个取号。
#[derive(Debug, PartialEq)]
struct DeliveryCallerSnapshot {
    refs: Vec<(RefId, TargetSnapshot)>,
    owned: Vec<(DataId, Option<ScopeId>, bool)>,
    next_data_id: Option<u64>,
}

fn delivery_caller_snapshot(
    guard: &InvocationGuard<'_>,
    caller: &ScopeId,
) -> DeliveryCallerSnapshot {
    let (refs, owned) = guard
        .snapshot_targets_probe(caller)
        .expect("caller snapshot before/after the delivery");
    DeliveryCallerSnapshot {
        refs,
        owned: owned
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    guard.owner_probe(id).ok(),
                    guard.alive_probe(id),
                )
            })
            .collect(),
        next_data_id: guard.next_data_id_probe(),
    }
}

/// 按 `PositionSource` 取回真实 Definition 中的位置身份（分配顺序即槽位顺序）。
fn definition_positions(
    definition: &Definition<'_>,
    mut which: impl FnMut(PositionSource) -> bool,
) -> Vec<RefId> {
    definition
        .positions()
        .iter()
        .filter(|record| which(record.source()))
        .map(|record| record.core().clone())
        .collect()
}

/// 一次交付出口后项拒绝的核对输入。
struct DeliveryRejection<'a> {
    /// 该出口的稳定说明文本（`BoundaryLabels::export`）。
    note: &'static str,
    /// 该出口在 `ScopeError` 中的确切变体名。
    variant: &'static str,
    root: &'a ScopeId,
    controller: &'a ScopeId,
    destinations: &'a [RefId],
    bridges: &'a [RefId],
    /// 被占位的后项（late）槽下标。
    late: usize,
    occupant: &'a DataId,
}

/// 交付出口后项拒绝的公共断言：精确诊断（确切变体＋目的位置身份）、来源／目的两侧完整前后快照、
/// 第一跳已完成而第二跳一个位置都没有绑定（不半导出）、拒绝不取号。
fn assert_delivery_rejection(failure: &ExecFailure, observed: &DeliveryRejection<'_>) {
    let late = &observed.destinations[observed.late];
    let ExecFailure::Runtime(runtime_error) = failure else {
        panic!("expected the delivery exit's framework failure, got {failure:?}");
    };
    assert_eq!(runtime_error.note(), observed.note);
    let source = std::error::Error::source(runtime_error)
        .expect("the original Scope diagnostic is preserved");
    assert_eq!(
        source.to_string(),
        format!("{late} is already bound in {}", observed.root),
        "the framework text names the exact late destination position and the caller scope"
    );
    let framework = source
        .downcast_ref::<FrameworkDiagnostic>()
        .expect("the delivery diagnostic is the framework's original Scope diagnostic");
    let variant = format!("{framework:?}");
    assert!(
        variant.starts_with(&format!(
            "FrameworkDiagnostic {{ scope_error: {} {{",
            observed.variant
        )),
        "exact diagnostic variant: {variant}"
    );
    assert!(
        variant.contains(&format!("scope: {}", observed.root)),
        "the conflicting scope is the caller: {variant}"
    );
    assert!(
        variant.contains(&format!("position: {late}")),
        "the conflicting position is the late destination slot: {variant}"
    );

    // 交付出口自身的完整前后观察（生产路径记录，独立通道）。
    let snapshots = take_export_pre_cleanup();
    let hop_two: Vec<&ExportPreCleanupSnapshot> = snapshots
        .iter()
        .filter(|snapshot| snapshot.caller.as_ref() == Some(observed.root))
        .collect();
    assert_eq!(
        hop_two.len(),
        2,
        "before + after-reject of the second hop: {snapshots:?}"
    );
    assert_eq!(hop_two[0].phase, ExportSnapshotPhase::Before);
    assert_eq!(hop_two[1].phase, ExportSnapshotPhase::AfterReject);
    for snapshot in &hop_two {
        assert_eq!(snapshot.observation_error, None);
        assert_eq!(snapshot.child.as_ref(), Some(observed.controller));
        assert_eq!(snapshot.caller.as_ref(), Some(observed.root));
        assert_eq!(
            snapshot
                .slots
                .iter()
                .map(|(_, caller, _)| caller.clone())
                .collect::<Vec<_>>(),
            observed.destinations.to_vec(),
            "the observed slots are the builder-allocated destinations"
        );
        assert_eq!(
            snapshot.conflict_target,
            Some(TargetSnapshot::Data(observed.occupant.clone())),
            "the occupant target is read back at the conflicting position"
        );
        assert_eq!(snapshot.conflict_owner.as_ref(), Some(observed.root));
        assert!(snapshot.conflict_alive, "the occupant stays alive");
        assert_eq!(snapshot.caller_state, Some(ScopeState::Active));
    }
    assert_eq!(
        hop_two[0].child_refs, hop_two[1].child_refs,
        "来源 refs 未变"
    );
    assert_eq!(
        hop_two[0].child_owned, hop_two[1].child_owned,
        "来源 owned 未变"
    );
    assert_eq!(
        hop_two[0].caller_refs, hop_two[1].caller_refs,
        "caller refs 未变"
    );
    assert_eq!(
        hop_two[0].caller_owned, hop_two[1].caller_owned,
        "caller owned 未变"
    );
    assert_eq!(hop_two[0].slots, hop_two[1].slots);
    assert_eq!(
        hop_two[0].next_data_id, hop_two[1].next_data_id,
        "拒绝不取号"
    );

    // 第一跳已合法完成（真实时序保留）：controller 已绑定本次交付的桥并接收实例；
    // 第二跳对 caller 的**每一个**槽都没有绑定（后项拒绝仍整组原子，首项不提前提交）。
    let child_refs = hop_two[1].child_refs.as_ref().expect("controller refs");
    for bridge in observed.bridges {
        assert!(
            child_refs.iter().any(|(position, _)| position == bridge),
            "the first hop bound the controller bridge {bridge}"
        );
    }
    let caller_refs = hop_two[1].caller_refs.as_ref().expect("caller refs");
    for (index, destination) in observed.destinations.iter().enumerate() {
        if index != observed.late {
            assert!(
                !caller_refs
                    .iter()
                    .any(|(position, _)| position == destination),
                "the legal slot {index} is not half-exported"
            );
        }
    }
}

/// E28（Retry 交付出口）：第二跳（controller → caller）的**后项**目的位置已被另一合法绑定占位
/// → 整组交付在任何提交之前拒绝；caller 前后完整快照相等、零 take、产物按失败清理一次。
#[test]
fn e28_retry_delivery_late_destination_occupied_rejects_without_partial_export() {
    reset_all!();
    let log = Rc::new(RefCell::new(Vec::new()));
    let definition = build_definition::<Tracked, (Tracked, Tracked), _>(|flow, seed| {
        let out_refs: (Ref<Tracked>, Ref<Tracked>) = flow.retry_output(1, |attempt| {
            let first = attempt.then_with_output(&bump_ctx, seed);
            let second = attempt.then_with_output(&bump_ctx, seed);
            let out_refs = (first, second);
            attempt.output_with(out_refs);
        });
        flow.output_with(out_refs);
    })
    .expect("the retry definition builds");

    let destinations = definition_positions(&definition, |source| {
        matches!(source, PositionSource::RetryOutput { .. })
    });
    let bridges = definition_positions(&definition, |source| {
        matches!(source, PositionSource::RetryBridge { .. })
    });
    let exits = definition_positions(&definition, |source| {
        matches!(source, PositionSource::RetryExit { .. })
    });
    assert_eq!(
        (destinations.len(), bridges.len(), exits.len()),
        (2, 2, 2),
        "每个槽各有构建器新分配的目的位置／桥／出口"
    );
    assert_ne!(destinations[0], destinations[1]);

    let mut ctx = ExecutionContext::new(ExecutionIdentity::new());
    let root = ctx.root_scope();
    let mut guard = ctx
        .enter(InvocationKind::Root, &root, true)
        .expect("root frame");
    let input = definition
        .root_inputs()
        .next()
        .expect("one root input")
        .core()
        .clone();
    guard
        .register_owned(
            &root,
            &input,
            Tracked {
                value: 0,
                tag: "seed",
                log: Rc::clone(&log),
            },
        )
        .expect("root input");
    let free = guard.snapshot_targets_probe(&root).expect("root snapshot");
    assert!(
        free.0
            .iter()
            .all(|(position, _)| !destinations.contains(position)),
        "交付目的位置在占位前必须是自由的"
    );
    // 合法占位：后项（第 1 槽）目的位置由 caller 自己的正式绑定占据；第 0 槽保持自由。
    let occupant = guard
        .register_owned(&root, &destinations[1], OCCUPANT)
        .expect("legal occupant");
    let before = delivery_caller_snapshot(&guard, &root);

    let step = definition.root().steps()[0].as_ref();
    let failure = block_on(step.execute(&definition, &mut guard, &root))
        .expect_err("an occupied late destination rejects the whole delivery");

    let records = rounds();
    let controller = controller_of(&records, "retry");
    let round = records
        .iter()
        .find(|record| record.kind == "retry" && record.at == "round-created")
        .and_then(|record| record.round.clone())
        .expect("the round scope is observed");

    assert_delivery_rejection(
        &failure,
        &DeliveryRejection {
            note: "retry output handover rejected",
            variant: "RefAlreadyBound",
            root: &root,
            controller: &controller,
            destinations: &destinations,
            bridges: &bridges,
            late: 1,
            occupant: &occupant,
        },
    );

    // caller 侧完整前后快照：refs／owned（逐项 owner／alive）前后相等；占位绑定不被覆盖。
    // 「取号前后相等」的判据针对**拒绝本身**：本轮业务实例的取号发生在拒绝之前且恰好两个，
    // 因此交付拒绝不引入任何新序号（拒绝点的 Before／AfterReject 取号相等已在上方断言）。
    let after = delivery_caller_snapshot(&guard, &root);
    assert_eq!(before.refs, after.refs, "caller refs 前后相等");
    assert_eq!(
        before.owned, after.owned,
        "caller owned／owner／alive 前后相等"
    );
    assert_eq!(
        after.next_data_id,
        before.next_data_id.map(|seq| seq + 2),
        "交付拒绝本身不取号：新增序号只来自本轮两个业务实例"
    );
    assert_eq!(
        guard.target_data_id_probe(&root, &destinations[1]).unwrap(),
        Some(occupant.clone()),
        "占位绑定未被覆盖"
    );
    assert!(guard.alive_probe(&occupant));
    assert_eq!(guard.owner_probe(&occupant).unwrap(), root);

    // 零 Root take：交付拒绝先于 Root 冻结／取号（Root 全程 Active），没有任何 take。
    assert!(
        guard.is_terminated(),
        "the delivery failure terminates the execution"
    );
    assert_eq!(guard.state(&root).unwrap(), ScopeState::Active);
    assert!(take_log_snapshot().is_empty(), "no take happened");
    assert!(
        take_root_snapshots()
            .iter()
            .all(|snapshot| snapshot.planned_takes == 0)
    );

    // 产物失败清理：本轮两个实例随失败收口各销毁一次；caller 从未接管它们，Root 输入未被触碰。
    assert_eq!(
        log.borrow()
            .iter()
            .filter(|event| *event == "drop:next")
            .count(),
        2,
        "each produced instance is cleaned exactly once: {:?}",
        log.borrow()
    );
    assert!(!log.borrow().iter().any(|event| *event == "drop:seed"));
    assert!(
        take_termination_saved()
            .iter()
            .any(|termination| termination.note == "retry export rejected"),
        "the delivery failure is attributed to the Retry delivery exit"
    );
    assert_eq!(
        closed_scope_snapshot()
            .iter()
            .filter(|scope| *scope == &round)
            .count(),
        1,
        "the first hop closed the round exactly once"
    );
    guard.failed("e28 retry delivery rejection sample");
    reset_all!();
}

// ================================================================ E31：顺序轮先关闭再新建

async fn chain_first(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

async fn chain_second(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 10)
}

/// E31：顺序 Chain —— 后者创建前前者已 Closed、ScopeId 更大、旧 Scope 的进入／读取被拒绝。
#[test]
fn e31_sequential_chains_close_before_recreating_and_reject_old_scope_access() {
    reset_all!();
    let runtime = Runtime::new();
    let value = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, seed: Ref<u32>| {
            let first: Ref<u32> = flow.chain_output(|sub| {
                let out_refs = sub.then_with_output(&chain_first, seed);
                sub.output_with(out_refs);
            });
            let out_refs: Ref<u32> = flow.chain_output(|sub| {
                let out_refs = sub.then_with_output(&chain_second, first);
                sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        0u32,
    ))
    .expect("both chains run in order");
    assert_eq!(value, 11);

    // 顺序链的"先关闭再创建"：第二个 Chain 建立时，第一个 child 已在 closed_before 集合里。
    let creations = observe::take_chain_creations();
    assert_eq!(creations.len(), 2, "two chain children: {creations:?}");
    let first_child = creations[0].child.clone();
    let second_child = creations[1].child.clone();
    assert_ne!(first_child, second_child);
    assert!(
        first_child.seq() < second_child.seq(),
        "later child has a larger ScopeId"
    );
    assert!(
        !creations[0].closed_before.contains(&first_child),
        "a child is not closed when it is created"
    );
    assert!(
        creations[1].closed_before.contains(&first_child),
        "the first child is already closed when the second is created: {creations:?}"
    );

    // 旧 Scope 的进入／读取被真实拒绝。
    let rejections = observe::take_scope_rejections();
    assert!(
        rejections
            .iter()
            .any(|record| record.scope == first_child && record.operation == "enter-closed-child"),
        "closed child cannot be entered again: {rejections:?}"
    );
    assert!(
        rejections
            .iter()
            .any(|record| record.scope == first_child && record.operation == "resolve-closed-child"),
        "closed child's old position cannot be read: {rejections:?}"
    );
    reset_all!();
}

// ================================================================ E33：最小约束

/// 产生非 `Clone`／非 `Copy`／非 `Send` 的业务对象。
async fn make_opaque(query: Query<&Prompt>) -> Result<Opaque, BodyError> {
    Ok(Opaque {
        cell: Rc::new(RefCell::new(query.get().0)),
    })
}

/// 借用 Node：把输入借用在 await 期间保持存活，返回时读取其载荷。
async fn read_borrowing_await(query: Query<&Opaque>) -> Result<Report, BodyError> {
    let borrowed: &Rc<RefCell<u32>> = &query.get().cell;
    std::future::ready(()).await;
    Ok(Report(*borrowed.borrow()))
}

/// E33：业务 Data／Future／Node 保持原最小约束（无 `Clone`／`Copy`／`Send` 要求）。
#[test]
fn e33_business_objects_and_borrowing_nodes_keep_minimal_bounds() {
    reset_all!();
    let runtime = Runtime::new();
    let report = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let opaque = flow.then_with_output(&make_opaque, prompt);
            let out_refs = flow.then_with_output(&read_borrowing_await, opaque);
            flow.output_with(out_refs);
        },
        Prompt(77),
    ))
    .expect("non-Send, non-Clone data crosses a borrowing node");
    assert_eq!(report.0, 77);
    reset_all!();
}

// ================================================================ E24：内部 Iter 从原始 state 重启

/// 第一轮评审要求重试，第二轮成功并交付 state 本身（E24 外层 Retry 的重试驱动）。
async fn judge_retry_once(query: Query<&Tracked>) -> Result<Tracked, BodyError> {
    let call = CALLS.with(|calls| {
        let next = calls.get() + 1;
        calls.set(next);
        next
    });
    if call == 1 {
        Err(BodyError::retry(std::io::Error::other("retry once")))
    } else {
        let current = query.get();
        Ok(Tracked {
            value: current.value,
            tag: current.tag,
            log: Rc::clone(&current.log),
        })
    }
}

/// E24：外 Retry → 内 Iter → 内 Retry 结果 —— 外层重试时内部 Iter 从原始 state 重启。
#[test]
fn e24_outer_retry_restarts_the_inner_iter_from_the_original_state() {
    reset_all!();
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute::<_, Tracked, _>(
        |flow: &mut Flow<'_>, state: Ref<Tracked>| {
            let out_refs: Ref<Tracked> = flow.retry_output(1, |attempt| {
                let iterated = attempt.iter_output(state, 3, |round| {
                    let current = round.current::<Tracked>();
                    let next = round.then_with_output(&bump_ctx, current);
                    round.then_with_output(&break_after_first, current);
                    round.next_with(next);
                });
                let out_refs = attempt.then_with_output(&judge_retry_once, iterated);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Tracked {
            value: 0,
            tag: "seed",
            log: Rc::clone(&log),
        },
    ))
    .expect("the second outer attempt succeeds");
    // 若内部 Iter 未重启，第二轮会从 value=1 继续得到 2；实际仍为 1。
    assert_eq!(
        delivered.value, 1,
        "the inner iter restarts from the original state"
    );
    assert_eq!(calls(), 2, "two outer attempts called the inner judgement");

    let rounds = rounds();
    let iter_controllers: Vec<ScopeId> = rounds
        .iter()
        .filter(|record| record.kind == "iter" && record.at == "controller-created")
        .map(|record| record.controller.clone())
        .collect();
    assert_eq!(
        iter_controllers.len(),
        2,
        "one fresh iter per outer attempt"
    );
    assert_ne!(iter_controllers[0], iter_controllers[1]);
    assert!(iter_controllers[0].seq() < iter_controllers[1].seq());
    // 每个内层 Iter 都走"一轮 Normal、一轮 Break"。
    for controller in &iter_controllers {
        let ats: Vec<&'static str> = rounds
            .iter()
            .filter(|record| record.kind == "iter" && &record.controller == controller)
            .map(|record| record.at)
            .collect();
        assert_eq!(
            ats,
            [
                "controller-created",
                "round-created",
                "round-promoted",
                "round-created",
                "round-caught"
            ],
            "inner iter rounds: {ats:?}"
        );
    }
    reset_all!();
}

// ================================================================ E03：child-local 位置与未选分支来源

/// 匹配键（与公开样本一致的最小约束：`Data + PartialEq`）。
#[derive(PartialEq)]
struct Key(u8);

impl Data for Key {}

/// 由 u32 产生匹配键（Choose selector）。
async fn key_from_u32(query: Query<&u32>) -> Result<Key, BodyError> {
    Ok(Key(*query.get() as u8))
}

/// 由 u32 产生一个新的 u32 位置（round／branch 内产生的 child-local 位置；计数一次调用）。
async fn produce_u32(query: Query<&u32>) -> Result<u32, BodyError> {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(query.get() + 1)
}

/// 第一轮即发出 IterBreak（E04(ii)：`usize::MAX` 预算下的首轮 Break 正例）。
async fn break_now(_: Query<&u32>) -> Result<(), BodyError> {
    CALLS.with(|calls| calls.set(calls.get() + 1));
    Err(BodyError::iter_break())
}

fn assert_definition_out_of_scope(error: &RunError, expected: Visibility, context: &str) {
    match error {
        RunError::Definition(build) => assert!(
            matches!(
                build.detail,
                BuildErrorDetail::OutOfScope { relation, .. } if relation == expected
            ),
            "{context}: expected OutOfScope({expected:?}), got {build}"
        ),
        other => panic!("{context}: expected Definition, got {other}"),
    }
}

/// E03(ii)：未选 Choose branch 内伪造的前向位置作为 Iter 的 initial → Definition 拒绝、零 Node。
///
/// 该位置在未选分支内由 `next_position_id()` 伪造、随该分支的 Node 输出登记，因此运行时
/// 永远不会被绑定；Definition 在静态来源／可见性验证阶段就拒绝，未选分支的 Node 不执行。
#[test]
fn e03_unselected_branch_position_as_iter_initial_is_rejected() {
    reset_all!();
    let mut forged: Option<Ref<u32>> = None;
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, seed: Ref<u32>| {
            let selector = flow.then_with_output(&key_from_u32, seed);
            flow.choose_with(selector, |choice| {
                choice.case(Key(1), |branch| {
                    let out_refs = branch.then_with_output(&produce_u32, seed);
                    branch.output_with(out_refs);
                });
                choice.case(Key(2), |branch| {
                    let definition = branch.definition_id();
                    let future = branch.next_position_id();
                    forged = Some(Ref::<u32>::from_handle(PositionHandle {
                        definition,
                        position: future,
                    }));
                    let out_refs = branch.then_with_output(&produce_u32, seed);
                    branch.output_with(out_refs);
                });
            });
            let initial = forged.expect("captured forged ref");
            let out_refs = flow.iter_output(initial, 1, |round| {
                let current = round.current::<u32>();
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            flow.output_with(out_refs);
        },
        1u32,
    ));
    let error =
        outcome.expect_err("an unselected branch's position must not become the iter initial");
    assert_definition_out_of_scope(&error, Visibility::ChildLocal, "iter initial");
    assert_eq!(
        calls(),
        0,
        "the unselected branch's Node must not run for an invalid Definition"
    );
    reset_all!();
}

// ================================================================ E04：预算边界正例与内部最后一次转换证据

/// E04(i)：`usize::MAX - 1` 的 Retry 在首次 attempt 成功 → 恰好一次 Node 调用、正常交付。
///
/// 内部预算证据（可观测面）：`round-created` 恰为一条、无 `budget-exhausted`、无第二条
/// `round-created`，业务 Node 调用计数恰为 1，最终正常交付。当前 cfg(test) 面不暴露
/// `RetryStep::max_retries`，故不以读取内部字段替代上述真实执行轨迹；不虚构内部值。
/// （实现层 `register_retry` 原样保存 `max_retries`，运行期只做 `attempts <= max_retries`
/// 比较，不计算 N+1，故 `MAX-1` 不会在首次成功路径上发生溢出或额外轮次。）
#[test]
fn e04_retry_max_minus_one_first_attempt_success_runs_one_attempt() {
    reset_all!();
    let runtime = Runtime::new();
    let report = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: Ref<Report> = flow.retry_output(usize::MAX - 1, |attempt| {
                let out_refs = attempt.then_with_output(&generate, prompt);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(41),
    ))
    .expect("the largest representable retry budget still delivers a first-attempt success");
    assert_eq!(report.0, 42);
    assert_eq!(calls(), 1, "exactly one attempt Node call");

    let records = rounds();
    let retry_ats: Vec<&str> = records
        .iter()
        .filter(|record| record.kind == "retry")
        .map(|record| record.at)
        .collect();
    assert_eq!(
        retry_ats,
        ["controller-created", "round-created", "round-normal"],
        "one created round, no budget close, no extra attempt: {records:?}"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == "retry" && record.at == "round-created")
            .count(),
        1,
        "no extra round at usize::MAX - 1"
    );
    assert!(
        !records.iter().any(|record| record.at == "budget-exhausted"),
        "no budget exhaustion on the success path: {records:?}"
    );
    reset_all!();
}

/// E04(ii)：`usize::MAX` 的 Iter 预算在首轮 Break → 正例（MAX 不被排除，无额外轮次）。
#[test]
fn e04_iter_max_budget_first_round_break_is_legal() {
    reset_all!();
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, initial: Ref<u32>| {
            let out_refs = flow.iter_output(initial, usize::MAX, |round| {
                let current = round.current::<u32>();
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            flow.output_with(out_refs);
        },
        11u32,
    ))
    .expect("usize::MAX is a legal iter budget; a first-round break delivers the current");
    assert_eq!(delivered, 11, "the entry current is delivered unchanged");
    assert_eq!(calls(), 1, "the round body runs exactly once");

    let records = rounds();
    let iter_ats: Vec<&str> = records
        .iter()
        .filter(|record| record.kind == "iter")
        .map(|record| record.at)
        .collect();
    assert_eq!(
        iter_ats,
        ["controller-created", "round-created", "round-caught"],
        "exactly one round, then the break: {records:?}"
    );
    assert!(
        !records.iter().any(|record| record.at == "budget-exhausted"),
        "MAX does not force extra rounds: {records:?}"
    );
    reset_all!();
}
