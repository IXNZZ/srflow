//! V22-T02 重复执行与取消证据（B10～B12）。
//!
//! 沿**现有** Each／Retry 路径验证 Factory 的重复 Body 语义：每项/每次独立构造、前轮清理
//! 先于下一轮、Factory 输出随真实 Scope 交接与清理、取消时借用 Future 先于其数据析构。
//! 本文件不验收控制器新门面（那是 T04～T06），也不新增执行器或观察框架。
//!
//! 证据面：`crate::core::test_support::take_consume_group()` 的逐槽 DataId/owner、
//! 业务 `Drop` 事件序列（本文件线程局部事件日志）与正式 `Runtime::execute` 真实执行。

use super::data::Data;
use super::error::{BodyError, RunError};
use super::observe;
use super::runtime::Runtime;
use super::shape::{Query, Ref};
use crate::core::identity::{DataId, ScopeId};
use crate::core::orchestrator::ScopeRole;
use crate::core::scope::{ScopeState, TargetSnapshot};
use crate::core::test_support::{
    self, ConsumeGroupSnapshotPhase, ExportSnapshotPhase, RootSnapshotPhase,
};
use futures::executor::block_on;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

// ---------------------------------------------------------------- 事件日志与业务 Data

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn push_event(event: impl Into<String>) {
    EVENTS.with(|log| log.borrow_mut().push(event.into()));
}

fn take_events() -> Vec<String> {
    EVENTS.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

/// Factory 产出的 owned Data：Drop 记入事件日志。
#[derive(Debug, PartialEq, Eq)]
struct Item(u32);
impl Data for Item {}

impl Drop for Item {
    fn drop(&mut self) {
        push_event(format!("drop:item:{}", self.0));
    }
}

/// 重试判定输入：前 `limit` 次请求 Retry，之后成功。
async fn retry_until(query: Query<&Item>) -> Result<(), BodyError> {
    let _ = query.get();
    let call = RETRY_JUDGE_CALLS.fetch_add(1, Ordering::SeqCst);
    push_event(format!("retry:{}", call + 1));
    if call < 2 {
        Err(BodyError::retry(AttemptRejected))
    } else {
        Ok(())
    }
}

static RETRY_JUDGE_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Retry 的动态原因载体（合法业务错误）。
#[derive(Debug)]
struct AttemptRejected;

impl std::fmt::Display for AttemptRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("attempt rejected")
    }
}

impl std::error::Error for AttemptRejected {}

/// 业务失败（非 Retry）：验证 Failure 不会被自动重试。
async fn always_fail(query: Query<&Item>) -> Result<(), BodyError> {
    let _ = query.get();
    Err(BodyError::fail(AttemptRejected))
}

/// 零输入失败 Node：`Query<()>`，用于构造“其后步骤被跳过”的顺序。
async fn always_fail_zero(_query: Query<()>) -> Result<(), BodyError> {
    Err(BodyError::fail(AttemptRejected))
}

/// 真正挂起的借用 Node：读取输入后停在自唤醒 Pending。
async fn pending_borrow(query: Query<&Item>) -> Result<(), BodyError> {
    let _held = query.get();
    PendingGate.await;
    Ok(())
}

/// 自唤醒的一次挂起；Drop 记入事件日志（取消见证）。
struct PendingGate;

impl std::future::Future for PendingGate {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for PendingGate {
    fn drop(&mut self) {
        push_event("drop:pending-gate");
    }
}

/// Choose 选择键。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Route(bool);
impl Data for Route {}

// ---------------------------------------------------------------- B10 Each 重复

#[test]
fn b10_each_runs_one_factory_per_item_with_independent_identity() {
    test_support::reset_observations();
    take_events();
    let factory_calls = Rc::new(std::cell::Cell::new(0u32));
    let calls = Rc::clone(&factory_calls);
    let definition_builds = Rc::new(RefCell::new(0usize));
    let item_body_builds = Rc::new(RefCell::new(0usize));
    let root_counter = Rc::clone(&definition_builds);
    let body_counter = Rc::clone(&item_body_builds);
    let runtime = Runtime;
    let collected: Vec<Item> = block_on(runtime.execute(
        |flow, items: Ref<Vec<u32>>| {
            *root_counter.borrow_mut() += 1;
            let out_refs = flow.each(items, move |item, sub| {
                *body_counter.borrow_mut() += 1;
                // item 由旧签名给出；Factory 每项独立构造新的 owned Data。
                let _ = item;
                let calls = Rc::clone(&calls);
                sub.provide_output(move || {
                    calls.set(calls.get() + 1);
                    Item(calls.get())
                })
            });
            flow.output_with(out_refs);
        },
        vec![1u32, 2, 3],
    ))
    .expect("Each with a per-item factory should build and run");

    assert_eq!(
        *definition_builds.borrow(),
        1,
        "definition closure builds once"
    );
    assert_eq!(
        *item_body_builds.borrow(),
        1,
        "item body builds once, runs per item"
    );
    assert_eq!(factory_calls.get(), 3, "one call per item");
    assert_eq!(
        collected.iter().map(|item| item.0).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "items aggregate in collection order"
    );

    // 逐项身份与 owner：按明确的 Consume 快照阶段核对真实字段。
    let groups = test_support::take_consume_group();
    let before: Vec<_> = groups
        .iter()
        .filter(|group| matches!(group.phase, ConsumeGroupSnapshotPhase::Before))
        .collect();
    let committed: Vec<_> = groups
        .iter()
        .filter(|group| matches!(group.phase, ConsumeGroupSnapshotPhase::AfterCommit))
        .collect();
    assert_eq!(
        before.len(),
        3,
        "one pre-consume snapshot per item: {groups:?}"
    );
    assert_eq!(
        committed.len(),
        3,
        "one committed snapshot per item: {groups:?}"
    );

    // Scope 父子关系：一个 EachScope，三个 ItemScope 且 parent 均为该 EachScope。
    let creations = test_support::boundary_creation_snapshot();
    let each_scopes: Vec<ScopeId> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(child, _, _)| child.clone())
        .collect();
    assert_eq!(
        each_scopes.len(),
        1,
        "exactly one Each scope: {creations:?}"
    );
    let item_creations: Vec<(ScopeId, ScopeId)> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(child, parent, _)| (child.clone(), parent.clone()))
        .collect();
    assert_eq!(item_creations.len(), 3, "three ItemScopes: {creations:?}");
    for (item_scope, parent) in &item_creations {
        assert_eq!(
            *parent, each_scopes[0],
            "ItemScope {item_scope:?} is a child of the Each scope"
        );
    }

    let mut ids: Vec<DataId> = Vec::new();
    for (index, group) in before.iter().enumerate() {
        let slot = group.slots.first().expect("one declared output slot");
        assert_eq!(
            group.each.clone(),
            Some(each_scopes[0].clone()),
            "snapshot records the real Each scope"
        );
        assert!(
            item_creations.iter().any(|(item, _)| *item == group.item),
            "snapshot item is one of the created ItemScopes"
        );
        assert_eq!(
            slot.owner.clone(),
            Some(group.item.clone()),
            "Factory data is owned by the current ItemScope before the move"
        );
        assert_eq!(
            slot.collector_owner.clone(),
            Some(each_scopes[0].clone()),
            "collector is owned by the Each scope"
        );
        assert_eq!(slot.collector_element, Some(std::any::type_name::<Item>()));
        assert_eq!(
            slot.collector_moves,
            Some(index),
            "collector moves accumulate one per finished item: {group:?}"
        );
        assert!(slot.alive, "Factory data is alive before the move");
        let data_id = slot
            .data_id
            .clone()
            .expect("Factory data resolves a DataId before the move");
        assert!(
            group.item_owned.contains(&data_id),
            "ItemScope owns the Factory data before the move: {group:?}"
        );
        assert!(!ids.contains(&data_id), "per-item DataIds stay independent");
        ids.push(data_id);
    }
    for (index, group) in committed.iter().enumerate() {
        let slot = group.slots.first().expect("one declared output slot");
        assert_eq!(
            slot.collector_moves,
            Some(index + 1),
            "the current element has been moved into the collector"
        );
        assert!(
            slot.owner.is_none(),
            "owner is revoked after the physical move"
        );
        assert!(
            group.item_owned.is_empty(),
            "no leftover owned data in the ItemScope after the move: {group:?}"
        );
    }
    assert_eq!(ids.len(), 3, "three independent per-item DataIds");

    // owned 交接：成功项被移入 collector；返回的 Vec 承担其后清理。
    assert!(
        take_events().is_empty(),
        "no early drops before the owned result is released"
    );
    drop(collected);
    let mut drops = take_events();
    drops.sort();
    assert_eq!(
        drops,
        vec!["drop:item:1", "drop:item:2", "drop:item:3"],
        "collected values drop exactly once as owned data"
    );
}

#[test]
fn b10_empty_collection_still_builds_the_body_without_factory_calls() {
    test_support::reset_observations();
    take_events();
    let factory_calls = Rc::new(std::cell::Cell::new(0u32));
    let calls = Rc::clone(&factory_calls);
    let item_body_builds = Rc::new(RefCell::new(0usize));
    let body_counter = Rc::clone(&item_body_builds);
    let runtime = Runtime;
    let collected: Vec<Item> = block_on(runtime.execute(
        |flow, items: Ref<Vec<u32>>| {
            let out_refs = flow.each(items, move |_item, sub| {
                *body_counter.borrow_mut() += 1;
                let calls = Rc::clone(&calls);
                sub.provide_output(move || {
                    calls.set(calls.get() + 1);
                    Item(0)
                })
            });
            flow.output_with(out_refs);
        },
        Vec::<u32>::new(),
    ))
    .expect("empty collection still builds and validates the item body");
    assert!(collected.is_empty());
    assert_eq!(
        *item_body_builds.borrow(),
        1,
        "item body is built exactly once even for an empty collection"
    );
    assert_eq!(factory_calls.get(), 0, "no factory call without an item");
    assert!(take_events().is_empty());
}

// ---------------------------------------------------------------- B11 Retry 重复

#[test]
fn b11_retry_rebuilds_factory_per_attempt_and_cleans_before_next() {
    test_support::reset_observations();
    take_events();
    RETRY_JUDGE_CALLS.store(0, Ordering::SeqCst);
    let attempt_factory_calls = AtomicUsize::new(0);
    let closed_at_factory: Rc<RefCell<Vec<Vec<ScopeId>>>> = Rc::new(RefCell::new(Vec::new()));
    let closed_capture = Rc::clone(&closed_at_factory);
    let runtime = Runtime;
    let delivered: Item = block_on(runtime.execute(
        |flow, _input| {
            let out_refs = flow.retry(3, |attempt| {
                let closed_capture = Rc::clone(&closed_capture);
                let produced = attempt.provide_output(move || {
                    let call = attempt_factory_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    push_event(format!("factory:{call}"));
                    // 记录本次 Factory 执行时已经关闭的 Scope 集合。
                    closed_capture
                        .borrow_mut()
                        .push(test_support::closed_scope_snapshot());
                    Item(call as u32)
                });
                attempt.then_with_output(&retry_until, produced);
                produced
            });
            flow.output_with(out_refs);
        },
        (),
    ))
    .expect("retry should deliver the successful attempt result");

    // 失败轮先清理（含其 owned Data 的 Drop），再执行下一轮 Factory。
    assert_eq!(
        take_events(),
        vec![
            "factory:1",
            "retry:1",
            "drop:item:1",
            "factory:2",
            "retry:2",
            "drop:item:2",
            "factory:3",
            "retry:3",
        ],
        "each attempt builds its own value and cleans the failed round first"
    );
    // 三个独立 Attempt（Round）与共享控制器：读取真实 ControllerRoundRecord。
    let rounds = observe::take_controller_rounds();
    let retry_rounds: Vec<_> = rounds
        .iter()
        .filter(|record| record.kind == "retry")
        .collect();
    assert_eq!(
        retry_rounds
            .iter()
            .filter(|record| record.at == "controller-created")
            .count(),
        1,
        "one retry controller: {retry_rounds:?}"
    );
    let created: Vec<_> = retry_rounds
        .iter()
        .filter(|record| record.at == "round-created")
        .collect();
    assert_eq!(
        created
            .iter()
            .map(|record| record.index)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "three attempts in order: {retry_rounds:?}"
    );
    let mut round_scopes: Vec<ScopeId> = Vec::new();
    for record in &created {
        let scope = record.round.clone().expect("round scope is recorded");
        assert!(
            !round_scopes.contains(&scope),
            "each attempt runs in its own Round scope: {round_scopes:?}"
        );
        round_scopes.push(scope);
    }
    assert!(
        retry_rounds
            .iter()
            .any(|record| record.at == "round-normal"),
        "final attempt completed normally: {retry_rounds:?}"
    );

    // 前一轮 Round 已关闭，下一轮 Factory 才执行（工厂回调内捕获的关闭集合）。
    let seen = closed_at_factory.borrow();
    assert_eq!(seen.len(), 3, "factory ran once per attempt");
    assert!(
        !seen[0].contains(&round_scopes[0]),
        "first factory runs before any round closes: {seen:?}"
    );
    assert!(
        seen[1].contains(&round_scopes[0]),
        "previous round closed before the next factory: {seen:?}"
    );
    assert!(
        seen[2].contains(&round_scopes[1]),
        "previous round closed before the next factory: {seen:?}"
    );
    let closed_after_run = test_support::closed_scope_snapshot();
    for scope in &round_scopes {
        assert!(
            closed_after_run.contains(scope),
            "round scope {scope:?} is closed by the end: {closed_after_run:?}"
        );
    }
    drop(seen);

    // 成功输出的正常交接：两跳交付 + Root 侧 DataId 被整组提取。
    let deliveries = observe::take_controller_deliveries();
    let retry_hops: Vec<&'static str> = deliveries
        .iter()
        .filter(|record| record.kind == "retry")
        .map(|record| record.hop)
        .collect();
    assert!(
        retry_hops.contains(&"round-to-controller"),
        "round → controller delivery recorded: {retry_hops:?}"
    );
    assert!(
        retry_hops.contains(&"controller-to-caller"),
        "controller → caller delivery recorded: {retry_hops:?}"
    );
    let upward = deliveries
        .iter()
        .find(|record| record.kind == "retry" && record.hop == "round-to-controller")
        .expect("successful round delivery");
    assert_eq!(
        upward.from, round_scopes[2],
        "successful attempt delivers from its own round"
    );
    assert!(
        !upward.pairs.is_empty(),
        "delivery carries real slot pairs: {upward:?}"
    );
    let snapshots = test_support::take_root_snapshots();
    let committed = snapshots
        .iter()
        .find(|snapshot| matches!(snapshot.phase, RootSnapshotPhase::AfterCommit))
        .expect("root commit snapshot");
    assert_eq!(
        committed.root_refs.len(),
        1,
        "one declared Root output position: {committed:?}"
    );
    assert_eq!(
        committed.next_data_id,
        Some(3),
        "each attempt allocated its own Data (failed rounds cleaned, ids not reused)"
    );
    let after_close = snapshots
        .iter()
        .find(|snapshot| matches!(snapshot.phase, RootSnapshotPhase::AfterClose))
        .expect("root close snapshot");
    assert_eq!(
        after_close.taken_alive,
        vec![false],
        "extracted value left the container"
    );
    assert_eq!(
        after_close.taken_owned_by,
        vec![None],
        "caller now owns the delivered value"
    );

    // 逐轮 DataId 归属：前两轮「清理前」快照（Round 角色）中，Factory Data 存活且 owner 是该轮 Scope。
    let controller_snapshots = test_support::take_controller_snapshots();
    let cleanup_rounds: Vec<&_> = controller_snapshots
        .iter()
        .filter(|snapshot| snapshot.phase == "retry-cleanup-before" && snapshot.role == "round")
        .collect();
    assert_eq!(
        cleanup_rounds.len(),
        2,
        "two failed rounds are observed before cleanup: {controller_snapshots:?}"
    );
    let mut failed_ids: Vec<DataId> = Vec::new();
    for (index, snapshot) in cleanup_rounds.iter().enumerate() {
        assert_eq!(
            snapshot.scope, round_scopes[index],
            "cleanup snapshot belongs to the failed round"
        );
        assert_eq!(
            snapshot.owned.len(),
            1,
            "the failed round owns exactly its Factory value: {snapshot:?}"
        );
        let data_id = snapshot.owned[0].clone();
        let entry = snapshot
            .owned_alive
            .iter()
            .find(|(candidate, _, _)| *candidate == data_id)
            .expect("owned entry records liveness and owner");
        assert!(entry.1, "Factory data is alive before the round cleanup");
        assert_eq!(
            entry.2.clone(),
            Some(snapshot.scope.clone()),
            "owner of the Factory data is the round scope that produced it"
        );
        assert!(
            !failed_ids.contains(&data_id),
            "failed rounds allocate distinct Data identities"
        );
        failed_ids.push(data_id);
    }
    // 成功轮：Export 前来源快照的 DataId 必须就是最终交付的同一实例。
    let exports = test_support::take_export_pre_cleanup();
    let successful_export = exports
        .iter()
        .find(|snapshot| {
            snapshot.phase == ExportSnapshotPhase::Before
                && snapshot.child.as_ref() == Some(&round_scopes[2])
        })
        .expect("successful round export before-snapshot");
    assert_eq!(successful_export.observation_error, None);
    let child_owned = successful_export.child_owned.clone().unwrap_or_default();
    assert_eq!(
        child_owned.len(),
        1,
        "successful round owns exactly its Factory value: {successful_export:?}"
    );
    let delivered_id = child_owned[0].clone();
    assert!(
        !failed_ids.contains(&delivered_id),
        "the delivered Data is a new instance, not a cleaned failed one"
    );
    assert!(
        committed
            .root_refs
            .iter()
            .any(|(_, target)| matches!(target, TargetSnapshot::Data(id) if *id == delivered_id)),
        "delivered Root output resolves to the same DataId owned by the successful round: {committed:?}"
    );
    assert!(
        !exports
            .iter()
            .any(|snapshot| snapshot.phase == ExportSnapshotPhase::AfterReject),
        "no export rejection is observed: {exports:?}"
    );

    assert_eq!(
        delivered.0, 3,
        "successful attempt output is delivered as owned"
    );
    drop(delivered);
    assert_eq!(take_events(), vec!["drop:item:3"]);
}

#[test]
fn b11_plain_failure_is_never_retried() {
    take_events();
    let factory_calls = AtomicUsize::new(0);
    let runtime = Runtime;
    let error = block_on(runtime.execute::<_, (), _>(
        |flow, _input| {
            let () = flow.retry(3, |attempt| {
                let produced = attempt.provide_output(|| {
                    factory_calls.fetch_add(1, Ordering::SeqCst);
                    push_event("factory");
                    Item(1)
                });
                attempt.then_with_output(&always_fail, produced);
            });
            flow.output_with(());
        },
        (),
    ))
    .expect_err("a plain Failure must not be retried");
    assert!(
        matches!(error, RunError::Body(_)),
        "failure keeps its category: {error:?}"
    );
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1, "attempt runs once");
    assert_eq!(
        take_events(),
        vec!["factory", "drop:item:1"],
        "failed attempt cleans its own value"
    );
}

// ---------------------------------------------------------------- B12 Scope、跳过与取消

#[test]
fn b12_child_factory_registers_in_current_scope_and_exports_owned() {
    test_support::reset_observations();
    take_events();
    let runtime = Runtime;
    let delivered: Item = block_on(runtime.execute(
        |flow, _input| {
            // chain child 内的 Factory：输出经 Export 交给 parent，再作为 Root owned 交付。
            let exported: Ref<Item> = flow.chain(|sub| sub.provide_output(|| Item(9)));
            let out_refs = exported;
            flow.output_with(out_refs);
        },
        (),
    ))
    .expect("child factory output should export and deliver");
    assert!(
        take_events().is_empty(),
        "exported value is owned by the caller now"
    );
    assert_eq!(delivered.0, 9);

    // 创建记录：child 是 caller 的直接 child（ScopeRole::Flow）。
    let creations = test_support::boundary_creation_snapshot();
    let flow_children: Vec<(ScopeId, ScopeId)> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Flow)
        .map(|(child, parent, _)| (child.clone(), parent.clone()))
        .collect();
    assert_eq!(flow_children.len(), 1, "one chain child: {creations:?}");
    let (child_scope, caller_scope) = flow_children[0].clone();

    // Export 交接：committed 快照把该 DataId 记为 caller 拥有；提交前 caller 尚无该绑定。
    let exports = observe::take_export_snapshots();
    let before = exports
        .iter()
        .find(|record| record.phase == "before")
        .expect("export pre-snapshot");
    let committed = exports
        .iter()
        .find(|record| record.phase == "committed")
        .expect("export commit snapshot");
    assert_eq!(
        before.scope, caller_scope,
        "pre-snapshot observes the real caller scope"
    );
    assert_eq!(committed.scope, caller_scope);
    let exported_id = committed
        .owned_alive
        .iter()
        .find(|(_, alive, owner)| *alive && owner.as_ref() == Some(&caller_scope))
        .map(|(id, _, _)| id.clone())
        .expect("committed export records the caller-owned DataId");
    assert!(
        !before.owned.contains(&exported_id),
        "caller does not own the value before the export commits"
    );
    assert!(committed.owned.contains(&exported_id));
    assert!(
        test_support::closed_scope_snapshot().contains(&child_scope),
        "child scope is closed after the export"
    );

    // 来源责任 → 目的责任必须落到同一个 DataId 实例。
    let pre_cleanup = test_support::take_export_pre_cleanup();
    let handover = pre_cleanup
        .iter()
        .find(|snapshot| {
            snapshot.phase == ExportSnapshotPhase::Before
                && snapshot.child.as_ref() == Some(&child_scope)
                && snapshot.caller.as_ref() == Some(&caller_scope)
        })
        .expect("export before-snapshot for this handover");
    assert_eq!(handover.observation_error, None);
    let child_owned = handover.child_owned.clone().unwrap_or_default();
    assert_eq!(
        child_owned.len(),
        1,
        "child owns the Factory value before the export: {handover:?}"
    );
    assert_eq!(
        child_owned[0], exported_id,
        "source (child) and destination (caller) reference the same DataId instance"
    );
    assert!(
        !handover
            .caller_owned
            .clone()
            .unwrap_or_default()
            .contains(&exported_id),
        "caller does not own the value before the handover"
    );
    assert_eq!(
        handover.slots.len(),
        1,
        "one declared slot maps the child output to the caller position: {handover:?}"
    );
}

#[test]
fn b12_unselected_branch_never_runs_its_factory() {
    take_events();
    let selected_calls = AtomicUsize::new(0);
    let unselected_calls = AtomicUsize::new(0);
    let runtime = Runtime;
    let selected: Item = block_on(runtime.execute(
        |flow, selector: Ref<Route>| {
            let out_refs = flow.choose(selector, |choice| {
                choice.case(Route(true), |sub| {
                    sub.provide_output(|| {
                        selected_calls.fetch_add(1, Ordering::SeqCst);
                        Item(1)
                    })
                });
                // 未选分支的 Factory 只是登记能力，运行期不执行。
                choice.case(Route(false), |sub| {
                    sub.provide_output(|| {
                        unselected_calls.fetch_add(1, Ordering::SeqCst);
                        Item(2)
                    })
                })
            });
            flow.output_with(out_refs);
        },
        Route(true),
    ))
    .expect("selected branch should run");
    assert_eq!(selected.0, 1);
    assert_eq!(
        selected_calls.load(Ordering::SeqCst),
        1,
        "selected branch runs its factory"
    );
    assert_eq!(
        unselected_calls.load(Ordering::SeqCst),
        0,
        "unselected branch factory is never called"
    );
}

#[test]
fn b12_step_before_the_factory_failure_skips_it() {
    take_events();
    let factory_calls = AtomicUsize::new(0);
    let runtime = Runtime;
    let error = block_on(runtime.execute::<_, (), _>(
        |flow, _input| {
            // 顺序在前的一个零输入 Node 直接 Failure：其后的 Factory 步不执行。
            let _: () = flow.then_output(&always_fail_zero);
            flow.provide(|| {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Item(6)
            });

            flow.output_with(());
        },
        (),
    ))
    .expect_err("earlier failure terminates the path");
    let _ = error;
    assert_eq!(
        factory_calls.load(Ordering::SeqCst),
        0,
        "factory after a failing step is never called"
    );
    assert!(
        take_events().is_empty(),
        "no factory-owned data exists to clean"
    );
}

#[test]
fn b12_factory_value_is_cleaned_after_a_failure() {
    take_events();
    let runtime = Runtime;
    let error = block_on(runtime.execute::<_, (), _>(
        |flow, _input| {
            let produced = flow.provide_output(|| {
                push_event("factory");
                Item(5)
            });
            flow.then_with_output(&always_fail, produced);

            flow.output_with(());
        },
        (),
    ))
    .expect_err("failing step terminates the body");
    let _ = error;
    assert_eq!(
        take_events(),
        vec!["factory", "drop:item:5"],
        "factory-owned data is cleaned when the body fails"
    );
    // 清理关联真实 Scope 关闭：失败退出后 Root 已 Closed 且不遗留 owned。
    let snapshots = test_support::take_root_snapshots();
    let cleanup = snapshots
        .iter()
        .find(|snapshot| matches!(snapshot.phase, RootSnapshotPhase::AfterFailureCleanup))
        .expect("failure cleanup snapshot");
    assert!(
        cleanup.root_owned.is_empty(),
        "no owned data survives the failure cleanup: {cleanup:?}"
    );
    assert_eq!(
        cleanup.root_state,
        ScopeState::Closed,
        "root scope closed after the failure"
    );
}

#[test]
fn b12_pending_cancel_drops_the_borrowing_future_before_its_data() {
    test_support::reset_observations();
    take_events();
    let runtime = Runtime;
    let mut future = Box::pin(runtime.execute::<_, (), _>(
        |flow, _input| {
            let produced = flow.provide_output(|| {
                push_event("factory");
                Item(4)
            });
            flow.then_with_output(&pending_borrow, produced);

            flow.output_with(());
        },
        (),
    ));
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(
        matches!(future.as_mut().poll(&mut context), Poll::Pending),
        "borrowing node stays truly pending"
    );
    drop(future);
    assert_eq!(
        take_events(),
        vec!["factory", "drop:pending-gate", "drop:item:4"],
        "borrowing future is dropped before the data it borrows is cleaned"
    );
    assert!(
        !test_support::closed_scope_snapshot().is_empty(),
        "cancelled execution closed its scopes"
    );
}
