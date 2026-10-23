//! WE-T05 Each 成组 Consume／Collector／Export 定向证据（P09～P28）。
//!
//! 每条样本对应任务书 §8 的一行，先经公开 `Runtime::execute` 走真实主链；需要精确目的
//! 位置诊断或许可门禁时，用 crate 内真实路径补足——`build_definition`＋`run_body`（同一
//! `StepEntry::execute → boundary::invoke_each`）或真实 frame 链上的成组收口入口
//! `InvocationGuard::consume_group_in_item_boundary`（`invoke_each` 逐项收口调用的是同一
//! 个入口）。不新增伪公开入口，不复制 Probe。
//!
//! 逐行对应（任务书 §8）：
//! - P09 `p09_single_output_aggregates_in_index_order`
//! - P10 `p10_heterogeneous_and_same_type_slots_stay_independent`、
//!   `p10_sixteen_slot_each_keeps_every_slot_independent`
//! - P11 `p11_unit_each_runs_the_body_without_collectors_or_fake_slots`
//! - P13 `p13_engine_item_output_is_rejected_by_the_real_group_preflight`、
//!   `p13_engine_ancestor_alias_output_is_rejected_as_illegal_owner`、
//!   `p13_core_item_borrow_and_ancestor_alias_are_rejected_with_exact_targets`
//! - P14 `p14_child_chain_choose_and_retry_results_are_collectible`
//! - P15 `p15_duplicate_owned_alias_group_is_rejected_before_any_move`、
//!   `p15_same_ref_in_two_slots_is_rejected_before_freeze`
//! - P16 `p16_repeated_reads_and_two_each_steps_over_one_collection_are_legal`
//! - P17 `p17_item_cap_survives_the_iter_state_import_path`、
//!   `p17_core_cap_escape_and_closed_item_resolve_are_rejected`
//! - P18 `p18_consume_invalidates_the_old_identity_and_never_reuses_it`
//! - P19 `p19_group_permit_does_not_relax_the_general_gates`
//! - P20 `p20_live_descendant_and_cleanup_premise_are_checked_before_commit`
//! - P21 `p21_later_slot_reject_leaves_the_first_slot_unmoved`
//! - P22 `p22_duplicate_targets_and_element_types_are_rejected`
//! - P23 `p23_group_commit_moves_every_slot_and_closes_the_item_once`
//! - P24 `p24_unfinished_collector_is_not_business_data`
//! - P25 `p25_finished_vec_is_an_each_owned_data_and_exports_correctly`
//! - P26 `p26_post_finish_reject_leaves_no_partial_caller_success`
//! - P27 `p27_late_destination_reject_keeps_the_caller_group_unchanged`
//! - P28 `p28_direct_duplicate_root_ref_is_rejected_by_the_definition`、
//!   `p28_legal_alias_duplicate_root_target_takes_nothing`

#![allow(
    clippy::unused_unit,
    clippy::let_unit_value,
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::err_expect
)]

use crate::core::context::{ExecutionContext, InvocationKind, ItemGroupPermit};
use crate::core::engine::boundary::run_body;
use crate::core::engine::data::Data;
use crate::core::engine::definition::{PositionSource, build_definition};
use crate::core::engine::error::{
    BodyError, BuildErrorDetail, ExecFailure, FrameworkDiagnostic, RunError,
};
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::BodyId;
use crate::core::engine::observe::{self, ControllerRoundRecord};
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use crate::core::identity::{CollectorId, DataId, ScopeId};
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::{RefId, RefIdAllocator, RefIdSource};
use crate::core::runtime::RootExecution;
use crate::core::scope::{
    AliasSlot, CollectionRoot, ConsumeGroupOutcome, ExportSlot, ImportSlot, ItemAccess, RefTarget,
    ScopeState, TargetSnapshot,
};
use crate::core::test_support::{
    self, ConsumeGroupFault, ConsumeGroupSnapshot, ConsumeGroupSnapshotPhase,
    closed_scope_snapshot, install_consume_group_fault, take_consume_group, take_log_snapshot,
};
use futures::executor::block_on;
use std::any::{TypeId, type_name};
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::rc::Rc;

// ================================================================ 业务类型与事件记录

thread_local! {
    /// 单线程执行下的事件序列：业务调用顺序与 Drop 顺序。
    static LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn log(event: impl Into<String>) {
    LOG.with(|entries| entries.borrow_mut().push(event.into()));
}

fn take_log() -> Vec<String> {
    LOG.with(|entries| std::mem::take(&mut *entries.borrow_mut()))
}

/// 每样本起点：复位引擎观察通道、Core 观察通道与事件序列。
fn reset() {
    observe::reset();
    test_support::reset_observations();
    take_log();
}

#[derive(Debug, PartialEq)]
struct Item(u32);
impl Data for Item {}
impl Drop for Item {
    fn drop(&mut self) {
        log(format!("drop:item:{}", self.0));
    }
}

#[derive(Debug, PartialEq)]
struct Out(u32);
impl Data for Out {}
impl Drop for Out {
    fn drop(&mut self) {
        log(format!("drop:out:{}", self.0));
    }
}

#[derive(Debug, PartialEq)]
struct Tag(u32);
impl Data for Tag {}
impl Drop for Tag {
    fn drop(&mut self) {
        log(format!("drop:tag:{}", self.0));
    }
}

/// 计数型业务值（Core 样本用；只累计析构次数）。
#[derive(Debug)]
struct Tracked {
    value: u32,
    drops: Rc<Cell<usize>>,
}
impl Data for Tracked {}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

// ---------------------------------------------------------------- Node 业务函数

async fn score(query: Query<&Item>) -> Result<Out, BodyError> {
    let value = query.get().0;
    log(format!("node:score:{value}"));
    Ok(Out(value * 2))
}

async fn tagger(query: Query<&Item>) -> Result<Tag, BodyError> {
    let value = query.get().0;
    log(format!("node:tagger:{value}"));
    Ok(Tag(value + 10))
}

async fn tens(query: Query<&Item>) -> Result<Out, BodyError> {
    let value = query.get().0;
    log(format!("node:tens:{value}"));
    Ok(Out(value * 10))
}

async fn rescore(query: Query<&Out>) -> Result<Out, BodyError> {
    let value = query.get().0;
    log(format!("node:rescore:{value}"));
    Ok(Out(value * 2))
}

async fn audit(query: Query<&Item>) -> Result<(), BodyError> {
    let value = query.get().0;
    log(format!("node:audit:{value}"));
    Ok(())
}

/// 同一个 item 借用出现两次（重复只读输入）仍合法。
async fn pair(query: Query<(&Item, &Item)>) -> Result<Out, BodyError> {
    let (first, second) = query.get();
    log(format!("node:pair:{}:{}", first.0, second.0));
    Ok(Out(first.0 + second.0))
}

/// 同时借用 item 与原集合（Each 输入与 Body 祖先捕获指向同一逻辑位置）。
async fn collection_size(query: Query<(&Item, &Vec<Item>)>) -> Result<Out, BodyError> {
    let (item, collection) = query.get();
    log(format!("node:collection:{}:{}", item.0, collection.len()));
    Ok(Out(item.0 * 100 + collection.len() as u32))
}

/// 把交付的 current 与项内 item 借用混合成新的 owned 结果（Iter StateImport 路径）。
async fn blend(query: Query<(&Item, &Out)>) -> Result<Out, BodyError> {
    let (item, current) = query.get();
    log(format!("node:blend:{}:{}", item.0, current.0));
    Ok(Out(item.0 * 2 + current.0))
}

/// 立即请求 IterBreak：本轮不产生输出，由匹配的 Iter 边界交付进入本轮时的 current。
async fn break_now(query: Query<&Out>) -> Result<Out, BodyError> {
    log(format!("node:break:{}", query.get().0));
    Err(BodyError::iter_break())
}

/// 调用者自有的祖先结果（Caller-owned），供 P13 祖先 alias 反例使用。
async fn singleton(query: Query<&Vec<Item>>) -> Result<Out, BodyError> {
    let total: u32 = query.get().iter().map(|item| item.0).sum();
    log(format!("node:singleton:{total}"));
    Ok(Out(total))
}

/// 常量因子节点：16 槽样本的逐槽独立结果。
macro_rules! const_nodes {
    ($($name:ident = $factor:expr;)+) => {
        $(
            async fn $name(query: Query<&Item>) -> Result<Out, BodyError> {
                Ok(Out(query.get().0 * $factor))
            }
        )+
    };
}

const_nodes!(
    times01 = 1;
    times02 = 2;
    times03 = 3;
    times04 = 4;
    times05 = 5;
    times06 = 6;
    times07 = 7;
    times08 = 8;
    times09 = 9;
    times10 = 10;
    times11 = 11;
    times12 = 12;
    times13 = 13;
    times14 = 14;
    times15 = 15;
    times16 = 16;
);

// ---------------------------------------------------------------- 观察与夹具辅助

/// Each 控制器记录序列（`(at, index)`，按发生顺序）。
fn each_events(rounds: &[ControllerRoundRecord]) -> Vec<(&'static str, usize)> {
    rounds
        .iter()
        .filter(|record| record.kind == "each")
        .map(|record| (record.at, record.index))
        .collect()
}

/// 控制器记录中真实建立的 ItemScope（按建立顺序）。
fn item_scopes(rounds: &[ControllerRoundRecord]) -> Vec<ScopeId> {
    rounds
        .iter()
        .filter(|record| record.at == "item-created")
        .map(|record| {
            record
                .round
                .clone()
                .expect("item-created always carries the item scope")
        })
        .collect()
}

/// 该 Scope 在真实关闭记录中恰好出现一次。
fn closed_once(scope: &ScopeId) {
    assert_eq!(
        closed_scope_snapshot()
            .iter()
            .filter(|closed| *closed == scope)
            .count(),
        1,
        "{scope} must be recorded as closed exactly once"
    );
}

fn group_snapshots() -> Vec<ConsumeGroupSnapshot> {
    take_consume_group()
}

fn in_phase(
    snapshots: &[ConsumeGroupSnapshot],
    phase: ConsumeGroupSnapshotPhase,
) -> Vec<&ConsumeGroupSnapshot> {
    snapshots
        .iter()
        .filter(|snapshot| snapshot.phase == phase)
        .collect()
}

fn assert_no_observation_error(snapshots: &[ConsumeGroupSnapshot]) {
    for snapshot in snapshots {
        assert_eq!(
            snapshot.observation_error, None,
            "every group snapshot must be exact: {snapshot:?}"
        );
    }
}

/// 真实 frame 链上的成组收口许可（与 `invoke_each` 装配的许可同形）。
fn group_permit(
    item: &ScopeId,
    each: &ScopeId,
    group: &[RefId],
    collectors: &[CollectorId],
) -> ItemGroupPermit {
    ItemGroupPermit {
        item: item.clone(),
        each: each.clone(),
        group: group.to_vec(),
        collectors: collectors.to_vec(),
    }
}

fn owned_of(ctx: &ExecutionContext, scope: &ScopeId) -> Vec<DataId> {
    ctx.snapshot_targets_probe(scope)
        .map(|(_, owned)| owned)
        .unwrap_or_default()
}

fn refs_of(ctx: &ExecutionContext, scope: &ScopeId) -> Vec<(RefId, TargetSnapshot)> {
    ctx.snapshot_targets_probe(scope)
        .map(|(refs, _)| refs)
        .unwrap_or_default()
}

fn moves(ctx: &ExecutionContext, collector: &CollectorId) -> usize {
    ctx.collector_moves_probe(collector).unwrap_or_default()
}

// ================================================================ P09

#[test]
fn p09_single_output_aggregates_in_index_order() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<Vec<Out>, RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: Ref<Vec<Out>> = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let out_refs = sub.then_with_output(score, item);
                sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    ));
    let collected = outcome.expect("single-output each over three items succeeds");
    assert_eq!(
        collected.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4, 6],
        "aggregated values follow the collection order"
    );
    let events = take_log();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("node:"))
            .cloned()
            .collect::<Vec<_>>(),
        vec!["node:score:1", "node:score:2", "node:score:3"],
        "the body runs once per index, in index order"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("drop:item:"))
            .cloned()
            .collect::<Vec<_>>(),
        vec!["drop:item:1", "drop:item:2", "drop:item:3"],
        "the caller releases the collection, once per element"
    );

    // 逐 index：创建 → 消费 → 下一个创建（消费后才创建下一 Item）。
    let rounds = observe::take_controller_rounds();
    assert_eq!(
        each_events(&rounds),
        vec![
            ("scope-created", 0),
            ("item-created", 0),
            ("item-consumed", 0),
            ("item-created", 1),
            ("item-consumed", 1),
            ("item-created", 2),
            ("item-consumed", 2),
            ("exported", 0),
        ],
        "the next Item is created only after the previous one was consumed"
    );

    // 集合由 caller 释放（Each 不移动元素）已在上面核对；聚合结果逐值析构一次。
    drop(collected);
    assert_eq!(
        take_log(),
        vec!["drop:out:2", "drop:out:4", "drop:out:6"],
        "aggregated values drop once each, in slot order"
    );
}

// ================================================================ P10

#[test]
fn p10_heterogeneous_and_same_type_slots_stay_independent() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Vec<Out>, Vec<Tag>, Vec<Out>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: (Ref<Vec<Out>>, Ref<Vec<Tag>>, Ref<Vec<Out>>) =
                flow.each_output(items, |sub| {
                    let item = sub.ref_of::<Item>();

                    let doubled = sub.then_with_output(score, item);
                    let labelled = sub.then_with_output(tagger, item);
                    let scaled = sub.then_with_output(tens, item);
                    let out_refs = (doubled, labelled, scaled);
                    sub.output_with(out_refs);
                });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));
    let (doubled, labelled, scaled) = outcome.expect("three-slot each succeeds");
    assert_eq!(
        doubled.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4]
    );
    assert_eq!(
        labelled.iter().map(|tag| tag.0).collect::<Vec<_>>(),
        vec![11, 12]
    );
    assert_eq!(
        scaled.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![10, 20]
    );

    // 同类型不同逻辑位置不合并：slot0 与 slot2 都是 Vec<Out>，各自独立的最终 DataId。
    let takes = take_log_snapshot();
    assert_eq!(takes.len(), 3, "one aggregated Vec per slot: {takes:?}");
    assert_ne!(takes[0], takes[1]);
    assert_ne!(takes[1], takes[2]);
    assert_ne!(takes[0], takes[2]);
    assert!(
        takes[2].seq() > takes[0].seq(),
        "the later same-type slot owns a larger identity"
    );

    // 调用者侧整组绑定：三个 EachOutput 位置都指向各自聚合值。
    let exports = observe::take_export_snapshots();
    let before = exports
        .iter()
        .find(|snapshot| snapshot.phase == "before")
        .expect("export snapshot before the handover");
    let committed = exports
        .iter()
        .find(|snapshot| snapshot.phase == "committed")
        .expect("export snapshot after the handover");
    assert_eq!(
        before.refs.len(),
        1,
        "before the handover the caller only binds its own collection"
    );
    assert_eq!(
        committed.refs.len(),
        4,
        "collection + three caller output slots: {:?}",
        committed.refs
    );
    assert_eq!(committed.owned.len(), 4);
}

#[test]
fn p10_sixteen_slot_each_keeps_every_slot_independent() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
        Vec<Out>,
    ), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: (
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
                Ref<Vec<Out>>,
            ) = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let out_refs = (
                    sub.then_with_output(times01, item),
                    sub.then_with_output(times02, item),
                    sub.then_with_output(times03, item),
                    sub.then_with_output(times04, item),
                    sub.then_with_output(times05, item),
                    sub.then_with_output(times06, item),
                    sub.then_with_output(times07, item),
                    sub.then_with_output(times08, item),
                    sub.then_with_output(times09, item),
                    sub.then_with_output(times10, item),
                    sub.then_with_output(times11, item),
                    sub.then_with_output(times12, item),
                    sub.then_with_output(times13, item),
                    sub.then_with_output(times14, item),
                    sub.then_with_output(times15, item),
                    sub.then_with_output(times16, item),
                );
                sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));
    let slots = outcome.expect("16-slot each succeeds");
    macro_rules! check_slot {
        ($values:expr, $factor:expr) => {
            assert_eq!(
                $values.iter().map(|out| out.0).collect::<Vec<_>>(),
                vec![$factor, $factor * 2],
                "slot with factor {} follows the collection order",
                $factor
            );
        };
    }
    check_slot!(slots.0, 1);
    check_slot!(slots.1, 2);
    check_slot!(slots.2, 3);
    check_slot!(slots.3, 4);
    check_slot!(slots.4, 5);
    check_slot!(slots.5, 6);
    check_slot!(slots.6, 7);
    check_slot!(slots.7, 8);
    check_slot!(slots.8, 9);
    check_slot!(slots.9, 10);
    check_slot!(slots.10, 11);
    check_slot!(slots.11, 12);
    check_slot!(slots.12, 13);
    check_slot!(slots.13, 14);
    check_slot!(slots.14, 15);
    check_slot!(slots.15, 16);

    let takes = take_log_snapshot();
    assert_eq!(takes.len(), 16, "one aggregated Vec per slot");
    let mut seqs: Vec<u64> = takes.iter().map(DataId::seq).collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len(), 16, "16 distinct same-type Vec identities");
}

// ================================================================ P11

#[test]
fn p11_unit_each_runs_the_body_without_collectors_or_fake_slots() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<(), RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let () = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let out_refs = sub.then_with_output(audit, item);
                sub.output_with(out_refs);
            });
            flow.output_with(());
        },
        vec![Item(1), Item(2), Item(3)],
    ));
    assert!(
        outcome.is_ok(),
        "unit each is a normal success: {outcome:?}"
    );
    assert_eq!(
        take_log()
            .iter()
            .filter(|event| event.starts_with("node:"))
            .cloned()
            .collect::<Vec<_>>(),
        vec!["node:audit:1", "node:audit:2", "node:audit:3"],
        "every item still runs its business body"
    );

    // 无 collector：unit 不进入成组 Consume，没有整组建构观察。
    assert!(
        group_snapshots().is_empty(),
        "unit each registers no collector and calls no empty consume"
    );

    // 无假槽／假最终 DataId：调用者只保留自己的 Root 输入绑定，取号前后不变。
    let exports = observe::take_export_snapshots();
    let before = exports
        .iter()
        .find(|snapshot| snapshot.phase == "before")
        .expect("unit each still records its handover points");
    let committed = exports
        .iter()
        .find(|snapshot| snapshot.phase == "committed")
        .expect("unit each completes its handover");
    assert_eq!(
        before.refs, committed.refs,
        "no caller slot is bound for unit"
    );
    assert_eq!(before.owned, committed.owned);
    assert_eq!(
        before.next_data_id, committed.next_data_id,
        "unit allocates no unit Data and no fake final DataId"
    );

    // Item 正常关闭：每个 ItemScope 恰好关闭一次。
    let rounds = observe::take_controller_rounds();
    let items_scopes = item_scopes(&rounds);
    assert_eq!(items_scopes.len(), 3);
    for scope in &items_scopes {
        closed_once(scope);
    }
}

// ================================================================ P13

#[test]
fn p13_engine_item_output_is_rejected_by_the_real_group_preflight() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<Vec<Item>, RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: Ref<Vec<Item>> = flow.each_output(items, |_sub| {
                let item = _sub.ref_of::<Item>();

                let out_refs = item;
                _sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));
    let error = outcome.err().expect("the item itself cannot be collected");
    let runtime_error = match &error {
        RunError::Runtime(failure) => failure,
        other => panic!("expected the item-level consume rejection, got {other:?}"),
    };
    assert_eq!(runtime_error.note(), "each item consume rejected");
    let source = runtime_error
        .source()
        .expect("the original Scope diagnostic is preserved");
    let diagnostic = source
        .downcast_ref::<FrameworkDiagnostic>()
        .expect("the first rejection is the framework's Scope diagnostic");
    let text = format!("{diagnostic:?}");
    assert!(
        text.contains("NonCompleteTarget"),
        "the item borrow is not complete Data: {text}"
    );
    assert_eq!(
        item_scopes(&observe::take_controller_rounds()).len(),
        1,
        "the first item already rejects; the second is never created"
    );
    // 集合元素没有被移动：两个元素各析构一次。
    let drops: Vec<String> = take_log()
        .into_iter()
        .filter(|event| event.starts_with("drop:item:"))
        .collect();
    assert_eq!(drops, vec!["drop:item:1", "drop:item:2"]);
}

#[test]
fn p13_engine_ancestor_alias_output_is_rejected_as_illegal_owner() {
    reset();
    let runtime = Runtime::new();
    let outcome: Result<Vec<Out>, RunError> = block_on(runtime.execute(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let anchor = flow.then_with_output(singleton, items);
            let out_refs: Ref<Vec<Out>> = flow.each_output(items, |_sub| {
                let _item = _sub.ref_of::<Item>();

                let out_refs = anchor;
                _sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));
    let error = outcome
        .err()
        .expect("a caller-owned alias cannot be collected");
    let runtime_error = match &error {
        RunError::Runtime(failure) => failure,
        other => panic!("expected the item-level consume rejection, got {other:?}"),
    };
    assert_eq!(runtime_error.note(), "each item consume rejected");
    let source = runtime_error
        .source()
        .expect("the original Scope diagnostic is preserved");
    let diagnostic = source
        .downcast_ref::<FrameworkDiagnostic>()
        .expect("the first rejection is the framework's Scope diagnostic");
    let text = format!("{diagnostic:?}");
    assert!(
        text.contains("IllegalOwner"),
        "the item does not own the caller-owned alias: {text}"
    );
    // 集合元素没有被移动：各析构一次（anchor 随后由 Root 释放）。
    let drops = take_log();
    assert!(
        drops.iter().any(|event| event == "drop:item:1")
            && drops.iter().any(|event| event == "drop:item:2"),
        "the collection is untouched: {drops:?}"
    );
}

/// Core 侧真实目标反例：item 自身（CollectionItem）与祖先 owned Data 都不能被消费。
#[test]
fn p13_core_item_borrow_and_ancestor_alias_are_rejected_with_exact_targets() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let collection_position = ids.allocate().unwrap();
    let item_position = ids.allocate().unwrap();
    let ancestor_root_position = ids.allocate().unwrap();
    let each_ancestor_position = ids.allocate().unwrap();
    let item_ancestor_position = ids.allocate().unwrap();

    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collection = root_guard
        .register_owned(&each, &collection_position, vec![Item(7)])
        .unwrap();
    let ancestor = root_guard
        .register_owned(&root, &ancestor_root_position, Out(9))
        .unwrap();
    root_guard
        .import_batch(
            &each,
            &root,
            &[ImportSlot::with_type(
                &ancestor_root_position,
                each_ancestor_position.clone(),
                TypeId::of::<Out>(),
                type_name::<Out>(),
            )],
        )
        .unwrap();
    let collector_item = root_guard.begin_collector::<Item>(&each).unwrap();
    let collector_ancestor = root_guard.begin_collector::<Out>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();

    // (a) item 自身：formal item 位置是 CollectionItem，不是完整 owned Data。
    let item_a = each_guard.create_child(&each).unwrap();
    each_guard
        .bind_item_input::<Item>(&item_a, &each, &collection_position, &item_position, 0)
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item_a, true)
        .unwrap();
    let permit = group_permit(
        &item_a,
        &each,
        std::slice::from_ref(&item_position),
        std::slice::from_ref(&collector_item),
    );
    match item_guard.consume_group_in_item_boundary(
        &item_a,
        std::slice::from_ref(&item_position),
        &permit,
    ) {
        ConsumeGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } => {
            assert!(
                matches!(&primary, ScopeError::NonCompleteTarget { position } if position == &item_position),
                "got {primary:?}"
            );
            assert!(cleanup_failure.is_none(), "clean item: {cleanup_failure:?}");
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    assert!(
        owned_of(&item_guard, &item_a).is_empty(),
        "zero physical move"
    );
    assert_eq!(moves(&item_guard, &collector_item), 0);
    item_guard.release_responsibility(&item_a);
    item_guard.complete();
    assert_eq!(each_guard.state(&item_a).unwrap(), ScopeState::Closed);

    // (b) 祖先 Data：item 只是 alias，实际 owner 在祖先 Scope。
    let item_b = each_guard.create_child(&each).unwrap();
    each_guard
        .import_batch(
            &item_b,
            &each,
            &[ImportSlot::with_type(
                &each_ancestor_position,
                item_ancestor_position.clone(),
                TypeId::of::<Out>(),
                type_name::<Out>(),
            )],
        )
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item_b, true)
        .unwrap();
    let permit = group_permit(
        &item_b,
        &each,
        std::slice::from_ref(&item_ancestor_position),
        std::slice::from_ref(&collector_ancestor),
    );
    match item_guard.consume_group_in_item_boundary(
        &item_b,
        std::slice::from_ref(&item_ancestor_position),
        &permit,
    ) {
        ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::IllegalOwner { id, owner, boundary }
                if id == &ancestor && owner == &root && boundary == &item_b),
            "got {primary:?}"
        ),
        other => panic!("expected rejection, got {other:?}"),
    }
    assert_eq!(moves(&item_guard, &collector_ancestor), 0);

    // 原集合／祖先 owner 不变、零物理移动。
    assert!(
        item_guard.alive_probe(&collection),
        "the collection stays alive"
    );
    assert!(owned_of(&item_guard, &each).contains(&collection));
    assert!(
        item_guard.alive_probe(&ancestor),
        "the ancestor value stays alive"
    );
    assert_eq!(item_guard.owner_probe(&ancestor).unwrap(), root);
    assert_eq!(owned_of(&item_guard, &root), vec![ancestor.clone()]);
    assert_eq!(drops.get(), 0, "nothing was moved or destroyed");

    item_guard.release_responsibility(&item_b);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
    assert_eq!(
        drops.get(),
        0,
        "the ancestor value survives the rejected group"
    );
}

// ================================================================ P14

#[test]
fn p14_child_chain_choose_and_retry_results_are_collectible() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Vec<Out>, Vec<Out>, Vec<Out>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: (Ref<Vec<Out>>, Ref<Vec<Out>>, Ref<Vec<Out>>) =
                flow.each_output(items, |sub| {
                    let item = sub.ref_of::<Item>();

                    let chained: Ref<Out> = sub.chain_output(|inner| {
                        let out_refs = inner.then_with_output(score, item);
                        inner.output_with(out_refs);
                    });
                    let chosen: Ref<Out> = sub.choose_with_output(item, |choice| {
                        choice.case(Item(1), |branch| {
                            let out_refs = branch.then_with_output(score, item);
                            branch.output_with(out_refs);
                        });
                        choice.otherwise(|branch| {
                            let out_refs = branch.then_with_output(tens, item);
                            branch.output_with(out_refs);
                        });
                    });
                    let retried: Ref<Out> = sub.retry_output(1, |attempt| {
                        let out_refs = attempt.then_with_output(score, item);
                        attempt.output_with(out_refs);
                    });
                    let out_refs = (chained, chosen, retried);
                    sub.output_with(out_refs);
                });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    ));
    let (chained, chosen, retried) = outcome.expect("child-produced owned results are collectible");
    assert_eq!(
        chained.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4, 6]
    );
    assert_eq!(
        chosen.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 20, 30],
        "the chosen branch's own result is the collected one"
    );
    assert_eq!(
        retried.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4, 6]
    );

    // 真实 owner 与移动观察：每项每槽在冻结前由 Item own，提交后旧身份失效。
    let snapshots = group_snapshots();
    assert_no_observation_error(&snapshots);
    let before = in_phase(&snapshots, ConsumeGroupSnapshotPhase::Before);
    let after = in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterCommit);
    assert_eq!(before.len(), 3, "one freeze per item");
    assert_eq!(after.len(), 3, "one commit per item");
    for (index, snapshot) in before.iter().enumerate() {
        assert_eq!(snapshot.slots.len(), 3);
        for slot in &snapshot.slots {
            let data = slot.data_id.clone().expect("complete Data target");
            assert_eq!(
                slot.owner.clone(),
                Some(snapshot.item.clone()),
                "the item owns each collected value"
            );
            assert!(slot.alive);
            assert_eq!(
                slot.collector_moves,
                Some(index),
                "the building area holds exactly one value per earlier item"
            );
            assert!(snapshot.item_owned.contains(&data));
        }
        assert!(
            snapshot.each_owned.is_empty(),
            "the each owns no per-item slot"
        );
    }
    for (index, snapshot) in after.iter().enumerate() {
        for slot in &snapshot.slots {
            assert!(!slot.alive, "the moved value's old identity is gone");
            assert_eq!(slot.collector_moves, Some(index + 1));
        }
        assert!(snapshot.item_owned.is_empty());
    }
}

// ================================================================ P15

#[test]
fn p15_duplicate_owned_alias_group_is_rejected_before_any_move() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let first = ids.allocate().unwrap();
    let alias = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    let data = each_guard
        .register_owned(
            &item,
            &first,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    each_guard
        .bind_local_aliases(
            &item,
            &[AliasSlot::with_type(
                &first,
                alias.clone(),
                TypeId::of::<Tracked>(),
                type_name::<Tracked>(),
            )],
        )
        .expect("a local alias to the same owned value is legal");
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();

    let group = vec![first.clone(), alias.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);
    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
        ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::DuplicateConsumeTarget {
                first_position,
                duplicate_position,
                data_id,
            } if first_position == &first && duplicate_position == &alias && data_id == &data),
            "the later slot names the exact duplicate identity: {primary:?}"
        ),
        other => panic!("expected rejection, got {other:?}"),
    }

    // 全部 collector 长度／moves 不变；拒绝时点的源 owned／存活取自 Before／AfterReject 快照。
    assert_eq!(moves(&item_guard, &c0), 0);
    assert_eq!(moves(&item_guard, &c1), 0);

    let snapshots = group_snapshots();
    assert_no_observation_error(&snapshots);
    let before = in_phase(&snapshots, ConsumeGroupSnapshotPhase::Before);
    let rejected = in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterReject);
    assert_eq!(before.len(), 1);
    assert_eq!(rejected.len(), 1);
    assert_eq!(
        before[0].item_refs, rejected[0].item_refs,
        "item refs unchanged"
    );
    assert_eq!(
        before[0].item_owned, rejected[0].item_owned,
        "item owned unchanged"
    );
    assert_eq!(before[0].each_refs, rejected[0].each_refs);
    assert_eq!(before[0].each_owned, rejected[0].each_owned);
    assert_eq!(
        before[0].slots, rejected[0].slots,
        "per-slot target/owner/alive/collector/moves unchanged"
    );
    assert_eq!(
        before[0].next_data_id, rejected[0].next_data_id,
        "no identity was taken"
    );
    assert!(
        rejected[0].slots.iter().all(|slot| slot.alive),
        "at the rejection point the source values are still alive"
    );
    assert!(
        rejected[0].item_owned.contains(&data),
        "at the rejection point the item still owns both slot targets"
    );
    // 拒绝时点仍为 Active（cleanup 之前）；随后按失败路径清理。
    assert_ne!(
        rejected[0].item_state,
        Some(ScopeState::Closed),
        "the rejection point is before the failure cleanup"
    );
    assert!(
        in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterCommit).is_empty(),
        "a rejected group never commits"
    );

    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
    assert_eq!(
        drops.get(),
        1,
        "the failed group is cleaned up; the value drops once"
    );
}

#[test]
fn p15_same_ref_in_two_slots_is_rejected_before_freeze() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let position = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    each_guard
        .register_owned(
            &item,
            &position,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();

    let group = vec![position.clone(), position.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);
    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
        ConsumeGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } => {
            assert!(
                matches!(&primary, ScopeError::DuplicatePosition { position: duplicate } if duplicate == &position),
                "got {primary:?}"
            );
            assert!(
                cleanup_failure.is_none(),
                "pure shape error: {cleanup_failure:?}"
            );
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    assert_eq!(
        item_guard.state(&item).unwrap(),
        ScopeState::Active,
        "pure shape errors keep the item Active"
    );
    assert_eq!(moves(&item_guard, &c0), 0);
    assert_eq!(moves(&item_guard, &c1), 0);
    assert!(
        group_snapshots().is_empty(),
        "pure structural rejection happens before the freeze: no group snapshot at all"
    );
    assert_eq!(drops.get(), 0);

    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
}

// ================================================================ P16

#[test]
fn p16_repeated_reads_and_two_each_steps_over_one_collection_are_legal() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(
        runtime.execute::<_, (Vec<Out>, Vec<Out>, Vec<Out>, Vec<Out>), _>(
            |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
                // 同一个集合同时是 Each 输入与 item Body 的祖先捕获；item 借用还出现两次。
                let (doubled, sized, again): (Ref<Vec<Out>>, Ref<Vec<Out>>, Ref<Vec<Out>>) = flow
                    .each_output(items, |sub| {
                        let item = sub.ref_of::<Item>();

                        let doubled = sub.then_with_output(pair, (item, item));
                        let sized = sub.then_with_output(collection_size, (item, items));
                        let again = sub.then_with_output(score, item);
                        let out_refs = (doubled, sized, again);
                        sub.output_with(out_refs);
                    });
                // 同一次执行里对同一集合再跑一次 Each。
                let second: Ref<Vec<Out>> = flow.each_output(items, |sub| {
                    let item = sub.ref_of::<Item>();

                    let out_refs = sub.then_with_output(tens, item);
                    sub.output_with(out_refs);
                });
                let out_refs = (doubled, sized, again, second);
                flow.output_with(out_refs);
            },
            vec![Item(1), Item(2)],
        ),
    );
    let (doubled, sized, again, second) =
        outcome.expect("repeated reads and a repeated each are legal");
    assert_eq!(
        doubled.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4]
    );
    assert_eq!(
        sized.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![102, 202]
    );
    assert_eq!(
        again.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4]
    );
    assert_eq!(
        second.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![10, 20]
    );

    // 两个 Each 各自的 Item 与输出身份都不同。
    let rounds = observe::take_controller_rounds();
    let scopes = item_scopes(&rounds);
    assert_eq!(scopes.len(), 4, "two each steps over two items");
    let mut unique: Vec<u64> = scopes.iter().map(ScopeId::seq).collect();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 4, "each item is a distinct ItemScope");
    let takes = take_log_snapshot();
    assert_eq!(
        takes.len(),
        4,
        "three slots from the first each plus one from the second"
    );
    let mut seqs: Vec<u64> = takes.iter().map(DataId::seq).collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(
        seqs.len(),
        4,
        "every each takes a distinct aggregated Vec: {takes:?}"
    );

    // 集合仍可借用（两次 Each 都没移动它）：每次交接快照里它都存活且属于调用者。
    let exports = observe::take_export_snapshots();
    assert!(!exports.is_empty());
    assert!(
        exports.iter().all(|snapshot| snapshot
            .owned_alive
            .iter()
            .all(|(_, alive, owner)| *alive && owner.is_some())),
        "the collection stays alive and owned during both each steps: {exports:?}"
    );
    let item_drops: Vec<String> = take_log()
        .into_iter()
        .filter(|event| event.starts_with("drop:item:"))
        .collect();
    assert_eq!(
        item_drops,
        vec!["drop:item:1", "drop:item:2"],
        "the collection elements drop once each, never moved by an each"
    );
}

// ================================================================ P17

#[test]
fn p17_item_cap_survives_the_iter_state_import_path() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Vec<Out>, _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: Ref<Vec<Out>> = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let initial = sub.then_with_output(score, item);
                let delivered = sub.iter(initial, 2, |current, round| {
                    let blended = round.then_with_output(blend, (item, current));
                    round.then_with_output(break_now, blended)
                });
                let out_refs = sub.then_with_output(rescore, delivered);
                sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    ));
    let collected = outcome.expect("the item borrow stays valid through the iter state path");
    assert_eq!(
        collected.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![4, 8, 12],
        "the round legally borrows the item; the delivered current feeds one more owned result"
    );
    assert!(
        take_log().iter().any(|event| event == "node:blend:1:2"),
        "the round body read both the item and the imported current"
    );

    // 每项每槽仍由 Item own 并被整组移动（cap 保持后结果仍是 item-owned 完整 Data）。
    let snapshots = group_snapshots();
    assert_no_observation_error(&snapshots);
    let after = in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterCommit);
    assert_eq!(after.len(), 3);
    for (index, snapshot) in after.iter().enumerate() {
        assert_eq!(snapshot.slots.len(), 1);
        assert_eq!(snapshot.slots[0].collector_moves, Some(index + 1));
        assert!(!snapshot.slots[0].alive);
    }
}

#[test]
fn p17_core_cap_escape_and_closed_item_resolve_are_rejected() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let root = execution.context().root_scope();
    let collection_position = ids.allocate().unwrap();
    let item_position = ids.allocate().unwrap();
    let descendant_slot = ids.allocate().unwrap();
    let escaped = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collection = root_guard
        .register_owned(&each, &collection_position, vec![Item(5), Item(6)])
        .unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    each_guard
        .bind_item_input::<Item>(&item, &each, &collection_position, &item_position, 0)
        .expect("binding inside the cap");

    // (1) cap 内：合法借用（读取值对应原 Vec 元素）。
    let borrowed = each_guard
        .resolve::<Item>(&item, &item_position)
        .expect("the item borrow is readable inside its cap");
    assert_eq!(borrowed.0, 5);

    // (2) cap 内的后代：Import 作为 alias 合法，cap 与 requester 都真实。
    let descendant = each_guard.create_child(&item).unwrap();
    each_guard
        .import_batch(
            &descendant,
            &item,
            &[ImportSlot::with_type(
                &item_position,
                descendant_slot.clone(),
                TypeId::of::<Item>(),
                type_name::<Item>(),
            )],
        )
        .expect("a descendant inside the cap may alias the item");
    assert_eq!(
        each_guard
            .resolve::<Item>(&descendant, &descendant_slot)
            .expect("the alias stays readable inside the cap")
            .0,
        5
    );

    // (3) cap 外：兄弟 ItemScope 指向旧 cap 的同型目标被拒绝，判据携带真实 cap 与 requester。
    let sibling = each_guard.create_child(&each).unwrap();
    each_guard.inject_scope_target_probe(
        &sibling,
        &escaped,
        RefTarget::CollectionItem {
            collection: CollectionRoot::Data(collection.clone()),
            index: 1,
            lifetime_cap: item.clone(),
            access: ItemAccess::for_collection::<Item>(),
        },
    );
    let error = each_guard
        .resolve::<Item>(&sibling, &escaped)
        .expect_err("a sibling is outside the item cap");
    assert!(
        matches!(&error, ScopeError::ItemOutsideCap { position, cap, requester }
            if position == &escaped && cap == &item && requester == &sibling),
        "got {error:?}"
    );

    // (4) cap 外 Import：请求方在 cap 之外时被真实拒绝（判据携带真实 cap 与 requester）。
    let grandchild = each_guard.create_child(&sibling).unwrap();
    let escape_target = ids.allocate().unwrap();
    let error = each_guard
        .import_batch(
            &grandchild,
            &sibling,
            &[ImportSlot::with_type(
                &escaped,
                escape_target.clone(),
                TypeId::of::<Item>(),
                type_name::<Item>(),
            )],
        )
        .expect_err("a requester outside the item cap cannot import the item");
    assert!(
        matches!(&error, ScopeError::ItemOutsideCap { position, cap, requester }
            if position == &escaped && cap == &item && requester == &sibling),
        "got {error:?}"
    );
    // caller 本地没有该 item 绑定时同样拒绝（不凭容器中存在就导入）。
    let other_child = each_guard.create_child(&each).unwrap();
    let error = each_guard
        .import_batch(
            &other_child,
            &each,
            &[ImportSlot::with_type(
                &item_position,
                ids.allocate().unwrap(),
                TypeId::of::<Item>(),
                type_name::<Item>(),
            )],
        )
        .expect_err("the item position is not a local binding of the each");
    assert!(
        matches!(&error, ScopeError::RefNotBound { scope, position }
            if scope == &each && position == &item_position),
        "got {error:?}"
    );

    // (5) 旧 Item resolve：关闭后不再解析（cap 不再代表有效身份）。先结束 cap 内后代。
    each_guard
        .abort(&descendant)
        .expect("the descendant closes");
    each_guard
        .finalize(&item, &[], &mut Vec::new())
        .expect("the item closes normally without outputs");
    assert_eq!(each_guard.state(&item).unwrap(), ScopeState::Closed);
    let error = each_guard
        .resolve::<Item>(&item, &item_position)
        .expect_err("a closed item no longer resolves");
    assert!(
        matches!(&error, ScopeError::ScopeClosed { scope } if scope == &item),
        "got {error:?}"
    );
    assert!(
        each_guard.alive_probe(&collection),
        "the collection survives the item's normal close"
    );

    // (6) cap 外 Export：item 借用作为声明出口交给 cap 之外的 caller 被拒绝。
    let second_item = each_guard.create_child(&each).unwrap();
    let second_position = ids.allocate().unwrap();
    each_guard
        .bind_item_input::<Item>(
            &second_item,
            &each,
            &collection_position,
            &second_position,
            1,
        )
        .expect("the second item borrow binds inside its own cap");
    let caller_position = ids.allocate().unwrap();
    let mut slots = vec![ExportSlot::new::<Item>(&second_position, &caller_position)];
    let error = each_guard
        .finalize(
            &second_item,
            std::slice::from_ref(&second_position),
            &mut slots,
        )
        .expect_err("the item borrow cannot leave its cap");
    assert!(
        matches!(&error, ScopeError::ItemCapEscape { position, cap, destination }
            if position == &second_position && cap == &second_item && destination == &each),
        "got {error:?}"
    );
    assert_eq!(
        each_guard.state(&second_item).unwrap(),
        ScopeState::Closed,
        "the rejected export closes its source by the failure cleanup"
    );

    drop(each_guard);
    root_guard.complete();
}

// ================================================================ P18

#[test]
fn p18_consume_invalidates_the_old_identity_and_never_reuses_it() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let collection_position = ids.allocate().unwrap();
    let p0 = ids.allocate().unwrap();
    let p1 = ids.allocate().unwrap();
    let next_position = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collection = root_guard
        .register_owned(&each, &collection_position, vec![Item(3)])
        .unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    let id0 = each_guard
        .register_owned(
            &item,
            &p0,
            Tracked {
                value: 0,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let id1 = each_guard
        .register_owned(
            &item,
            &p1,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    assert!(
        id1.seq() > id0.seq(),
        "the second slot owns a larger identity"
    );
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();

    let group = vec![p0.clone(), p1.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);
    let outcome = item_guard.consume_group_in_item_boundary(&item, &group, &permit);
    assert!(
        matches!(outcome, ConsumeGroupOutcome::Consumed),
        "got {outcome:?}"
    );

    // 旧身份失效、旧 Item Closed、责任撤销。
    assert!(
        !item_guard.alive_probe(&id0),
        "the old identity no longer resolves"
    );
    assert!(!item_guard.alive_probe(&id1));
    assert_eq!(item_guard.state(&item).unwrap(), ScopeState::Closed);
    assert!(
        owned_of(&item_guard, &item).is_empty(),
        "every moved value's responsibility is revoked"
    );
    assert!(
        refs_of(&item_guard, &item).is_empty(),
        "the closed item keeps no local refs"
    );
    assert!(
        item_guard.alive_probe(&collection),
        "the collection is still alive"
    );
    assert_eq!(moves(&item_guard, &c0), 1);
    assert_eq!(moves(&item_guard, &c1), 1);
    assert_eq!(drops.get(), 0, "moved values are held by the collectors");

    item_guard.release_responsibility(&item);
    item_guard.complete();

    // 单域不复用：后续 Item 与后续取号都更大。
    let next_item = each_guard.create_child(&each).unwrap();
    assert!(
        next_item.seq() > item.seq(),
        "ScopeId never repeats within one execution"
    );
    let next_id = each_guard
        .register_owned(
            &next_item,
            &next_position,
            Tracked {
                value: 2,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    assert!(
        next_id.seq() > id1.seq(),
        "DataId never repeats within one execution"
    );
    assert!(owned_of(&each_guard, &each).contains(&collection));
    assert!(next_id.seq() > 0);

    // 每个移动值责任刚好撤销一次：Each 清理销毁建构区与后续值。
    drop(each_guard);
    root_guard.complete();
    assert_eq!(
        drops.get(),
        3,
        "two moved values plus the next item value, once each"
    );
}

// ================================================================ P19

#[test]
fn p19_group_permit_does_not_relax_the_general_gates() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let p0 = ids.allocate().unwrap();
    let p1 = ids.allocate().unwrap();
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();

    // 另一个 Execution 的 collector（外域身份）。
    let mut foreign_execution = RootExecution::start();
    let foreign_root = foreign_execution.context().root_scope();
    let foreign_collector = {
        let context = foreign_execution.context_mut();
        let mut guard = context
            .enter(InvocationKind::Root, &foreign_root, true)
            .unwrap();
        let collector = guard.begin_collector::<Tracked>(&foreign_root).unwrap();
        guard.exit_for_probe();
        collector
    };

    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let other_each = root_guard.create_child(&root).unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    each_guard
        .register_owned(
            &item,
            &p0,
            Tracked {
                value: 0,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    each_guard
        .register_owned(
            &item,
            &p1,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let group = vec![p0.clone(), p1.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);

    fn rejected(outcome: ConsumeGroupOutcome, expected: &str) {
        match outcome {
            ConsumeGroupOutcome::Rejected {
                primary,
                cleanup_failure,
            } => {
                assert!(
                    matches!(&primary, ScopeError::Invariant { violated } if *violated == expected),
                    "expected `{expected}`, got {primary:?}"
                );
                assert!(
                    cleanup_failure.is_none(),
                    "a gate rejection needs no cleanup: {cleanup_failure:?}"
                );
            }
            other => panic!("expected the gate rejection `{expected}`, got {other:?}"),
        }
    }

    // (1) 错 frame：当前 frame 还是 Each（Item 尚未进入）。
    rejected(
        each_guard.consume_group_in_item_boundary(&item, &group, &permit),
        "group item consumption requires the current frame to be the item scope",
    );
    assert_eq!(each_guard.state(&item).unwrap(), ScopeState::Active);

    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    // (2) 错 Item：许可登记的 Item 不是当前 Item。
    let wrong_item = ItemGroupPermit {
        item: each.clone(),
        ..permit.clone()
    };
    rejected(
        item_guard.consume_group_in_item_boundary(&item, &group, &wrong_item),
        "group item collect requires the permitted item scope",
    );
    // (3) 错 Each：许可的 caller 不是本 Item 的真实 caller。
    let wrong_each = ItemGroupPermit {
        each: other_each.clone(),
        ..permit.clone()
    };
    rejected(
        item_guard.consume_group_in_item_boundary(&item, &group, &wrong_each),
        "group item frame caller is not the registered each scope",
    );
    // (4) 完整声明组长度错配（裁剪一槽）。
    rejected(
        item_guard.consume_group_in_item_boundary(&item, std::slice::from_ref(&p0), &permit),
        "group item collection length does not match the declared group",
    );
    // (5) 位置错配（长度相同但换项）。
    rejected(
        item_guard.consume_group_in_item_boundary(&item, &[p1.clone(), p0.clone()], &permit),
        "group item collection slot does not match the declared group position",
    );
    // (6) 外域 collector：身份先于 registry 查表被拒。
    let foreign_permit = ItemGroupPermit {
        collectors: vec![c0.clone(), foreign_collector.clone()],
        ..permit.clone()
    };
    match item_guard.consume_group_in_item_boundary(&item, &group, &foreign_permit) {
        ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::CollectorForeignExecution { collector } if collector == &foreign_collector),
            "got {primary:?}"
        ),
        other => panic!("expected the foreign collector rejection, got {other:?}"),
    }

    // 冻结前 typed 拒绝：Item 保持 Active、零 moves、零提交、零成组观察。
    assert_eq!(item_guard.state(&item).unwrap(), ScopeState::Active);
    assert_eq!(moves(&item_guard, &c0), 0);
    assert_eq!(moves(&item_guard, &c1), 0);
    assert_eq!(owned_of(&item_guard, &item).len(), 2);
    assert!(
        group_snapshots().is_empty(),
        "no freeze happened before any of these rejections"
    );
    assert_eq!(drops.get(), 0);

    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
    assert_eq!(
        drops.get(),
        2,
        "the untouched item values are destroyed once"
    );
}

// ================================================================ P20

#[test]
fn p20_live_descendant_and_cleanup_premise_are_checked_before_commit() {
    reset();
    // (a) live descendant：Item 仍有未关闭的 child。
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let position = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collector = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    let value = each_guard
        .register_owned(
            &item,
            &position,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let child = each_guard.create_child(&item).unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![position.clone()];
    let permit = group_permit(&item, &each, &group, std::slice::from_ref(&collector));
    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
        ConsumeGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } => {
            assert!(
                matches!(&primary, ScopeError::ActiveDescendants { scope } if scope == &item),
                "got {primary:?}"
            );
            assert!(cleanup_failure.is_none());
        }
        other => panic!("expected the live-descendant rejection, got {other:?}"),
    }
    assert_eq!(
        item_guard.state(&item).unwrap(),
        ScopeState::Active,
        "the early check keeps the item Active"
    );
    assert_eq!(moves(&item_guard, &collector), 0);
    assert!(item_guard.alive_probe(&value));
    assert!(
        group_snapshots().is_empty(),
        "the live-descendant check runs before the freeze: no group snapshot"
    );
    item_guard.release_responsibility(&item);
    item_guard.complete();
    assert!(each_guard.abort(&child).is_ok());
    drop(each_guard);
    root_guard.complete();

    // (b) 清理前提：冻结后真实前提失败，首拒绝与首清理诊断分开保留。
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let selected = ids.allocate().unwrap();
    let extra = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collector = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    let selected_id = each_guard
        .register_owned(
            &item,
            &selected,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let extra_id = each_guard
        .register_owned(
            &item,
            &extra,
            Tracked {
                value: 2,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    install_consume_group_fault(ConsumeGroupFault::FailCleanup);
    let group = vec![selected.clone()];
    let permit = group_permit(&item, &each, &group, std::slice::from_ref(&collector));
    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
        ConsumeGroupOutcome::Rejected {
            primary,
            cleanup_failure,
        } => {
            assert!(
                matches!(&primary, ScopeError::Invariant { violated }
                    if *violated == "owned entry must exist until its scope closes"),
                "the first rejection is the real cleanup premise failure: {primary:?}"
            );
            assert!(
                cleanup_failure.is_some(),
                "the cleanup premise failure is reported separately, not as the first rejection"
            );
        }
        other => panic!("expected the premise rejection, got {other:?}"),
    }
    assert_eq!(
        moves(&item_guard, &collector),
        0,
        "no physical move before the premise check"
    );
    assert_ne!(
        item_guard.state(&item).unwrap(),
        ScopeState::Closed,
        "a cleanup fault must not claim the item closed"
    );
    assert!(
        !item_guard.alive_probe(&extra_id),
        "the injected fault destroyed the extra entry"
    );
    assert!(item_guard.alive_probe(&selected_id));
    assert!(
        owned_of(&item_guard, &item).contains(&selected_id),
        "responsibility is not silently dropped"
    );
    let snapshots = group_snapshots();
    assert_eq!(
        in_phase(&snapshots, ConsumeGroupSnapshotPhase::Before).len(),
        1
    );
    let rejected = in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterReject);
    assert_eq!(rejected.len(), 1);
    assert_eq!(
        rejected[0].item_state,
        Some(ScopeState::Finalizing),
        "frozen before the premise check"
    );
    assert!(
        in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterCommit).is_empty(),
        "a premise rejection never commits"
    );
    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
}

// ================================================================ P21

#[test]
fn p21_later_slot_reject_leaves_the_first_slot_unmoved() {
    reset();
    let runtime = Runtime::new();
    install_consume_group_fault(ConsumeGroupFault::RejectPreflightAt(1));
    let outcome = block_on(runtime.execute::<_, (Vec<Out>, Vec<Tag>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: (Ref<Vec<Out>>, Ref<Vec<Tag>>) = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let doubled = sub.then_with_output(score, item);
                let tag = sub.then_with_output(tagger, item);
                let out_refs = (doubled, tag);
                sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));
    let error = outcome
        .err()
        .expect("the injected later-slot rejection fails the run");
    let runtime_error = match &error {
        RunError::Runtime(failure) => failure,
        other => panic!("expected the item consume rejection, got {other:?}"),
    };
    assert_eq!(runtime_error.note(), "each item consume rejected");

    let snapshots = group_snapshots();
    assert_no_observation_error(&snapshots);
    let before = in_phase(&snapshots, ConsumeGroupSnapshotPhase::Before);
    let rejected = in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterReject);
    assert_eq!(
        before.len(),
        1,
        "only the first item reaches the group consume"
    );
    assert_eq!(rejected.len(), 1);
    // 两侧完整快照相等：cleanup 之前首项一个槽都没有进入 collector。
    assert_eq!(before[0].item_refs, rejected[0].item_refs);
    assert_eq!(before[0].item_owned, rejected[0].item_owned);
    assert_eq!(before[0].each_refs, rejected[0].each_refs);
    assert_eq!(before[0].each_owned, rejected[0].each_owned);
    assert_eq!(before[0].next_data_id, rejected[0].next_data_id);
    assert_eq!(
        rejected[0].slots.len(),
        2,
        "both declared slots are observed"
    );
    for (first, second) in before[0].slots.iter().zip(rejected[0].slots.iter()) {
        assert_eq!(
            first.data_id, second.data_id,
            "the target identity is unchanged"
        );
        assert_eq!(first.target, second.target);
        assert_eq!(first.owner, second.owner);
        assert!(
            first.alive && second.alive,
            "the rejected snapshot is taken before cleanup"
        );
        assert_eq!(first.collector_moves, Some(0));
        assert_eq!(second.collector_moves, Some(0));
        assert_eq!(first.collector, second.collector);
        assert_eq!(first.collector_element, second.collector_element);
        assert_eq!(first.collector_owner, second.collector_owner);
    }
    assert!(in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterCommit).is_empty());

    // 业务值随 Item 清理各析构一次（一个也没有进入 collector）。
    let drops = take_log();
    assert_eq!(
        drops
            .iter()
            .filter(|event| event.starts_with("drop:out:") || event.starts_with("drop:tag:"))
            .count(),
        2,
        "the produced values drop exactly once: {drops:?}"
    );
}

// ================================================================ P22

#[test]
fn p22_duplicate_targets_and_element_types_are_rejected() {
    reset();
    // (a) 重复目的 collector：许可层只校验长度与 owner，Core 在冻结前拒绝重复目的身份。
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let p0 = ids.allocate().unwrap();
    let p1 = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collector = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    each_guard
        .register_owned(
            &item,
            &p0,
            Tracked {
                value: 0,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    each_guard
        .register_owned(
            &item,
            &p1,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![p0.clone(), p1.clone()];
    let permit = group_permit(
        &item,
        &each,
        &group,
        &[collector.clone(), collector.clone()],
    );
    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
        ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::DuplicateCollectorDestination { collector: repeated }
                if repeated == &collector),
            "got {primary:?}"
        ),
        other => panic!("expected the duplicate destination rejection, got {other:?}"),
    }
    assert_eq!(item_guard.state(&item).unwrap(), ScopeState::Active);
    assert_eq!(moves(&item_guard, &collector), 0);
    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();

    // (b) 元素类型不符：collector 的真实登记元素类型与目标实际类型都不匹配即拒绝。
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let p0 = ids.allocate().unwrap();
    let p1 = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    each_guard
        .register_owned(
            &item,
            &p0,
            Tracked {
                value: 0,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let other = each_guard
        .register_owned(&item, &p1, vec![Item(9)])
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![p0.clone(), p1.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);
    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
        ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::TypeMismatch { position, .. } if position == &p1),
            "got {primary:?}"
        ),
        other => panic!("expected the element type rejection, got {other:?}"),
    }
    assert_eq!(moves(&item_guard, &c0), 0, "the first slot did not move");
    assert_eq!(moves(&item_guard, &c1), 0);
    let snapshots = group_snapshots();
    assert_no_observation_error(&snapshots);
    let rejected = in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterReject);
    assert_eq!(rejected.len(), 1);
    assert_eq!(
        rejected[0].slots[1].data_id.clone(),
        Some(other.clone()),
        "the mistyped target is the rejected slot"
    );
    assert!(
        rejected[0].slots.iter().all(|slot| slot.alive),
        "at the rejection point neither value moved"
    );
    assert!(
        rejected[0]
            .slots
            .iter()
            .all(|slot| slot.collector_moves == Some(0))
    );
    assert_eq!(
        in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterCommit).len(),
        0,
        "no slot committed"
    );
    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
    assert_eq!(drops.get(), 1, "the failed group is cleaned up once");
    assert!(
        take_log().iter().any(|event| event == "drop:item:9"),
        "the mistyped value's element is destroyed by the failure cleanup"
    );

    // (c) 重复被消费 DataId：两个不同位置 alias 同一 owned 实例，整组拒绝并指名首／重复位置。
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let first = ids.allocate().unwrap();
    let alias = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    let data = each_guard
        .register_owned(
            &item,
            &first,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    each_guard
        .bind_local_aliases(
            &item,
            &[AliasSlot::with_type(
                &first,
                alias.clone(),
                TypeId::of::<Tracked>(),
                type_name::<Tracked>(),
            )],
        )
        .expect("a local alias to the same owned value is legal");
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![first.clone(), alias.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);
    match item_guard.consume_group_in_item_boundary(&item, &group, &permit) {
        ConsumeGroupOutcome::Rejected { primary, .. } => assert!(
            matches!(&primary, ScopeError::DuplicateConsumeTarget {
                first_position,
                duplicate_position,
                data_id,
            } if first_position == &first && duplicate_position == &alias && data_id == &data),
            "got {primary:?}"
        ),
        other => panic!("expected the duplicate target rejection, got {other:?}"),
    }
    assert_eq!(moves(&item_guard, &c0), 0);
    assert_eq!(moves(&item_guard, &c1), 0);
    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
    assert_eq!(drops.get(), 1, "the rejected duplicate value drops once");

    // (d) 同类型不同 DataId／不同 collector 合法（不按 TypeId 合并）。
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let p0 = ids.allocate().unwrap();
    let p1 = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    let id0 = each_guard
        .register_owned(
            &item,
            &p0,
            Tracked {
                value: 0,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let id1 = each_guard
        .register_owned(
            &item,
            &p1,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![p0.clone(), p1.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);
    let outcome = item_guard.consume_group_in_item_boundary(&item, &group, &permit);
    assert!(
        matches!(outcome, ConsumeGroupOutcome::Consumed),
        "got {outcome:?}"
    );
    assert_ne!(
        id0, id1,
        "the same TypeId does not make two identities equal"
    );
    assert_eq!(moves(&item_guard, &c0), 1);
    assert_eq!(moves(&item_guard, &c1), 1);
    assert!(owned_of(&item_guard, &item).is_empty());
    item_guard.release_responsibility(&item);
    item_guard.complete();
    drop(each_guard);
    root_guard.complete();
    assert_eq!(drops.get(), 2);
}

// ================================================================ P23

#[test]
fn p23_group_commit_moves_every_slot_and_closes_the_item_once() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Vec<Out>, Vec<Tag>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: (Ref<Vec<Out>>, Ref<Vec<Tag>>) = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let doubled = sub.then_with_output(score, item);
                let tag = sub.then_with_output(tagger, item);
                let out_refs = (doubled, tag);
                sub.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    ));
    let (doubled, labels) = outcome.expect("three items, two slots each");
    assert_eq!(
        doubled.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![2, 4, 6]
    );
    assert_eq!(
        labels.iter().map(|tag| tag.0).collect::<Vec<_>>(),
        vec![11, 12, 13]
    );

    let rounds = observe::take_controller_rounds();
    let scopes = item_scopes(&rounds);
    assert_eq!(scopes.len(), 3);
    assert_eq!(
        each_events(&rounds),
        vec![
            ("scope-created", 0),
            ("item-created", 0),
            ("item-consumed", 0),
            ("item-created", 1),
            ("item-consumed", 1),
            ("item-created", 2),
            ("item-consumed", 2),
            ("exported", 0),
        ],
        "one group consume per item, no extra per-slot commit"
    );

    let snapshots = group_snapshots();
    assert_no_observation_error(&snapshots);
    let before = in_phase(&snapshots, ConsumeGroupSnapshotPhase::Before);
    assert_eq!(before.len(), 3, "one freeze per item");
    for (index, snapshot) in before.iter().enumerate() {
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| slot.collector_moves == Some(index)),
            "the building area holds exactly one value per earlier item"
        );
    }
    let after = in_phase(&snapshots, ConsumeGroupSnapshotPhase::AfterCommit);
    assert_eq!(after.len(), 3, "exactly one commit per item");
    let mut seen: Vec<DataId> = Vec::new();
    for (index, (snapshot, scope)) in after.iter().zip(scopes.iter()).enumerate() {
        assert_eq!(&snapshot.item, scope, "each commit belongs to its own item");
        assert_eq!(snapshot.slots.len(), 2);
        for slot in &snapshot.slots {
            assert_eq!(
                slot.collector_moves,
                Some(index + 1),
                "every slot moves exactly one more value per item"
            );
            assert!(!slot.alive, "the source identity is invalid after the move");
            let data = slot.data_id.clone().expect("complete Data target");
            assert!(!seen.contains(&data), "identities are never reused: {data}");
            seen.push(data);
        }
        assert!(
            snapshot.item_owned.is_empty(),
            "all moved values left the item's responsibility"
        );
        assert!(
            snapshot.each_owned.is_empty(),
            "the each never binds a per-item output RefId"
        );
        assert_eq!(
            snapshot.item_refs.len(),
            3,
            "item borrow + two declared slots stay until the single close"
        );
        assert_eq!(snapshot.item_state, Some(ScopeState::Finalizing));
        closed_once(scope);
    }
    assert_eq!(seen.len(), 6);
}

// ================================================================ P24

#[test]
fn p24_unfinished_collector_is_not_business_data() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let position = ids.allocate().unwrap();
    let local = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collector = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    let value = each_guard
        .register_owned(
            &item,
            &position,
            Tracked {
                value: 7,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let before = each_guard.next_data_id_probe();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![position.clone()];
    let permit = group_permit(&item, &each, &group, std::slice::from_ref(&collector));
    let outcome = item_guard.consume_group_in_item_boundary(&item, &group, &permit);
    assert!(
        matches!(outcome, ConsumeGroupOutcome::Consumed),
        "got {outcome:?}"
    );
    assert_eq!(
        moves(&item_guard, &collector),
        1,
        "the element entered the building area"
    );
    assert!(
        !item_guard.alive_probe(&value),
        "the moved value left the item identity"
    );
    assert_eq!(drops.get(), 0, "the value lives inside the building area");
    let snapshot = group_snapshots();
    let committed = in_phase(&snapshot, ConsumeGroupSnapshotPhase::AfterCommit);
    assert_eq!(committed.len(), 1);
    let collector_slot = &committed[0].slots[0];
    assert!(
        collector_slot
            .collector_element
            .is_some_and(|name| name.ends_with("Tracked")),
        "the real registry element type is observed: {collector_slot:?}"
    );
    assert_eq!(collector_slot.collector_owner.clone(), Some(each.clone()));
    assert_eq!(collector_slot.collector_moves, Some(1));
    item_guard.release_responsibility(&item);
    item_guard.complete();

    // 最终位置未绑定：没有普通 Vec DataId，也没有业务借用入口。
    assert!(
        refs_of(&each_guard, &each).is_empty(),
        "the each binds no final output before finish"
    );
    assert_eq!(
        each_guard.next_data_id_probe(),
        before,
        "an unfinished collector takes no ordinary DataId"
    );
    let error = each_guard
        .resolve::<Vec<Tracked>>(&each, &local)
        .expect_err("the unfinished aggregate is not addressable");
    assert!(
        matches!(&error, ScopeError::RefNotBound { scope, position: unbound }
            if scope == &each && unbound == &local),
        "got {error:?}"
    );

    drop(each_guard);
    root_guard.complete();
    assert_eq!(
        drops.get(),
        1,
        "the partial collector is destroyed exactly once"
    );
}

// ================================================================ P25

#[test]
fn p25_finished_vec_is_an_each_owned_data_and_exports_correctly() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let position = ids.allocate().unwrap();
    let local = ids.allocate().unwrap();
    let caller_position = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let collector = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    each_guard
        .register_owned(
            &item,
            &position,
            Tracked {
                value: 7,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![position.clone()];
    let permit = group_permit(&item, &each, &group, std::slice::from_ref(&collector));
    let outcome = item_guard.consume_group_in_item_boundary(&item, &group, &permit);
    assert!(
        matches!(outcome, ConsumeGroupOutcome::Consumed),
        "got {outcome:?}"
    );
    item_guard.release_responsibility(&item);
    item_guard.complete();

    let vec_id = each_guard
        .finish_collector(&each, &collector, &local)
        .expect("finish produces the ordinary Vec");

    // finish 后：普通 Vec Data、Each-owned、collector 登记与物理建构值都消失。
    let values = each_guard
        .resolve::<Vec<Tracked>>(&each, &local)
        .expect("the local output position is bound");
    assert_eq!(
        values
            .iter()
            .map(|tracked| tracked.value)
            .collect::<Vec<_>>(),
        vec![7]
    );
    assert!(each_guard.alive_probe(&vec_id));
    assert!(owned_of(&each_guard, &each).contains(&vec_id));
    assert!(
        each_guard.collector_moves_probe(&collector).is_err(),
        "the collector registration is gone"
    );
    assert!(
        matches!(
            each_guard.finish_collector(&each, &collector, &ids.allocate().unwrap()),
            Err(ScopeError::CollectorNotRegistered { .. })
        ),
        "a finished collector cannot be finished twice"
    );

    // 最终 Export：caller 整组绑定，责任转移，Each 关闭。
    let mut slots = vec![ExportSlot::new::<Vec<Tracked>>(&local, &caller_position)];
    each_guard
        .finalize(&each, std::slice::from_ref(&local), &mut slots)
        .expect("the Each hands its aggregate to the caller");
    let bound = refs_of(&each_guard, &root)
        .into_iter()
        .find(|(position, _)| position == &caller_position)
        .map(|(_, target)| target);
    assert_eq!(
        bound,
        Some(TargetSnapshot::Data(vec_id.clone())),
        "the caller position binds the same physical Vec"
    );
    assert!(
        owned_of(&each_guard, &root).contains(&vec_id),
        "responsibility moved"
    );
    assert!(!owned_of(&each_guard, &each).contains(&vec_id));
    assert_eq!(each_guard.state(&each).unwrap(), ScopeState::Closed);
    assert_eq!(drops.get(), 0, "the exported Vec is alive at the caller");

    drop(each_guard);
    root_guard.complete();
    assert_eq!(
        drops.get(),
        0,
        "the exported Vec is still owned by the caller"
    );
    drop(execution);
    assert_eq!(
        drops.get(),
        1,
        "the caller releases the exported Vec exactly once"
    );
}

// ================================================================ P26

#[test]
fn p26_post_finish_reject_leaves_no_partial_caller_success() {
    reset();
    let mut execution = RootExecution::start();
    let ids = RefIdAllocator::new(RefIdSource::new());
    let drops = Rc::new(Cell::new(0));
    let root = execution.context().root_scope();
    let p0 = ids.allocate().unwrap();
    let p1 = ids.allocate().unwrap();
    let local0 = ids.allocate().unwrap();
    let local1 = ids.allocate().unwrap();
    let context = execution.context_mut();
    let mut root_guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let each = root_guard.create_child(&root).unwrap();
    let c0 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let c1 = root_guard.begin_collector::<Tracked>(&each).unwrap();
    let mut each_guard = root_guard
        .enter(InvocationKind::Boundary, &each, true)
        .unwrap();
    let item = each_guard.create_child(&each).unwrap();
    each_guard
        .register_owned(
            &item,
            &p0,
            Tracked {
                value: 1,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    each_guard
        .register_owned(
            &item,
            &p1,
            Tracked {
                value: 2,
                drops: Rc::clone(&drops),
            },
        )
        .unwrap();
    let mut item_guard = each_guard
        .enter(InvocationKind::Boundary, &item, true)
        .unwrap();
    let group = vec![p0.clone(), p1.clone()];
    let permit = group_permit(&item, &each, &group, &[c0.clone(), c1.clone()]);
    let outcome = item_guard.consume_group_in_item_boundary(&item, &group, &permit);
    assert!(
        matches!(outcome, ConsumeGroupOutcome::Consumed),
        "got {outcome:?}"
    );
    item_guard.release_responsibility(&item);
    item_guard.complete();

    // 后项 finish 目标被真实占用 → 第二槽拒绝；第一槽的 Vec 已形成。
    let occupant = each_guard
        .register_owned(
            &each,
            &local1,
            Tracked {
                value: 99,
                drops: Rc::clone(&drops),
            },
        )
        .expect("the sample occupies the later finish target");
    let vec_id = each_guard
        .finish_collector(&each, &c0, &local0)
        .expect("the first slot finishes");
    let error = each_guard
        .finish_collector(&each, &c1, &local1)
        .expect_err("the occupied later target rejects the second finish");
    assert!(
        matches!(&error, ScopeError::RefAlreadyBound { scope, position }
            if scope == &each && position == &local1),
        "got {error:?}"
    );

    // 真实中间态：第一槽是 Each-owned Vec，第二槽仍在未完成建构区；调用者一个槽都没绑定。
    assert!(each_guard.alive_probe(&vec_id));
    assert!(owned_of(&each_guard, &each).contains(&vec_id));
    assert!(owned_of(&each_guard, &each).contains(&occupant));
    assert_eq!(moves(&each_guard, &c1), 1);
    assert!(
        refs_of(&each_guard, &root).is_empty(),
        "no partial caller success"
    );
    assert_eq!(drops.get(), 0);

    // 失败清理：已完成 Vec、未完成建构值与占位各析构一次。
    each_guard
        .abort(&each)
        .expect("the each cleanup destroys everything");
    assert_eq!(
        drops.get(),
        3,
        "finished Vec + unfinished collector + occupant, once each"
    );
    assert_eq!(each_guard.state(&each).unwrap(), ScopeState::Closed);
    drop(each_guard);
    root_guard.complete();
    assert_eq!(drops.get(), 3);
}

// ================================================================ P27

/// Each 最终 Export 的后项（late）目的位置预占：由样本自己用正式 `register_owned` 施加，
/// 驱动仍是真实主链（`run_body → invoke_each → export_group → InvocationGuard::finalize`）。
#[test]
fn p27_late_destination_reject_keeps_the_caller_group_unchanged() {
    reset();
    let definition = |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
        let out_refs: (Ref<Vec<Out>>, Ref<Vec<Tag>>) = flow.each_output(items, |sub| {
            let item = sub.ref_of::<Item>();

            let doubled = sub.then_with_output(score, item);
            let tag = sub.then_with_output(tagger, item);
            let out_refs = (doubled, tag);
            sub.output_with(out_refs);
        });
        flow.output_with(out_refs);
    };
    let built = build_definition::<Vec<Item>, (Vec<Out>, Vec<Tag>), _>(definition)
        .expect("the definition builds");
    let root_input = built
        .root_inputs()
        .next()
        .expect("the collection declaration")
        .core()
        .clone();
    let destinations: Vec<RefId> = built
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::EachOutput { .. }))
        .map(|record| record.core().clone())
        .collect();
    assert_eq!(destinations.len(), 2, "two caller output slots");
    let early = destinations[0].clone();
    let late = destinations[1].clone();

    let mut execution = RootExecution::start();
    let root = execution.context().root_scope();
    execution
        .context_mut()
        .register_owned_erased(
            &root,
            &root_input,
            type_name::<Vec<Item>>(),
            Box::new(vec![Item(1), Item(2), Item(3)]),
        )
        .expect("the root input registers");

    let context = execution.context_mut();
    let mut guard = context.enter(InvocationKind::Root, &root, true).unwrap();
    let occupant = guard
        .register_owned(&root, &late, Out(42))
        .expect("the late destination is occupied by a formal binding");

    let outcome = block_on(run_body(&mut guard, &built, BodyId::ROOT, &root));
    let failure = outcome.expect_err("the occupied late destination rejects the each handover");
    let ExecFailure::Runtime(runtime_error) = &failure else {
        panic!("expected the each handover failure, got {failure:?}");
    };
    assert_eq!(runtime_error.note(), "each output handover rejected");
    let source = runtime_error
        .source()
        .expect("the original Scope diagnostic is preserved");
    let diagnostic = source
        .downcast_ref::<FrameworkDiagnostic>()
        .expect("the handover diagnostic is the framework's Scope diagnostic");
    let text = format!("{diagnostic:?}");
    assert!(
        text.starts_with("FrameworkDiagnostic { scope_error: RefAlreadyBound {"),
        "exact diagnostic variant: {text}"
    );
    assert!(
        text.contains(&format!("scope: {root}")),
        "the caller scope: {text}"
    );
    assert!(
        text.contains(&format!("position: {late}")),
        "the exact late slot: {text}"
    );

    // caller 全组 refs／owned／owner／alive／取号前后相等：不半导出。
    let exports = observe::take_export_snapshots();
    let before = exports
        .iter()
        .find(|snapshot| snapshot.phase == "before")
        .expect("before the handover");
    let rejected = exports
        .iter()
        .find(|snapshot| snapshot.phase == "rejected")
        .expect("the rejected handover is observed");
    assert_eq!(before.refs, rejected.refs, "caller refs unchanged");
    assert_eq!(before.owned, rejected.owned, "caller owned unchanged");
    assert_eq!(
        before.owned_alive, rejected.owned_alive,
        "owner/alive unchanged"
    );
    assert_eq!(
        before.next_data_id, rejected.next_data_id,
        "no identity was taken"
    );
    assert!(
        !before.refs.iter().any(|(position, _)| position == &early),
        "the early destination is never bound: nothing half-exported"
    );
    assert!(before.refs.iter().any(|(position, _)| position == &late));
    assert_eq!(before.refs.len(), 2, "the collection + the occupant");
    assert_eq!(before.owned.len(), 2, "the collection + the occupant");
    assert!(
        guard.alive_probe(&occupant),
        "the caller's occupant stays alive"
    );
    assert!(owned_of(&guard, &root).contains(&occupant));

    // Each 产物失败清理：两个聚合 Vec 的六个元素各析构一次；占位留给调用者。
    guard.failed("each handover rejected");
    let events = take_log();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                (event.starts_with("drop:out:") && *event != "drop:out:42")
                    || event.starts_with("drop:tag:")
            })
            .count(),
        6,
        "both aggregated Vecs are destroyed exactly once by the each cleanup: {events:?}"
    );
    assert!(
        events.iter().any(|event| event == "drop:out:42"),
        "the caller's occupant is released by the caller, not by the each: {events:?}"
    );
    assert!(
        take_log_snapshot().is_empty(),
        "the rejected handover takes no root target"
    );
}

// ================================================================ P28

#[test]
fn p28_direct_duplicate_root_ref_is_rejected_by_the_definition() {
    reset();
    let error = build_definition::<Vec<Item>, (Vec<Out>, Vec<Out>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            // 同一个逻辑 Ref 直接占用两个 Root 输出槽：Definition 阶段拒绝。
            let collected: Ref<Vec<Out>> = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let out_refs = sub.then_with_output(score, item);
                sub.output_with(out_refs);
            });
            let out_refs = (collected, collected);
            flow.output_with(out_refs);
        },
    )
    .err()
    .expect("a directly duplicated logical Ref is rejected by the definition");
    assert!(
        matches!(error, BuildErrorDetail::DuplicateRootOutput { .. }),
        "got {error:?}"
    );

    // 对照：不同逻辑位置的两次 Each 是合法的 Root 输出。
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Vec<Out>, Vec<Out>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let left: Ref<Vec<Out>> = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let out_refs = sub.then_with_output(score, item);
                sub.output_with(out_refs);
            });
            let right: Ref<Vec<Out>> = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();

                let out_refs = sub.then_with_output(tens, item);
                sub.output_with(out_refs);
            });
            let out_refs = (left, right);
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));
    let (left, right) = outcome.expect("two distinct each results are legal root outputs");
    assert_eq!(left.iter().map(|out| out.0).collect::<Vec<_>>(), vec![2, 4]);
    assert_eq!(
        right.iter().map(|out| out.0).collect::<Vec<_>>(),
        vec![10, 20]
    );
    let takes = take_log_snapshot();
    assert_eq!(takes.len(), 2, "both distinct Vec identities are taken");
    assert_ne!(takes[0], takes[1]);
}

#[test]
fn p28_legal_alias_duplicate_root_target_takes_nothing() {
    reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Vec<Out>, Vec<Out>), _>(
        |flow: &mut Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs: (Ref<Vec<Out>>, Ref<Vec<Out>>) = flow.retry_output(0, |attempt| {
                let collected: Ref<Vec<Out>> = attempt.each_output(items, |sub| {
                    let item = sub.ref_of::<Item>();

                    let out_refs = sub.then_with_output(score, item);
                    sub.output_with(out_refs);
                });
                // 经合法 alias 把同一个聚合 Vec 交付到两个 Root 端口。
                let declared = (collected, collected);
                attempt.output_with(declared);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));
    let error = outcome
        .err()
        .expect("the duplicate physical Vec is rejected at root");
    let runtime_error = match &error {
        RunError::Runtime(failure) => failure,
        other => panic!("expected the root extraction rejection, got {other:?}"),
    };
    assert!(
        runtime_error
            .note()
            .contains("root extraction preflight rejected"),
        "actual note: {}",
        runtime_error.note()
    );
    assert!(
        take_log_snapshot().is_empty(),
        "zero take: no root target is removed"
    );
    let drops = take_log();
    assert_eq!(
        drops
            .iter()
            .filter(|event| event.starts_with("drop:out:"))
            .count(),
        2,
        "the aggregated Vec is destroyed once by the failed root cleanup: {drops:?}"
    );
}
