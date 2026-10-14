//! WE-T05 Each Control／Failure／取消／cap 定向证据（P29～P37）。
//!
//! 全部样本经公开入口 `Runtime::new().execute(...)`（P36 的外域 collector 访问额外使用
//! 真实 `RootExecution` 取得第二域 Context），只通过既有 cfg(test) 观察／窄故障通道读
//! 元数据与物理身份，不触碰内部结构：
//!
//! - P29 部分成功后 Control 的 Item→Each 两阶段退出；
//! - P30 Iter→Each→Choose 的 IterBreak 最近捕获；
//! - P31 Retry→Each→Chain 的完整 attempt 重启；
//! - P32 Failure／NoMatchingCase／预算耗尽的 terminal 行为；
//! - P33 未处理 Control 的 Root 分类与清理故障组合；
//! - P34 Item／Each 两层 Control 退出清理故障、责任与首诊断；
//! - P35 真 Pending 取消的借用先结束与逐实例 Drop 恰一次；
//! - P36 Collector／Data／Scope 外域与 Item cap 身份。
//!
//! P37 的定向变异由主代理统一实施，本文件只提供可被变异捕获的生产断言。

// `.err().expect(..)` 是本目录测试统一的 RunError 断言风格（等价于 `expect_err`，仅 panic 文本不同），保留不改断言。
#![allow(clippy::err_expect)]

use crate::Data;
use crate::core::engine::boundary::{
    install_control_cleanup_fault, install_each_control_cleanup_fault, install_foreign_access_fault,
};
use crate::core::engine::error::{BodyError, ControlSignal, RunError};
use crate::core::engine::observe;
use crate::core::engine::runtime::{Runtime, install_root_cleanup_fault};
use crate::core::engine::shape::{Query, Ref};
use crate::core::identity::{CollectorId, DataId, ScopeId};
use crate::core::internal_error::{InternalError, ScopeError};
use crate::core::orchestrator::ScopeRole;
use crate::core::runtime::RootExecution;
use crate::core::scope::{ScopeState, TargetSnapshot};
use crate::core::test_support;
use futures::executor::block_on;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

// ---------------------------------------------------------------- 业务类型

#[derive(Data, Debug, PartialEq)]
struct Item(u32);
#[derive(Data, Debug, PartialEq)]
struct Score(u32);
#[derive(Data, Debug, PartialEq)]
struct Context(u32);
#[derive(Data, Debug, PartialEq)]
struct Amount(u32);
/// 刻意不被业务读取的 Root 输入：供 Root 退出清理故障销毁（不影响 Each 的集合输入）。
#[derive(Data)]
struct Filler(#[allow(dead_code)] u32);

/// 不可恢复业务 Failure 的动态载荷。
#[derive(Debug)]
struct Rejected(&'static str);

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rejected: {}", self.0)
    }
}

impl Error for Rejected {}

/// Retry 控制的动态原因（供 last_error 源链检查）。
#[derive(Debug)]
struct NotReady(u32);

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate {} is not ready", self.0)
    }
}

impl Error for NotReady {}

// ---------------------------------------------------------------- 业务节点与节点日志

thread_local! {
    /// 业务节点调用日志（标签按调用顺序）。线程局部隔离平行用例。
    static NODE_LOG: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    /// 控制器状态推进时实际看到的输入值（Iter state 重启证据）。
    static STEP_VALUES: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
}

fn log_node(tag: &'static str) {
    NODE_LOG.with(|log| log.borrow_mut().push(tag));
}

fn node_log() -> Vec<&'static str> {
    NODE_LOG.with(|log| log.borrow().clone())
}

fn node_count(tag: &'static str) -> usize {
    NODE_LOG.with(|log| log.borrow().iter().filter(|entry| **entry == tag).count())
}

fn push_step(value: u32) {
    STEP_VALUES.with(|values| values.borrow_mut().push(value));
}

fn step_values() -> Vec<u32> {
    STEP_VALUES.with(|values| values.borrow().clone())
}

fn reset_nodes() {
    NODE_LOG.with(|log| log.borrow_mut().clear());
    STEP_VALUES.with(|values| values.borrow_mut().clear());
}

/// 常规产物：每个 item 产生一个 owned `Score`。
async fn score_of_item(query: Query<&Item>) -> Result<Score, BodyError> {
    log_node("score");
    Ok(Score(query.get().0 * 10))
}

/// 原样再产出一个 `Score`（作为 item Body 的声明输出）。
async fn keep_score(query: Query<&Score>) -> Result<Score, BodyError> {
    log_node("keep");
    Ok(Score(query.get().0))
}

/// 产物为 30（第三个 item）时发出 `IterBreak`。
async fn break_on_thirty(query: Query<&Score>) -> Result<Score, BodyError> {
    log_node("gate");
    let score = query.get().0;
    if score == 30 {
        Err(BodyError::iter_break())
    } else {
        Ok(Score(score))
    }
}

/// 无条件发出 `IterBreak`（Choose 的 otherwise 分支）。
async fn break_now(query: Query<&Score>) -> Result<Score, BodyError> {
    log_node("break");
    let _ = query.get();
    Err(BodyError::iter_break())
}

/// 产物为 30 时发出不可恢复 Failure。
async fn fail_on_thirty(query: Query<&Score>) -> Result<Score, BodyError> {
    log_node("fail");
    let score = query.get().0;
    if score == 30 {
        Err(BodyError::fail(Rejected("business refusal")))
    } else {
        Ok(Score(score))
    }
}

/// 产物为 30 时发出 `Retry`（携带动态原因）。
async fn retry_on_thirty(query: Query<&Score>) -> Result<Score, BodyError> {
    log_node("gate");
    let score = query.get().0;
    if score == 30 {
        Err(BodyError::retry(NotReady(score)))
    } else {
        Ok(Score(score))
    }
}

/// 产物序号（1／2／3），供 Choose 选择。
async fn selector_tenth(query: Query<&Score>) -> Result<u32, BodyError> {
    log_node("select");
    Ok(query.get().0 / 10)
}

/// 当前轮次编号，作为 item Body 内 Choose 的选择键（祖先捕获）。
async fn round_number(query: Query<&Context>) -> Result<u32, BodyError> {
    log_node("round_no");
    Ok(query.get().0)
}

/// 推进 Iter 状态一格的 next；记录实际看到的输入值。
async fn bump_context(query: Query<&Context>) -> Result<Context, BodyError> {
    log_node("bump");
    let current = query.get().0;
    push_step(current);
    Ok(Context(current + 1))
}

/// Iter 的 next：第 1 轮后发出 `IterBreak`（交付进入本轮时的 current）。
async fn break_at_one(query: Query<&Context>) -> Result<Context, BodyError> {
    log_node("iter");
    let current = query.get().0;
    push_step(current);
    if current >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Context(current + 1))
    }
}

/// 产生一个候选并计数一次 attempt（P32(c) 预算耗尽样本）。
async fn candidate_of_item(query: Query<&Item>) -> Result<Score, BodyError> {
    log_node("gen");
    Ok(Score(query.get().0 * 10))
}

/// 永远要求重试（P32(c) 样本）。
async fn always_retry(query: Query<&Score>) -> Result<Score, BodyError> {
    log_node("retry");
    Err(BodyError::retry(NotReady(query.get().0)))
}

/// 立即发出 `IterBreak`（P33 样本）。
async fn always_break(query: Query<&Item>) -> Result<Score, BodyError> {
    log_node("break");
    let _ = query.get();
    Err(BodyError::iter_break())
}

/// 第二域业务节点（P36 跨执行访问样本）。
async fn double_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

// ---------------------------------------------------------------- 观察工具

fn root_frame_scope() -> ScopeId {
    observe::take_frames()
        .into_iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .expect("the root frame is recorded")
        .scope
}

fn each_ats(rounds: &[observe::ControllerRoundRecord]) -> Vec<&'static str> {
    rounds
        .iter()
        .filter(|record| record.kind == "each")
        .map(|record| record.at)
        .collect()
}

/// `kind` 控制器的全部观察记录（按记录顺序）。
fn records<'a>(
    rounds: &'a [observe::ControllerRoundRecord],
    kind: &'static str,
) -> Vec<&'a observe::ControllerRoundRecord> {
    rounds.iter().filter(|record| record.kind == kind).collect()
}

/// Each 控制器建立的唯一 EachScope（`scope-created` 记录）。
fn each_scope(rounds: &[observe::ControllerRoundRecord]) -> ScopeId {
    records(rounds, "each")
        .into_iter()
        .find(|record| record.at == "scope-created")
        .expect("the each scope creation is recorded")
        .controller
        .clone()
}

/// 在第 `index` 个 item 上被观察到的 ItemScope。
fn item_scope(
    rounds: &[observe::ControllerRoundRecord],
    at: &'static str,
    index: usize,
) -> ScopeId {
    records(rounds, "each")
        .into_iter()
        .find(|record| record.at == at && record.index == index)
        .unwrap_or_else(|| panic!("missing each record {at} at index {index}"))
        .round
        .clone()
        .expect("item records carry the ItemScope")
}

fn role_count(role: ScopeRole) -> usize {
    test_support::boundary_creation_snapshot()
        .into_iter()
        .filter(|(_, _, recorded)| *recorded == role)
        .count()
}

/// 真实清理故障的统一诊断文本（`ScopeError::Invariant`）。
const CLEANUP_INVARIANT: &str =
    "internal invariant violated: owned entry must exist until its scope closes";

fn expect_cleanup_invariant(error: &ScopeError) -> &'static str {
    match error {
        ScopeError::Invariant { violated } => violated,
        other => panic!("expected the real Invariant cleanup diagnostic, got {other:?}"),
    }
}

// ================================================================ P29

// P29：部分成功后 Control 的 Item→Each 两阶段退出。前两项已 Consume、第三项产物后 Control、
// 第四项零调用；当前 Item 未 Consume；阶段间 collector 仍有前两项；Each 退出后 registry／物理
// 建构值撤销；finish／caller 正常绑定零。
#[test]
fn p29_control_partial_success_exits_item_then_each() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let score = each.then(&score_of_item, item);
                each.then(&break_on_thirty, score)
            })
        },
        vec![Item(1), Item(2), Item(3), Item(4)],
    ));

    // 未处理 Control 逃出 Root；不返回部分集合。
    let error = outcome
        .err()
        .expect("an unmatched control escapes the root");
    assert!(
        matches!(
            error,
            RunError::UnhandledControl(ControlSignal::IterBreak(_))
        ),
        "expected UnhandledControl(IterBreak), got {error}"
    );

    // 第四项零调用、第三项产物后 Control：业务节点各 3 次。
    assert_eq!(
        node_log(),
        ["score", "gate", "score", "gate", "score", "gate"]
    );
    assert_eq!(node_count("score"), 3);
    assert_eq!(node_count("gate"), 3);
    assert_eq!(
        role_count(ScopeRole::Item),
        3,
        "the fourth item is never created"
    );

    let rounds = observe::take_controller_rounds();
    assert_eq!(
        each_ats(&rounds),
        [
            "scope-created",
            "item-created",
            "item-consumed",
            "item-created",
            "item-consumed",
            "item-created",
            "item-control-exit",
            "each-control-exit",
        ],
        "no finish／export and no fourth item: {rounds:?}"
    );
    assert_eq!(
        records(&rounds, "each")
            .into_iter()
            .map(|record| record.index)
            .collect::<Vec<_>>(),
        [0, 0, 0, 1, 1, 2, 2, 2]
    );
    let each = each_scope(&rounds);
    let caller = root_frame_scope();
    let first = item_scope(&rounds, "item-consumed", 0);
    let second = item_scope(&rounds, "item-consumed", 1);
    let third = item_scope(&rounds, "item-control-exit", 2);
    assert_ne!(first, second);
    assert_ne!(second, third);
    assert!(
        first.seq() < second.seq() && second.seq() < third.seq(),
        "item identities grow monotonically"
    );
    assert!(
        std::sync::Arc::ptr_eq(first.execution(), caller.execution()),
        "one execution domain"
    );
    assert!(
        records(&rounds, "each")
            .into_iter()
            .all(|record| record.caller == caller),
        "the each runs in the real root caller"
    );

    // 阶段一：Item 已 Closed，而 Each 尚在执行（记录发生时 Each 还未收口）。
    let states = observe::take_scope_states();
    let item_closed = states
        .iter()
        .find(|record| record.at == "each-item-control-closed")
        .expect("the item control close is observed");
    assert_eq!(item_closed.scope, third);
    assert_eq!(item_closed.state, ScopeState::Closed);

    // 两阶段的先后由控制器记录固定：item-control-exit 早于 each-control-exit。
    let ats = each_ats(&rounds);
    let item_exit = ats
        .iter()
        .position(|at| *at == "item-control-exit")
        .expect("phase one is recorded");
    let each_exit = ats
        .iter()
        .position(|at| *at == "each-control-exit")
        .expect("phase two is recorded");
    assert!(item_exit < each_exit);

    // 阶段二：Each 自己 Closed，Context 未被标记不可恢复终止。
    let completions = observe::take_control_completions();
    assert_eq!(completions.len(), 1, "one each control completion");
    assert_eq!(completions[0].child, each);
    assert_eq!(completions[0].child_state, Some(ScopeState::Closed));
    assert!(
        !completions[0].terminated,
        "a legal control exit does not terminate the context"
    );

    // finish／caller 正常绑定零：没有导出快照（finish_collectors + export_group 都未运行）。
    assert!(
        observe::take_export_snapshots().is_empty(),
        "no finish and no caller binding on a control exit"
    );

    // 阶段间 collector 仍有前两项：最后一个整组 Consume 观察是第二项的 AfterCommit（moves=2），
    // 其后没有任何进一步的 collector 变更记录，第三项也没有 Consume。
    let group = test_support::take_consume_group();
    assert_eq!(
        group
            .iter()
            .map(|snapshot| (snapshot.item.clone(), snapshot.phase))
            .collect::<Vec<_>>(),
        vec![
            (
                first.clone(),
                test_support::ConsumeGroupSnapshotPhase::Before
            ),
            (
                first.clone(),
                test_support::ConsumeGroupSnapshotPhase::AfterCommit
            ),
            (
                second.clone(),
                test_support::ConsumeGroupSnapshotPhase::Before
            ),
            (
                second.clone(),
                test_support::ConsumeGroupSnapshotPhase::AfterCommit
            ),
        ],
        "exactly the first two items are consumed, in order: {group:?}"
    );
    assert!(
        !group.iter().any(|snapshot| snapshot.item == third),
        "the control item is never consumed"
    );
    for snapshot in &group {
        assert_eq!(snapshot.each.as_ref(), Some(&each));
        assert_eq!(snapshot.slots.len(), 1);
        assert_eq!(
            snapshot.slots[0].collector_owner.as_ref(),
            Some(&each),
            "the destination collector belongs to the each"
        );
        assert!(
            snapshot.slots[0]
                .collector_element
                .is_some_and(|name| name.contains("Score")),
            "the collector is typed to the real element: {:?}",
            snapshot.slots[0].collector_element
        );
    }
    let moves: Vec<Option<usize>> = group
        .iter()
        .map(|snapshot| snapshot.slots[0].collector_moves)
        .collect();
    assert_eq!(
        moves,
        [Some(0), Some(1), Some(1), Some(2)],
        "the collector holds exactly the first two products at the inter-phase moment"
    );

    // Each 退出后 registry／物理建构值都撤销：Item 与 Each 都在关闭记录中，Root 也在。
    let closed = test_support::closed_scope_snapshot();
    for scope in [&first, &second, &third, &each, &caller] {
        assert!(closed.contains(scope), "{scope:?} is closed: {closed:?}");
    }

    // Each 与 Item 的真实父子关系（在真实创建点记录）。
    let creations = test_support::boundary_creation_snapshot();
    let each_creation = creations
        .iter()
        .find(|(_, _, role)| *role == ScopeRole::Each)
        .expect("the each scope creation is recorded");
    assert_eq!(each_creation.0, each);
    assert_eq!(each_creation.1, caller);
    let items: Vec<ScopeId> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(child, parent, _)| {
            assert_eq!(*parent, each, "every item is a direct child of the each");
            child.clone()
        })
        .collect();
    assert_eq!(items, [first, second, third]);

    observe::reset();
}

// ================================================================ P30

// P30：Iter→Each→Choose 的 IterBreak 由最近的 Iter 捕获。真实外层 Iter 的 current／未接受
// next、停止剩余项、partial collectors 清理、原集合与输入 owner 保留；外层实际成功继续并交付
// 进入本轮时的 current。
#[test]
fn p30_iter_captures_choose_break_and_delivers_current() {
    observe::reset();
    test_support::reset_observations();
    test_support::take_log_reset();
    reset_nodes();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, (items, context): (Ref<Vec<Item>>, Ref<Context>)| {
            flow.iter(context, 2, |current, round| {
                let round_no = round.then(&round_number, current);
                let _scores = round.each(items, |item, each| {
                    let score = each.then(&score_of_item, item);
                    each.choose(round_no, |choice| {
                        choice.case(0u32, |branch| branch.then(&keep_score, score));
                        choice.otherwise(|branch| branch.then(&break_now, score));
                    })
                });
                round.then(&bump_context, current)
            })
        },
        (vec![Item(1), Item(2), Item(3)], Context(0)),
    ));

    // 外层 Iter 实际成功收口，交付进入本轮时的 current。
    assert_eq!(
        outcome.expect("the nearest iter captures the break"),
        Context(1)
    );
    assert_eq!(
        step_values(),
        [0],
        "round one breaks before its next is accepted"
    );

    // 停止剩余项：第 0 轮 3 项全部完成，第 1 轮只跑第 1 项即被捕获。
    assert_eq!(node_count("round_no"), 2);
    assert_eq!(node_count("score"), 4);
    assert_eq!(node_count("keep"), 3);
    assert_eq!(node_count("break"), 1);
    assert_eq!(node_count("bump"), 1);
    assert_eq!(
        role_count(ScopeRole::Item),
        4,
        "only the items actually entered are created"
    );

    let rounds = observe::take_controller_rounds();
    let iter_ats: Vec<&'static str> = records(&rounds, "iter")
        .into_iter()
        .map(|record| record.at)
        .collect();
    assert_eq!(
        iter_ats,
        [
            "controller-created",
            "round-created",
            "round-promoted",
            "round-created",
            "round-caught",
        ],
        "the iter catches its own signal in the nearest round: {rounds:?}"
    );

    // 交付两跳都发生：round → controller → caller，各一槽，终点是真实 Root caller。
    let root = root_frame_scope();
    let deliveries = observe::take_controller_deliveries();
    let iter_deliveries: Vec<&observe::ControllerDeliveryRecord> = deliveries
        .iter()
        .filter(|record| record.kind == "iter")
        .collect();
    assert_eq!(
        iter_deliveries
            .iter()
            .map(|record| (record.hop, record.pairs.len()))
            .collect::<Vec<_>>(),
        [("round-to-controller", 1), ("controller-to-caller", 1)]
    );
    assert_eq!(
        iter_deliveries
            .iter()
            .find(|record| record.hop == "controller-to-caller")
            .expect("the outer hop is recorded")
            .to,
        root,
        "the delivered current really reaches the root caller"
    );

    // Choose 的真实选择：第 0 轮命中 case 0；第 1 轮无 case 命中并改走 otherwise。
    let selections = observe::take_choose_selections();
    assert_eq!(selections.len(), 4, "four choose invocations");
    assert_eq!(
        selections
            .iter()
            .map(|record| (record.selected, record.otherwise_used))
            .collect::<Vec<_>>(),
        [
            (Some(0), false),
            (Some(0), false),
            (Some(0), false),
            (None, true),
        ]
    );

    // partial collectors 清理：只有第 0 轮的 3 项真实 Consume；被中断的第 1 轮 Each 从未 finish。
    let group = test_support::take_consume_group();
    assert_eq!(
        group
            .iter()
            .filter(
                |snapshot| snapshot.phase == test_support::ConsumeGroupSnapshotPhase::AfterCommit
            )
            .count(),
        3,
        "the interrupted round's partial collectors never commit: {group:?}"
    );
    let each_records = records(&rounds, "each");
    let each_scopes: Vec<ScopeId> = each_records
        .iter()
        .filter(|record| record.at == "scope-created")
        .map(|record| record.controller.clone())
        .collect();
    assert_eq!(each_scopes.len(), 2, "one each per iter round");
    assert_ne!(each_scopes[0], each_scopes[1]);
    assert!(each_scopes[0].seq() < each_scopes[1].seq());
    assert_eq!(
        each_records
            .iter()
            .filter(|record| record.at == "exported")
            .count(),
        1,
        "only the completed round's each reaches finish／export: {rounds:?}"
    );
    assert_eq!(
        each_records
            .iter()
            .filter(|record| record.at == "each-control-exit")
            .count(),
        1,
        "only the interrupted round's each takes the control exit"
    );

    // 原集合与输入 owner 保留：Root 提取后两个原始输入仍属于 Root（Iter 的 state 替换不改变
    // 原 owner），只有交付的 current 被真实取走。
    let snapshots = test_support::take_root_snapshots();
    let after_commit = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == test_support::RootSnapshotPhase::AfterCommit)
        .expect("the root extraction commit is observed");
    assert_eq!(after_commit.planned_takes, 1, "only the delivered current");
    assert_eq!(
        after_commit.taken_alive,
        [false],
        "the taken instance left the container"
    );
    assert_eq!(
        after_commit.taken_owned_by,
        [None],
        "root responsibility is released on take"
    );
    let bound_ids: Vec<DataId> = after_commit
        .root_refs
        .iter()
        .filter_map(|(_, target)| match target {
            TargetSnapshot::Data(id) => Some(id.clone()),
            TargetSnapshot::CollectionItem { .. } => None,
        })
        .collect();
    assert_eq!(
        bound_ids.len(),
        3,
        "the two inputs plus the iter output are bound in the root: {after_commit:?}"
    );
    assert_eq!(
        after_commit.root_owned,
        bound_ids[..2].to_vec(),
        "both root inputs keep their owner across the iter: {after_commit:?}"
    );
    assert_eq!(
        after_commit.root_owned.len(),
        2,
        "the iter consumes nothing from the root: {after_commit:?}"
    );
    assert!(
        !after_commit.root_owned.contains(&bound_ids[2]),
        "the delivered current is no longer root-owned"
    );
    let take_log = test_support::take_log_snapshot();
    assert_eq!(take_log.len(), 1, "exactly one root value is taken");
    assert_eq!(
        take_log[0], bound_ids[2],
        "the single take is the delivered current"
    );
    assert_eq!(
        after_commit.terminated, None,
        "an iter capture is not a termination"
    );

    observe::reset();
}

// ================================================================ P31

// P31：Retry→Each→Chain 的 Retry 由最近的外层 Retry 完整重启 attempt。原动态原因／source
// 保留、旧部分结果清理后才开新 attempt；所有 item 从原集合重新开始；内部 Iter 从原始 state
// 重建；内层同型捕获优先（Iter 自捕获，不上浮为 Retry 请求）。
#[test]
fn p31_retry_restarts_the_whole_each_attempt() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, (items, context): (Ref<Vec<Item>>, Ref<Context>)| {
            flow.retry(1, |attempt| {
                let current = attempt.iter(context, 2, |current, round| {
                    round.then(&break_at_one, current)
                });
                let _scores = attempt.each(items, |item, each| {
                    each.chain(|sub| {
                        let score = sub.then(&score_of_item, item);
                        sub.then(&retry_on_thirty, score)
                    })
                });
                current
            })
        },
        (vec![Item(1), Item(2), Item(3)], Context(0)),
    ));

    // 预算耗尽：原动态原因与 source 保留。
    let error = outcome.err().expect("the exhausted retry is terminal");
    let RunError::RetryExhausted {
        max_retries,
        attempts,
        last_error,
    } = &error
    else {
        panic!("expected RetryExhausted, got {error}");
    };
    assert_eq!((*max_retries, *attempts), (1, 2));
    let reason = Error::source(last_error).expect("the dynamic reason survives");
    assert_eq!(
        reason.to_string(),
        "candidate 30 is not ready",
        "the original dynamic reason object is preserved"
    );
    assert!(
        reason.downcast_ref::<NotReady>().is_some(),
        "the source chain keeps the original error object"
    );

    // 所有 item 从原集合重新开始：每次 attempt 三个 item 各跑一次。
    assert_eq!(node_count("score"), 6);
    assert_eq!(node_count("gate"), 6);
    assert_eq!(node_count("iter"), 4);
    assert_eq!(
        step_values(),
        [0, 1, 0, 1],
        "the inner iter restarts from its original imported state on the new attempt"
    );

    let rounds = observe::take_controller_rounds();
    let retry_ats: Vec<&'static str> = records(&rounds, "retry")
        .into_iter()
        .map(|record| record.at)
        .collect();
    assert_eq!(
        retry_ats,
        [
            "controller-created",
            "round-created",
            "round-caught",
            "round-created",
            "round-caught",
            "budget-exhausted",
        ],
        "exactly two attempts and no further round: {rounds:?}"
    );
    assert!(
        !retry_ats.contains(&"round-propagated"),
        "the inner iter captured its own signal; the outer retry never saw an IterBreak"
    );
    let iter_ats: Vec<&'static str> = records(&rounds, "iter")
        .into_iter()
        .map(|record| record.at)
        .collect();
    assert_eq!(
        iter_ats,
        [
            "controller-created",
            "round-created",
            "round-promoted",
            "round-created",
            "round-caught",
            "controller-created",
            "round-created",
            "round-promoted",
            "round-created",
            "round-caught",
        ],
        "each attempt runs its own inner iter to the same capture: {rounds:?}"
    );

    // 旧部分结果清理后新 attempt：两次 attempt 各建一套 Each／Item Scope，身份不复用。
    let creations = test_support::boundary_creation_snapshot();
    let each_scopes: Vec<ScopeId> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Each)
        .map(|(child, _, _)| child.clone())
        .collect();
    assert_eq!(each_scopes.len(), 2, "one each per attempt");
    assert_ne!(each_scopes[0], each_scopes[1]);
    assert!(each_scopes[0].seq() < each_scopes[1].seq());
    let item_scopes: Vec<ScopeId> = creations
        .iter()
        .filter(|(_, _, role)| *role == ScopeRole::Item)
        .map(|(child, _, _)| child.clone())
        .collect();
    assert_eq!(item_scopes.len(), 6, "three items per attempt");
    assert!(
        item_scopes
            .windows(2)
            .all(|pair| pair[0].seq() < pair[1].seq()),
        "no item identity is reused across attempts"
    );
    let group = test_support::take_consume_group();
    assert_eq!(
        group
            .iter()
            .filter(
                |snapshot| snapshot.phase == test_support::ConsumeGroupSnapshotPhase::AfterCommit
            )
            .count(),
        4,
        "each attempt consumes its first two items before the retry cleans up: {group:?}"
    );
    let closed = test_support::closed_scope_snapshot();
    assert!(
        item_scopes.iter().all(|scope| closed.contains(scope)),
        "the first attempt's items were really cleaned before the second ran: {closed:?}"
    );

    observe::reset();
}

// ================================================================ P32

// P32(a)：item Body 内的不可恢复 Failure 是 terminal：停止余项与后续业务，不新建 item／轮次，
// 原动态对象与 source chain 保留。
#[test]
fn p32a_body_failure_is_terminal_and_keeps_the_dynamic_object() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let score = each.then(&score_of_item, item);
                each.then(&fail_on_thirty, score)
            })
        },
        vec![Item(1), Item(2), Item(3), Item(4)],
    ));

    let error = outcome.err().expect("a body failure is terminal");
    let RunError::Body(failure) = &error else {
        panic!("expected RunError::Body, got {error}");
    };
    let reason = failure.source().expect("the dynamic reason is reachable");
    assert_eq!(reason.to_string(), "rejected: business refusal");
    assert!(reason.downcast_ref::<Rejected>().is_some());
    assert!(
        failure.cleanup_source().is_none(),
        "no cleanup fault installed"
    );
    assert_eq!(
        node_log(),
        ["score", "fail", "score", "fail", "score", "fail"]
    );
    assert_eq!(role_count(ScopeRole::Item), 3, "no further item is created");

    let rounds = observe::take_controller_rounds();
    assert_eq!(
        each_ats(&rounds),
        [
            "scope-created",
            "item-created",
            "item-consumed",
            "item-created",
            "item-consumed",
            "item-created",
            "item-failed",
        ],
        "no further item and no finish: {rounds:?}"
    );
    assert!(
        observe::take_export_snapshots().is_empty(),
        "no partial collection is bound"
    );
    let third = item_scope(&rounds, "item-failed", 2);
    let each = each_scope(&rounds);
    let closed = test_support::closed_scope_snapshot();
    assert!(closed.contains(&third) && closed.contains(&each));
    assert_eq!(
        test_support::take_consume_group()
            .iter()
            .filter(
                |snapshot| snapshot.phase == test_support::ConsumeGroupSnapshotPhase::AfterCommit
            )
            .count(),
        2
    );
    observe::reset();
}

// P32(b)：item Body 内 Choose 无匹配且无 otherwise → terminal；不换分支、不新建 item。
#[test]
fn p32b_no_matching_case_is_terminal_and_switches_no_branch() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let score = each.then(&score_of_item, item);
                let selector = each.then(&selector_tenth, score);
                each.choose(selector, |choice| {
                    choice.case(1u32, |branch| branch.then(&keep_score, score));
                    choice.case(2u32, |branch| branch.then(&keep_score, score));
                })
            })
        },
        vec![Item(1), Item(2), Item(3), Item(4)],
    ));

    assert!(
        matches!(outcome, Err(RunError::NoMatchingCase)),
        "expected NoMatchingCase, got {outcome:?}"
    );
    assert_eq!(node_count("score"), 3);
    assert_eq!(node_count("select"), 3);
    assert_eq!(node_count("keep"), 2);
    assert_eq!(role_count(ScopeRole::Item), 3, "no further item is created");
    let selections = observe::take_choose_selections();
    assert_eq!(
        selections
            .iter()
            .map(|record| (record.selected, record.otherwise_used))
            .collect::<Vec<_>>(),
        [(Some(0), false), (Some(1), false), (None, false)],
        "the mismatch uses no branch at all"
    );
    let rounds = observe::take_controller_rounds();
    assert_eq!(
        each_ats(&rounds),
        [
            "scope-created",
            "item-created",
            "item-consumed",
            "item-created",
            "item-consumed",
            "item-created",
            "item-failed",
        ],
        "the no-match exit never reaches finish／export: {rounds:?}"
    );
    observe::reset();
}

// P32(c)：item Body 内控制器预算耗尽 → terminal；不新建下一轮／下一 item，字段保留。
#[test]
fn p32c_budget_exhaustion_is_terminal_inside_the_each() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                each.retry(1, |attempt| {
                    let candidate = attempt.then(&candidate_of_item, item);
                    attempt.then(&always_retry, candidate)
                })
            })
        },
        vec![Item(1), Item(2), Item(3)],
    ));

    let error = outcome.err().expect("budget exhaustion is terminal");
    let RunError::RetryExhausted {
        max_retries,
        attempts,
        last_error,
    } = &error
    else {
        panic!("expected RetryExhausted, got {error}");
    };
    assert_eq!((*max_retries, *attempts), (1, 2));
    assert_eq!(
        Error::source(last_error)
            .expect("the dynamic reason survives")
            .to_string(),
        "candidate 10 is not ready"
    );
    assert_eq!(node_log(), ["gen", "retry", "gen", "retry"]);
    assert_eq!(role_count(ScopeRole::Item), 1, "no further item is created");
    let rounds = observe::take_controller_rounds();
    let retry_ats: Vec<&'static str> = records(&rounds, "retry")
        .into_iter()
        .map(|record| record.at)
        .collect();
    assert_eq!(
        retry_ats,
        [
            "controller-created",
            "round-created",
            "round-caught",
            "round-created",
            "round-caught",
            "budget-exhausted",
        ],
        "the exhausted inner controller drives no further round: {rounds:?}"
    );
    assert_eq!(
        each_ats(&rounds),
        ["scope-created", "item-created", "item-failed"],
        "the budget error stops the item loop at the first item: {rounds:?}"
    );
    assert!(
        observe::take_export_snapshots().is_empty(),
        "no partial collection is bound"
    );
    observe::reset();
}

// ================================================================ P33

// P33(a)：无匹配边界时形成的未处理 Control 由 Root 分类为 UnhandledControl，不返回部分集合。
#[test]
fn p33a_unhandled_control_keeps_the_original_signal() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| flow.each(items, |item, each| each.then(&always_break, item)),
        vec![Item(1), Item(2), Item(3)],
    ));

    let error = outcome
        .err()
        .expect("an unmatched control escapes the root");
    assert!(
        matches!(
            error,
            RunError::UnhandledControl(ControlSignal::IterBreak(_))
        ),
        "expected UnhandledControl(IterBreak), got {error}"
    );
    assert_eq!(node_count("break"), 1, "the remaining items do not run");
    assert_eq!(role_count(ScopeRole::Item), 1);
    observe::reset();
}

// P33(b)：未处理 Control 与真实 Root 清理故障组合按 SPEC §12 → Runtime；原 ControlSignal 与
// 首清理诊断分别保留，不返回部分集合。
#[test]
fn p33b_unhandled_control_with_cleanup_fault_becomes_runtime() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    install_root_cleanup_fault();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, (_filler, items): (Ref<Filler>, Ref<Vec<Item>>)| {
            flow.each(items, |item, each| each.then(&always_break, item))
        },
        (Filler(0), vec![Item(1), Item(2), Item(3)]),
    ));

    let error = outcome.err().expect("the combined failure is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "unhandled control ended with a cleanup failure"
    );
    let signal = Error::source(runtime_error).expect("the original signal survives");
    assert!(
        signal.downcast_ref::<ControlSignal>().is_some(),
        "the original control signal is preserved as the framework source"
    );
    assert!(
        matches!(signal.to_string().as_str(), "iter break"),
        "the preserved signal is the real IterBreak, got {signal}"
    );
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(cleanup.to_string(), CLEANUP_INVARIANT);
    let root = root_frame_scope();
    let failures = test_support::take_cleanup_failures();
    assert_eq!(
        failures
            .iter()
            .map(|record| (record.scope.clone(), record.stored_first))
            .collect::<Vec<_>>(),
        vec![(root, true)],
        "the failing root scope is the single cleanup failure here: {failures:?}"
    );
    assert_eq!(
        expect_cleanup_invariant(&failures[0].error),
        "owned entry must exist until its scope closes"
    );
    assert_eq!(cleanup.to_string(), failures[0].error.to_string());
    assert_eq!(
        node_count("break"),
        1,
        "no partial collection is returned as output"
    );
    observe::reset();
}

// ================================================================ P34

// P34(a)：Item Control 退出阶段的窄真实清理故障（`install_control_cleanup_fault`）。责任未被
// 错误解除、Item 与 Each 都未 Closed；首框架与首清理诊断的具体身份（变体／字段／Scope）保留，
// 后续 Scope 的清理失败不覆盖首条；后续业务零。
#[test]
fn p34a_item_control_exit_cleanup_fault_keeps_first_diagnostic() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    install_control_cleanup_fault();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let score = each.then(&score_of_item, item);
                each.then(&break_on_thirty, score)
            })
        },
        vec![Item(1), Item(2), Item(3), Item(4)],
    ));

    let error = outcome.err().expect("a real cleanup failure is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(runtime_error.note(), "each item exit cleanup failed");
    let framework =
        Error::source(runtime_error).expect("the first framework diagnostic is preserved");
    assert_eq!(framework.to_string(), CLEANUP_INVARIANT);
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(cleanup.to_string(), CLEANUP_INVARIANT);

    let rounds = observe::take_controller_rounds();
    assert_eq!(
        each_ats(&rounds),
        [
            "scope-created",
            "item-created",
            "item-consumed",
            "item-created",
            "item-consumed",
            "item-created",
            "item-control-exit",
        ],
        "an item-level cleanup fault is terminal and never reaches the each exit: {rounds:?}"
    );
    let first = item_scope(&rounds, "item-consumed", 0);
    let second = item_scope(&rounds, "item-consumed", 1);
    let third = item_scope(&rounds, "item-control-exit", 2);
    let each = each_scope(&rounds);
    let root = root_frame_scope();

    // 责任未被错误解除：三个失败退出的 frame 各自仍负责自己的 Scope。
    let guard_failures = test_support::take_guard_failures();
    assert_eq!(
        guard_failures
            .iter()
            .map(|record| (record.frame_scope.clone(), record.responsible.clone()))
            .collect::<Vec<_>>(),
        vec![
            (Some(third.clone()), vec![third.clone()]),
            (Some(each.clone()), vec![each.clone()]),
            (Some(root.clone()), vec![root.clone()]),
        ],
        "no failing layer releases its own responsibility: {guard_failures:?}"
    );

    // 真实清理尝试：三层都真的失败（Item 的坏 entry 让整棵子树收口失败）。
    let guard_cleanups = test_support::take_guard_cleanups();
    assert_eq!(
        guard_cleanups
            .iter()
            .map(|record| (record.frame_scope.clone(), record.scope.clone(), record.ok))
            .collect::<Vec<_>>(),
        vec![
            (Some(third.clone()), third.clone(), false),
            (Some(each.clone()), each.clone(), false),
            (Some(root.clone()), root.clone(), false),
        ],
        "each layer's teardown really failed: {guard_cleanups:?}"
    );

    // 首框架／首清理诊断的身份：首条诊断落在第三个 Item 的真实 Scope 上，后续不覆盖。
    let failures = test_support::take_cleanup_failures();
    assert_eq!(
        failures
            .iter()
            .map(|record| (record.scope.clone(), record.stored_first))
            .collect::<Vec<_>>(),
        vec![
            (third.clone(), true),
            (each.clone(), false),
            (root.clone(), false),
        ],
        "the first cleanup is the item's; later scopes only register: {failures:?}"
    );
    assert_eq!(
        expect_cleanup_invariant(&failures[0].error),
        "owned entry must exist until its scope closes",
        "the stored first diagnostic is the real cleanup ScopeError, with its real field"
    );
    assert_eq!(cleanup.to_string(), failures[0].error.to_string());
    assert_eq!(framework.to_string(), failures[0].error.to_string());

    // 本域存活快照：Root 帧的错误退出观察点仍在 Root 域里保留了输入（未被错误宣告关闭）。
    let snapshots = test_support::take_root_snapshots();
    let failed_root = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == test_support::RootSnapshotPhase::AfterFailureCleanup)
        .expect("the root failure-exit snapshot is recorded");
    assert_ne!(
        failed_root.root_state,
        ScopeState::Closed,
        "the surviving domain is not falsely closed: {failed_root:?}"
    );
    assert_eq!(
        failed_root.root_owned.len(),
        1,
        "the root input keeps its owner at the failure instant: {failed_root:?}"
    );
    assert_eq!(
        failed_root.terminated,
        Some(crate::core::context::TerminationKind::BodyError)
    );

    // 生存状态：失败的清理 Scope 都不是 Closed（未错误宣告收口）。
    let states = observe::take_scope_states();
    assert_eq!(
        states
            .iter()
            .filter(|record| record.at == "control-cleanup-failed")
            .map(|record| record.scope.clone())
            .collect::<Vec<_>>(),
        vec![third.clone()],
        "the real failing scope is observed once"
    );
    let closed = test_support::closed_scope_snapshot();
    assert!(
        !closed.contains(&third) && !closed.contains(&each),
        "a failed cleanup never declares the scope closed: {closed:?}"
    );
    assert!(
        closed.contains(&first) && closed.contains(&second),
        "the genuinely consumed items did close"
    );

    // 后续业务零。
    assert_eq!(node_count("score"), 3);
    assert_eq!(node_count("gate"), 3);
    assert_eq!(role_count(ScopeRole::Item), 3);
    observe::reset();
}

// P34(b)：Each collector 退出阶段的窄真实清理故障（`install_each_control_cleanup_fault`）。阶段一
// 已真实结束（Item Closed），阶段二失败（Each 未 Closed、未解除责任）；首诊断是 Each 自己的
// Scope，Root 之后只登记不覆盖；后续业务零。
#[test]
fn p34b_each_collector_exit_cleanup_fault_keeps_first_diagnostic() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    install_each_control_cleanup_fault();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let score = each.then(&score_of_item, item);
                each.then(&break_on_thirty, score)
            })
        },
        vec![Item(1), Item(2), Item(3), Item(4)],
    ));

    let error = outcome.err().expect("a real cleanup failure is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(runtime_error.note(), "each boundary exit cleanup failed");
    let framework =
        Error::source(runtime_error).expect("the first framework diagnostic is preserved");
    assert_eq!(framework.to_string(), CLEANUP_INVARIANT);
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(cleanup.to_string(), CLEANUP_INVARIANT);

    let rounds = observe::take_controller_rounds();
    assert_eq!(
        each_ats(&rounds),
        [
            "scope-created",
            "item-created",
            "item-consumed",
            "item-created",
            "item-consumed",
            "item-created",
            "item-control-exit",
            "each-control-exit",
        ],
        "phase one finished normally; the fault only hits the each exit: {rounds:?}"
    );
    let each = each_scope(&rounds);
    let third = item_scope(&rounds, "item-control-exit", 2);
    let root = root_frame_scope();

    // 阶段一真实结束：Item 的收口成功且被观察为 Closed。
    let states = observe::take_scope_states();
    let item_closed = states
        .iter()
        .find(|record| record.at == "each-item-control-closed")
        .expect("the item close is observed before the each exit");
    assert_eq!(item_closed.scope, third);
    assert_eq!(item_closed.state, ScopeState::Closed);
    // 阶段二失败：失败清理的真实 Scope 是 Each，且未被宣告 Closed。
    let failing = states
        .iter()
        .find(|record| record.at == "control-cleanup-failed")
        .expect("the failing cleanup scope is observed");
    assert_eq!(failing.scope, each);
    assert_ne!(failing.state, ScopeState::Closed);

    let guard_failures = test_support::take_guard_failures();
    assert_eq!(
        guard_failures
            .iter()
            .map(|record| (record.frame_scope.clone(), record.responsible.clone()))
            .collect::<Vec<_>>(),
        vec![
            (Some(each.clone()), vec![each.clone()]),
            (Some(root.clone()), vec![root.clone()]),
        ],
        "the each keeps its own responsibility after the failed exit: {guard_failures:?}"
    );
    let guard_cleanups = test_support::take_guard_cleanups();
    assert_eq!(
        guard_cleanups
            .iter()
            .map(|record| (record.frame_scope.clone(), record.scope.clone(), record.ok))
            .collect::<Vec<_>>(),
        vec![
            (Some(each.clone()), each.clone(), false),
            (Some(root.clone()), root.clone(), false),
        ],
        "the each's teardown really failed, and the legal root layer took over: {guard_cleanups:?}"
    );

    let failures = test_support::take_cleanup_failures();
    assert_eq!(
        failures
            .iter()
            .map(|record| (record.scope.clone(), record.stored_first))
            .collect::<Vec<_>>(),
        vec![(each.clone(), true), (root.clone(), false)],
        "the first cleanup diagnostic is the each's own scope: {failures:?}"
    );
    assert_eq!(
        expect_cleanup_invariant(&failures[0].error),
        "owned entry must exist until its scope closes"
    );
    assert_eq!(cleanup.to_string(), failures[0].error.to_string());
    assert_eq!(framework.to_string(), failures[0].error.to_string());

    let closed = test_support::closed_scope_snapshot();
    assert!(
        closed.contains(&third),
        "phase one really closed the item: {closed:?}"
    );
    assert!(
        !closed.contains(&each),
        "the failed each exit never declares the each closed: {closed:?}"
    );

    // 本域存活快照：Each 退出失败后，Root 域的输入与状态仍在真实观察点上被保留。
    let snapshots = test_support::take_root_snapshots();
    let failed_root = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == test_support::RootSnapshotPhase::AfterFailureCleanup)
        .expect("the root failure-exit snapshot is recorded");
    assert_ne!(
        failed_root.root_state,
        ScopeState::Closed,
        "the surviving domain is not falsely closed: {failed_root:?}"
    );
    assert_eq!(
        failed_root.root_owned.len(),
        1,
        "the root input keeps its owner at the failure instant: {failed_root:?}"
    );
    assert_eq!(
        failed_root.terminated,
        Some(crate::core::context::TerminationKind::BodyError)
    );

    assert_eq!(node_count("score"), 3);
    assert_eq!(node_count("gate"), 3);
    assert_eq!(
        role_count(ScopeRole::Item),
        3,
        "no fourth item after the fault"
    );
    observe::reset();
}

// ================================================================ P35

/// 带 Drop 事件的集合元素（Root 输入逐实例 Drop 观察）。
#[derive(Data)]
struct TrackItem {
    value: u32,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for TrackItem {
    fn drop(&mut self) {
        self.log
            .borrow_mut()
            .push(format!("drop:item:{}", self.value));
    }
}

/// 带 Drop 事件的当前产物。
#[derive(Data)]
struct TrackScore {
    value: u32,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for TrackScore {
    fn drop(&mut self) {
        self.log
            .borrow_mut()
            .push(format!("drop:score:{}", self.value));
    }
}

/// 带 Drop 事件的 Root 输入。
#[derive(Data)]
struct TrackSeed {
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for TrackSeed {
    fn drop(&mut self) {
        self.log.borrow_mut().push(String::from("drop:seed"));
    }
}

/// 第一次 poll 返回真实 `Pending`；Drop 记录一次借用 Future 的结束。
struct PendingOnThird {
    yielded: bool,
    log: Rc<RefCell<Vec<String>>>,
}

impl Future for PendingOnThird {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl Drop for PendingOnThird {
    fn drop(&mut self) {
        self.log
            .borrow_mut()
            .push(String::from("drop:pending-future"));
    }
}

/// 第三项：先产生当前产物（挂起期间由借用 Future 持有），再进入真 Pending。
async fn produce_then_pend(
    query: Query<(&TrackItem, &TrackSeed)>,
) -> Result<TrackScore, BodyError> {
    let (item, seed) = query.get();
    let score = TrackScore {
        value: item.value * 10,
        log: Rc::clone(&seed.log),
    };
    if item.value == 3 {
        PendingOnThird {
            yielded: false,
            log: Rc::clone(&seed.log),
        }
        .await;
    }
    Ok(score)
}

// P35：已收两项、第三项真 Pending；挂起时当前产物仍活；取消后借用 Future 先结束，当前产物／
// collector 部分成果／Root 输入逐实例 Drop 恰一次；无 detached child。
#[test]
fn p35_pending_cancel_ends_the_borrow_first_and_drops_each_instance_once() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let mut future = Box::pin(runtime.execute(
        |flow, (items, seed): (Ref<Vec<TrackItem>>, Ref<TrackSeed>)| {
            flow.each(items, |item, each| {
                each.then(&produce_then_pend, (item, seed))
            })
        },
        (
            vec![
                TrackItem {
                    value: 1,
                    log: Rc::clone(&log),
                },
                TrackItem {
                    value: 2,
                    log: Rc::clone(&log),
                },
                TrackItem {
                    value: 3,
                    log: Rc::clone(&log),
                },
            ],
            TrackSeed {
                log: Rc::clone(&log),
            },
        ),
    ));

    // 真 Pending：第一次 poll 不返回 Ready，也没有用 sleep／立即 Ready 模拟。
    let waker = futures::task::noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    assert!(
        matches!(future.as_mut().poll(&mut cx), Poll::Pending),
        "the third item really suspends"
    );
    assert!(
        log.borrow().is_empty(),
        "nothing is dropped while the pending node is suspended: {:?}",
        log.borrow()
    );

    // 挂起时：已收两项（collector moves 到达 2），第三项的 Item 已建立但未 Consume。
    let rounds = observe::take_controller_rounds();
    assert_eq!(
        each_ats(&rounds),
        [
            "scope-created",
            "item-created",
            "item-consumed",
            "item-created",
            "item-consumed",
            "item-created",
        ],
        "the third item is entered but not consumed: {rounds:?}"
    );
    let third = item_scope(&rounds, "item-created", 2);
    let group = test_support::take_consume_group();
    assert_eq!(
        group
            .iter()
            .filter(
                |snapshot| snapshot.phase == test_support::ConsumeGroupSnapshotPhase::AfterCommit
            )
            .map(|snapshot| snapshot.slots[0].collector_moves)
            .collect::<Vec<_>>(),
        [Some(1), Some(2)],
        "the collector holds the first two products while suspended: {group:?}"
    );

    drop(future);

    // 取消：借用 Future 先结束，然后当前产物、collector 部分成果、Root 输入逐实例恰好一次。
    let events = log.borrow().clone();
    let position = |needle: &str| {
        events
            .iter()
            .position(|event| event == needle)
            .unwrap_or_else(|| panic!("missing drop event {needle}: {events:?}"))
    };
    let pending = position("drop:pending-future");
    assert!(
        pending < position("drop:score:30"),
        "the borrowed future ends before its owner's values are released: {events:?}"
    );
    assert!(
        pending < position("drop:score:10") && pending < position("drop:score:20"),
        "the borrowed future ends before the collector's partial results: {events:?}"
    );
    assert!(
        pending < position("drop:item:1"),
        "the borrowed future ends before the root inputs: {events:?}"
    );
    for expected in [
        "drop:pending-future",
        "drop:score:30",
        "drop:score:10",
        "drop:score:20",
        "drop:item:1",
        "drop:item:2",
        "drop:item:3",
        "drop:seed",
    ] {
        assert_eq!(
            events.iter().filter(|event| *event == expected).count(),
            1,
            "{expected} drops exactly once: {events:?}"
        );
    }
    assert_eq!(events.len(), 8, "no extra instance: {events:?}");

    // 无 detached child：取消后全部真实建立的 Scope 都已关闭。
    let closed = test_support::closed_scope_snapshot();
    let created: Vec<ScopeId> = test_support::boundary_creation_snapshot()
        .into_iter()
        .map(|(child, _, _)| child)
        .collect();
    assert!(
        created.contains(&third),
        "the suspended item scope is a real created scope"
    );
    assert!(
        created.iter().all(|scope| closed.contains(scope)),
        "every created scope is closed after cancellation: {closed:?}"
    );
    assert_eq!(
        node_count("score"),
        0,
        "the cancellation runs no further node"
    );
    observe::reset();
}

// ================================================================ P36

/// 第一域的 Each 产物（复合返回值，供跨执行身份访问使用）。
struct FirstDomain {
    root: ScopeId,
    item: ScopeId,
    collector: CollectorId,
    data: DataId,
}

/// 真实运行一个 Each，取得其 ItemScope／collector／DataId 完整身份。
fn run_first_domain() -> FirstDomain {
    let runtime = Runtime::new();
    let out = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let score = each.then(&score_of_item, item);
                each.then(&keep_score, score)
            })
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .expect("the first domain runs");
    assert_eq!(
        out.iter().map(|score| score.0).collect::<Vec<_>>(),
        [10, 20, 30]
    );
    let root = root_frame_scope();
    let rounds = observe::take_controller_rounds();
    let item = item_scope(&rounds, "item-consumed", 0);
    let collector = test_support::take_consume_group()
        .iter()
        .find_map(|snapshot| snapshot.slots[0].collector.clone())
        .expect("the destination collector identity is observed");
    let data = test_support::take_log_snapshot()
        .into_iter()
        .next()
        .expect("a real taken DataId is observed");
    FirstDomain {
        root,
        item,
        collector,
        data,
    }
}

// P36(a)：相同序号的跨执行真实访问（Scope／Data／Collector）被 typed 拒绝，本域完整快照不变。
#[test]
fn p36a_foreign_domain_accesses_are_typed_rejections() {
    observe::reset();
    test_support::reset_observations();
    test_support::take_log_reset();
    reset_nodes();
    let first = run_first_domain();

    // 第二次执行：在真实 caller frame 中尝试访问第一域身份。
    observe::reset();
    install_foreign_access_fault(first.item.clone(), first.data.clone());
    let runtime = Runtime::new();
    let second = block_on(runtime.execute(
        |flow, amount: Ref<Amount>| flow.chain(|sub| sub.then(&double_amount, amount)),
        Amount(5),
    ))
    .expect("the second domain runs");
    assert_eq!(second, Amount(10));
    let second_root = root_frame_scope();
    assert!(
        !std::sync::Arc::ptr_eq(first.root.execution(), second_root.execution()),
        "the two executions have different identity roots"
    );
    assert_eq!(
        first.root.seq(),
        second_root.seq(),
        "the same sequence number is issued independently per execution"
    );
    assert_ne!(
        first.root, second_root,
        "the same sequence number is a different identity"
    );
    assert_ne!(
        first.item.execution().as_ref() as *const _,
        second_root.execution().as_ref() as *const _
    );

    let accesses = observe::take_foreign_accesses();
    assert_eq!(accesses.len(), 2, "scope and data probes are recorded");
    let scope_access = &accesses[0];
    assert_eq!(scope_access.operation, "state(foreign-scope)");
    assert!(scope_access.rejected);
    assert!(
        scope_access.detail.contains("belongs to another execution"),
        "original framework diagnostic: {}",
        scope_access.detail
    );
    assert_eq!(
        scope_access.local_refs_before,
        scope_access.local_refs_after
    );
    assert_eq!(
        scope_access.local_owned_before,
        scope_access.local_owned_after
    );
    let data_access = &accesses[1];
    assert_eq!(data_access.operation, "alive/owner(foreign-data)");
    assert!(data_access.rejected);
    assert!(
        data_access.detail.contains("alive=false"),
        "the foreign instance is not visible in this domain: {}",
        data_access.detail
    );
    assert_eq!(data_access.local_refs_before, data_access.local_refs_after);
    assert_eq!(
        data_access.local_owned_before,
        data_access.local_owned_after
    );

    // Collector 外域：在第二域真实 Context 上访问第一域的 collector 身份 → typed 拒绝。
    let second_domain = RootExecution::start();
    let local_root = second_domain.context().root_scope();
    let before = second_domain
        .context()
        .snapshot_targets_probe(&local_root)
        .expect("local snapshot");
    let error = second_domain
        .context()
        .collector_moves_probe(&first.collector)
        .expect_err("a foreign collector is rejected");
    match &error {
        ScopeError::Storage {
            source: InternalError::CollectorForeignExecution { requested, .. },
        } => assert_eq!(
            requested, &first.collector,
            "the rejected identity is the real collector"
        ),
        other => panic!("expected CollectorForeignExecution, got {other:?}"),
    }
    assert!(
        error.to_string().contains("belongs to another execution"),
        "original framework diagnostic: {error}"
    );
    let after = second_domain
        .context()
        .snapshot_targets_probe(&local_root)
        .expect("local snapshot");
    assert_eq!(before, after, "the local domain snapshot is unchanged");

    observe::reset();
}

// P36(b)：Item cap 的真实身份 —— 每个 item 的 lifetime cap 就是它自己的 ItemScope；旧 Item
// Closed 之后新建 Item 拿到更大的身份，旧 cap 从不恢复。
#[test]
fn p36b_item_cap_identity_is_never_reused() {
    observe::reset();
    test_support::reset_observations();
    reset_nodes();
    let runtime = Runtime::new();
    let out = block_on(runtime.execute(
        |flow, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| each.then(&score_of_item, item))
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .expect("the cap sample runs");
    assert_eq!(
        out.iter().map(|score| score.0).collect::<Vec<_>>(),
        [10, 20, 30]
    );

    let rounds = observe::take_controller_rounds();
    let each = each_scope(&rounds);
    let group = test_support::take_consume_group();
    let mut caps: Vec<(ScopeId, ScopeId, usize)> = Vec::new();
    for snapshot in &group {
        if snapshot.phase != test_support::ConsumeGroupSnapshotPhase::AfterCommit {
            continue;
        }
        assert_eq!(snapshot.each.as_ref(), Some(&each));
        let cap = snapshot
            .item_refs
            .iter()
            .find_map(|(_, target)| match target {
                TargetSnapshot::CollectionItem { lifetime_cap, .. } => Some(lifetime_cap.clone()),
                TargetSnapshot::Data(_) => None,
            })
            .expect("the item binding is a real CollectionItem target");
        let index = snapshot
            .item_refs
            .iter()
            .find_map(|(_, target)| match target {
                TargetSnapshot::CollectionItem { index, .. } => Some(*index),
                TargetSnapshot::Data(_) => None,
            })
            .unwrap();
        assert_eq!(
            cap, snapshot.item,
            "the item's cap is its own ItemScope: {snapshot:?}"
        );
        caps.push((snapshot.item.clone(), cap, index));
    }
    assert_eq!(caps.len(), 3, "one commit per item: {group:?}");
    assert_eq!(
        caps.iter().map(|(_, _, index)| *index).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(
        caps.windows(2)
            .all(|pair| pair[0].1.seq() < pair[1].1.seq()),
        "a later item never reuses an earlier cap: {caps:?}"
    );
    let caps_only: Vec<ScopeId> = caps.iter().map(|(_, cap, _)| cap.clone()).collect();
    let mut unique = caps_only.clone();
    unique.sort_by_key(|scope| scope.seq());
    unique.dedup();
    assert_eq!(unique.len(), 3, "each cap is a distinct identity");

    // 旧 Item 已真实 Closed，身份序列没有回卷，也没有任何新 Scope 复活旧 cap。
    let closed = test_support::closed_scope_snapshot();
    assert_eq!(
        caps_only.iter().filter(|cap| closed.contains(cap)).count(),
        3,
        "all three item caps really closed: {closed:?}"
    );
    assert_eq!(
        test_support::boundary_creation_snapshot()
            .into_iter()
            .filter(|(_, _, role)| *role == ScopeRole::Item)
            .count(),
        3,
        "no further item reuses a closed cap"
    );
    observe::reset();
}
