//! WE-T05 Each 静态／Definition 侧定向证据（P01～P08、P12）。
//!
//! 只使用真实入口：Definition 侧经 [`build_definition`]（与 `Runtime::execute` 同一条构建
//! 路径）或真实 [`Runtime::execute`]；不模拟执行器、不伪造底层返回。
//!
//! 观察点（全部为既有 cfg(test) 通道；take 型接口是一次性语义，本模块每样本起点复位）：
//! - [`observe`]：`take_controller_rounds`（Each 的 `scope-created`／`item-created`／
//!   `item-consumed`／`exported` 逐项记录）、`take_export_snapshots`（Each 整组 Export 前后的
//!   caller 快照，含 `next_data_id`）、`take_chain_imports`（深层捕获的逐层 Import 对）、
//!   `take_frames`（root／leaf frame）、`take_choose_selections`；
//! - [`test_support`]：`boundary_creation_snapshot`（真实 `(child, parent, role)`）、
//!   `boundary_address_snapshot`（唯一 Context／Coordinator／Container 地址）、
//!   `take_consume_group`（成组 Consume 的 Item／Each／collector owner 快照）、
//!   `closed_scope_snapshot`、`take_root_snapshots`、`take_log_snapshot`；
//! - `creation_counts`：Context／Coordinator／Container 创建次数与 frame 事件序列。
//!
//! 不在本文件内声明已覆盖：非法 T／O 形状、17 槽与 nested Ref tuple 的**编译拒绝**由 UI
//! 夹具承担；P09～P11、P13～P40 的运行期／成组 Consume／Control 证据由
//! `tests_each_consume.rs`／`tests_each_control.rs` 承担。

#![allow(clippy::unused_unit, clippy::let_unit_value, clippy::type_complexity)]

use super::data::Data;
use super::definition::{PositionSource, StepKind, build_definition};
use super::error::{BodyError, BuildErrorDetail, RunError, Visibility};
use super::flow::Flow;
use super::identity::{BodyId, DefinitionId, PositionHandle, PositionId};
use super::node::Node;
use super::observe;
use super::runtime::Runtime;
use super::shape::{EachShape, Query, Ref, ShapeOps, TypeDesc};
use crate::core::context::creation_counts;
use crate::core::orchestrator::ScopeRole;
use crate::core::ref_id::RefId;
use crate::core::scope::{ScopeState, TargetSnapshot};
use crate::core::test_support::{self, ConsumeGroupSnapshotPhase, RootSnapshotPhase};
use futures::executor::block_on;
use std::cell::{Cell, RefCell};

// ---------------------------------------------------------------- 业务类型与 Node

/// 集合元素（只读借用对象）。
#[derive(Debug, PartialEq, Eq)]
struct Mark(u32);

impl Data for Mark {}

/// 非 Clone／Copy 的集合元素（P07 的“借用而非复制”证据）。
#[derive(Debug)]
struct Heavy(u32);

impl Data for Heavy {}

/// 非 Clone／Copy 的业务输出。
#[derive(Debug, PartialEq, Eq)]
struct Score(u32);

impl Data for Score {}

#[derive(Debug, PartialEq, Eq)]
struct Report(u32);

impl Data for Report {}

#[derive(Debug, PartialEq, Eq)]
struct Rules(u32);

impl Data for Rules {}

#[derive(Debug, PartialEq, Eq)]
struct Total(u64);

impl Data for Total {}

#[derive(Debug, PartialEq, Eq)]
struct Key(u32);

impl Data for Key {}

// ---------------------------------------------------------------- 计数与复位

thread_local! {
    static BODY_CALLS: Cell<usize> = const { Cell::new(0) };
    static ITEM_BODY_BUILDS: Cell<usize> = const { Cell::new(0) };
    /// P03 运行期样本：Retry 节点连续失败计数（成功即复位），驱动真实重试次数。
    static FLAKY_FAILS: Cell<usize> = const { Cell::new(0) };
    /// P03 运行期样本：内层 Each 逐项真实看到的输入值（按调用顺序）。
    static INNER_ITEM_INPUTS: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    /// P03 运行期样本：Choose 分支真实消费到的 Chain 声明输出值（按调用顺序）。
    static CHAIN_OUTPUTS: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
    static HEAVY_BORROWS: RefCell<Vec<(usize, usize, *const (), *const ())>> =
        const { RefCell::new(Vec::new()) };
}

fn bump_body_calls() {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
}

/// 业务 Node 的真实调用次数（与“item 闭包构建次数”分开计量）。
fn body_calls() -> usize {
    BODY_CALLS.with(Cell::get)
}

fn bump_item_body_builds() {
    ITEM_BODY_BUILDS.with(|calls| calls.set(calls.get() + 1));
}

/// item 闭包（每次 `Flow::each` 登记时调用一次）的次数。
fn item_body_builds() -> usize {
    ITEM_BODY_BUILDS.with(Cell::get)
}

fn heavy_borrows() -> Vec<(usize, usize, *const (), *const ())> {
    HEAVY_BORROWS.with(|records| records.borrow().clone())
}

/// 复位本模块使用的全部观察通道（含 Core 侧与 `observe` 侧的一次性 take 通道）。
fn reset_all() {
    observe::reset();
    test_support::reset_observations();
    test_support::take_log_reset();
    let _ = creation_counts::take_events();
    BODY_CALLS.with(|calls| calls.set(0));
    ITEM_BODY_BUILDS.with(|calls| calls.set(0));
    FLAKY_FAILS.with(|fails| fails.set(0));
    INNER_ITEM_INPUTS.with(|log| log.borrow_mut().clear());
    CHAIN_OUTPUTS.with(|log| log.borrow_mut().clear());
    HEAVY_BORROWS.with(|records| records.borrow_mut().clear());
}

async fn score_of(query: Query<(&Mark, &Rules)>) -> Result<Score, BodyError> {
    bump_body_calls();
    let (mark, rules) = query.get();
    Ok(Score(mark.0 + rules.0))
}

async fn report_of(query: Query<(&Mark, &Score)>) -> Result<Report, BodyError> {
    bump_body_calls();
    let (mark, score) = query.get();
    Ok(Report(mark.0 * score.0))
}

async fn count_marks(query: Query<&Mark>) -> Result<Mark, BodyError> {
    bump_body_calls();
    Ok(Mark(query.get().0))
}

async fn count_unit(query: Query<&Mark>) -> Result<(), BodyError> {
    bump_body_calls();
    let _ = query.get();
    Ok(())
}

async fn key_of(query: Query<&Mark>) -> Result<Key, BodyError> {
    Ok(Key(query.get().0 % 2))
}

async fn key_of_collection(query: Query<&Vec<Mark>>) -> Result<Key, BodyError> {
    Ok(Key(query.get().len() as u32))
}

async fn sum_scores(query: Query<&Vec<Score>>) -> Result<Total, BodyError> {
    Ok(Total(
        query.get().iter().map(|score| u64::from(score.0)).sum(),
    ))
}

async fn first_report(query: Query<&Vec<Report>>) -> Result<Report, BodyError> {
    let reports = query.get();
    Ok(Report(reports.first().expect("non-empty reports").0))
}

/// 只读两个位置（深层捕获样本使用；不计数）。
async fn read_pair(query: Query<(&Mark, &Rules)>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// 只读集合本身（深层捕获样本使用；不计数）。
async fn read_marks(query: Query<&Vec<Mark>>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

/// 记录“当前 item 借用指向原集合哪个元素、该元素地址、集合地址与长度”，并返回一个非 Clone
/// 输出。`ptr::eq` 在同一个 `Vec<Heavy>` 上比较，因此这就是“item 借用地址 = 原 Vec 元素
/// 地址”的真实判据。
async fn note_heavy(query: Query<(&Heavy, &Vec<Heavy>)>) -> Result<Score, BodyError> {
    bump_body_calls();
    let (item, items) = query.get();
    let index = items
        .iter()
        .position(|candidate| std::ptr::eq(candidate, item))
        .expect("the item borrow is an element of the original collection");
    let element = std::ptr::from_ref(item) as *const ();
    let collection = std::ptr::from_ref(items) as *const ();
    let length = items.len() as u32;
    HEAVY_BORROWS.with(|records| {
        records
            .borrow_mut()
            .push((index, items.len(), element, collection))
    });
    Ok(Score(item.0 + 10 * length))
}

// ------------------------------------------------- P03 运行期样本：业务节点与 Fragment

/// Retry 控制的动态原因（携带真实原因对象，不被字符串化重建）。
#[derive(Debug)]
struct NotReady(usize);

impl std::fmt::Display for NotReady {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "attempt {} is not ready", self.0)
    }
}

impl std::error::Error for NotReady {}

/// 结构体 Node：把 item 的 `Score` 翻倍（SPEC §7 Fragment 的被捕获 Node 句柄）。
struct Doubler;

impl Node for Doubler {
    type Input = Score;
    type Output = Score;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        bump_body_calls();
        Ok(Score(query.get().0 * 2))
    }
}

/// 常驻 `Doubler`：Fragment 按普通 lifetime 借用的定义期 Node 句柄（`'static` 可满足任意 `'n`）。
static DOUBLER: Doubler = Doubler;

/// SPEC §7 首选 Fragment：普通函数返回 closure，保留非 static 的 Node 句柄借用寿命。
fn frag_double<'n>(node: &'n Doubler, input: Ref<Score>) -> impl FnOnce(&mut Flow<'n>) + 'n {
    move |sub| {
        let out_refs = sub.then_with_output(node, input);
        sub.output_with(out_refs);
    }
}

/// Choose 命中分支（`Key(0)`）：`Score + 10`。
async fn add_ten(query: Query<&Score>) -> Result<Score, BodyError> {
    bump_body_calls();
    let value = query.get().0;
    CHAIN_OUTPUTS.with(|log| log.borrow_mut().push(value));
    Ok(Score(value + 10))
}

/// Choose otherwise 分支：`Score + 100`。
async fn add_hundred(query: Query<&Score>) -> Result<Score, BodyError> {
    bump_body_calls();
    let value = query.get().0;
    CHAIN_OUTPUTS.with(|log| log.borrow_mut().push(value));
    Ok(Score(value + 100))
}

/// 前两次返回显式 `Retry`（携带动态原因），第三次成功并复位失败计数——每个 item 真实重试 2 次。
async fn flaky(query: Query<&Score>) -> Result<Score, BodyError> {
    bump_body_calls();
    let fails = FLAKY_FAILS.with(Cell::get);
    if fails < 2 {
        FLAKY_FAILS.with(|counter| counter.set(fails + 1));
        Err(BodyError::retry(NotReady(fails + 1)))
    } else {
        FLAKY_FAILS.with(|counter| counter.set(0));
        Ok(Score(query.get().0))
    }
}

/// Iter 的 next：低于 4 则 +1 继续；达到 4 时发出 `IterBreak` 交付进入本轮时的 current。
async fn step_toward_four(query: Query<&Score>) -> Result<Score, BodyError> {
    bump_body_calls();
    let value = query.get().0;
    if value >= 4 {
        Err(BodyError::iter_break())
    } else {
        Ok(Score(value + 1))
    }
}

/// 合并 Retry 结果与 Iter 结果，作为 item body 的一个声明输出槽。
async fn combine(query: Query<(&Score, &Score)>) -> Result<Score, BodyError> {
    bump_body_calls();
    let (left, right) = query.get();
    Ok(Score(left.0 + right.0))
}

/// 内层 Each 的逐项节点：记录真实输入值并原样输出（内层聚合槽的来源）。
async fn inner_mark(query: Query<&Mark>) -> Result<Mark, BodyError> {
    bump_body_calls();
    let value = query.get().0;
    INNER_ITEM_INPUTS.with(|log| log.borrow_mut().push(value));
    Ok(Mark(value))
}

/// 取内层聚合 `Vec<Mark>` 的首个元素作为 item body 的第二个声明输出槽。
async fn first_mark(query: Query<&Vec<Mark>>) -> Result<Mark, BodyError> {
    bump_body_calls();
    Ok(Mark(
        query
            .get()
            .first()
            .expect("the inner collection is non-empty")
            .0,
    ))
}

/// P03 的唯一 body 构建函数：`build_definition` 与 `Runtime::execute` 共用它，因此静态登记与
/// 运行期执行必然是同一份计划。item body 内依次使用 then／chain(Fragment)／choose／retry／
/// iter／嵌套 each，并产出两个声明槽 `(Ref<Score>, Ref<Mark>)`。
fn full_item_flow<'n>(
    flow: &mut Flow<'n>,
    (items, rules, inner, seed): (Ref<Vec<Mark>>, Ref<Rules>, Ref<Vec<Mark>>, Ref<Score>),
) {
    let out_refs = flow.each(items, |item, each| {
        bump_item_body_builds();
        // 1) then：item 与祖先 `Rules` 共同产生 Score。
        let scored = each.then_with_output(&score_of, (item, rules));
        // 2) chain + Fragment：关闭 child 边界后只保留翻倍后的声明输出。
        let doubled: Ref<Score> = each.chain_output(frag_double(&DOUBLER, scored));
        // 3) choose：按 item 奇偶真实选取一条分支（另一分支只登记、不建立运行 Scope）。
        let selector = each.then_with_output(&key_of, item);
        let chosen = each.choose(selector, |choice| {
            choice.case(Key(0), |branch| branch.then_with_output(&add_ten, doubled));
            choice.otherwise(|branch| branch.then_with_output(&add_hundred, doubled));
        });
        // 4) retry：最后一次 attempt 才成功的真实重试。
        let retried: Ref<Score> = each.retry_output(2, |attempt| {
            let out_refs = attempt.then_with_output(&flaky, chosen);
            attempt.output_with(out_refs);
        });
        // 5) iter：固定初始状态逐轮推进，第 5 轮以 IterBreak 交付 current。
        let iter_done = each.iter(seed, 5, |current, round| {
            round.then_with_output(&step_toward_four, current)
        });
        let total = each.then_with_output(&combine, (retried, iter_done));
        // 6) 嵌套 each：对独立内层集合逐项执行，输出再交给普通节点。
        let inner_vec = each.each(inner, |nested_item, nested| {
            bump_item_body_builds();
            nested.then_with_output(&inner_mark, nested_item)
        });
        let first = each.then_with_output(&first_mark, inner_vec);
        (total, first)
    });
    flow.output_with(out_refs);
}

// ================================================================ P01：冻结自然用法

/// P01：SPEC §8 的冻结用法（`each.then(&node, (item, capture))` 两次 + 二元返回）可构建、
/// 可执行；Each 的自然返回形状可交给后续 `then`／`Chain`／Root。
#[test]
fn p01_frozen_usage_feeds_later_then_chain_and_root() {
    reset_all();
    let (total, head) = block_on(Runtime::new().execute::<_, (Total, Report), _>(
        |flow, (items, rules): (Ref<Vec<Mark>>, Ref<Rules>)| {
            let (scores, reports) = flow.each(items, |item, each| {
                let score = each.then_with_output(&score_of, (item, rules));
                let report = each.then_with_output(&report_of, (item, score));
                (score, report)
            });
            // 后续 then 直接消费 Each 的自然返回形状 `Ref<Vec<Score>>`。
            let total = flow.then_with_output(&sum_scores, scores);
            // 后续 Chain 的 child 深层捕获另一个聚合槽 `Ref<Vec<Report>>`。
            let head: Ref<Report> = flow.chain_output(|sub| {
                let out_refs = sub.then_with_output(&first_report, reports);
                sub.output_with(out_refs);
            });
            let out_refs = (total, head);
            flow.output_with(out_refs);
        },
        (vec![Mark(1), Mark(2), Mark(3)], Rules(10)),
    ))
    .expect("the frozen SPEC §8 usage must build and run");

    assert_eq!(
        total,
        Total(11 + 12 + 13),
        "per item score = element + ancestor"
    );
    // element = 1、score = 11，故首个 report = 1 * 11 = 11。
    assert_eq!(head, Report(11), "first report = element * score");
    assert_eq!(body_calls(), 6, "two business Nodes per item, three items");
}

/// P01（静态面）：冻结用法只登记一个 Each Step，集合是唯一静态输入，两个输出槽为独立的
/// `Vec` 类型；item Body 完整登记且 parent 是使用它的 Body。
#[test]
fn p01_frozen_usage_registers_one_each_step_with_declared_slots() {
    let definition = build_definition::<(Vec<Mark>, Rules), (), _>(|flow, (items, rules)| {
        let (scores, reports) = flow.each(items, |item, each| {
            let score = each.then_with_output(&score_of, (item, rules));
            let report = each.then_with_output(&report_of, (item, score));
            (score, report)
        });
        let _ = (scores, reports);

        flow.output_with(());
    })
    .expect("the frozen usage builds");

    let root = definition.root();
    assert_eq!(root.steps().len(), 1);
    assert_eq!(root.steps()[0].kind(), StepKind::Each);
    assert_eq!(
        root.steps()[0].inputs().len(),
        1,
        "the collection is the single static input of the Each Step"
    );
    assert_eq!(
        root.steps()[0].inputs()[0].type_desc,
        TypeDesc::of::<Vec<Mark>>()
    );
    assert_eq!(root.steps()[0].outputs().len(), 2);

    let step = root.steps()[0].each().expect("each step view");
    let item_body = definition.body(step.item());
    assert_eq!(item_body.parent(), Some(root.id()));
    assert_eq!(
        item_body.steps().len(),
        2,
        "the full item body is registered"
    );
    assert_eq!(item_body.steps()[0].kind(), StepKind::Node);
    assert_eq!(item_body.steps()[1].kind(), StepKind::Node);

    assert_eq!(step.slots().len(), 2);
    assert_eq!(step.slots()[0].collected_type, TypeDesc::of::<Vec<Score>>());
    assert_eq!(
        step.slots()[1].collected_type,
        TypeDesc::of::<Vec<Report>>()
    );
    assert_ne!(
        step.slots()[0].source,
        step.slots()[1].source,
        "the two declared slots keep independent source positions"
    );
    assert_ne!(step.slots()[0].local, step.slots()[1].local);
    assert_ne!(step.slots()[0].output, step.slots()[1].output);
    for (slot, declared) in step.slots().iter().zip(item_body.output().iter()) {
        assert_eq!(
            slot.source,
            *definition
                .record(declared.handle)
                .expect("the declared item output is a registered position")
                .core(),
            "each slot's consume source is the item body's declared output"
        );
    }

    let items: Vec<_> = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::EachItem { .. }))
        .collect();
    assert_eq!(items.len(), 1, "one formal item position per Each Step");
    assert_eq!(items[0].owner(), step.item());
    assert_eq!(items[0].type_desc(), TypeDesc::of::<Mark>());
    assert_eq!(items[0].handle().position, step.item_slot());
}

// ================================================================ P02：形状映射

/// P02（crate 内可判定部分）：`Collected` 的逐槽映射——unit 零槽、单 Ref → `Vec`、flat tuple
/// 逐叶变 `Vec` 且同类型不合并、Box tuple 是单个 Data 叶子。
#[test]
fn p02_collected_shapes_map_slot_by_slot_without_merging() {
    assert!(
        <<() as EachShape>::Collected as ShapeOps>::type_descs().is_empty(),
        "unit Each has zero collected slots"
    );
    assert_eq!(
        <<Ref<Score> as EachShape>::Collected as ShapeOps>::type_descs(),
        vec![TypeDesc::of::<Vec<Score>>()]
    );
    assert_eq!(
        <<(Ref<Score>, Ref<Report>) as EachShape>::Collected as ShapeOps>::type_descs(),
        vec![TypeDesc::of::<Vec<Score>>(), TypeDesc::of::<Vec<Report>>()]
    );
    // 同类型两槽：类型列表相同，但仍是两个不同位置（不按 TypeId 合并）。
    assert_eq!(
        <<(Ref<Score>, Ref<Score>) as EachShape>::Collected as ShapeOps>::type_descs(),
        vec![TypeDesc::of::<Vec<Score>>(), TypeDesc::of::<Vec<Score>>()]
    );
    let definition = DefinitionId::fresh();
    let mut handles = [PositionId::new(7), PositionId::new(8)]
        .into_iter()
        .map(|position| PositionHandle {
            definition,
            position,
        });
    let (first, second) =
        <<(Ref<Score>, Ref<Score>) as EachShape>::Collected as ShapeOps>::from_positions(
            &mut handles,
        );
    assert_ne!(first.handle().position, second.handle().position);
    // Box tuple 是单个 Data 叶子，不拆 native tuple（推定：§3.3 单业务值 + §8）。
    assert_eq!(
        <<Ref<Box<(u8, u16)>> as EachShape>::Collected as ShapeOps>::type_descs(),
        vec![TypeDesc::of::<Vec<Box<(u8, u16)>>>()]
    );
    assert_eq!(
        <<(Ref<u8>, Ref<u16>) as EachShape>::Collected as ShapeOps>::type_descs().len(),
        2,
        "a native tuple is two slots, not one leaf"
    );
}

/// P02：unit／单槽／2 槽／16 槽的 Definition 级正例；16 个同类型槽仍各有独立位置。
#[test]
fn p02_definition_registers_unit_single_two_and_sixteen_slots() {
    // unit：零槽，不登记任何 Each 输出／Each 本地位置。
    let definition = build_definition::<Vec<Mark>, (), _>(|flow, items| {
        let _collected: () = flow.each(items, |item, each| {
            let _: () = each.then_with_output(&count_unit, item);
        });

        flow.output_with(());
    })
    .expect("a unit each builds");
    assert!(definition.root().steps()[0].outputs().is_empty());
    assert!(
        definition.root().steps()[0]
            .each()
            .expect("each step view")
            .slots()
            .is_empty()
    );
    assert!(!definition.positions().iter().any(|record| matches!(
        record.source(),
        PositionSource::EachLocal { .. } | PositionSource::EachOutput { .. }
    )));

    // 单槽：O = Ref<Mark> → Collected = Ref<Vec<Mark>>。
    let definition = build_definition::<Vec<Mark>, (), _>(|flow, items| {
        let collected: Ref<Vec<Mark>> = flow.each(items, |item, each| {
            each.then_with_output(&count_marks, item)
        });
        let _ = collected;

        flow.output_with(());
    })
    .expect("a single-slot each builds");
    let outputs: Vec<_> = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::EachOutput { slot: 0, .. }))
        .collect();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].type_desc(), TypeDesc::of::<Vec<Mark>>());

    // 2 槽同类型：两个槽的 source／local／output 全是不同位置。
    let definition = build_definition::<Vec<Mark>, (), _>(|flow, items| {
        let _collected = flow.each(items, |item, each| {
            let first = each.then_with_output(&count_marks, item);
            let second = each.then_with_output(&count_marks, item);
            (first, second)
        });

        flow.output_with(());
    })
    .expect("two same-type slots build");
    let step = definition.root().steps()[0].each().expect("each step view");
    assert_eq!(step.slots().len(), 2);
    assert!(
        step.slots()
            .iter()
            .all(|slot| slot.collected_type == TypeDesc::of::<Vec<Mark>>())
    );
    assert_ne!(step.slots()[0].source, step.slots()[1].source);
    assert_ne!(step.slots()[0].local, step.slots()[1].local);
    assert_ne!(step.slots()[0].output, step.slots()[1].output);

    // 16 槽（上限）：完整 flat tuple 正例。
    let definition = build_definition::<Vec<Mark>, (), _>(|flow, items| {
        let _collected = flow.each(items, |item, each| {
            let s0 = each.then_with_output(&count_marks, item);
            let s1 = each.then_with_output(&count_marks, item);
            let s2 = each.then_with_output(&count_marks, item);
            let s3 = each.then_with_output(&count_marks, item);
            let s4 = each.then_with_output(&count_marks, item);
            let s5 = each.then_with_output(&count_marks, item);
            let s6 = each.then_with_output(&count_marks, item);
            let s7 = each.then_with_output(&count_marks, item);
            let s8 = each.then_with_output(&count_marks, item);
            let s9 = each.then_with_output(&count_marks, item);
            let s10 = each.then_with_output(&count_marks, item);
            let s11 = each.then_with_output(&count_marks, item);
            let s12 = each.then_with_output(&count_marks, item);
            let s13 = each.then_with_output(&count_marks, item);
            let s14 = each.then_with_output(&count_marks, item);
            let s15 = each.then_with_output(&count_marks, item);
            (
                s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15,
            )
        });

        flow.output_with(());
    })
    .expect("sixteen flat slots build");
    let root = definition.root();
    assert_eq!(root.steps()[0].outputs().len(), 16);
    let step = root.steps()[0].each().expect("each step view");
    assert_eq!(step.slots().len(), 16);
    let mut sources: Vec<RefId> = step
        .slots()
        .iter()
        .map(|slot| slot.source.clone())
        .collect();
    sources.sort_by_key(RefId::seq);
    sources.dedup();
    assert_eq!(
        sources.len(),
        16,
        "all sixteen slots keep independent sources"
    );
    let mut locals: Vec<RefId> = step.slots().iter().map(|slot| slot.local.clone()).collect();
    locals.sort_by_key(RefId::seq);
    locals.dedup();
    assert_eq!(locals.len(), 16);
    let mut outputs: Vec<RefId> = step
        .slots()
        .iter()
        .map(|slot| slot.output.clone())
        .collect();
    outputs.sort_by_key(RefId::seq);
    outputs.dedup();
    assert_eq!(outputs.len(), 16);
}

// ================================================================ P03：统一 Step 与完整 Body

/// P03：Each 是统一有序 Step；item Body 是完整 Body（then／chain／choose／retry／iter／嵌套
/// each 都受支持）；只生成一份计划（逐 Body 只登记一次，顺序单调）。
#[test]
fn p03_each_is_a_unified_step_with_a_full_body_and_a_single_plan() {
    let definition = build_definition::<Vec<Mark>, (), _>(|flow, items| {
        let _collected: () = flow.each(items, |item, each| {
            let _: () = each.then_with_output(&count_unit, item);
            let chained: Ref<Mark> = each.chain_output(|sub| {
                let out_refs = sub.then_with_output(&count_marks, item);
                sub.output_with(out_refs);
            });
            let selector = each.then_with_output(&key_of, item);
            each.choose(selector, |choice| {
                choice.case(Key(0), |branch| {
                    let _: () = branch.then_with_output(&count_unit, item);
                });
                choice.otherwise(|branch| {
                    let _: () = branch.then_with_output(&count_unit, item);
                });
            });
            each.retry(1, |attempt| {
                let _: () = attempt.then_with_output(&count_unit, item);
                attempt.output::<()>();
            });
            each.iter(item, 2, |current, round| {
                round.then_with_output(&count_unit, current);
                current
            });
            each.each(items, |nested_item, nested| {
                let _: () = nested.then_with_output(&count_unit, nested_item);
            });
            let _ = chained;
        });

        flow.output_with(());
    })
    .expect("a full item body with every child capability builds");

    let root = definition.root();
    assert_eq!(
        root.steps().len(),
        1,
        "one ordered Each Step in the root body"
    );
    assert_eq!(root.steps()[0].kind(), StepKind::Each);

    let outer = root.steps()[0].each().expect("each step view");
    let item_body = definition.body(outer.item());
    let kinds: Vec<StepKind> = item_body.steps().iter().map(|step| step.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            StepKind::Node,
            StepKind::Chain,
            StepKind::Node,
            StepKind::Choose,
            StepKind::Retry,
            StepKind::Iter,
            StepKind::Each,
        ],
        "the full item body keeps every declared Step in definition order"
    );

    let orders: Vec<u64> = item_body.steps().iter().map(|step| step.order()).collect();
    assert!(
        orders.windows(2).all(|pair| pair[0] < pair[1]),
        "step orders increase monotonically: {orders:?}"
    );

    // 嵌套 Each 的 item Body 同样只登记一次。
    let nested = item_body.steps()[6].each().expect("nested each step view");
    let nested_body = definition.body(nested.item());
    assert_eq!(nested_body.parent(), Some(item_body.id()));
    assert_eq!(nested_body.steps().len(), 1);
    assert_eq!(nested_body.steps()[0].kind(), StepKind::Node);

    // 单一计划：Body 数与逐类 Step 数固定，没有第二份重复登记。
    assert_eq!(
        definition.bodies().len(),
        8,
        "root + item + chain + 2 choose branches + retry attempt + iter round + nested item"
    );
    assert_eq!(
        definition
            .positions()
            .iter()
            .filter(|record| matches!(record.source(), PositionSource::EachItem { .. }))
            .count(),
        2,
        "one formal item position per Each Step"
    );
    assert!(
        !definition.positions().iter().any(|record| matches!(
            record.source(),
            PositionSource::EachLocal { .. } | PositionSource::EachOutput { .. }
        )),
        "both Each Steps are unit: no collector or final-vec positions"
    );

    // Chain 的 child 是 item Body 的词法后代，并深层捕获正式 item 位置。
    let chain = item_body.steps()[1].chain().expect("chain step view");
    assert!(
        definition
            .body(chain.child())
            .imports()
            .contains(&outer.item_slot())
    );
}

/// P03（运行期）：item body 在一次真实 `Runtime::execute` 中同时执行 then／chain(Fragment)／
/// choose／retry／iter／嵌套 each，且 Definition 只登记一份计划（bodies／steps 数与静态登记
/// 一致；同一静态 Body 每 Body 只构建一次、多次执行），外层 Each 的逐槽聚合仍正确。
#[test]
fn p03_item_body_executes_nested_flows_and_fragment_under_one_plan() {
    // 静态面：与 execute 共用 `full_item_flow`，所以登记的就是运行期那份计划。
    let definition =
        build_definition::<(Vec<Mark>, Rules, Vec<Mark>, Score), (Vec<Score>, Vec<Mark>), _>(
            full_item_flow,
        )
        .expect("the full item body builds");
    assert_eq!(
        definition.bodies().len(),
        8,
        "root + outer item + chain child + 2 choose branches + retry attempt + iter round + nested item"
    );
    let root = definition.root();
    assert_eq!(
        root.steps().len(),
        1,
        "one ordered Each Step in the root body"
    );
    assert_eq!(root.steps()[0].kind(), StepKind::Each);
    let outer = root.steps()[0].each().expect("each step view");
    assert_eq!(
        outer.slots().len(),
        2,
        "declared slots: Ref<Score> + Ref<Mark>"
    );
    assert_eq!(
        outer.slots()[0].collected_type,
        TypeDesc::of::<Vec<Score>>()
    );
    assert_eq!(outer.slots()[1].collected_type, TypeDesc::of::<Vec<Mark>>());

    let item_body = definition.body(outer.item());
    let kinds: Vec<StepKind> = item_body.steps().iter().map(|step| step.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            StepKind::Node,
            StepKind::Chain,
            StepKind::Node,
            StepKind::Choose,
            StepKind::Retry,
            StepKind::Iter,
            StepKind::Node,
            StepKind::Each,
            StepKind::Node,
        ],
        "then, chain(Fragment), choose selector, choose, retry, iter, combine, nested each, first"
    );
    let nested = item_body.steps()[7].each().expect("nested each step view");
    let nested_body = definition.body(nested.item());
    assert_eq!(nested_body.parent(), Some(item_body.id()));
    assert_eq!(nested_body.steps().len(), 1);
    assert_eq!(nested_body.steps()[0].kind(), StepKind::Node);
    // 每个 Each 恰好一个 formal item 位置；没有任何重复登记（不生成第二份计划）。
    assert_eq!(
        definition
            .positions()
            .iter()
            .filter(|record| matches!(record.source(), PositionSource::EachItem { .. }))
            .count(),
        2,
        "one formal item position per Each Step"
    );

    // 运行期：真实执行同一 body 构建函数。
    reset_all();
    let (scores, firsts) = block_on(Runtime::new().execute::<_, (Vec<Score>, Vec<Mark>), _>(
        full_item_flow,
        (
            vec![Mark(1), Mark(2), Mark(3)],
            Rules(10),
            vec![Mark(7), Mark(8)],
            Score(0),
        ),
    ))
    .expect("the full item body runs");

    // 外层 Each 输出与逐槽聚合：score 槽经由 then→chain→choose→retry→iter→combine；
    // mark 槽来自内层 Each 聚合后的首值。
    assert_eq!(
        scores,
        vec![Score(126), Score(38), Score(130)],
        "score slot: item1 (11*2+100)+4, item2 (12*2+10)+4, item3 (13*2+100)+4"
    );
    assert_eq!(
        firsts,
        vec![Mark(7), Mark(7), Mark(7)],
        "mark slot: first element of the inner aggregate"
    );

    // 内层 Each 真执行：每个外层 item 对 [7,8] 逐项执行一次；输出值是真实元素。
    assert_eq!(
        INNER_ITEM_INPUTS.with(|log| log.borrow().clone()),
        vec![7, 8, 7, 8, 7, 8],
        "the nested Each runs its body per inner element, per outer item"
    );
    // 单一静态 Body 被复用：两个 item Body 各构建一次（外层 1 + 内层 1），业务节点执行 45 次。
    assert_eq!(
        item_body_builds(),
        2,
        "the outer and nested item bodies are built once each, not per item"
    );
    assert_eq!(
        body_calls(),
        45,
        "3 score + 3 double + 3 add + 9 flaky + 15 iter + 3 combine + 6 inner + 3 first"
    );

    // Choose 真实分支选择：item1/3 走 otherwise，item2 命中 case 0。
    let choices = observe::take_choose_selections();
    assert_eq!(choices.len(), 3, "one selection per outer item");
    assert_eq!(
        choices
            .iter()
            .map(|record| record.selected)
            .collect::<Vec<_>>(),
        vec![None, Some(0), None]
    );
    assert_eq!(
        choices
            .iter()
            .map(|record| record.otherwise_used)
            .collect::<Vec<_>>(),
        vec![true, false, true]
    );

    // Chain 边界：翻倍后的声明输出被后续 choose 真实消费（每外层 item 一次），
    // 证明 Chain 只保留声明输出并把它交给下一步。
    assert_eq!(
        CHAIN_OUTPUTS.with(|log| log.borrow().clone()),
        vec![22, 24, 26],
        "choose consumes the Chain's declared output (2 * (item + rules))"
    );

    // 不生成第二执行计划：唯一的 Each 边界（外层 1 + 内层 3）、item（3 + 6）与控制器轮次。
    let rounds = observe::take_controller_rounds();
    let round_count = |kind: &str, at: &str| {
        rounds
            .iter()
            .filter(|record| record.kind == kind && record.at == at)
            .count()
    };
    assert_eq!(
        round_count("each", "scope-created"),
        4,
        "outer 1 + nested 3"
    );
    assert_eq!(round_count("each", "item-created"), 9, "outer 3 + nested 6");
    // Retry 真实重试：每个 item 3 次 attempt（2 次重试后成功）。
    assert_eq!(round_count("retry", "controller-created"), 3);
    assert_eq!(round_count("retry", "round-created"), 9);
    assert_eq!(
        round_count("retry", "round-caught"),
        6,
        "two retries per item"
    );
    assert_eq!(round_count("retry", "round-normal"), 3);
    assert_eq!(round_count("retry", "budget-exhausted"), 0);
    // Iter 真实轮次：每个 item 5 轮，末轮以 IterBreak 交付 current。
    assert_eq!(round_count("iter", "controller-created"), 3);
    assert_eq!(
        round_count("iter", "round-created"),
        15,
        "five rounds per item"
    );
    assert_eq!(
        round_count("iter", "round-caught"),
        3,
        "IterBreak once per item"
    );
    assert_eq!(round_count("iter", "budget-exhausted"), 0);
}

// ================================================================ P04：formal item 来源与可见性

/// P04-a：parent Body 使用 formal item → Definition 拒绝（`OutOfScope(ChildLocal)`，owner
/// 是 item Body），业务 Node 与 Root 创建均为 0。
#[test]
fn p04_parent_use_of_formal_item_is_rejected() {
    reset_all();
    let before = creation_counts::snapshot();
    let outcome = block_on(Runtime::new().execute::<_, Mark, _>(
        |flow, items: Ref<Vec<Mark>>| {
            let mut escaped: Option<Ref<Mark>> = None;
            let _collected: () = flow.each(items, |item, each| {
                escaped = Some(item);
                let _: () = each.then_with_output(&count_unit, item);
            });
            let leaked = escaped.expect("captured the formal item");
            let out_refs = flow.then_with_output(&count_marks, leaked);
            flow.output_with(out_refs);
        },
        vec![Mark(1)],
    ));
    let error = match outcome {
        Err(RunError::Definition(build)) => build.detail,
        Ok(_) => panic!("parent use of the formal item must be rejected"),
        Err(other) => panic!("expected a Definition rejection, got {other}"),
    };
    assert_eq!(
        error,
        BuildErrorDetail::OutOfScope {
            body: BodyId::ROOT,
            step: Some(1),
            position: PositionId::new(1),
            owner: BodyId::new(1),
            relation: Visibility::ChildLocal,
        },
        "the parent Body must be rejected with the item Body as owner"
    );
    assert_eq!(
        body_calls(),
        0,
        "no business Node runs for an invalid Definition"
    );
    assert_eq!(
        creation_counts::snapshot(),
        before,
        "no Context (hence no Root) is created for a rejected Definition"
    );
    assert!(test_support::boundary_creation_snapshot().is_empty());
    assert!(observe::take_frames().is_empty());
    assert!(test_support::take_log_snapshot().is_empty());
}

/// P04-b：formal item 作为 Root 输出外泄 → Definition 拒绝，零业务 Node。
#[test]
fn p04_root_output_leak_of_formal_item_is_rejected() {
    reset_all();
    let before = creation_counts::snapshot();
    let outcome = block_on(Runtime::new().execute::<_, Mark, _>(
        |flow, items: Ref<Vec<Mark>>| {
            let mut escaped: Option<Ref<Mark>> = None;
            let _collected: () = flow.each(items, |item, each| {
                escaped = Some(item);
                let _: () = each.then_with_output(&count_unit, item);
            });
            let out_refs = escaped.expect("captured the formal item");
            flow.output_with(out_refs);
        },
        vec![Mark(1)],
    ));
    let error = match outcome {
        Err(RunError::Definition(build)) => build.detail,
        Ok(_) => panic!("a Root output leak must be rejected"),
        Err(other) => panic!("expected a Definition rejection, got {other}"),
    };
    assert_eq!(
        error,
        BuildErrorDetail::OutOfScope {
            body: BodyId::ROOT,
            step: None,
            position: PositionId::new(1),
            owner: BodyId::new(1),
            relation: Visibility::ChildLocal,
        },
        "a Root output use has no Step context and is still ChildLocal"
    );
    assert_eq!(body_calls(), 0);
    assert_eq!(creation_counts::snapshot(), before);
    assert!(test_support::boundary_creation_snapshot().is_empty());
}

/// P04-c：sibling Body 使用 formal item → Definition 拒绝（`OutOfScope(Sibling)`）。
#[test]
fn p04_sibling_use_of_formal_item_is_rejected() {
    reset_all();
    let outcome = block_on(Runtime::new().execute::<_, (), _>(
        |flow, items: Ref<Vec<Mark>>| {
            let mut escaped: Option<Ref<Mark>> = None;
            let _first: () = flow.chain_output(|sub| {
                let _collected: () = sub.each(items, |item, each| {
                    escaped = Some(item);
                    let _: () = each.then_with_output(&count_unit, item);
                });
                sub.output::<()>();
            });
            let sibling_local = escaped.expect("captured the formal item");
            let () = flow.chain_output(move |sub| {
                let _: () = sub.then_with_output(&count_unit, sibling_local);
                sub.output::<()>();
            });
            flow.output_with(());
        },
        vec![Mark(1)],
    ));
    let error = match outcome {
        Err(RunError::Definition(build)) => build.detail,
        Ok(_) => panic!("sibling use of the formal item must be rejected"),
        Err(other) => panic!("expected a Definition rejection, got {other}"),
    };
    assert_eq!(
        error,
        BuildErrorDetail::OutOfScope {
            body: BodyId::new(3),
            step: Some(0),
            position: PositionId::new(1),
            owner: BodyId::new(2),
            relation: Visibility::Sibling,
        },
        "a sibling subtree use is rejected as Sibling"
    );
    assert_eq!(body_calls(), 0);
}

/// P04-d：来自另一个 Definition 的位置作为集合来源 → `ForeignDefinition` 拒绝。
#[test]
fn p04_position_from_another_definition_is_rejected() {
    reset_all();
    let mut foreign: Option<Ref<Vec<Mark>>> = None;
    let _reference = build_definition::<Vec<Mark>, Vec<Mark>, _>(|_flow, items| {
        foreign = Some(items);
        let out_refs = items;
        _flow.output_with(out_refs);
    })
    .expect("the reference definition builds");
    let foreign = foreign.expect("captured the reference definition's collection ref");

    let error = build_definition::<Vec<Mark>, (), _>(|flow, _items| {
        let _collected: () = flow.each(foreign, |item, each| {
            let _: () = each.then_with_output(&count_unit, item);
        });

        flow.output_with(());
    })
    .err()
    .expect("a foreign-definition collection must be rejected");
    match error {
        BuildErrorDetail::ForeignDefinition {
            body,
            step,
            position,
            expected,
            found,
        } => {
            assert_eq!(body, BodyId::ROOT);
            assert_eq!(step, Some(0));
            assert_eq!(
                position,
                PositionId::new(0),
                "the reference root input position"
            );
            assert_ne!(expected, found, "the two Definition identities differ");
        }
        other => panic!("expected ForeignDefinition, got {other}"),
    }
    assert_eq!(body_calls(), 0);
}

/// P04-e：前向来源——集合句柄指向本次构建的 Each 自身输出位置（在该使用点之后才登记）→
/// `NotYetAvailable` 拒绝。
///
/// 序号取自形状相同的参照 Definition（位置分配与反例逐槽一致），不靠手写偏移量。
#[test]
fn p04_forward_collection_position_is_rejected() {
    reset_all();
    let reference = build_definition::<Vec<Mark>, (), _>(|flow, items| {
        let _collected: Ref<Vec<Mark>> = flow.each(items, |item, each| {
            each.then_with_output(&count_marks, item)
        });

        flow.output_with(());
    })
    .expect("the reference definition builds");
    let forward = reference
        .positions()
        .iter()
        .find(|record| {
            matches!(
                record.source(),
                PositionSource::EachOutput { step: 0, slot: 0 }
            )
        })
        .expect("the reference Each has a caller output position")
        .handle()
        .position;

    let error = build_definition::<Vec<Mark>, (), _>(|flow, _items| {
        let definition = flow.definition_id();
        let forged = Ref::<Vec<Mark>>::from_handle(PositionHandle {
            definition,
            position: forward,
        });
        let _collected: Ref<Vec<Mark>> = flow.each(forged, |item, each| {
            each.then_with_output(&count_marks, item)
        });

        flow.output_with(());
    })
    .err()
    .expect("a forward collection position must be rejected");
    assert_eq!(
        error,
        BuildErrorDetail::NotYetAvailable {
            body: BodyId::ROOT,
            step: Some(0),
            position: forward,
        }
    );
    assert_eq!(body_calls(), 0);
}

/// P04-f：未选 Choose 分支内的 child-local 位置作为 Each 集合来源 → Definition 在静态阶段
/// 拒绝（分支 Node 不执行），`OutOfScope(ChildLocal)`。
#[test]
fn p04_unselected_branch_child_local_as_collection_is_rejected() {
    reset_all();
    let mut forged: Option<Ref<Vec<Mark>>> = None;
    let mut forged_position: Option<PositionId> = None;
    let outcome = block_on(Runtime::new().execute::<_, (), _>(
        |flow, items: Ref<Vec<Mark>>| {
            let selector = flow.then_with_output(&key_of_collection, items);
            flow.choose(selector, |choice| {
                choice.case(Key(2), |branch| {
                    branch.then_with_output(&key_of_collection, items)
                });
                choice.case(Key(1), |branch| {
                    let definition = branch.definition_id();
                    let forward = branch.next_position_id();
                    forged_position = Some(forward);
                    forged = Some(Ref::<Vec<Mark>>::from_handle(PositionHandle {
                        definition,
                        position: forward,
                    }));
                    // 该分支真实登记这个位置（Node 输出），因此其 owner 就是分支 Body。
                    branch.then_with_output(&key_of_collection, items)
                });
            });
            let collection = forged.expect("captured the unselected branch's position");
            let _collected: () = flow.each(collection, |item, each| {
                let _: () = each.then_with_output(&count_unit, item);
            });

            flow.output_with(());
        },
        vec![Mark(1), Mark(2)],
    ));
    let error = match outcome {
        Err(RunError::Definition(build)) => build.detail,
        Ok(_) => panic!("an unselected branch position must not become the collection"),
        Err(other) => panic!("expected a Definition rejection, got {other}"),
    };
    assert_eq!(
        error,
        BuildErrorDetail::OutOfScope {
            // steps: 0 = selector Node, 1 = Choose, 2 = the Each under test.
            body: BodyId::ROOT,
            step: Some(2),
            position: forged_position.expect("captured the forged position"),
            // the second registered branch Body is the owner of the forged position.
            owner: BodyId::new(2),
            relation: Visibility::ChildLocal,
        },
        "the branch body owns the position and the use site is its ancestor"
    );
    assert_eq!(
        body_calls(),
        0,
        "the unselected branch's Node must not run for an invalid Definition"
    );
    assert!(observe::take_choose_selections().is_empty());
}

/// P04 正例：item Body 合法深层捕获祖先 Ref 与正式 item（经 Chain），Definition 通过并真实
/// 执行（合法捕获不因来源检查被误拒）。
#[test]
fn p04_legal_deep_capture_inside_item_body_runs() {
    reset_all();
    let collected = block_on(Runtime::new().execute::<_, (), _>(
        |flow, (items, rules): (Ref<Vec<Mark>>, Ref<Rules>)| {
            let _: () = flow.each(items, |item, each| {
                each.chain(|sub| {
                    let scored = sub.then_with_output(&score_of, (item, rules));
                    let out_refs = sub.then_with_output(&report_of, (item, scored));
                    sub.output_with(out_refs);
                });
            });

            flow.output_with(());
        },
        (vec![Mark(1), Mark(2)], Rules(5)),
    ))
    .expect("legal deep capture builds and runs");
    assert_eq!(collected, ());
    assert_eq!(body_calls(), 4, "two nodes per item, two items");
}

// ================================================================ P05：构建一次、运行期复用

/// P05：两次 `execute`（不同长度）各构建一次 item Body；业务 Node 计数随长度变化；同一静态
/// item 位置在不同 ItemScope 重复绑定。
#[test]
fn p05_body_builds_once_per_execute_and_rebinds_per_item_scope() {
    let runtime = Runtime::new();

    // 第一次执行：2 项。
    reset_all();
    let first = block_on(runtime.execute::<_, Vec<Mark>, _>(
        |flow, items: Ref<Vec<Mark>>| {
            let out_refs = flow.each(items, |item, each| {
                bump_item_body_builds();
                each.then_with_output(&count_marks, item)
            });
            flow.output_with(out_refs);
        },
        vec![Mark(1), Mark(2)],
    ))
    .expect("the first execution runs");
    assert_eq!(first, vec![Mark(1), Mark(2)]);
    assert_eq!(
        item_body_builds(),
        1,
        "the item closure is built exactly once"
    );
    assert_eq!(body_calls(), 2, "one business Node per item");
    let records = observe::take_controller_rounds();
    let items: Vec<_> = records
        .iter()
        .filter(|record| record.kind == "each" && record.at == "item-created")
        .map(|record| (record.index, record.round.clone().expect("item scope")))
        .collect();
    assert_eq!(items.len(), 2);
    assert_eq!((items[0].0, items[1].0), (0, 1));
    assert_ne!(items[0].1, items[1].1, "each item gets its own ItemScope");
    assert!(records.iter().all(|record| record.kind == "each"));

    // 第二次执行：3 项。
    reset_all();
    let second = block_on(runtime.execute::<_, Vec<Mark>, _>(
        |flow, items: Ref<Vec<Mark>>| {
            let out_refs = flow.each(items, |item, each| {
                bump_item_body_builds();
                each.then_with_output(&count_marks, item)
            });
            flow.output_with(out_refs);
        },
        vec![Mark(3), Mark(4), Mark(5)],
    ))
    .expect("the second execution runs");
    assert_eq!(second, vec![Mark(3), Mark(4), Mark(5)]);
    assert_eq!(
        item_body_builds(),
        1,
        "each execute builds its own item Body exactly once"
    );
    assert_eq!(
        body_calls(),
        3,
        "the business count follows the collection length"
    );
    assert_eq!(
        observe::take_controller_rounds()
            .iter()
            .filter(|record| record.kind == "each" && record.at == "item-created")
            .count(),
        3
    );

    // 同一静态 item RefId 在不同 ItemScope 重复绑定：每项 item_refs 里恰有一个
    // CollectionItem 目标，位置相同、cap 是各自的 ItemScope、index 逐项递增。
    let groups = test_support::take_consume_group();
    let before: Vec<_> = groups
        .iter()
        .filter(|snapshot| snapshot.phase == ConsumeGroupSnapshotPhase::Before)
        .collect();
    assert_eq!(before.len(), 3, "one group consume per item");
    let mut aliases: Vec<(RefId, usize, crate::core::identity::ScopeId)> = Vec::new();
    for snapshot in before {
        let mut aliased: Vec<_> = snapshot
            .item_refs
            .iter()
            .filter(|(_, target)| matches!(target, TargetSnapshot::CollectionItem { .. }))
            .collect();
        assert_eq!(
            aliased.len(),
            1,
            "exactly one formal item alias per ItemScope"
        );
        let (position, target) = aliased.remove(0);
        match target {
            TargetSnapshot::CollectionItem {
                index,
                lifetime_cap,
                ..
            } => {
                assert_eq!(
                    *lifetime_cap, snapshot.item,
                    "the alias cap is this ItemScope"
                );
                aliases.push((position.clone(), *index, snapshot.item.clone()));
            }
            other => panic!("expected a CollectionItem alias, got {other:?}"),
        }
    }
    aliases.sort_by_key(|(_, index, _)| *index);
    assert_eq!(
        aliases
            .iter()
            .map(|(_, index, _)| *index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2],
        "the formal item re-binds to element index by index"
    );
    assert!(
        aliases.windows(2).all(|pair| pair[0].0 == pair[1].0),
        "the same static item RefId is reused in every ItemScope"
    );
    assert_ne!(aliases[0].2, aliases[1].2);
    assert_ne!(aliases[1].2, aliases[2].2);
}

/// P05（静态面）：item Body 只登记一次；形式 item 是单一逻辑位置（不按 index 动态分配）；
/// Body 的 parent 是包含该 Each 的 Body。
#[test]
fn p05_one_item_body_and_one_formal_item_position_per_each() {
    let definition = build_definition::<Vec<Mark>, (), _>(|flow, items| {
        let _collected = flow.each(items, |item, each| {
            let first = each.then_with_output(&count_marks, item);
            each.then_with_output(&count_marks, first)
        });

        flow.output_with(());
    })
    .expect("each builds");
    let step = definition.root().steps()[0].each().expect("each step view");
    let records: Vec<_> = definition
        .positions()
        .iter()
        .filter(|record| {
            matches!(
                record.source(),
                PositionSource::EachItem { step: 0, slot: 0 }
            )
        })
        .collect();
    assert_eq!(records.len(), 1, "one logical item position for all Items");
    assert_eq!(records[0].handle().position, step.item_slot());
    assert_eq!(records[0].owner(), step.item());
    assert_eq!(records[0].type_desc(), TypeDesc::of::<Mark>());
    assert_eq!(
        definition.body(step.item()).parent(),
        Some(definition.root().id())
    );
}

// ================================================================ P06：真实 frame 与执行域

/// P06：真实 caller → Each → Item Scope 链（role／parent／单调序号／同一身份根）、frame 退出
/// 顺序，以及一次执行只有一个 Context／Coordinator／Container。
#[test]
fn p06_real_scopes_frames_and_single_execution_domain() {
    reset_all();
    let output = block_on(Runtime::new().execute::<_, Vec<Mark>, _>(
        |flow, items: Ref<Vec<Mark>>| {
            let out_refs = flow.each(items, |item, each| {
                each.then_with_output(&count_marks, item)
            });
            flow.output_with(out_refs);
        },
        vec![Mark(1), Mark(2)],
    ))
    .expect("each runs");
    assert_eq!(output, vec![Mark(1), Mark(2)]);

    let creations = test_support::boundary_creation_snapshot();
    assert_eq!(creations.len(), 3, "one EachScope + one ItemScope per item");
    let (each, caller, role) = &creations[0];
    assert_eq!(*role, ScopeRole::Each);
    assert_eq!(creations[1].2, ScopeRole::Item);
    assert_eq!(creations[2].2, ScopeRole::Item);
    assert_eq!(
        creations[1].1, *each,
        "ItemScope is a direct child of EachScope"
    );
    assert_eq!(creations[2].1, *each);
    assert_ne!(creations[1].0, creations[2].0, "distinct ItemScopes");
    assert_ne!(creations[1].0, *each);
    assert!(
        caller.seq() < each.seq() && each.seq() < creations[1].0.seq(),
        "ScopeId sequence is monotonic and never reused"
    );
    assert!(
        creations[1].0.seq() < creations[2].0.seq(),
        "items are created in index order"
    );
    for (child, _, _) in &creations {
        assert!(
            std::sync::Arc::ptr_eq(child.execution(), caller.execution()),
            "every Scope in the run shares one execution identity root"
        );
    }

    let addresses = test_support::boundary_address_snapshot();
    assert_eq!(addresses.len(), 3);
    let (_, identity, coordinator, container) = addresses[0];
    assert!(!identity.is_null() && !coordinator.is_null() && !container.is_null());
    assert!(
        addresses
            .iter()
            .all(|(_, i, c, ct)| *i == identity && *c == coordinator && *ct == container),
        "one Context / coordinator / container for the whole execution"
    );

    // frame：业务 Node 在真实 ItemScope 的 leaf frame 内执行，root frame 包住全程。
    let frames = observe::take_frames();
    let kinds: Vec<(&str, &str)> = frames
        .iter()
        .map(|frame| (frame.kind, frame.phase))
        .collect();
    assert_eq!(kinds.first().copied(), Some(("root", "enter")));
    assert_eq!(kinds.last().copied(), Some(("root", "exit")));
    let item_scopes: Vec<_> = creations[1..]
        .iter()
        .map(|(child, _, _)| child.clone())
        .collect();
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame.kind == "leaf" && frame.phase == "enter")
            .count(),
        2,
        "one leaf frame per item business Node"
    );
    assert!(
        frames
            .iter()
            .filter(|frame| frame.kind == "leaf" && frame.phase == "enter")
            .all(|frame| item_scopes.contains(&frame.scope)),
        "every leaf frame uses a real ItemScope"
    );

    // frame 退出顺序由真实 guard 收口事件给出（LIFO，且无 frame 关系违反）。
    let events = creation_counts::take_events();
    assert!(
        !events
            .iter()
            .any(|event| event == "frame-relation-violated"),
        "every frame relation check passed: {events:?}"
    );
    let position_of = |needle: String| {
        events
            .iter()
            .position(|event| *event == needle)
            .unwrap_or_else(|| panic!("missing event {needle} in {events:?}"))
    };
    let item0_exit = position_of(format!("frame-exit:boundary:{}", creations[1].0.seq()));
    let item1_exit = position_of(format!("frame-exit:boundary:{}", creations[2].0.seq()));
    let each_exit = position_of(format!("frame-exit:boundary:{}", each.seq()));
    let root_exit = position_of(format!("frame-exit:root:{}", caller.seq()));
    assert!(item0_exit < item1_exit && item1_exit < each_exit && each_exit < root_exit);
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("cleanup-start:") || event.starts_with("guard-cleanup:")),
        "the normal path runs no guard cleanup: {events:?}"
    );

    // 逐项生命周期顺序：Each 建立 → 逐项 (created → consumed) → exported。
    let records = observe::take_controller_rounds();
    let timeline: Vec<&str> = records
        .iter()
        .filter(|record| record.kind == "each")
        .map(|record| record.at)
        .collect();
    assert_eq!(
        timeline,
        vec![
            "scope-created",
            "item-created",
            "item-consumed",
            "item-created",
            "item-consumed",
            "exported",
        ],
        "items are consumed before the next Item is created"
    );
    assert!(
        records.iter().all(|record| record.kind != "each"
            || (record.caller == *caller && record.controller == *each)),
        "every Each record names the real caller and Each Scope"
    );

    // 两个 Item 都真实关闭（含 Each）。
    let closed = test_support::closed_scope_snapshot();
    for scope in [each.clone(), creations[1].0.clone(), creations[2].0.clone()] {
        assert!(closed.contains(&scope), "{scope:?} must be closed");
    }
}

/// P06（collector owner）：成组 Consume 只有在“当前 frame 就是该 Item、其调用父 frame 是该
/// Each、且全部 collector 由该 Each 负责”时才成功，因此真实成功即是 frame 链的证据；快照
/// 另外给出可断言的字段值（Item／Each Scope、collector owner、逐项移动数）。
#[test]
fn p06_group_consume_reports_the_item_and_its_each_owner() {
    reset_all();
    let output = block_on(Runtime::new().execute::<_, (Vec<Mark>, Vec<Mark>), _>(
        |flow, items: Ref<Vec<Mark>>| {
            let out_refs = flow.each(items, |item, each| {
                let first = each.then_with_output(&count_marks, item);
                let second = each.then_with_output(&count_marks, item);
                (first, second)
            });
            flow.output_with(out_refs);
        },
        vec![Mark(1), Mark(2)],
    ))
    .expect("two-slot each runs");
    assert_eq!(output, (vec![Mark(1), Mark(2)], vec![Mark(1), Mark(2)]));

    let creations = test_support::boundary_creation_snapshot();
    let (each, _, _) = &creations[0];
    let groups = test_support::take_consume_group();
    let before: Vec<_> = groups
        .iter()
        .filter(|snapshot| snapshot.phase == ConsumeGroupSnapshotPhase::Before)
        .collect();
    let after: Vec<_> = groups
        .iter()
        .filter(|snapshot| snapshot.phase == ConsumeGroupSnapshotPhase::AfterCommit)
        .collect();
    assert_eq!(before.len(), 2, "one group consume per item");
    assert_eq!(after.len(), 2);
    for snapshot in before.iter().chain(after.iter()) {
        assert_eq!(
            snapshot.each.as_ref(),
            Some(each),
            "the collector owner is the EachScope"
        );
        assert_eq!(snapshot.observation_error, None);
        assert_eq!(snapshot.slots.len(), 2);
        for slot in &snapshot.slots {
            assert_eq!(
                slot.collector_owner.as_ref(),
                Some(each),
                "each declared slot's collector is owned by the EachScope"
            );
            assert_eq!(slot.collector_element, Some(std::any::type_name::<Mark>()));
            assert!(
                slot.data_id.is_some(),
                "the declared slot targets complete item Data"
            );
        }
    }
    // 预检前：值仍完整存活且由当前 Item 负责（不是祖先）。
    for snapshot in &before {
        assert_eq!(snapshot.item_state, Some(ScopeState::Finalizing));
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| slot.alive && slot.owner.as_ref() == Some(&snapshot.item)),
            "before the commit each slot is alive and owned by the Item itself"
        );
    }
    // 提交后：值已物理移入 collector，旧 DataId 不再是任何 Scope 的责任。
    // （AfterCommit 观察点在 close_scope 之前取得，因此 Item 仍是 Finalizing；真实关闭由
    // `closed_scope_snapshot` 另行断言。）
    for snapshot in &after {
        assert_eq!(snapshot.item_state, Some(ScopeState::Finalizing));
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| !slot.alive && slot.owner.is_none()),
            "after the commit the values have left the Item and the container entry"
        );
        assert!(
            snapshot.item_owned.is_empty(),
            "the Item no longer owns its outputs"
        );
    }
    let closed = test_support::closed_scope_snapshot();
    for scope in [each.clone(), creations[1].0.clone(), creations[2].0.clone()] {
        assert!(
            closed.contains(&scope),
            "{scope:?} must be closed after the run"
        );
    }
    assert_eq!(before[0].item, creations[1].0);
    assert_eq!(before[1].item, creations[2].0);
    // 逐项：提交前 collector 里恰好是先前进项已移动的值数。
    assert_eq!(
        before
            .iter()
            .map(|snapshot| snapshot
                .slots
                .iter()
                .map(|slot| slot.collector_moves)
                .collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec![Some(0), Some(0)], vec![Some(1), Some(1)]],
        "collectors hold exactly the values moved by earlier items"
    );
}

// ================================================================ P07：item 借用与非 Clone

/// P07：item 是原 Vec 元素的受限借用——非 Clone 元素、Node 借用地址等于原元素地址（同一
/// `Vec<Heavy>` 内 `ptr::eq`）、集合长度与元素值不变、item 没有独立 DataId。
#[test]
fn p07_item_borrow_is_the_original_element_of_the_collection() {
    reset_all();
    let output = block_on(Runtime::new().execute::<_, Vec<Score>, _>(
        |flow, items: Ref<Vec<Heavy>>| {
            let out_refs = flow.each(items, |item, each| {
                each.then_with_output(&note_heavy, (item, items))
            });
            flow.output_with(out_refs);
        },
        vec![Heavy(1), Heavy(2), Heavy(3)],
    ))
    .expect("each over non-Clone elements runs");
    assert_eq!(
        output,
        vec![Score(31), Score(32), Score(33)],
        "one collected value per item: element + 10 * collection length"
    );
    assert_eq!(body_calls(), 3);

    let borrowed = heavy_borrows();
    assert_eq!(borrowed.len(), 3);
    assert_eq!(
        borrowed
            .iter()
            .map(|(index, _, _, _)| *index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2],
        "item i borrows collection element i (address identity, not value equality)"
    );
    assert!(
        borrowed.iter().all(|(_, length, _, _)| *length == 3),
        "the collection keeps its length while items are borrowed"
    );
    let element_addresses: Vec<*const ()> =
        borrowed.iter().map(|(_, _, element, _)| *element).collect();
    assert!(
        element_addresses
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            == 3,
        "the three item borrows have three distinct addresses"
    );
    let collection_addresses: Vec<*const ()> = borrowed
        .iter()
        .map(|(_, _, _, collection)| *collection)
        .collect();
    assert!(
        collection_addresses
            .iter()
            .all(|address| *address == collection_addresses[0]),
        "all items borrow from one and the same collection value"
    );
    assert!(
        element_addresses
            .iter()
            .all(|element| !collection_addresses.contains(element)),
        "an element address differs from the collection value's own address"
    );

    // item 没有独立 DataId：每个 ItemScope 的正式 item 目标都是 CollectionItem 别名，指向
    // 同一个（原集合）DataId，且 ItemScope 只拥有自己的业务输出。
    let groups = test_support::take_consume_group();
    let mut alias_collections = Vec::new();
    let mut alias_scopes = Vec::new();
    let mut owned_counts = Vec::new();
    for snapshot in groups
        .iter()
        .filter(|snapshot| snapshot.phase == ConsumeGroupSnapshotPhase::Before)
    {
        owned_counts.push(snapshot.item_owned.len());
        let aliased: Vec<_> = snapshot
            .item_refs
            .iter()
            .filter(|(_, target)| matches!(target, TargetSnapshot::CollectionItem { .. }))
            .collect();
        assert_eq!(aliased.len(), 1, "no independent item Data exists");
        let (_, target) = aliased[0];
        match target {
            TargetSnapshot::CollectionItem {
                collection,
                index,
                lifetime_cap,
                ..
            } => {
                assert_eq!(*index, alias_collections.len());
                assert_eq!(*lifetime_cap, snapshot.item);
                alias_collections.push(collection.clone());
                alias_scopes.push(snapshot.item.clone());
            }
            other => panic!("expected a CollectionItem alias, got {other:?}"),
        }
    }
    assert_eq!(alias_collections.len(), 3);
    assert_eq!(
        owned_counts,
        vec![1, 1, 1],
        "the ItemScope owns only its own output"
    );
    assert!(
        alias_collections
            .iter()
            .all(|collection| *collection == alias_collections[0]),
        "every item aliases the one original collection Data"
    );
    assert_ne!(alias_scopes[0], alias_scopes[1]);

    // 集合未被替换或搬移：Root 的输入责任仍是同一个 DataId（首项 seq 0），产物是新的 Vec。
    let taken = test_support::take_log_snapshot();
    assert_eq!(taken.len(), 1, "Root takes the one collected Vec");
    assert!(
        taken[0].seq() > 0,
        "the collected Vec is a newly allocated Data, not the item alias"
    );
    let roots = test_support::take_root_snapshots();
    let committed = roots
        .iter()
        .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit)
        .expect("root after-commit snapshot");
    assert_eq!(
        committed
            .root_owned
            .iter()
            .map(|id| id.seq())
            .collect::<Vec<_>>(),
        vec![0],
        "after the group take only the original collection stays root-owned"
    );
    assert_eq!(
        committed.root_owned[0], alias_collections[0],
        "the surviving root input is the very Data the items alias"
    );
    assert_eq!(committed.taken_alive, vec![false]);
    assert_eq!(committed.taken_owned_by, vec![None]);
}

/// P07：DataId 序列与真实产物数相符——item 不消耗 DataId，每个 item 每槽恰好产生一个业务
/// 值，finish 才为每槽分配一个 Vec，最终 Export 不分配。
#[test]
fn p07_data_id_sequence_matches_produced_values() {
    for items in [0usize, 1, 3] {
        reset_all();
        let collection: Vec<Mark> = (0..items).map(|index| Mark(index as u32)).collect();
        let output = block_on(Runtime::new().execute::<_, (Vec<Mark>, Vec<Mark>), _>(
            |flow, marks: Ref<Vec<Mark>>| {
                let out_refs = flow.each(marks, |item, each| {
                    let first = each.then_with_output(&count_marks, item);
                    let second = each.then_with_output(&count_marks, item);
                    (first, second)
                });
                flow.output_with(out_refs);
            },
            collection,
        ))
        .expect("each runs");
        assert_eq!(output.0.len(), items);
        assert_eq!(output.1.len(), items);
        assert_eq!(body_calls(), 2 * items);

        let snapshots = observe::take_export_snapshots();
        let before = snapshots
            .iter()
            .find(|snapshot| snapshot.phase == "before")
            .expect("Each records its pre-export caller snapshot");
        assert_eq!(
            before.owned.len(),
            1,
            "before the Each export the caller owns only its input"
        );
        // 0: 集合；每项每槽一个业务值；finish 后每槽一个 Vec。
        assert_eq!(
            before.next_data_id,
            Some(1 + 2 * items as u64 + 2),
            "items produced exactly two DataIds each, plus the two finished Vecs"
        );
        let committed = snapshots
            .iter()
            .find(|snapshot| snapshot.phase == "committed")
            .expect("Each records its committed caller snapshot");
        assert_eq!(
            committed.next_data_id, before.next_data_id,
            "the final export allocates no new DataId"
        );
        assert_eq!(
            committed.owned.len(),
            1 + 2,
            "the caller owns its input plus the two collected Vecs"
        );

        // Root 整组提取：恰好取走两个产物，且两个产物是 finish 时分配的那两个 DataId。
        let taken = test_support::take_log_snapshot();
        assert_eq!(
            taken.len(),
            2,
            "Root takes exactly the two declared outputs"
        );
        assert_eq!(
            taken.iter().map(|id| id.seq()).collect::<Vec<_>>(),
            vec![1 + 2 * items as u64, 2 + 2 * items as u64],
            "the taken instances are the two finished Vecs, in slot order"
        );
        let roots = test_support::take_root_snapshots();
        let root_committed = roots
            .iter()
            .find(|snapshot| snapshot.phase == RootSnapshotPhase::AfterCommit)
            .expect("root after-commit snapshot");
        assert_eq!(root_committed.planned_takes, 2);
        assert_eq!(
            root_committed
                .root_owned
                .iter()
                .map(|id| id.seq())
                .collect::<Vec<_>>(),
            vec![0],
            "after the group commit only the root input stays responsible in the Root"
        );
        assert_eq!(
            root_committed.taken_alive,
            vec![false, false],
            "both products have left the container (no item Data of either kind remains)"
        );
        assert_eq!(root_committed.taken_owned_by, vec![None, None]);
    }
}

// ================================================================ P08：祖先捕获不重复 Import

/// P08：同一集合同时作为 Each 输入与 item Body 祖先，且 Body 内有多层 Chain／Retry／嵌套
/// Each 捕获同一祖先——逐层 Import 每个逻辑位置只登记一次。
///
/// Each 的整组 Import 按 `(source, destination, 类型)` 去重：集合既在静态输入描述符里，又
/// 出现在 item Body 的 import 列表里；若不去重，同一目的位置的批内重复会被
/// `DuplicatePosition` 拒绝。因此“真实执行成功”本身即是运行期去重的见证；静态侧另外断言
/// 每个 Body 的 import 列表对同一逻辑位置只记一次、且无任何位置重复。
#[test]
fn p08_collection_and_ancestors_are_imported_once_per_layer() {
    reset_all();
    let mut collection_position: Option<PositionId> = None;
    let mut rules_position: Option<PositionId> = None;
    let definition = build_definition::<(Vec<Mark>, Rules), (), _>(|flow, (marks, rules)| {
        collection_position = Some(marks.handle().position);
        rules_position = Some(rules.handle().position);
        let _collected: () = flow.each(marks, |item, each| {
            let _: () = each.then_with_output(&read_pair, (item, rules));
            let _: () = each.then_with_output(&read_marks, marks);
            each.chain(|sub| {
                let _: () = sub.then_with_output(&read_pair, (item, rules));
                let _: () = sub.then_with_output(&read_marks, marks);
                sub.output::<()>();
            });
            each.retry(0, |attempt| {
                let _: () = attempt.then_with_output(&read_pair, (item, rules));
                let _: () = attempt.then_with_output(&read_marks, marks);
                attempt.output::<()>();
            });
            each.each(marks, |nested_item, nested| {
                let _: () = nested.then_with_output(&read_pair, (nested_item, rules));
                let _: () = nested.then_with_output(&read_marks, marks);
            });
        });

        flow.output_with(());
    })
    .expect("deep capture over the same collection builds");

    let collection_position = collection_position.expect("captured the collection position");
    let rules_position = rules_position.expect("captured the ancestor position");
    let step = definition.root().steps()[0].each().expect("each step view");
    assert_eq!(step.collection().handle.position, collection_position);
    assert!(
        definition.root().imports().is_empty(),
        "the Each's own caller imports nothing it already owns"
    );

    let mut bodies_with_collection = 0;
    let mut bodies_with_rules = 0;
    for body in definition.bodies() {
        for needle in [collection_position, rules_position] {
            let count = body
                .imports()
                .iter()
                .filter(|position| **position == needle)
                .count();
            assert!(
                count <= 1,
                "body {:?} imports {needle:?} at most once",
                body.id()
            );
        }
        let mut positions: Vec<PositionId> = body.imports().to_vec();
        let total = positions.len();
        positions.sort_by_key(|position| position.index());
        positions.dedup();
        assert_eq!(
            positions.len(),
            total,
            "no body repeats the same logical position in its import list"
        );
        if body
            .imports()
            .iter()
            .filter(|position| **position == collection_position)
            .count()
            == 1
        {
            bodies_with_collection += 1;
        }
        if body
            .imports()
            .iter()
            .filter(|position| **position == rules_position)
            .count()
            == 1
        {
            bodies_with_rules += 1;
        }
    }
    assert_eq!(
        bodies_with_collection, 4,
        "item body + chain child + retry attempt + nested item import the collection once each"
    );
    assert_eq!(
        bodies_with_rules, 4,
        "the same four bodies import the ancestor once each"
    );
    // 正式 item 位置：每个 Body 最多导入一次；item Body 自己不导入自己的 item 位置，而天然
    // 引用它的深层 child（Chain child 与 Retry attempt）各导入一次。
    let mut item_importing_bodies = 0;
    for body in definition.bodies() {
        let count = body
            .imports()
            .iter()
            .filter(|position| **position == step.item_slot())
            .count();
        assert!(
            count <= 1,
            "body {:?} imports the item position at most once",
            body.id()
        );
        if count == 1 {
            item_importing_bodies += 1;
        }
    }
    assert_eq!(
        item_importing_bodies, 2,
        "the chain child and the retry attempt each capture the formal item once"
    );
    assert!(
        !definition
            .body(step.item())
            .imports()
            .contains(&step.item_slot()),
        "an item body never imports its own formal item position"
    );

    // 运行期：集合同时是 Each 的静态输入与 Body 祖先时整组 Import 仍成功（无批内重复目的
    // 位置），EachScope 只持有（集合 + 祖先）两个不同位置各一份。
    let output = block_on(Runtime::new().execute::<_, Vec<Score>, _>(
        |flow, (marks, rules): (Ref<Vec<Mark>>, Ref<Rules>)| {
            let out_refs = flow.each(marks, |item, each| {
                let _: () = each.then_with_output(&read_pair, (item, rules));
                each.then_with_output(&score_of, (item, rules))
            });
            flow.output_with(out_refs);
        },
        (vec![Mark(1), Mark(2)], Rules(3)),
    ))
    .expect("the same collection as Each input and Body ancestor imports once");
    assert_eq!(output, vec![Score(4), Score(5)]);

    let groups = test_support::take_consume_group();
    let snapshot = groups
        .iter()
        .find(|snapshot| snapshot.phase == ConsumeGroupSnapshotPhase::Before)
        .expect("a group consume happened");
    assert_eq!(
        snapshot.each_refs.len(),
        2,
        "the EachScope holds exactly the collection and the ancestor: {:?}",
        snapshot.each_refs
    );
    assert!(
        snapshot
            .each_refs
            .iter()
            .all(|(_, target)| matches!(target, TargetSnapshot::Data(_))),
        "both EachScope bindings are complete Data (the collection is not an item)"
    );
    // 两个绑定的目标就是 Root 自己登记的那两个 Data（seq 0/1，owner 为 Root），即祖先按
    // 只读 alias 导入而不是新分配。
    let mut each_data_seqs: Vec<u64> = snapshot
        .each_refs
        .iter()
        .map(|(_, target)| match target {
            TargetSnapshot::Data(id) => id.seq(),
            other => panic!("the EachScope may only bind complete Data, got {other:?}"),
        })
        .collect();
    each_data_seqs.sort_unstable();
    assert_eq!(
        each_data_seqs,
        vec![0, 1],
        "collection and ancestor are the root inputs"
    );
    let mut each_positions: Vec<RefId> = snapshot
        .each_refs
        .iter()
        .map(|(position, _)| position.clone())
        .collect();
    let distinct = each_positions.len();
    each_positions.sort_by_key(RefId::seq);
    each_positions.dedup();
    assert_eq!(
        each_positions.len(),
        distinct,
        "no duplicated EachScope binding"
    );
    // ItemScope：正式 item 别名 + 两条祖先捕获（集合与 rules）。
    assert_eq!(
        snapshot.item_refs.len(),
        3,
        "item alias + collection + rules"
    );
    assert_eq!(
        snapshot
            .item_refs
            .iter()
            .filter(|(_, target)| matches!(target, TargetSnapshot::CollectionItem { .. }))
            .count(),
        1,
        "exactly one item alias is bound"
    );

    // 深层捕获的逐层 Chain Import 对：source 与 destination 是同一静态 RefId，且无重复。
    for record in observe::take_chain_imports() {
        assert!(
            record
                .pairs
                .iter()
                .all(|(source, destination)| source == destination),
            "a chain import binds the same static RefId at source and destination"
        );
        let mut positions: Vec<RefId> = record
            .pairs
            .iter()
            .map(|(source, _)| source.clone())
            .collect();
        let distinct = positions.len();
        positions.sort_by_key(RefId::seq);
        positions.dedup();
        assert_eq!(
            positions.len(),
            distinct,
            "no duplicated logical position per layer"
        );
    }
}

// ================================================================ P12：空集合

/// P12：空集合不执行 Body、不创建 Item／业务 Node，但仍完成构建与静态验证，并产出每槽空
/// `Vec`（unit 为 `()`，无 collector、无 unit Data）。
#[test]
fn p12_empty_collection_finishes_empty_vecs_without_items() {
    // 单槽。
    reset_all();
    let output = block_on(Runtime::new().execute::<_, Vec<Mark>, _>(
        |flow, marks: Ref<Vec<Mark>>| {
            let out_refs = flow.each(marks, |item, each| {
                bump_item_body_builds();
                each.then_with_output(&count_marks, item)
            });
            flow.output_with(out_refs);
        },
        Vec::new(),
    ))
    .expect("an empty collection still builds and runs");
    assert!(output.is_empty(), "a single slot finishes an empty Vec");
    assert_eq!(item_body_builds(), 1, "the item Body is built once");
    assert_eq!(body_calls(), 0, "no business Node runs for zero items");
    assert!(
        !observe::take_controller_rounds()
            .iter()
            .any(|record| record.at == "item-created"),
        "no Item is created for an empty collection"
    );
    let creations = test_support::boundary_creation_snapshot();
    assert_eq!(creations.len(), 1, "only the EachScope is created");
    assert_eq!(creations[0].2, ScopeRole::Each);
    let snapshots = observe::take_export_snapshots();
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == "before")
        .expect("pre-export snapshot");
    assert_eq!(
        before.next_data_id,
        Some(2),
        "collection (0) + the finished empty Vec (1); no item Data"
    );
    assert!(
        test_support::take_consume_group().is_empty(),
        "no item means no group consume"
    );

    // 2 槽：两个独立空 Vec。
    reset_all();
    let output = block_on(Runtime::new().execute::<_, (Vec<Mark>, Vec<Mark>), _>(
        |flow, marks: Ref<Vec<Mark>>| {
            let out_refs = flow.each(marks, |item, each| {
                bump_item_body_builds();
                let first = each.then_with_output(&count_marks, item);
                let second = each.then_with_output(&count_marks, item);
                (first, second)
            });
            flow.output_with(out_refs);
        },
        Vec::new(),
    ))
    .expect("a two-slot empty each runs");
    assert!(output.0.is_empty() && output.1.is_empty());
    assert_eq!(item_body_builds(), 1);
    assert_eq!(body_calls(), 0);
    assert_eq!(
        observe::take_controller_rounds()
            .iter()
            .filter(|record| record.at == "item-created")
            .count(),
        0
    );

    // unit：不建 collector、不制造 unit Data、无最终伪 DataId。
    reset_all();
    let output = block_on(Runtime::new().execute::<_, (), _>(
        |flow, marks: Ref<Vec<Mark>>| {
            let _collected: () = flow.each(marks, |item, each| {
                bump_item_body_builds();
                let _: () = each.then_with_output(&count_unit, item);
            });

            flow.output_with(());
        },
        Vec::new(),
    ))
    .expect("a unit empty each runs");
    assert_eq!(output, ());
    assert_eq!(item_body_builds(), 1);
    assert_eq!(body_calls(), 0);
    let snapshots = observe::take_export_snapshots();
    let before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == "before")
        .expect("pre-export snapshot");
    let committed = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == "committed")
        .expect("committed snapshot");
    assert_eq!(
        before.next_data_id,
        Some(1),
        "unit creates no Vec Data and no collector"
    );
    assert_eq!(
        before.owned.len(),
        1,
        "only the root input is owned at that point"
    );
    assert_eq!(
        committed.owned.len(),
        before.owned.len(),
        "unit exports nothing new"
    );
    assert_eq!(
        committed.refs.len(),
        before.refs.len(),
        "unit binds no caller output position"
    );
}

/// P12：非法静态来源在空集合下仍被拒绝——Definition 在任何输入长度被读取之前完成构建与
/// 验证，因此空输入不绕过 P04 的可见性判据。
#[test]
fn p12_illegal_static_source_is_rejected_with_an_empty_collection() {
    reset_all();
    let before = creation_counts::snapshot();
    let outcome = block_on(Runtime::new().execute::<_, Mark, _>(
        |flow, items: Ref<Vec<Mark>>| {
            let mut escaped: Option<Ref<Mark>> = None;
            let _collected: () = flow.each(items, |item, each| {
                escaped = Some(item);
                let _: () = each.then_with_output(&count_unit, item);
            });
            let leaked = escaped.expect("captured the formal item");
            let out_refs = flow.then_with_output(&count_marks, leaked);
            flow.output_with(out_refs);
        },
        Vec::new(),
    ));
    let error = match outcome {
        Err(RunError::Definition(build)) => build.detail,
        Ok(_) => panic!("an empty collection must not bypass the visibility check"),
        Err(other) => panic!("expected a Definition rejection, got {other}"),
    };
    assert_eq!(
        error,
        BuildErrorDetail::OutOfScope {
            body: BodyId::ROOT,
            step: Some(1),
            position: PositionId::new(1),
            owner: BodyId::new(1),
            relation: Visibility::ChildLocal,
        }
    );
    assert_eq!(body_calls(), 0);
    assert_eq!(
        creation_counts::snapshot(),
        before,
        "the rejection happens before any Root／Context creation"
    );
    assert!(test_support::boundary_creation_snapshot().is_empty());
}
