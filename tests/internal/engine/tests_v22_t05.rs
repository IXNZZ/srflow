//! V22-T05 定向测试：Choose／Each 新构建接口（四门面、selector 固定、分支/Item 契约、
//! 聚合与 typed 能力、失败隔离、过渡组合）。
//!
//! 覆盖判别性证据：
//! - 四门面（自动/显式 selector × 普通/取输出）共用同一条登记路径：结构画像逐项相同、
//!   同场景实跑值与调用计数相同；`each`／`each_output` 同理；
//! - 自动链式 `.then(...).choose(...).then(...)`／`.then(...).each(...).then(...)` 的后继 Node
//!   真实读到父输出（断言数值），并与显式 `ref_of` + `then_with` 接线同构；
//! - 显式 selector 精确保留所给 Ref；自动 selector 固定在入口时点的来源，分支构建期间
//!   新出现的同类型来源（Choose 共享父输出）不改变它；
//! - 分支声明状态（Undeclared／Unit／NonUnit）与共同契约的三种拒绝（state／arity／slot type）
//!   全部先于任何业务 Node；配置错误（空配置、重复 case/otherwise）保留首因与首配置；
//! - Each 的 Item 在 Body 开始即可 `ref_of`、同类型遮蔽后必须用提前保存的 Ref、unit Item 零
//!   collector、单/双槽聚合形状与数值、空集合仍构建且零执行；
//! - `_output` 期望（含漏聚合）与手工声明缺 `MissingEachBridge` 的 Definition 拒绝（断言
//!   body／slot／expected／found）；
//! - 失败隔离：分支／Item 重复声明后共享父输出保持失效，首因保留（含正向对照）；
//! - 过渡组合：新 Choose／Each 内嵌已验收的 chain／retry，取输出经 `output_with` 后值正确。
#![allow(clippy::let_unit_value, clippy::unused_unit)]

use crate::core::engine::choice::Choice;
use crate::core::engine::data::Data;
use crate::core::engine::definition::{
    Body, BodyRole, Builder, DeclarationKind, DeclarationState, Definition, EachBridge,
    PositionRecord, PositionSource, StepKind, build_definition,
};
use crate::core::engine::error::{BodyError, BuildErrorDetail, RunError};
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::{BodyId, PositionHandle, PositionId};
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{
    PositionDesc, Query, Ref, TypeDesc, each_bind_item, each_collection_len,
};
use crate::core::ref_id::RefId;
use futures::executor::block_on;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

// ---------------------------------------------------------------- 业务类型与计数

#[derive(Debug, PartialEq, Eq)]
struct Key(u32);
impl Data for Key {}

#[derive(Debug, PartialEq, Eq)]
struct Item(u32);
impl Data for Item {}

#[derive(Debug, PartialEq, Eq)]
struct Score(u32);
impl Data for Score {}

#[derive(Debug, PartialEq, Eq)]
struct Tag(u32);
impl Data for Tag {}

#[derive(Debug, PartialEq, Eq)]
struct Report(u32);
impl Data for Report {}

thread_local! {
    /// 业务 Node／Factory 的真实调用计数。
    static CALLS: Cell<u32> = const { Cell::new(0) };
    /// 逐分支 Factory 调用计数（未选分支必须为零）。
    static BRANCH: Cell<[u32; 3]> = const { Cell::new([0; 3]) };
    /// 定义期分支闭包执行次数（重复配置必须不执行闭包）。
    static BUILDS: Cell<u32> = const { Cell::new(0) };
}

fn call() {
    CALLS.with(|calls| calls.set(calls.get() + 1));
}

fn calls() -> u32 {
    CALLS.with(Cell::get)
}

fn mark_branch(index: usize) {
    BRANCH.with(|calls| {
        let mut counts = calls.get();
        counts[index] += 1;
        calls.set(counts);
    });
}

fn branch_calls(index: usize) -> u32 {
    BRANCH.with(|calls| calls.get()[index])
}

fn mark_build() {
    BUILDS.with(|builds| builds.set(builds.get() + 1));
}

fn builds() -> u32 {
    BUILDS.with(Cell::get)
}

fn reset_counters() {
    CALLS.with(|calls| calls.set(0));
    BRANCH.with(|calls| calls.set([0; 3]));
    BUILDS.with(|builds| builds.set(0));
}

// ---------------------------------------------------------------- 业务 Node

async fn score_of_key(query: Query<&Key>) -> Result<Score, BodyError> {
    call();
    Ok(Score(query.get().0 + 10))
}

async fn alt_score_of_key(query: Query<&Key>) -> Result<Score, BodyError> {
    call();
    Ok(Score(query.get().0 + 100))
}

async fn tag_of_key(query: Query<&Key>) -> Result<Tag, BodyError> {
    call();
    Ok(Tag(query.get().0 + 1000))
}

async fn report_of_score(query: Query<&Score>) -> Result<Report, BodyError> {
    call();
    Ok(Report(query.get().0 * 2))
}

async fn key_after(query: Query<&Key>) -> Result<Key, BodyError> {
    call();
    Ok(Key(query.get().0 + 1))
}

async fn key_plus_ten(query: Query<&Key>) -> Result<Key, BodyError> {
    call();
    Ok(Key(query.get().0 + 10))
}

async fn score_of_item(query: Query<&Item>) -> Result<Score, BodyError> {
    call();
    Ok(Score(query.get().0))
}

async fn tag_of_item(query: Query<&Item>) -> Result<Tag, BodyError> {
    call();
    Ok(Tag(query.get().0 + 1000))
}

async fn relabel_item(query: Query<&Item>) -> Result<Item, BodyError> {
    call();
    Ok(Item(query.get().0 + 1000))
}

async fn key_of_item(query: Query<&Item>) -> Result<Key, BodyError> {
    call();
    Ok(Key(query.get().0))
}

async fn shift_item(query: Query<&Item>) -> Result<Score, BodyError> {
    call();
    Ok(Score(query.get().0 + 50))
}

async fn count_item(query: Query<&Item>) -> Result<(), BodyError> {
    call();
    let _ = query.get();
    Ok(())
}

async fn total_of_scores(query: Query<&Vec<Score>>) -> Result<Report, BodyError> {
    call();
    Ok(Report(query.get().iter().map(|score| score.0).sum()))
}

async fn pair_report(query: Query<(&Vec<Score>, &Vec<Tag>)>) -> Result<Report, BodyError> {
    call();
    let (scores, tags) = query.get();
    let left: u32 = scores.iter().map(|score| score.0 * 10).sum();
    let right: u32 = tags.iter().map(|tag| tag.0).sum();
    Ok(Report(left + right))
}

// ---------------------------------------------------------------- 共用分支与 Item 场景

/// 同一 Choose 场景：`case Key(1)` 走 +100 分支，`otherwise` 走 +10 分支，输出 `Score`。
fn score_choice(choice: &mut Choice<'_, Key>, key: Ref<Key>) {
    choice.case(Key(1), |branch| {
        let score = branch.then_with_output(alt_score_of_key, key);
        branch.output_with(score);
    });
    choice.otherwise(|branch| {
        let score = branch.then_with_output(score_of_key, key);
        branch.output_with(score);
    });
}

/// 同一 Choose 场景：`case Key(1)` → +1，`case Key(2)` → +10，输出 `Key`（用于 selector 判别）。
fn key_choice(choice: &mut Choice<'_, Key>, key: Ref<Key>) {
    choice.case(Key(1), |branch| {
        let bumped = branch.then_with_output(key_after, key);
        branch.output_with(bumped);
    });
    choice.case(Key(2), |branch| {
        let bumped = branch.then_with_output(key_plus_ten, key);
        branch.output_with(bumped);
    });
}

/// 全 Factory 分支的 Key 选择场景（用于“未选分支 Factory 零调用”）。
fn factory_key_choice(choice: &mut Choice<'_, Key>) {
    choice.case(Key(1), |branch| {
        let picked = branch.provide_output(|| {
            mark_branch(0);
            Score(1)
        });
        branch.output_with(picked);
    });
    choice.case(Key(2), |branch| {
        let picked = branch.provide_output(|| {
            mark_branch(1);
            Score(2)
        });
        branch.output_with(picked);
    });
    choice.otherwise(|branch| {
        let picked = branch.provide_output(|| {
            mark_branch(2);
            Score(9)
        });
        branch.output_with(picked);
    });
}

/// 仅两个 case 的 Factory 分支场景（无 otherwise，用于 NoMatchingCase 判别）。
fn factory_key_cases(choice: &mut Choice<'_, Key>) {
    choice.case(Key(1), |branch| {
        let picked = branch.provide_output(|| {
            mark_branch(0);
            Score(1)
        });
        branch.output_with(picked);
    });
    choice.case(Key(2), |branch| {
        let picked = branch.provide_output(|| {
            mark_branch(1);
            Score(2)
        });
        branch.output_with(picked);
    });
}

/// Item Body：声明单项 `Score`（单槽聚合）。
fn score_only_item(sub: &mut Flow<'_>) {
    let item = sub.ref_of::<Item>();
    let score = sub.then_with_output(score_of_item, item);
    sub.output_with(score);
}

/// Item Body：声明 `(Score, Tag)`（双槽异类型聚合）。
fn score_and_tag_item(sub: &mut Flow<'_>) {
    let item = sub.ref_of::<Item>();
    let score = sub.then_with_output(score_of_item, item);
    let tag = sub.then_with_output(tag_of_item, item);
    sub.output_with((score, tag));
}

// ---------------------------------------------------------------- 结构画像

fn ord_body(definition: &Definition<'_>, id: BodyId) -> usize {
    definition
        .bodies()
        .iter()
        .position(|body| body.id() == id)
        .unwrap_or(usize::MAX)
}

fn ord_pos(definition: &Definition<'_>, id: PositionId) -> usize {
    definition
        .positions()
        .iter()
        .position(|record| record.handle().position == id)
        .unwrap_or(usize::MAX)
}

fn ord_core(definition: &Definition<'_>, core: &RefId) -> usize {
    let mut seen: Vec<&RefId> = Vec::new();
    for record in definition.positions() {
        if !seen.iter().any(|known| *known == record.core()) {
            seen.push(record.core());
        }
    }
    seen.iter()
        .position(|known| *known == core)
        .unwrap_or(usize::MAX)
}

/// 时点归一化：声明时点与发布时点放在同一集合里排名；0 表示未声明／未发布。
fn rank_order(definition: &Definition<'_>, order: u64) -> String {
    if order == 0 {
        return "-".to_string();
    }
    let mut all: Vec<u64> = Vec::new();
    for body in definition.bodies() {
        all.push(body.output_declaration().order);
        all.push(body.next_declaration().order);
    }
    for record in definition.positions() {
        if let Some(published) = record.published_order() {
            all.push(published);
        }
    }
    all.retain(|known| *known != 0);
    all.sort_unstable();
    all.dedup();
    match all.iter().position(|known| *known == order) {
        Some(index) => index.to_string(),
        None => format!("#{order}"),
    }
}

fn fmt_record(definition: &Definition<'_>, record: &PositionRecord) -> String {
    format!(
        "owner#{} src={:?} ty={} pub={} tainted={}",
        ord_body(definition, record.owner()),
        record.source(),
        record.type_desc(),
        rank_order(definition, record.published_order().unwrap_or(0)),
        record.is_tainted()
    )
}

fn pos_str(definition: &Definition<'_>, id: PositionId) -> String {
    match definition
        .positions()
        .iter()
        .find(|record| record.handle().position == id)
    {
        Some(record) => fmt_record(definition, record),
        None => "missing".to_string(),
    }
}

fn handle_str(definition: &Definition<'_>, handle: PositionHandle) -> String {
    match definition.record(handle) {
        Some(record) => fmt_record(definition, record),
        None => "missing".to_string(),
    }
}

/// 某 Body 的直接子 Body（按创建顺序）。
fn child_bodies(definition: &Definition<'_>, parent: BodyId) -> Vec<BodyId> {
    definition
        .bodies()
        .iter()
        .filter(|body| body.parent() == Some(parent))
        .map(|body| body.id())
        .collect()
}

/// Choose 分支的声明状态表：按分支序（case 序 + otherwise）给出 Body 与声明状态。
fn choose_branch_states(
    definition: &Definition<'_>,
    parent: BodyId,
    step: usize,
) -> Vec<(u16, BodyId, DeclarationState)> {
    let mut rows: Vec<(u16, BodyId, DeclarationState)> = definition
        .positions()
        .iter()
        .filter_map(|record| match record.source() {
            PositionSource::ChooseExit {
                step: owner_step,
                branch,
                ..
            } if owner_step == step => {
                let owner = record.owner();
                if definition.body(owner).parent() == Some(parent) {
                    Some((
                        branch,
                        owner,
                        definition.body(owner).output_declaration().state(),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    rows.sort_by_key(|(branch, _, _)| *branch);
    rows.dedup_by_key(|(branch, _, _)| *branch);
    rows
}

/// 某一 Choose Step 的 `ChooseExit` 行（分支、分支 Body 序号、槽、出口类型），按分支/槽序。
fn choose_exit_rows(
    definition: &Definition<'_>,
    parent: BodyId,
    step: usize,
) -> Vec<(u16, usize, u16, TypeDesc)> {
    let mut rows: Vec<(u16, usize, u16, TypeDesc)> = definition
        .positions()
        .iter()
        .filter_map(|record| match record.source() {
            PositionSource::ChooseExit {
                step: owner_step,
                branch,
                slot,
            } if owner_step == step => {
                let owner = record.owner();
                if definition.body(owner).parent() == Some(parent) {
                    Some((
                        branch,
                        ord_body(definition, owner),
                        slot,
                        record.type_desc(),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    rows.sort_by_key(|(branch, _, slot, _)| (*branch, *slot));
    rows.dedup_by_key(|(branch, _, slot, _)| (*branch, *slot));
    rows
}

/// Choose Step 在 Body 内的下标（画像与 selector 断言都依赖真实 Step 位置）。
fn choose_step_index(definition: &Definition<'_>, body: BodyId) -> usize {
    definition
        .body(body)
        .steps()
        .iter()
        .position(|step| step.kind() == StepKind::Choose)
        .expect("body registers a choose step")
}

/// Choose Step 的 selector 逻辑来源（`StepEntry::inputs()[0]` 即 selector）。
fn choose_selector_source(
    definition: &Definition<'_>,
    body: BodyId,
    step: usize,
) -> PositionSource {
    let desc = definition.body(body).steps()[step].inputs()[0];
    definition
        .record(desc.handle)
        .expect("selector position is registered")
        .source()
}

/// 单个 Body 的逐 Step 画像：真实输入（位置归一化 + 类型）、输出身份、顺序，以及
/// Choose 的分支出口与 Each 的 item Body／逐槽聚合结构。
fn body_steps_str(definition: &Definition<'_>, body: &Body<'_>, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (index, step) in body.steps().iter().enumerate() {
        out.push(format!(
            "{prefix}step{index} kind={:?} order={} inputs={:?} outputs={:?}",
            step.kind(),
            rank_order(definition, step.order()),
            step.inputs()
                .iter()
                .map(|desc| format!(
                    "pos#{}:{}",
                    ord_pos(definition, desc.handle.position),
                    desc.type_desc
                ))
                .collect::<Vec<_>>(),
            step.outputs()
                .iter()
                .map(|id| pos_str(definition, *id))
                .collect::<Vec<_>>()
        ));
        if step.kind() == StepKind::Choose {
            for (branch, owner, slot, type_desc) in choose_exit_rows(definition, body.id(), index) {
                out.push(format!(
                    "{prefix}  choose-branch{branch} owner#{} slot{slot} exit_ty={type_desc}",
                    owner
                ));
            }
            // 真实逐槽交接映射（source→exit→output）：仅声明/位置数量相同不足以区分接线。
            for row in step.choose_slots() {
                out.push(format!(
                    "{prefix}  choose-handover branch{} slot{} source=core#{} exit=core#{} \
                     output=core#{} ty={}",
                    row.branch,
                    row.slot,
                    ord_core(definition, &row.source),
                    ord_core(definition, &row.exit),
                    ord_core(definition, &row.output),
                    row.type_desc
                ));
            }
        }
        if let Some(each) = step.each() {
            let item = definition.body(each.item());
            out.push(format!(
                "{prefix}  each item#{} collection=pos#{}:{} item_slot=pos#{}",
                ord_body(definition, item.id()),
                ord_pos(definition, each.collection().handle.position),
                each.collection().type_desc,
                ord_pos(definition, each.item_slot())
            ));
            for (slot, entry) in each.slots().iter().enumerate() {
                out.push(format!(
                    "{prefix}  each-slot{slot} source=core#{} local=core#{} output=core#{} collected={}",
                    ord_core(definition, &entry.source),
                    ord_core(definition, &entry.local),
                    ord_core(definition, &entry.output),
                    entry.collected_type
                ));
            }
        }
    }
    out
}

/// 深画像：全部 Body 的声明/imports/逐 Step 接线 + 全部位置记录，按身份归一化后比较。
fn deep_profile(definition: &Definition<'_>) -> Vec<String> {
    let mut out = vec![format!(
        "bodies={} positions={}",
        definition.bodies().len(),
        definition.positions().len()
    )];
    for body in definition.bodies() {
        out.push(format!(
            "body#{} parent={:?} role={:?} steps={} decl={:?} decl_order={} slots={:?} imports={:?}",
            ord_body(definition, body.id()),
            body.parent().map(|id| ord_body(definition, id)),
            body.role(),
            body.steps().len(),
            body.output_declaration().state(),
            rank_order(definition, body.output_declaration().order),
            body.output()
                .iter()
                .map(|desc| format!(
                    "pos#{}:{}",
                    ord_pos(definition, desc.handle.position),
                    desc.type_desc
                ))
                .collect::<Vec<_>>(),
            body.imports()
                .iter()
                .map(|id| format!("pos#{}", ord_pos(definition, *id)))
                .collect::<Vec<_>>()
        ));
        out.extend(body_steps_str(
            definition,
            body,
            &format!("  b{}/", ord_body(definition, body.id())),
        ));
    }
    for record in definition.positions() {
        out.push(format!(
            "pos#{} {}",
            ord_pos(definition, record.handle().position),
            fmt_record(definition, record)
        ));
    }
    out
}

/// 手工路径的 Root Body（不设置 Root 期望，收口只做统一验证）。
fn manual_root(builder: &Rc<RefCell<Builder<'static>>>) -> BodyId {
    builder.borrow_mut().new_body(None, BodyRole::Ordinary)
}

// ============================================================ E01／E02 门面与等价

#[test]
fn t05_choose_four_facades_share_structure_and_values() {
    reset_counters();

    let plain: Definition<'_> = build_definition::<Key, Score, _>(|flow, key| {
        flow.choose::<Key, _>(|choice| score_choice(choice, key));
        flow.output::<Score>();
    })
    .expect("普通 choose 构建成功");

    let auto_output: Definition<'_> = build_definition::<Key, Score, _>(|flow, key| {
        let picked: Ref<Score> =
            flow.choose_output::<Key, _, _>(|choice| score_choice(choice, key));
        flow.output_with(picked);
    })
    .expect("自动 selector 取输出 choose 构建成功");

    let explicit_plain: Definition<'_> = build_definition::<Key, Score, _>(|flow, key| {
        flow.choose_with(key, |choice| score_choice(choice, key));
        flow.output::<Score>();
    })
    .expect("显式 selector 普通 choose 构建成功");

    let explicit_output: Definition<'_> = build_definition::<Key, Score, _>(|flow, key| {
        let picked: Ref<Score> = flow.choose_with_output(key, |choice| score_choice(choice, key));
        flow.output_with(picked);
    })
    .expect("显式 selector 取输出 choose 构建成功");

    let reference = deep_profile(&plain);
    for (label, other) in [
        ("auto-output", &auto_output),
        ("explicit-plain", &explicit_plain),
        ("explicit-output", &explicit_output),
    ] {
        assert_eq!(
            reference,
            deep_profile(other),
            "{label} 必须与普通 choose 结构同构：{reference:#?}"
        );
    }
    for branch in ["choose-branch0", "choose-branch1"] {
        assert!(
            reference.iter().any(|line| line.contains(branch)),
            "画像必须包含 Choose 分支结构 {branch}：{reference:#?}"
        );
    }
    assert!(
        reference
            .iter()
            .any(|line| line.contains("src=ChooseOutput { step: 0, slot: 0 }")),
        "画像必须包含共享父输出：{reference:#?}"
    );

    // 实跑：四种门面在同一场景下值与调用计数一致。
    let mut observed = Vec::new();
    for variant in 0..4 {
        for key_value in [1u32, 2u32] {
            reset_counters();
            let produced = block_on(Runtime::new().execute::<Key, Score, _>(
                |flow, key| {
                    if variant == 0 {
                        flow.choose::<Key, _>(|choice| score_choice(choice, key));
                        flow.output::<Score>();
                    } else if variant == 1 {
                        let picked: Ref<Score> =
                            flow.choose_output::<Key, _, _>(|choice| score_choice(choice, key));
                        flow.output_with(picked);
                    } else if variant == 2 {
                        flow.choose_with(key, |choice| score_choice(choice, key));
                        flow.output::<Score>();
                    } else {
                        let picked: Ref<Score> =
                            flow.choose_with_output(key, |choice| score_choice(choice, key));
                        flow.output_with(picked);
                    }
                },
                Key(key_value),
            ))
            .expect("四种门面都必须真实执行");
            let expected = if key_value == 1 {
                Score(101)
            } else {
                Score(12)
            };
            assert_eq!(produced, expected, "variant={variant} key={key_value}");
            observed.push(calls());
        }
    }
    assert_eq!(
        observed,
        vec![1, 1, 1, 1, 1, 1, 1, 1],
        "四门面 × 两种输入都必须只执行选中分支的 Node 一次"
    );
}

#[test]
fn t05_each_plain_and_output_forms_share_structure_and_values() {
    reset_counters();

    let plain: Definition<'_> = build_definition::<Vec<Item>, Vec<Score>, _>(|flow, items| {
        flow.each(items, score_only_item);
        flow.output::<Vec<Score>>();
    })
    .expect("普通 each 构建成功");

    let output: Definition<'_> = build_definition::<Vec<Item>, Vec<Score>, _>(|flow, items| {
        let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
        flow.output_with(scores);
    })
    .expect("取聚合输出 each 构建成功");

    assert_eq!(
        deep_profile(&plain),
        deep_profile(&output),
        "each 与 each_output 必须共享同一条 item Body 构建与登记路径"
    );

    let mut observed = Vec::new();
    for variant in 0..2 {
        reset_counters();
        let produced = block_on(Runtime::new().execute::<Vec<Item>, Vec<Score>, _>(
            |flow, items| {
                if variant == 0 {
                    flow.each(items, score_only_item);
                    flow.output::<Vec<Score>>();
                } else {
                    let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
                    flow.output_with(scores);
                }
            },
            vec![Item(1), Item(2), Item(3)],
        ))
        .expect("两种 each 形式都必须真实执行");
        assert_eq!(produced, vec![Score(1), Score(2), Score(3)]);
        observed.push(calls());
    }
    assert_eq!(observed, vec![3, 3], "每种形式都逐项执行一次 Node");
}

// ============================================================ E03 自动链式

#[test]
fn t05_choose_auto_chain_reads_parent_output_value() {
    reset_counters();

    let auto: Definition<'_> = build_definition::<Key, Report, _>(|flow, key| {
        flow.choose::<Key, _>(|choice| score_choice(choice, key))
            .then(report_of_score);
        flow.output::<Report>();
    })
    .expect("Choose 之后 .then 自动链式可用");

    let explicit: Definition<'_> = build_definition::<Key, Report, _>(|flow, key| {
        let picked: Ref<Score> =
            flow.choose_output::<Key, _, _>(|choice| score_choice(choice, key));
        flow.then_with(report_of_score, picked);
        flow.output::<Report>();
    })
    .expect("显式接线可用");

    assert_eq!(
        deep_profile(&auto),
        deep_profile(&explicit),
        "自动 .then 与显式接线必须同构（含逐 Step 输入）"
    );

    let mut observed = Vec::new();
    for auto_form in [true, false] {
        for key_value in [1u32, 2u32] {
            reset_counters();
            let produced = block_on(Runtime::new().execute::<Key, Report, _>(
                |flow, key| {
                    if auto_form {
                        flow.choose::<Key, _>(|choice| score_choice(choice, key))
                            .then(report_of_score);
                    } else {
                        let picked: Ref<Score> =
                            flow.choose_output::<Key, _, _>(|choice| score_choice(choice, key));
                        flow.then_with(report_of_score, picked);
                    }
                    flow.output::<Report>();
                },
                Key(key_value),
            ))
            .expect("Choose 的共享父输出必须能被后继 Node 真实读取");
            let expected = if key_value == 1 {
                Report(202)
            } else {
                Report(24)
            };
            assert_eq!(produced, expected, "auto={auto_form} key={key_value}");
            observed.push(calls());
        }
    }
    assert_eq!(
        observed,
        vec![2, 2, 2, 2],
        "分支 Node 与后继 Node 各执行一次"
    );
}

#[test]
fn t05_each_auto_chain_reads_aggregated_parent_output_value() {
    reset_counters();

    let auto: Definition<'_> = build_definition::<Vec<Item>, Report, _>(|flow, items| {
        flow.each(items, score_only_item).then(total_of_scores);
        flow.output::<Report>();
    })
    .expect("Each 之后 .then 自动链式可用");

    let explicit: Definition<'_> = build_definition::<Vec<Item>, Report, _>(|flow, items| {
        let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
        flow.then_with(total_of_scores, scores);
        flow.output::<Report>();
    })
    .expect("显式接线可用");

    assert_eq!(
        deep_profile(&auto),
        deep_profile(&explicit),
        "Each 自动 .then 与显式接线必须同构（含逐 Step 输入）"
    );

    let mut observed = Vec::new();
    for auto_form in [true, false] {
        reset_counters();
        let produced = block_on(Runtime::new().execute::<Vec<Item>, Report, _>(
            |flow, items| {
                if auto_form {
                    flow.each(items, score_only_item).then(total_of_scores);
                } else {
                    let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
                    flow.then_with(total_of_scores, scores);
                }
                flow.output::<Report>();
            },
            vec![Item(1), Item(2), Item(3)],
        ))
        .expect("Each 的聚合父输出必须能被后继 Node 真实读取");
        assert_eq!(produced, Report(6), "sum(1,2,3)=6，auto={auto_form}");
        observed.push(calls());
    }
    assert_eq!(
        observed,
        vec![4, 4],
        "三项各一次 score_of_item，加一次 total_of_scores"
    );
}

// ============================================================ E04 selector 固定

#[test]
fn t05_explicit_selector_keeps_the_given_source() {
    reset_counters();

    // 两个 Key 来源：Root 输入，以及它的后继 Node 输出。
    let auto: Definition<'static> = build_definition::<Key, Key, _>(|flow, key| {
        let _second = flow.then_with_output(key_after, key);
        flow.choose::<Key, _>(|choice| key_choice(choice, key));
        flow.output::<Key>();
    })
    .expect("自动 selector 构建成功");

    let with_second: Definition<'static> = build_definition::<Key, Key, _>(|flow, key| {
        let second = flow.then_with_output(key_after, key);
        flow.choose_with(second, |choice| key_choice(choice, key));
        flow.output::<Key>();
    })
    .expect("显式 selector（第二个来源）构建成功");

    let with_first: Definition<'static> = build_definition::<Key, Key, _>(|flow, key| {
        let _second = flow.then_with_output(key_after, key);
        flow.choose_with(key, |choice| key_choice(choice, key));
        flow.output::<Key>();
    })
    .expect("显式 selector（第一个来源）构建成功");

    assert_eq!(
        choose_selector_source(&auto, BodyId::ROOT, choose_step_index(&auto, BodyId::ROOT)),
        PositionSource::NodeOutput { step: 0, slot: 0 },
        "自动 selector 必须取入口时点最近的 Key 来源"
    );
    assert_eq!(
        choose_selector_source(
            &with_second,
            BodyId::ROOT,
            choose_step_index(&with_second, BodyId::ROOT)
        ),
        PositionSource::NodeOutput { step: 0, slot: 0 },
        "显式 selector 精确保留所给 Ref"
    );
    assert_eq!(
        choose_selector_source(
            &with_first,
            BodyId::ROOT,
            choose_step_index(&with_first, BodyId::ROOT)
        ),
        PositionSource::RootInput { slot: 0 },
        "显式 selector 指向第一个来源时必须保留它，不得按类型重选最新来源"
    );

    assert_eq!(
        deep_profile(&auto),
        deep_profile(&with_second),
        "自动 selector 与“显式指向同一来源”必须同构"
    );
    let auto_profile = deep_profile(&auto);
    assert_ne!(
        auto_profile,
        deep_profile(&with_first),
        "不同 selector 来源必须得到不同画像：{auto_profile:#?}"
    );

    // 实跑区分：selector 读的是被固定的来源值（分支 Node 的输入始终是 Root 输入）。
    // variant 0/1：selector = key_after(Key(1)) = Key(2) → case Key(2) → Key(1)+10 = Key(11)。
    // variant 2：selector = Root 输入 Key(1) → case Key(1) → key_after(Key(1)) = Key(2)。
    for (variant, expected, expected_calls) in
        [(0u8, Key(11), 2u32), (1, Key(11), 2), (2, Key(2), 2)]
    {
        reset_counters();
        let produced = block_on(Runtime::new().execute::<Key, Key, _>(
            |flow, key| {
                let second = flow.then_with_output(key_after, key);
                if variant == 0 {
                    flow.choose::<Key, _>(|choice| key_choice(choice, key));
                } else if variant == 1 {
                    flow.choose_with(second, |choice| key_choice(choice, key));
                } else {
                    flow.choose_with(key, |choice| key_choice(choice, key));
                }
                flow.output::<Key>();
            },
            Key(1),
        ))
        .expect("三种 selector 配置都必须真实执行");
        assert_eq!(produced, expected, "variant={variant}");
        assert_eq!(
            calls(),
            expected_calls,
            "variant={variant}：入口 Node 与选中分支 Node 各一次"
        );
    }
}

#[test]
fn t05_auto_selector_is_fixed_at_entry_not_the_later_choose_output() {
    reset_counters();

    // 反例：分支各自声明 `Key` 输出，于是 Choose 的共享父输出也是 Key（更晚发布）。
    // 若 selector 延后到分支构建完成后解析，它就会绑到 Choose 自身的输出上。
    let auto: Definition<'static> = build_definition::<Key, Key, _>(|flow, key| {
        flow.choose::<Key, _>(|choice| {
            choice.case(Key(1), |branch| {
                let bumped = branch.then_with_output(key_after, key);
                branch.output_with(bumped);
            });
            choice.case(Key(2), |branch| {
                let bumped = branch.then_with_output(key_plus_ten, key);
                branch.output_with(bumped);
            });
        });
        flow.output::<Key>();
    })
    .expect("Key 分支契约合法");

    let explicit: Definition<'static> = build_definition::<Key, Key, _>(|flow, key| {
        flow.choose_with(key, |choice| {
            choice.case(Key(1), |branch| {
                let bumped = branch.then_with_output(key_after, key);
                branch.output_with(bumped);
            });
            choice.case(Key(2), |branch| {
                let bumped = branch.then_with_output(key_plus_ten, key);
                branch.output_with(bumped);
            });
        });
        flow.output::<Key>();
    })
    .expect("显式 selector 指向入口来源同样合法");

    assert_eq!(
        deep_profile(&auto),
        deep_profile(&explicit),
        "自动 selector 必须固定在入口时点的来源"
    );
    assert_eq!(
        choose_selector_source(&auto, BodyId::ROOT, choose_step_index(&auto, BodyId::ROOT)),
        PositionSource::RootInput { slot: 0 },
        "selector 只能取入口时点的 Key 来源，不得取分支构建期间新增的 Choose 输出"
    );

    // 区分性结构证据：Root 输出槽确实是 Choose 共享输出（同类型、更晚），与 selector 不同位置。
    let root_slot = auto.root().output()[0];
    assert_eq!(
        auto.record(root_slot.handle).expect("root slot").source(),
        PositionSource::ChooseOutput { step: 0, slot: 0 },
        "Root 输出槽必须来自 Choose 共享父输出"
    );
    let selector = auto.root().steps()[choose_step_index(&auto, BodyId::ROOT)].inputs()[0];
    assert_ne!(
        selector.handle.position,
        root_slot.handle.position,
        "selector 不得绑定到 Choose 自身的共享输出：{}\nselector={}",
        handle_str(&auto, root_slot.handle),
        handle_str(&auto, selector.handle)
    );

    // 实跑：selector 读的是入口 Key(1)，命中 case Key(1) → +1。
    let produced = block_on(Runtime::new().execute::<Key, Key, _>(
        |flow, key| {
            flow.choose::<Key, _>(|choice| {
                choice.case(Key(1), |branch| {
                    let bumped = branch.then_with_output(key_after, key);
                    branch.output_with(bumped);
                });
                choice.case(Key(2), |branch| {
                    let bumped = branch.then_with_output(key_plus_ten, key);
                    branch.output_with(bumped);
                });
            });
            flow.output::<Key>();
        },
        Key(1),
    ))
    .expect("自动 selector 固定在入口来源时必须能执行");
    assert_eq!(produced, Key(2), "入口 Key(1) 命中 case Key(1)");
    assert_eq!(calls(), 1, "只执行选中分支的 Node");
}

// ============================================================ E05 分支声明状态

#[test]
fn t05_choose_all_undeclared_and_all_unit_are_legal_and_distinct() {
    reset_counters();

    let undeclared: Definition<'_> = build_definition::<Key, (), _>(|flow, key| {
        flow.choose::<Key, _>(|choice| {
            choice.case(Key(1), |branch| {
                let _ = branch.then_with_output(score_of_key, key);
            });
            choice.otherwise(|branch| {
                let _ = branch.then_with_output(alt_score_of_key, key);
            });
        });
        flow.output::<()>();
    })
    .expect("全 Undeclared（零输出）分支合法");

    let unit: Definition<'_> = build_definition::<Key, (), _>(|flow, key| {
        flow.choose::<Key, _>(|choice| {
            choice.case(Key(1), |branch| {
                let _ = branch.then_with_output(score_of_key, key);
                branch.output::<()>();
            });
            choice.otherwise(|branch| {
                let _ = branch.then_with_output(alt_score_of_key, key);
                branch.output::<()>();
            });
        });
        flow.output::<()>();
    })
    .expect("全显式 Unit 分支合法");

    let states = |definition: &Definition<'_>| -> Vec<DeclarationState> {
        child_bodies(definition, BodyId::ROOT)
            .iter()
            .map(|id| definition.body(*id).output_declaration().state())
            .collect()
    };
    assert_eq!(
        states(&undeclared),
        vec![DeclarationState::Undeclared, DeclarationState::Undeclared]
    );
    assert_eq!(
        states(&unit),
        vec![DeclarationState::Unit, DeclarationState::Unit]
    );
    assert_ne!(
        states(&undeclared),
        states(&unit),
        "未声明与显式 unit 必须可区分"
    );
    assert!(choose_exit_rows(&undeclared, BodyId::ROOT, 0).is_empty());
    assert!(choose_exit_rows(&unit, BodyId::ROOT, 0).is_empty());
    for definition in [&undeclared, &unit] {
        assert_eq!(definition.root().steps()[0].kind(), StepKind::Choose);
        assert!(
            definition.root().steps()[0].outputs().is_empty(),
            "零业务输出的 Choose 不登记假槽"
        );
        assert!(
            !definition
                .positions()
                .iter()
                .any(|record| matches!(record.source(), PositionSource::ChooseOutput { .. })),
            "零槽分支契约不分配共享父输出"
        );
    }

    // 实跑：零输出 Choose 仍执行选中分支（未选分支不执行）。
    let () = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, key| {
            flow.choose::<Key, _>(|choice| {
                choice.case(Key(1), |branch| {
                    let _ = branch.then_with_output(score_of_key, key);
                });
                choice.otherwise(|branch| {
                    let _ = branch.then_with_output(alt_score_of_key, key);
                });
            });
            flow.output::<()>();
        },
        Key(1),
    ))
    .expect("零输出 Choose 必须真实执行");
    assert_eq!(calls(), 1, "只执行选中分支的 Node");
}

#[test]
fn t05_choose_mixed_declaration_states_rejected_before_business() {
    reset_counters();

    // case 显式 unit + otherwise 未声明。
    let unit_then_undeclared = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, key| {
            flow.choose::<Key, _>(|choice| {
                choice.case(Key(1), |branch| {
                    let _ = branch.then_with_output(score_of_key, key);
                    branch.output::<()>();
                });
                choice.otherwise(|branch| {
                    let _ = branch.then_with_output(alt_score_of_key, key);
                });
            });
            flow.output::<()>();
        },
        Key(1),
    ));
    match unit_then_undeclared {
        Err(RunError::Definition(error)) => match error.detail {
            BuildErrorDetail::ChooseDeclarationStateMismatch {
                body,
                step,
                first_branch,
                second_branch,
            } => {
                assert_eq!(body, BodyId::ROOT, "诊断必须指向 Choose 所在 Body");
                assert_eq!(step, Some(0));
                assert_eq!(first_branch, 0);
                assert_eq!(second_branch, 1);
            }
            other => panic!("期望 ChooseDeclarationStateMismatch，实际 {other}"),
        },
        other => panic!("期望 RunError::Definition，实际 {other:?}"),
    }
    assert_eq!(calls(), 0, "混合声明状态必须在任何业务之前拒绝");

    // case 未声明 + otherwise 显式 unit（方向相反）。
    let undeclared_then_unit = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, key| {
            flow.choose::<Key, _>(|choice| {
                choice.case(Key(1), |branch| {
                    let _ = branch.then_with_output(score_of_key, key);
                });
                choice.otherwise(|branch| {
                    let _ = branch.then_with_output(alt_score_of_key, key);
                    branch.output::<()>();
                });
            });
            flow.output::<()>();
        },
        Key(1),
    ));
    assert!(
        matches!(
            undeclared_then_unit,
            Err(RunError::Definition(error))
                if matches!(
                    error.detail,
                    BuildErrorDetail::ChooseDeclarationStateMismatch {
                        first_branch: 0,
                        second_branch: 1,
                        ..
                    }
                )
        ),
        "反向混合同样拒绝：{undeclared_then_unit:?}"
    );

    // NonUnit 与 Undeclared 混合。
    let non_unit_then_undeclared = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, key| {
            flow.choose::<Key, _>(|choice| {
                choice.case(Key(1), |branch| {
                    let score = branch.then_with_output(score_of_key, key);
                    branch.output_with(score);
                });
                choice.otherwise(|branch| {
                    let _ = branch.then_with_output(alt_score_of_key, key);
                });
            });
            flow.output::<()>();
        },
        Key(1),
    ));
    assert!(
        matches!(
            non_unit_then_undeclared,
            Err(RunError::Definition(error))
                if matches!(
                    error.detail,
                    BuildErrorDetail::ChooseDeclarationStateMismatch { .. }
                )
        ),
        "NonUnit 与 Undeclared 混合必须拒绝：{non_unit_then_undeclared:?}"
    );
    assert_eq!(calls(), 0);
}

// ============================================================ E06 配置与运行期

#[test]
fn t05_empty_choose_is_rejected_with_config_error() {
    reset_counters();

    let outcome = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, _key| {
            flow.choose::<Key, _>(|_choice| {});
            flow.output::<()>();
        },
        Key(1),
    ));
    match outcome {
        Err(RunError::Definition(error)) => match error.detail {
            BuildErrorDetail::EmptyChoose { body, step } => {
                assert_eq!(body, BodyId::ROOT);
                assert_eq!(step, Some(0));
            }
            other => panic!("期望 EmptyChoose，实际 {other}"),
        },
        other => panic!("期望 RunError::Definition，实际 {other:?}"),
    }
    assert_eq!(calls(), 0, "空配置必须在任何业务之前拒绝");
}

#[test]
fn t05_duplicate_case_and_otherwise_keep_first_config_and_reject() {
    reset_counters();

    // 重复 case：第二个闭包不得执行（首配置保留），Definition 拒绝。
    let duplicate_case = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, key| {
            flow.choose::<Key, _>(|choice| {
                choice.case(Key(1), |branch| {
                    mark_build();
                    let _ = branch.then_with_output(score_of_key, key);
                    branch.output::<()>();
                });
                choice.case(Key(1), |branch| {
                    mark_build();
                    let _ = branch.then_with_output(alt_score_of_key, key);
                    branch.output::<()>();
                });
            });
            flow.output::<()>();
        },
        Key(1),
    ));
    match duplicate_case {
        Err(RunError::Definition(error)) => match error.detail {
            BuildErrorDetail::DuplicateCase {
                body,
                step,
                first_slot,
                second_slot,
            } => {
                assert_eq!(body, BodyId::ROOT);
                assert_eq!(step, Some(0));
                assert_eq!(first_slot, 0, "首配置保留在第 0 个分支");
                assert_eq!(second_slot, 1, "重复配置被记录为第 1 个分支位置");
            }
            other => panic!("期望 DuplicateCase，实际 {other}"),
        },
        other => panic!("期望 RunError::Definition，实际 {other:?}"),
    }
    assert_eq!(builds(), 1, "重复 case 的定义闭包不得执行");
    assert_eq!(calls(), 0, "配置错误必须在任何业务之前拒绝");

    // 重复 otherwise：同样保留首次登记。
    reset_counters();
    let duplicate_otherwise = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, key| {
            flow.choose::<Key, _>(|choice| {
                choice.case(Key(1), |branch| {
                    let _ = branch.then_with_output(score_of_key, key);
                    branch.output::<()>();
                });
                choice.otherwise(|branch| {
                    mark_build();
                    let _ = branch.then_with_output(alt_score_of_key, key);
                    branch.output::<()>();
                });
                choice.otherwise(|branch| {
                    mark_build();
                    let _ = branch.then_with_output(score_of_key, key);
                    branch.output::<()>();
                });
            });
            flow.output::<()>();
        },
        Key(1),
    ));
    match duplicate_otherwise {
        Err(RunError::Definition(error)) => match error.detail {
            BuildErrorDetail::DuplicateOtherwise { body, step } => {
                assert_eq!(body, BodyId::ROOT);
                assert_eq!(step, Some(0));
            }
            other => panic!("期望 DuplicateOtherwise，实际 {other}"),
        },
        other => panic!("期望 RunError::Definition，实际 {other:?}"),
    }
    assert_eq!(builds(), 1, "重复 otherwise 的定义闭包不得执行");
    assert_eq!(calls(), 0);
}

#[test]
fn t05_unmatched_choose_reports_no_matching_case_and_skips_unselected() {
    reset_counters();

    // 无匹配且无 otherwise：RunError::NoMatchingCase，任何分支 Factory 都不调用。
    let unmatched = block_on(Runtime::new().execute::<Key, Score, _>(
        |flow, _key| {
            let picked: Ref<Score> =
                flow.choose_output::<Key, _, _>(|choice| factory_key_cases(choice));
            flow.output_with(picked);
        },
        Key(9),
    ));
    assert!(
        matches!(unmatched, Err(RunError::NoMatchingCase)),
        "无匹配且无 otherwise 必须返回 NoMatchingCase：{unmatched:?}"
    );
    assert_eq!(calls(), 0, "未选分支的 Factory 一次都不得执行");
    assert_eq!(branch_calls(0) + branch_calls(1) + branch_calls(2), 0);

    // 有匹配：只执行选中分支的 Factory（含 otherwise 的完整配置）。
    reset_counters();
    let produced = block_on(Runtime::new().execute::<Key, Score, _>(
        |flow, _key| {
            let picked: Ref<Score> =
                flow.choose_output::<Key, _, _>(|choice| factory_key_choice(choice));
            flow.output_with(picked);
        },
        Key(2),
    ))
    .expect("命中 case Key(2) 必须成功");
    assert_eq!(produced, Score(2));
    assert_eq!(branch_calls(1), 1, "选中分支的 Factory 执行一次");
    assert_eq!(
        branch_calls(0) + branch_calls(2),
        0,
        "未选分支的 Factory 零调用"
    );

    // otherwise 命中：只有 otherwise 的 Factory 执行。
    reset_counters();
    let fallback = block_on(Runtime::new().execute::<Key, Score, _>(
        |flow, _key| {
            let picked: Ref<Score> =
                flow.choose_output::<Key, _, _>(|choice| factory_key_choice(choice));
            flow.output_with(picked);
        },
        Key(9),
    ))
    .expect("无匹配但有 otherwise 时必须走 otherwise");
    assert_eq!(fallback, Score(9));
    assert_eq!(branch_calls(2), 1, "otherwise 的 Factory 执行一次");
    assert_eq!(branch_calls(0) + branch_calls(1), 0, "两个 case 分支零调用");
}

// ============================================================ E08／E09／E10 Each

#[test]
fn t05_each_item_ref_at_body_start_and_shadowing_needs_saved_ref() {
    reset_counters();

    let definition = build_definition::<Vec<Item>, Vec<Score>, _>(|flow, items| {
        let scores: Ref<Vec<Score>> = flow.each_output(items, |sub| {
            // Item 在 Body 开始即可取用（形式 item 位置已在该 Body 内登记）。
            let original = sub.ref_of::<Item>();
            // 同类型遮蔽：先前的 Node 产出新的 Item 来源。
            let produced = sub.then_with_output(relabel_item, original);
            let nearest = sub.ref_of::<Item>();
            assert_ne!(
                nearest.handle(),
                original.handle(),
                "同类型新来源必须遮蔽旧来源"
            );
            assert_eq!(nearest.handle(), produced.handle());
            // 必须用提前保存的 Ref 才能拿到原始 Item。
            let score = sub.then_with_output(score_of_item, original);
            sub.output_with(score);
        });
        flow.output_with(scores);
    })
    .expect("item Body 内的遮蔽可构建");

    let step = definition.root().steps()[0].each().expect("each step view");
    let item_body = definition.body(step.item());
    assert_eq!(item_body.parent(), Some(BodyId::ROOT));
    assert_eq!(
        item_body.steps()[0].inputs()[0].handle.position,
        step.item_slot(),
        "relabel 的输入必须是形式 item 位置"
    );
    assert_eq!(
        item_body.steps()[1].inputs()[0].handle.position,
        step.item_slot(),
        "score_of_item 必须使用提前保存的 Ref（尚未被遮蔽的 item 位置）"
    );
    assert_eq!(
        item_body.output()[0].handle.position,
        item_body.steps()[1].outputs()[0],
        "item Body 声明的是第二步（使用保存 Ref）的输出"
    );
    assert_eq!(
        definition
            .record(item_body.output()[0].handle)
            .expect("item declaration slot")
            .source(),
        PositionSource::NodeOutput { step: 1, slot: 0 }
    );

    let produced = block_on(Runtime::new().execute::<Vec<Item>, Vec<Score>, _>(
        |flow, items| {
            let scores: Ref<Vec<Score>> = flow.each_output(items, |sub| {
                let original = sub.ref_of::<Item>();
                let _produced = sub.then_with_output(relabel_item, original);
                let _nearest = sub.ref_of::<Item>();
                let score = sub.then_with_output(score_of_item, original);
                sub.output_with(score);
            });
            flow.output_with(scores);
        },
        vec![Item(1), Item(2)],
    ))
    .expect("item Body 必须真实执行");
    assert_eq!(
        produced,
        vec![Score(1), Score(2)],
        "取的是原始 Item，不是遮蔽后的 +1000 Item"
    );
    assert_eq!(calls(), 4, "每项两次 Node（relabel + score_of_item）");
}

#[test]
fn t05_each_aggregation_slots_unit_single_and_two_types() {
    reset_counters();

    // unit item Body：零 collector、零业务槽，但每项仍执行。
    let unit_item = build_definition::<Vec<Item>, (), _>(|flow, items| {
        let () = flow.each_output::<Item, (), _>(items, |sub| {
            let item = sub.ref_of::<Item>();
            let _ = sub.then_with_output(count_item, item);
            sub.output::<()>();
        });
        flow.output::<()>();
    })
    .expect("unit item Body 合法（零 collector）");
    let unit_step = unit_item.root().steps()[0].each().expect("each step view");
    assert!(unit_step.slots().is_empty(), "unit Item 零聚合槽");
    assert!(
        unit_item.root().steps()[0].outputs().is_empty(),
        "unit Item 不登记 caller 输出槽"
    );
    assert!(
        !unit_item.positions().iter().any(|record| matches!(
            record.source(),
            PositionSource::EachLocal { .. } | PositionSource::EachOutput { .. }
        )),
        "零 collector 不建假 Vec 位置"
    );
    let () = block_on(Runtime::new().execute::<Vec<Item>, (), _>(
        |flow, items| {
            let () = flow.each_output::<Item, (), _>(items, |sub| {
                let item = sub.ref_of::<Item>();
                let _ = sub.then_with_output(count_item, item);
                sub.output::<()>();
            });
            flow.output::<()>();
        },
        vec![Item(1), Item(2), Item(3)],
    ))
    .expect("unit item Body 必须逐项执行");
    assert_eq!(calls(), 3, "三项各执行一次 Node");

    // 单槽：聚合为 Ref<Vec<Score>>。
    reset_counters();
    let single = build_definition::<Vec<Item>, Vec<Score>, _>(|flow, items| {
        let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
        flow.output_with(scores);
    })
    .expect("单槽聚合构建成功");
    let single_step = single.root().steps()[0].each().expect("each step view");
    assert_eq!(single_step.slots().len(), 1);
    assert_eq!(
        single_step.slots()[0].collected_type,
        TypeDesc::of::<Vec<Score>>()
    );
    assert_ne!(single_step.slots()[0].local, single_step.slots()[0].output);
    assert_eq!(
        single.root().output()[0].type_desc,
        TypeDesc::of::<Vec<Score>>()
    );
    assert_eq!(
        single
            .record(single.root().output()[0].handle)
            .expect("root slot")
            .source(),
        PositionSource::EachOutput { step: 0, slot: 0 }
    );
    let scores = block_on(Runtime::new().execute::<Vec<Item>, Vec<Score>, _>(
        |flow, items| {
            let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
            flow.output_with(scores);
        },
        vec![Item(4), Item(5)],
    ))
    .expect("单槽聚合必须真实执行");
    assert_eq!(scores, vec![Score(4), Score(5)]);

    // 双槽异类型：逐槽独立聚合、逐槽独立位置与 Core。
    reset_counters();
    let double = build_definition::<Vec<Item>, (Vec<Score>, Vec<Tag>), _>(|flow, items| {
        let (scores, tags): (Ref<Vec<Score>>, Ref<Vec<Tag>>) =
            flow.each_output(items, score_and_tag_item);
        flow.output_with((scores, tags));
    })
    .expect("双槽异类型聚合构建成功");
    let double_step = double.root().steps()[0].each().expect("each step view");
    assert_eq!(double_step.slots().len(), 2);
    assert_eq!(
        double_step.slots()[0].collected_type,
        TypeDesc::of::<Vec<Score>>()
    );
    assert_eq!(
        double_step.slots()[1].collected_type,
        TypeDesc::of::<Vec<Tag>>()
    );
    let mut cores = vec![
        double_step.slots()[0].source.clone(),
        double_step.slots()[0].local.clone(),
        double_step.slots()[0].output.clone(),
        double_step.slots()[1].source.clone(),
        double_step.slots()[1].local.clone(),
        double_step.slots()[1].output.clone(),
    ];
    cores.sort_by_key(|core| ord_core(&double, core));
    cores.dedup();
    assert_eq!(cores.len(), 6, "每个槽的位置必须各自独立：{cores:?}");

    let (score_values, tag_values) = block_on(
        Runtime::new().execute::<Vec<Item>, (Vec<Score>, Vec<Tag>), _>(
            |flow, items| {
                let (scores, tags): (Ref<Vec<Score>>, Ref<Vec<Tag>>) =
                    flow.each_output(items, score_and_tag_item);
                flow.output_with((scores, tags));
            },
            vec![Item(7), Item(8)],
        ),
    )
    .expect("双槽聚合必须真实执行");
    assert_eq!(score_values, vec![Score(7), Score(8)]);
    assert_eq!(tag_values, vec![Tag(1007), Tag(1008)]);
    assert_eq!(calls(), 4, "两项各执行两个 Node");
}

#[test]
fn t05_each_empty_collection_still_builds_and_runs_no_item() {
    reset_counters();

    let definition = build_definition::<Vec<Item>, Vec<Score>, _>(|flow, items| {
        let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
        flow.output_with(scores);
    })
    .expect("空集合不影响构建");

    let step = definition.root().steps()[0].each().expect("each step view");
    assert_eq!(step.slots().len(), 1, "空集合仍登记完整聚合槽");
    assert_eq!(step.slots()[0].collected_type, TypeDesc::of::<Vec<Score>>());
    assert_eq!(
        definition.body(step.item()).steps().len(),
        1,
        "item Body 完整构建"
    );

    let produced = block_on(Runtime::new().execute::<Vec<Item>, Vec<Score>, _>(
        |flow, items| {
            let scores: Ref<Vec<Score>> = flow.each_output(items, score_only_item);
            flow.output_with(scores);
        },
        Vec::<Item>::new(),
    ))
    .expect("空集合仍可执行");
    assert!(produced.is_empty(), "空集合聚合为空 Vec");
    assert_eq!(calls(), 0, "空集合不执行任何 item Body 的 Node／Factory");
}

// ============================================================ E11 O 期望

#[test]
fn t05_each_output_missing_aggregation_rejected_with_fields() {
    reset_counters();

    // O 漏聚合：item 声明 Score，聚合后应是 Vec<Score>。
    let outcome = block_on(Runtime::new().execute::<Vec<Item>, (), _>(
        |flow, items| {
            let _: Ref<Score> = flow.each_output(items, score_only_item);
        },
        vec![Item(1)],
    ));
    match outcome {
        Err(RunError::Definition(error)) => match error.detail {
            BuildErrorDetail::DeclarationShapeMismatch {
                body,
                kind,
                slot,
                expected,
                found,
            } => {
                assert_ne!(body, BodyId::ROOT, "诊断必须指向 item Body");
                assert_eq!(kind, DeclarationKind::Output);
                assert_eq!(slot, 0);
                assert_eq!(expected, TypeDesc::of::<Score>(), "期望是调用方 O 的槽类型");
                assert_eq!(
                    found,
                    Some(TypeDesc::of::<Vec<Score>>()),
                    "实际是聚合后类型"
                );
            }
            other => panic!("期望 DeclarationShapeMismatch，实际 {other}"),
        },
        other => panic!("期望 RunError::Definition，实际 {other:?}"),
    }
    assert_eq!(calls(), 0, "O 期望不符必须在任何业务之前拒绝");

    // 直接构建路径同样拒绝（同一条判据）。
    let error = build_definition::<Vec<Item>, (), _>(|flow, items| {
        let _: Ref<Score> = flow.each_output(items, score_only_item);
    })
    .err()
    .expect("漏聚合必须被拒绝");
    assert!(
        matches!(
            error,
            BuildErrorDetail::DeclarationShapeMismatch {
                expected,
                found: Some(found),
                slot: 0,
                ..
            } if expected == TypeDesc::of::<Score>() && found == TypeDesc::of::<Vec<Score>>()
        ),
        "实得 {error}"
    );
}

#[test]
fn t05_choose_output_expectation_mismatch_rejected_with_fields() {
    reset_counters();

    // 分支契约是 Score，调用方 O 期望 Report。
    let shape = build_definition::<Key, (), _>(|flow, key| {
        let _: Ref<Report> = flow.choose_output::<Key, _, _>(|choice| score_choice(choice, key));
    })
    .err()
    .expect("O 与分支契约不符必须被拒绝");
    match shape {
        BuildErrorDetail::DeclarationShapeMismatch {
            body,
            kind,
            slot,
            expected,
            found,
        } => {
            assert_ne!(body, BodyId::ROOT, "诊断必须指向首个分支 Body");
            assert_eq!(kind, DeclarationKind::Output);
            assert_eq!(slot, 0);
            assert_eq!(expected, TypeDesc::of::<Report>());
            assert_eq!(found, Some(TypeDesc::of::<Score>()));
        }
        other => panic!("期望 DeclarationShapeMismatch，实际 {other}"),
    }

    // arity 不符：分支 1 槽，O 期望 2 槽。
    let arity = build_definition::<Key, (), _>(|flow, key| {
        let _: (Ref<Score>, Ref<Tag>) =
            flow.choose_output::<Key, _, _>(|choice| score_choice(choice, key));
    })
    .err()
    .expect("O arity 与分支契约不符必须被拒绝");
    match arity {
        BuildErrorDetail::DeclarationArityMismatch {
            kind,
            slot,
            expected,
            found,
            ..
        } => {
            assert_eq!(kind, DeclarationKind::Output);
            assert_eq!(slot, 0);
            assert_eq!(expected, 2);
            assert_eq!(found, 1);
        }
        other => panic!("期望 DeclarationArityMismatch，实际 {other}"),
    }

    // 期望不符时共享父输出不得发布：同一 Definition 内的后继使用拿不到合法来源，
    // 且首因仍是 O 期望诊断（后继使用不得替换首因）。
    let unreachable = block_on(Runtime::new().execute::<Key, (), _>(
        |flow, key| {
            let _: Ref<Report> =
                flow.choose_output::<Key, _, _>(|choice| score_choice(choice, key));
            let selected = flow.ref_of::<Score>();
            let _ = flow.then_with_output(report_of_score, selected);
        },
        Key(1),
    ));
    match unreachable {
        Err(RunError::Definition(error)) => match error.detail {
            BuildErrorDetail::DeclarationShapeMismatch {
                expected, found, ..
            } => {
                assert_eq!(expected, TypeDesc::of::<Report>());
                assert_eq!(found, Some(TypeDesc::of::<Score>()));
            }
            other => panic!("期望首因 DeclarationShapeMismatch，实际 {other}"),
        },
        other => panic!("期望 RunError::Definition，实际 {other:?}"),
    }
    assert_eq!(calls(), 0);
}

// ============================================================ E12 失败隔离

#[test]
fn t05_failed_choose_keeps_parent_output_invalid_with_first_cause() {
    reset_counters();

    // 失败构造：otherwise 分支重复声明；共享父输出必须保持失效，首因保留。
    let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
    let root = manual_root(&builder);
    let key_handle = builder.borrow_mut().allocate(
        root,
        PositionSource::RootInput { slot: 0 },
        TypeDesc::of::<Key>(),
    );
    let key = Ref::<Key>::from_handle(key_handle);
    let legal = {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        let _picked: Ref<Score> = flow.choose_with_output(key, |choice| {
            choice.case(Key(1), |branch| {
                let score = branch.then_with_output(score_of_key, key);
                branch.output_with(score);
            });
            choice.otherwise(|branch| {
                let score = branch.then_with_output(alt_score_of_key, key);
                branch.output_with(score);
                branch.output_with(score);
            });
        });
        // 隔离的可观测通道：ref_of 不得选中失败 Choose 的共享父输出，
        // 后继显式使用（then_with）同样拿不到合法来源，且不得替换首因。
        let selected = flow.ref_of::<Score>();
        let _derived = flow.then_with_output(report_of_score, selected);
        let legal = builder
            .borrow()
            .position(selected.handle())
            .map(|record| !record.is_tainted())
            .unwrap_or(false);
        assert_eq!(
            builder.borrow().position(selected.handle()).is_some(),
            legal,
            "观察通道必须与登记结果一致"
        );
        legal
    };
    let error = Rc::try_unwrap(builder)
        .ok()
        .expect("测试不保留内部 Flow")
        .into_inner()
        .finish(root)
        .err()
        .expect("重复声明必须被拒绝");
    assert!(
        !legal,
        "分支声明失败后父域不得有合法 Score 来源（观察通道已生效）"
    );
    match error {
        BuildErrorDetail::DuplicateDeclaration {
            body,
            first,
            second,
        } => {
            assert_ne!(body, BodyId::ROOT, "首因必须指向实际分支 Body");
            assert_eq!(first, DeclarationKind::Output);
            assert_eq!(second, DeclarationKind::Output);
        }
        other => panic!("期望 DuplicateDeclaration，实际 {other}"),
    }
    assert_eq!(calls(), 0);
}

/// 正向对照：同一构造在分支声明合法时必须发布共享父输出并可被父域取用。
#[test]
fn t05_successful_choose_publishes_shared_parent_output() {
    let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
    let root = manual_root(&builder);
    let key_handle = builder.borrow_mut().allocate(
        root,
        PositionSource::RootInput { slot: 0 },
        TypeDesc::of::<Key>(),
    );
    let key = Ref::<Key>::from_handle(key_handle);
    let selected_legal = {
        let mut flow = Flow::open(Rc::clone(&builder), root);
        let _picked: Ref<Score> = flow.choose_with_output(key, |choice| {
            choice.case(Key(1), |branch| {
                let score = branch.then_with_output(score_of_key, key);
                branch.output_with(score);
            });
            choice.otherwise(|branch| {
                let score = branch.then_with_output(alt_score_of_key, key);
                branch.output_with(score);
            });
        });
        let selected = flow.ref_of::<Score>();
        builder.borrow().position(selected.handle()).map(|record| {
            (
                selected.handle(),
                !record.is_tainted(),
                record.published_order().is_some(),
            )
        })
    };
    let definition = Rc::try_unwrap(builder)
        .ok()
        .expect("测试不保留内部 Flow")
        .into_inner()
        .finish(root)
        .expect("合法 Choose 必须构建成功");
    let (handle, not_tainted, published) =
        selected_legal.expect("合法 Choose 必须发布共享父输出（观察通道必须在成功路径上为真）");
    assert!(not_tainted && published, "共享父输出必须已发布且未失效");
    assert_eq!(
        definition.record(handle).expect("shared output").source(),
        PositionSource::ChooseOutput { step: 0, slot: 0 }
    );
}

#[test]
fn t05_failed_each_keeps_parent_output_invalid_with_first_cause() {
    reset_counters();

    // 失败构造：item Body 重复声明 → caller 聚合输出必须保持失效（tainted），首因保留。
    let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
    let root = manual_root(&builder);
    let collection = builder.borrow_mut().allocate(
        root,
        PositionSource::RootInput { slot: 0 },
        TypeDesc::of::<Vec<Item>>(),
    );
    let step = builder.borrow().step_count(root);
    let item = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let item_position = builder
        .borrow_mut()
        .allocate_each_item(item, step, TypeDesc::of::<Item>());
    {
        let mut sub = Flow::open(Rc::clone(&builder), item);
        let item_ref = sub.ref_of::<Item>();
        let _ = sub.then_with_output(score_of_item, item_ref);
        sub.output_with(item_ref);
        sub.output_with(item_ref);
    }
    let capability = builder.borrow().declared_each_capability(item);
    assert!(
        capability.is_none(),
        "重复声明后能力不得被复用（声明已标记失败）"
    );
    let (outputs, ok) = builder.borrow_mut().register_each(
        root,
        item,
        step,
        PositionDesc {
            handle: collection,
            type_desc: TypeDesc::of::<Vec<Item>>(),
        },
        item_position,
        None,
        each_bind_item::<Item>,
        each_collection_len::<Item>,
    );
    assert!(!ok, "item 声明失败时 Each 收口必须失败");
    assert_eq!(outputs.len(), 1, "失败构建仍保持槽位对齐");
    let tainted = builder
        .borrow()
        .position(outputs[0])
        .map(|record| record.is_tainted())
        .expect("caller 输出位置已登记");
    let error = Rc::try_unwrap(builder)
        .ok()
        .expect("测试不保留内部 Flow")
        .into_inner()
        .finish(root)
        .err()
        .expect("item 重复声明必须被拒绝");
    assert!(tainted, "item 声明失败后 caller 聚合输出必须失效");
    match error {
        BuildErrorDetail::DuplicateDeclaration { body, .. } => {
            assert_ne!(body, BodyId::ROOT, "首因必须指向 item Body");
            assert_eq!(body, item);
        }
        other => panic!("期望 DuplicateDeclaration，实际 {other}"),
    }
    assert_eq!(calls(), 0, "定义期拒绝不得执行 item 业务");

    // 正向对照：同样的手工结构、单次声明 → 收口成功且 caller 输出未失效。
    let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
    let root = manual_root(&builder);
    let collection = builder.borrow_mut().allocate(
        root,
        PositionSource::RootInput { slot: 0 },
        TypeDesc::of::<Vec<Item>>(),
    );
    let step = builder.borrow().step_count(root);
    let item = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let item_position = builder
        .borrow_mut()
        .allocate_each_item(item, step, TypeDesc::of::<Item>());
    {
        let mut sub = Flow::open(Rc::clone(&builder), item);
        let item_ref = sub.ref_of::<Item>();
        let _ = sub.then_with_output(score_of_item, item_ref);
        sub.output_with(item_ref);
    }
    let (outputs, ok) = builder.borrow_mut().register_each(
        root,
        item,
        step,
        PositionDesc {
            handle: collection,
            type_desc: TypeDesc::of::<Vec<Item>>(),
        },
        item_position,
        None,
        each_bind_item::<Item>,
        each_collection_len::<Item>,
    );
    assert!(ok, "单次声明的 Each 收口必须成功");
    assert_eq!(outputs.len(), 1);
    let tainted = builder
        .borrow()
        .position(outputs[0])
        .map(|record| record.is_tainted())
        .expect("caller 输出位置已登记");
    assert!(!tainted, "合法 Each 的 caller 输出不得失效");
    let definition = Rc::try_unwrap(builder)
        .ok()
        .expect("测试不保留内部 Flow")
        .into_inner()
        .finish(root)
        .expect("合法 Each 必须构建成功");
    let each = definition.root().steps()[0].each().expect("each step view");
    assert_eq!(each.slots().len(), 1);
    assert_eq!(each.slots()[0].collected_type, TypeDesc::of::<Vec<Item>>());
}

// ============================================================ E13／E14 typed 能力

/// 手工 Builder 路径的 Each：item Body 用 `declare_output` 声明非空输出，可选附上能力。
/// 受控 Builder 路径的 Each 收口结果：定义结果、收口是否成立、item Body、逐槽输出与其 taint 快照。
type ManualEachOutcome = (
    Result<Definition<'static>, BuildErrorDetail>,
    bool,
    BodyId,
    Vec<(PositionHandle, bool)>,
);

fn manual_each_definition(bridge: Option<EachBridge>) -> ManualEachOutcome {
    let builder = Rc::new(RefCell::new(Builder::<'static>::new()));
    let root = manual_root(&builder);
    let collection = builder.borrow_mut().allocate(
        root,
        PositionSource::RootInput { slot: 0 },
        TypeDesc::of::<Vec<Item>>(),
    );
    let step = builder.borrow().step_count(root);
    let item = builder
        .borrow_mut()
        .new_body(Some(root), BodyRole::Ordinary);
    let item_position = builder
        .borrow_mut()
        .allocate_each_item(item, step, TypeDesc::of::<Item>());
    let order = builder.borrow_mut().fresh_order();
    let declared = builder.borrow_mut().declare_output(
        item,
        vec![PositionDesc {
            handle: item_position,
            type_desc: TypeDesc::of::<Item>(),
        }],
        order,
    );
    assert!(declared, "低层 declare_output 必须成功");
    // 低层声明不携带声明时捕获的 typed 能力；只有显式 attach 才有。
    let before = builder.borrow().output_declaration_of(item);
    assert_eq!(before.state(), DeclarationState::NonUnit);
    assert!(before.usable(), "声明本身必须成功（缺的是 typed 能力）");
    let expects_bridge = bridge.is_some();
    if let Some(bridge) = bridge {
        builder.borrow_mut().attach_each_bridge(item, bridge);
    }
    let capability = builder.borrow().declared_each_capability(item);
    assert_eq!(
        capability.is_some(),
        expects_bridge,
        "typed 能力只在显式捕获时可见"
    );
    let (outputs, ok) = builder.borrow_mut().register_each(
        root,
        item,
        step,
        PositionDesc {
            handle: collection,
            type_desc: TypeDesc::of::<Vec<Item>>(),
        },
        item_position,
        None,
        each_bind_item::<Item>,
        each_collection_len::<Item>,
    );
    let output_taints: Vec<(PositionHandle, bool)> = outputs
        .iter()
        .map(|handle| {
            let tainted = builder
                .borrow()
                .position(*handle)
                .expect("each output 已分配")
                .is_tainted();
            (*handle, tainted)
        })
        .collect();
    let state = Rc::try_unwrap(builder)
        .ok()
        .expect("测试不保留内部 Flow")
        .into_inner();
    (state.finish(root), ok, item, output_taints)
}

#[test]
fn t05_missing_each_bridge_on_manual_declaration_is_rejected() {
    let (without_bridge, ok_without_bridge, item, outputs_without_bridge) =
        manual_each_definition(None);
    assert!(!ok_without_bridge, "缺少 typed 能力的 Each 收口必须失败");
    match without_bridge {
        Err(BuildErrorDetail::MissingEachBridge { body }) => {
            assert_ne!(body, BodyId::ROOT, "诊断必须指向声明非空输出的 item Body");
            assert_eq!(body, item);
        }
        Err(other) => panic!("期望 MissingEachBridge，实际 {other}"),
        Ok(_) => panic!("缺少 typed 能力的 Each 不得构建成功"),
    }

    assert!(
        outputs_without_bridge.iter().all(|(_, tainted)| *tainted),
        "缺桥时聚合输出必须保持失效（不得发布为合法来源）"
    );
    // 正向对照：同一结构附上声明时捕获的 typed 能力即可构建。
    let (with_bridge, ok_with_bridge, _item, _outputs) =
        manual_each_definition(Some(EachBridge::of::<Ref<Item>>()));
    assert!(ok_with_bridge, "附上能力后 Each 收口必须成功");
    let definition = with_bridge.expect("附上能力的 Each 必须构建成功");
    let step = definition.root().steps()[0].each().expect("each step view");
    assert_eq!(step.slots().len(), 1);
    assert_eq!(step.slots()[0].collected_type, TypeDesc::of::<Vec<Item>>());
}

#[test]
fn t05_each_aggregated_values_are_really_read() {
    reset_counters();

    let definition = build_definition::<Vec<Item>, Report, _>(|flow, items| {
        let (scores, tags): (Ref<Vec<Score>>, Ref<Vec<Tag>>) =
            flow.each_output(items, score_and_tag_item);
        let report = flow.then_with_output(pair_report, (scores, tags));
        flow.output_with(report);
    })
    .expect("双槽聚合 + 后继读取必须构建成功");

    let step = definition.root().steps()[0].each().expect("each step view");
    assert_eq!(step.slots().len(), 2);
    let consumer = &definition.root().steps()[1];
    assert_eq!(consumer.kind(), StepKind::Node);
    assert_eq!(
        consumer.inputs().len(),
        2,
        "后继 Node 必须真实连接到两个聚合槽"
    );
    for (slot, desc) in consumer.inputs().iter().enumerate() {
        let expected = if slot == 0 {
            TypeDesc::of::<Vec<Score>>()
        } else {
            TypeDesc::of::<Vec<Tag>>()
        };
        assert_eq!(
            definition
                .record(desc.handle)
                .expect("aggregate slot")
                .type_desc(),
            expected,
            "槽 {slot} 的聚合类型必须被真实读取"
        );
    }

    let produced = block_on(Runtime::new().execute::<Vec<Item>, Report, _>(
        |flow, items| {
            let (scores, tags): (Ref<Vec<Score>>, Ref<Vec<Tag>>) =
                flow.each_output(items, score_and_tag_item);
            let report = flow.then_with_output(pair_report, (scores, tags));
            flow.output_with(report);
        },
        vec![Item(1), Item(2)],
    ))
    .expect("聚合值必须被后继 Node 真实读取");
    // scores=[1,2] → 3*10=30；tags=[1001,1002] → 2003。
    assert_eq!(produced, Report(2033));
    assert_eq!(calls(), 5, "两项各两个 Node，加一次 pair_report");
}

// ============================================================ E17 过渡组合

#[test]
fn t05_choose_and_each_embed_chain_and_retry() {
    reset_counters();

    for (key_value, expected) in [(1u32, Score(11)), (2u32, Score(102))] {
        reset_counters();
        let produced = block_on(Runtime::new().execute::<Key, Score, _>(
            |flow, key| {
                let picked: Ref<Score> = flow.choose_output::<Key, _, _>(|choice| {
                    choice.case(Key(1), |branch| {
                        let nested: Ref<Score> = branch.chain_output(|inner| {
                            let score = inner.then_with_output(score_of_key, key);
                            inner.output_with(score);
                        });
                        branch.output_with(nested);
                    });
                    choice.otherwise(|branch| {
                        let nested: Ref<Score> = branch.retry_output(0, |attempt| {
                            let score = attempt.then_with_output(alt_score_of_key, key);
                            attempt.output_with(score);
                        });
                        branch.output_with(nested);
                    });
                });
                flow.output_with(picked);
            },
            Key(key_value),
        ))
        .expect("分支内嵌 chain／retry 的 Choose 必须执行");
        assert_eq!(produced, expected, "key={key_value}");
        assert_eq!(calls(), 1, "只执行命中分支内嵌的 chain／retry 一步");
    }

    // 结构：两个分支都是 NonUnit，且声明槽分别来自 Chain／Retry 的取输出。
    let definition = build_definition::<Key, Score, _>(|flow, key| {
        let picked: Ref<Score> = flow.choose_output::<Key, _, _>(|choice| {
            choice.case(Key(1), |branch| {
                let nested: Ref<Score> = branch.chain_output(|inner| {
                    let score = inner.then_with_output(score_of_key, key);
                    inner.output_with(score);
                });
                branch.output_with(nested);
            });
            choice.otherwise(|branch| {
                let nested: Ref<Score> = branch.retry_output(0, |attempt| {
                    let score = attempt.then_with_output(alt_score_of_key, key);
                    attempt.output_with(score);
                });
                branch.output_with(nested);
            });
        });
        flow.output_with(picked);
    })
    .expect("过渡组合构建成功");
    let branches = choose_branch_states(&definition, BodyId::ROOT, 0);
    assert_eq!(branches.len(), 2, "两个分支都必须登记出口：{branches:?}");
    for (_branch, body, state) in &branches {
        assert_eq!(*state, DeclarationState::NonUnit);
        assert_eq!(definition.body(*body).parent(), Some(BodyId::ROOT));
    }
    let case_body = definition.body(branches[0].1);
    assert_eq!(case_body.steps()[0].kind(), StepKind::Chain);
    assert_eq!(
        definition
            .record(case_body.output()[0].handle)
            .expect("case declaration slot")
            .source(),
        PositionSource::ChainOutput { step: 0, slot: 0 }
    );
    let otherwise_body = definition.body(branches[1].1);
    assert_eq!(otherwise_body.steps()[0].kind(), StepKind::Retry);
    assert_eq!(
        definition
            .record(otherwise_body.output()[0].handle)
            .expect("otherwise declaration slot")
            .source(),
        PositionSource::RetryOutput { step: 0, slot: 0 }
    );

    // Each 内嵌 Choose（分支内再 chain）：
    reset_counters();
    let collected = block_on(Runtime::new().execute::<Vec<Item>, Vec<Score>, _>(
        |flow, items| {
            let scores: Ref<Vec<Score>> = flow.each_output(items, |sub| {
                let item = sub.ref_of::<Item>();
                let selector = sub.then_with_output(key_of_item, item);
                let chosen: Ref<Score> = sub.choose_with_output(selector, |choice| {
                    choice.case(Key(1), |branch| {
                        let nested: Ref<Score> = branch.chain_output(|inner| {
                            let score = inner.then_with_output(score_of_item, item);
                            inner.output_with(score);
                        });
                        branch.output_with(nested);
                    });
                    choice.otherwise(|branch| {
                        let shifted = branch.then_with_output(shift_item, item);
                        branch.output_with(shifted);
                    });
                });
                sub.output_with(chosen);
            });
            flow.output_with(scores);
        },
        vec![Item(1), Item(2)],
    ))
    .expect("Each 内嵌 Choose/Chain 必须执行");
    assert_eq!(
        collected,
        vec![Score(1), Score(52)],
        "Item(1) 走 case+Chain，Item(2) 走 otherwise(+50)"
    );
    assert_eq!(calls(), 4, "每项两步：selector Node 与命中分支的 Node");
}

// ---------------------------------------------------------------- 审查 R1 修订回归（R01～R03）

/// R01-a：全部分支未声明（合法零输出契约）但调用方要求**非空 O** —— 自动/显式都必须
/// 在业务执行前拒绝；未声明 + O=() 是正对照，且分支必须保持 Undeclared 状态。
#[test]
fn t05_choose_undeclared_branches_with_non_empty_o_are_rejected() {
    reset_counters();
    let auto = build_definition::<Key, (), _>(|flow, _key| {
        let _: Ref<Score> = flow.choose_output(|choice| {
            choice.case(Key(1), |sub| {
                sub.provide(|| {
                    mark_build();
                    Score(1)
                });
            });
            choice.otherwise(|sub| {
                sub.provide(|| {
                    mark_build();
                    Score(2)
                });
            });
        });
        flow.output::<()>();
    });
    match auto {
        Err(BuildErrorDetail::MissingDeclaration { body, kind }) => {
            assert_eq!(kind, DeclarationKind::Output);
            assert_ne!(body, BodyId::ROOT, "诊断必须指向未声明的分支 Body");
        }
        Err(other) => panic!("期望 MissingDeclaration，实际 {other}"),
        Ok(_) => panic!("未声明分支 + 非空 O 必须在业务执行前拒绝"),
    }
    assert_eq!(builds(), 0, "拒绝必须发生在任何 Factory/Node 之前");

    let explicit = build_definition::<Key, (), _>(|flow, key| {
        let _: Ref<Score> = flow.choose_with_output(key, |choice| {
            choice.case(Key(1), |sub| {
                sub.provide(|| {
                    mark_build();
                    Score(1)
                });
            });
        });
        flow.output::<()>();
    });
    assert!(
        matches!(explicit, Err(BuildErrorDetail::MissingDeclaration { .. })),
        "显式 selector 的未声明 + 非空 O 同样必须拒绝"
    );
    assert_eq!(builds(), 0);

    reset_counters();
    let legal = build_definition::<Key, (), _>(|flow, key| {
        flow.choose_with(key, |choice| {
            choice.case(Key(1), |sub| {
                sub.provide(|| {
                    mark_build();
                    Score(1)
                });
            });
        });
        flow.output::<()>();
    })
    .expect("未声明 + O=() 必须合法");
    let branch = child_bodies(&legal, BodyId::ROOT)[0];
    assert_eq!(
        legal.body(branch).output_declaration().state(),
        DeclarationState::Undeclared,
        "合法未声明不得被补成显式 Unit"
    );
}

/// R01-b：两分支分别声明不同类型、调用方给出第三种 O 时，**分支契约错误必须先于 O 期望**，
/// 且报告的必须是契约不一致本身（这里是逐槽类型），而不是 O 与首分支的比对结果。
#[test]
fn t05_choose_contract_error_precedes_o_expectation() {
    let result = build_definition::<Key, (), _>(|flow, key| {
        let _: Ref<Report> = flow.choose_with_output(key, |choice| {
            choice.case(Key(1), |sub| {
                let scored = sub.then_with_output(score_of_key, key);
                sub.output_with(scored);
            });
            choice.otherwise(|sub| {
                let tagged = sub.then_with_output(tag_of_key, key);
                sub.output_with(tagged);
            });
        });
        flow.output::<()>();
    });
    match result {
        Err(BuildErrorDetail::ChooseSlotTypeMismatch {
            branch,
            slot,
            expected,
            found,
            ..
        }) => {
            assert_eq!(branch, 1, "必须报告第二个分支与共同契约不一致");
            assert_eq!(slot, 0);
            assert_eq!(expected, TypeDesc::of::<Score>());
            assert_eq!(found, TypeDesc::of::<Tag>());
        }
        Err(other) => panic!("契约错误必须先于 O 期望，实际 {other}"),
        Ok(_) => panic!("分支契约不一致必须构建拒绝"),
    }
}

/// R02：受控 Builder 的 typed 能力必须确属这份冻结声明——错元素类型、错槽数分别拒绝，
/// 全部聚合输出保持失效；正确桥是正对照（输出为合法来源）。
#[test]
fn t05_each_bridge_must_match_the_frozen_declaration() {
    let (wrong_type, ok_wrong_type, item, taints) =
        manual_each_definition(Some(EachBridge::of::<Ref<Tag>>()));
    assert!(!ok_wrong_type, "元素类型不符的桥必须被拒绝");
    match wrong_type {
        Err(BuildErrorDetail::DeclarationShapeMismatch {
            body,
            slot,
            expected,
            found,
            ..
        }) => {
            assert_eq!(body, item);
            assert_eq!(slot, 0);
            assert_eq!(expected, TypeDesc::of::<Item>());
            assert_eq!(found, Some(TypeDesc::of::<Tag>()));
        }
        Err(other) => panic!("期望 DeclarationShapeMismatch，实际 {other}"),
        Ok(_) => panic!("错型桥不得构建成功"),
    }
    assert!(
        taints.iter().all(|(_, tainted)| *tainted),
        "错型桥不得把聚合输出发布为合法来源"
    );

    let (wrong_arity, ok_wrong_arity, item, taints) =
        manual_each_definition(Some(EachBridge::of::<()>()));
    assert!(!ok_wrong_arity, "一槽声明配零槽能力必须被拒绝");
    match wrong_arity {
        Err(BuildErrorDetail::DeclarationArityMismatch {
            body,
            expected,
            found,
            ..
        }) => {
            assert_eq!(body, item);
            assert_eq!((expected, found), (1, 0));
        }
        Err(other) => panic!("期望 DeclarationArityMismatch，实际 {other}"),
        Ok(_) => panic!("错槽数桥不得构建成功"),
    }
    assert!(taints.iter().all(|(_, tainted)| *tainted));

    let (good, ok_good, _item, taints) =
        manual_each_definition(Some(EachBridge::of::<Ref<Item>>()));
    assert!(ok_good && good.is_ok(), "正确桥必须构建成功");
    assert!(
        taints.iter().all(|(_, tainted)| !*tainted),
        "正确桥的输出必须是合法来源"
    );
}

/// R03：结构画像必须记录 Choose 的**真实逐槽交接**（source→exit→output），并能区分
/// “只把真实交接来源从 core#a 改到 core#b”——仅声明类型/位置数量相同不足以替代。
#[test]
fn t05_choose_handover_mapping_is_profiled_and_distinguishes_sources() {
    let build = |use_second: bool| {
        build_definition::<Key, (), _>(|flow, _key| {
            let first = flow.provide_output(|| Score(1));
            let second = flow.provide_output(|| Score(2));
            let chosen: Ref<Score> = flow.choose_output(|choice| {
                choice.case(Key(1), move |sub| {
                    let source = if use_second { second } else { first };
                    sub.output_with(source);
                });
            });
            let _ = chosen;
            flow.output::<()>();
        })
        .expect("同类型祖先的两种交接都应构建成功")
    };
    let left = build(false);
    let right = build(true);
    let left_profile = deep_profile(&left);
    assert!(
        left_profile
            .iter()
            .any(|line| line.contains("choose-handover")),
        "结构画像必须记录真实逐槽交接映射（source/exit/output）"
    );
    assert_ne!(
        left_profile,
        deep_profile(&right),
        "只改真实交接来源也必须使画像不同"
    );

    let step = left
        .root()
        .steps()
        .iter()
        .find(|step| step.kind() == StepKind::Choose)
        .expect("Choose Step 已在 root 登记");
    let rows = step.choose_slots();
    assert_eq!(rows.len(), 1, "一个 case、一个槽");
    let branch = child_bodies(&left, BodyId::ROOT)[0];
    let declared = left.body(branch).output_declaration().slots[0].handle;
    let declared_core = left.record(declared).expect("声明槽已登记").core();
    assert_eq!(
        &rows[0].source, declared_core,
        "交接 source 必须指向该分支真实声明来源"
    );
    assert_eq!((rows[0].branch, rows[0].slot), (0, 0));
}

/// R01（复审）：失败 Ref 继续被 Root `output_with` 或后继 `then_with` 消费时，仍必须报告
/// **原分支契约错误**，不能被失败形状派生的占位错误（UnknownPosition 等）遮蔽；
/// 失败来源不合法、无 panic、业务零调用。
#[test]
fn t05_failed_choose_ref_used_later_keeps_contract_first_cause() {
    reset_counters();
    // 变体一：失败 Ref 交给 Root 声明。
    let to_root = build_definition::<Key, (), _>(|flow, key| {
        let output: Ref<Score> = flow.choose_with_output(key, |choice| {
            choice.case(Key(1), |sub| {
                let scored = sub.then_with_output(score_of_key, key);
                sub.provide(|| {
                    mark_build();
                    Score(0)
                });
                sub.output_with(scored);
            });
            choice.otherwise(|sub| {
                let tagged = sub.then_with_output(tag_of_key, key);
                sub.provide(|| {
                    mark_build();
                    Tag(0)
                });
                sub.output_with(tagged);
            });
        });
        flow.output_with(output);
    });
    match to_root {
        Err(BuildErrorDetail::ChooseSlotTypeMismatch {
            branch,
            slot,
            expected,
            found,
            ..
        }) => {
            assert_eq!((branch, slot), (1, 0));
            assert_eq!(
                (expected, found),
                (TypeDesc::of::<Score>(), TypeDesc::of::<Tag>())
            );
        }
        Err(other) => panic!("交给 Root 后必须保留原分支契约首因，实际 {other}"),
        Ok(_) => panic!("分支契约不一致必须构建拒绝"),
    }
    assert_eq!(builds(), 0, "拒绝必须发生在任何 Factory/Node 之前");

    // 变体二：失败 Ref 被后继 Node 消费（then_with），仍报原契约错误。
    let to_node = build_definition::<Key, (), _>(|flow, key| {
        let output: Ref<Score> = flow.choose_with_output(key, |choice| {
            choice.case(Key(1), |sub| {
                let scored = sub.then_with_output(score_of_key, key);
                sub.output_with(scored);
            });
            choice.otherwise(|sub| {
                let tagged = sub.then_with_output(tag_of_key, key);
                sub.output_with(tagged);
            });
        });
        let report = flow.then_with_output(report_of_score, output);
        flow.output_with(report);
    });
    match to_node {
        Err(BuildErrorDetail::ChooseSlotTypeMismatch { branch: 1, .. }) => {}
        Err(other) => panic!("后继消费失败 Ref 时同样必须保留原契约首因，实际 {other}"),
        Ok(_) => panic!("分支契约不一致必须构建拒绝"),
    }
    assert_eq!(builds(), 0);
}
