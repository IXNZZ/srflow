//! WE-T06 S10：两种 SES 形态的 Definition 复用——**离线可控业务替身**。
//!
//! ## 这不是真实 SES 验收
//!
//! 本文件是**离线可控业务替身**：业务意图（四类 Root 输入、make_basis／make_plan／
//! write_prose／diagnose／revise／choose_revision／三类修改动作／stop_if_acceptable）取自
//! `examples/SRFlow_Public_API_v21_Probe/tests/ses.rs` 与场景基线 §10.10 的 S10。它**不**调用
//! 真实模型、**不**评估故事质量、**不**验证真实 SES 修改策略或生产方案，因此**不构成本任务的
//! 真实 SES 验收**。这里只验证冻结公开 API（SPEC §2～§12）能否用**同一批业务 Node 类型与协议**
//! 表达两种 Definition：
//!
//! - 形态 A：单次诊断修改（`then` ＋ child `chain`）；
//! - 形态 B：Iter ＋ Choose（`iter` 内 `then` 控制 adapter 与 `choose` 策略）。
//!
//! 执行模型不复制 Probe 的简化 Runtime／值存储：两种形态都直接走本 crate 的正式
//! [`Runtime::execute`]／[`Flow`]／child Body／Root owned 提取，也不链接 Probe crate。
//!
//! ## 本文件的实际判据（任务书 §4 S10／§8 X22）
//!
//! 1. 四 Root 输入 `(CharacterState, BackgroundFacts, ExpressionTask, WritingBoundary)` 由同一个
//!    `MakeBasis` Node 消费；观察记录随 `WritingBoundary` 数据进入工作流并被 `Prose`／`Diagnosis`
//!    带到后续轮次，因此事件顺序是真实调用顺序，而不是各节点局部日志的拼接。
//! 2. `basis` 位于 Iter 之外，**每次 `execute` 只产生一次**（`Trace::count` 调用计数断言；A／B
//!    两形态各有样本，并有一次「同一批 Node 连续两次 execute」的计数）。
//! 3. B 中 `stop_if_acceptable` 与 `choose_revision` 只共享 `Diagnosis`、彼此没有直接数据依赖，
//!    但定义顺序必须是执行顺序：每轮事件严格为 `diagnose:N` → `stop:N` → `choose:N` → 修改动作。
//! 4. 第 N 轮 `stop` 发出 `IterBreak` 后，策略 Node 与三类修改 Node **零调用**（调用计数断言，
//!    不是「没有字符串」的壳判断）。
//! 5. 三类策略 Reduce／Supplement／Rewrite 都有样本（各自调用计数与事件）。
//! 6. 第 N 轮仍 Continue 时返回 `RunError::IterationLimitReached` 且**不额外执行一次 Judge**
//!    （Judge 调用计数等于实际轮数）。

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, RunError, Runtime};

// ------------------------------------------------------------------ 观察：调用计数 + 事件顺序

/// 各业务节点的调用计数槽。
///
/// [`Trace::note`] 在同一个点上推进计数与事件序列，因此计数不是从事件串反推出来的。
#[derive(Default)]
struct Calls {
    basis: Cell<u32>,
    write: Cell<u32>,
    diagnose: Cell<u32>,
    stop: Cell<u32>,
    choose: Cell<u32>,
    reduce: Cell<u32>,
    supplement: Cell<u32>,
    rewrite: Cell<u32>,
    revise: Cell<u32>,
}

/// 离线业务替身的共享观察记录（事件序列 ＋ 调用计数）。
struct Trace {
    events: RefCell<Vec<String>>,
    calls: Calls,
}

impl Trace {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            events: RefCell::new(Vec::new()),
            calls: Calls::default(),
        })
    }

    /// 记录一次业务调用：调用计数与事件序列同时前进。
    fn note(&self, slot: fn(&Calls) -> &Cell<u32>, event: impl Into<String>) -> u32 {
        let counter = slot(&self.calls);
        let count = counter.get() + 1;
        counter.set(count);
        self.events.borrow_mut().push(event.into());
        count
    }

    /// 读取某个业务节点的调用次数。
    fn count(&self, slot: fn(&Calls) -> &Cell<u32>) -> u32 {
        slot(&self.calls).get()
    }

    /// 当前事件序列快照。
    fn events(&self) -> Vec<String> {
        self.events.borrow().clone()
    }
}

// ------------------------------------------------------------------ 业务数据（离线替身）

#[derive(Data)]
struct CharacterState(u32);

#[derive(Data)]
struct BackgroundFacts(u32);

#[derive(Data)]
struct ExpressionTask(u32);

/// 写作边界：本替身用它承载共享观察记录（真实 SES 中是写作约束）。
#[derive(Data)]
struct WritingBoundary(Rc<Trace>);

#[derive(Data)]
struct Basis(u32);

#[derive(Data)]
struct Plan(u32);

/// 正文：评分 ＋ 共享观察记录（记录随业务数据流动，不在 Node 里带执行状态）。
#[derive(Data)]
struct Prose {
    score: u32,
    trace: Rc<Trace>,
}

#[derive(Data)]
struct Diagnosis {
    score: u32,
    acceptable: bool,
    trace: Rc<Trace>,
}

/// 业务修改策略：只实现 `Data + PartialEq`，不是 SRFlow 控制类型（SPEC §10.10 基线）。
#[derive(Data, PartialEq)]
enum RevisionAction {
    Reduce,
    Supplement,
    Rewrite,
}

/// 四类 Root 输入的 owned 形态。
type SesInput = (
    CharacterState,
    BackgroundFacts,
    ExpressionTask,
    WritingBoundary,
);

// ------------------------------------------------------------------ 业务 Node（两形态共用）

/// 依据生成：唯一消费四类 Root 输入的 Node；位于 Iter 之外（基线 §10.10）。
struct MakeBasis;

impl Node for MakeBasis {
    type Input = (
        CharacterState,
        BackgroundFacts,
        ExpressionTask,
        WritingBoundary,
    );
    type Output = Basis;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Basis, BodyError> {
        let (_, _, task, boundary) = query.get();
        boundary.0.note(|calls| &calls.basis, "basis");
        Ok(Basis(task.0))
    }
}

/// 规划：函数 Node；A／B 共用同一签名与实现（SPEC §5.1）。
async fn make_plan(
    query: Query<(&CharacterState, &BackgroundFacts, &ExpressionTask)>,
) -> Result<Plan, BodyError> {
    let (characters, facts, _) = query.get();
    Ok(Plan(characters.0 + facts.0))
}

/// 初稿：Chain 的声明输出；`Plan` 只在该 child 内可见。
struct WriteProse;

impl Node for WriteProse {
    type Input = (Plan, CharacterState, BackgroundFacts, WritingBoundary);
    type Output = Prose;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Prose, BodyError> {
        let (plan, _, _, boundary) = query.get();
        boundary.0.note(|calls| &calls.write, "write");
        Ok(Prose {
            score: plan.0,
            trace: Rc::clone(&boundary.0),
        })
    }
}

/// 诊断：读初稿与依据，产出可否接受；A／B 共用同一签名与实现。
struct Diagnose;

impl Node for Diagnose {
    type Input = (Prose, Basis);
    type Output = Diagnosis;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Diagnosis, BodyError> {
        let (prose, basis) = query.get();
        let score = prose.score;
        prose
            .trace
            .note(|calls| &calls.diagnose, format!("diagnose:{score}"));
        Ok(Diagnosis {
            score,
            acceptable: score >= basis.0,
            trace: Rc::clone(&prose.trace),
        })
    }
}

/// 轻量控制 adapter：可接受即 `IterBreak`，否则 `()`。
struct StopIfAcceptable;

impl Node for StopIfAcceptable {
    type Input = Diagnosis;
    type Output = ();

    async fn run(&self, query: Query<&Diagnosis>) -> Result<(), BodyError> {
        let diagnosis = query.get();
        diagnosis
            .trace
            .note(|calls| &calls.stop, format!("stop:{}", diagnosis.score));
        if diagnosis.acceptable {
            Err(BodyError::iter_break())
        } else {
            Ok(())
        }
    }
}

/// 策略选择：只读 `Diagnosis`，与 [`StopIfAcceptable`] 没有直接数据依赖。
struct ChooseRevision;

impl Node for ChooseRevision {
    type Input = Diagnosis;
    type Output = RevisionAction;

    async fn run(&self, query: Query<&Diagnosis>) -> Result<RevisionAction, BodyError> {
        let diagnosis = query.get();
        diagnosis
            .trace
            .note(|calls| &calls.choose, format!("choose:{}", diagnosis.score));
        Ok(match diagnosis.score % 3 {
            0 => RevisionAction::Reduce,
            1 => RevisionAction::Supplement,
            _ => RevisionAction::Rewrite,
        })
    }
}

/// 策略 Reduce 对应的修改 Node：只声明 `Prose`／`Diagnosis`／`WritingBoundary`。
struct Reduce;

impl Node for Reduce {
    type Input = (Prose, Diagnosis, WritingBoundary);
    type Output = Prose;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Prose, BodyError> {
        let (prose, _, _) = query.get();
        prose.trace.note(|calls| &calls.reduce, "reduce");
        Ok(Prose {
            score: prose.score + 1,
            trace: Rc::clone(&prose.trace),
        })
    }
}

/// 策略 Supplement 对应的修改 Node：额外读 `ExpressionTask`。
struct Supplement;

impl Node for Supplement {
    type Input = (Prose, Diagnosis, ExpressionTask, WritingBoundary);
    type Output = Prose;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Prose, BodyError> {
        let (prose, _, _, _) = query.get();
        prose.trace.note(|calls| &calls.supplement, "supplement");
        Ok(Prose {
            score: prose.score + 1,
            trace: Rc::clone(&prose.trace),
        })
    }
}

/// 策略 Rewrite 对应的修改 Node：额外读 `CharacterState`／`BackgroundFacts`。
struct Rewrite;

impl Node for Rewrite {
    type Input = (Prose, Diagnosis, CharacterState, BackgroundFacts);
    type Output = Prose;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Prose, BodyError> {
        let (prose, _, _, _) = query.get();
        prose.trace.note(|calls| &calls.rewrite, "rewrite");
        Ok(Prose {
            score: prose.score + 1,
            trace: Rc::clone(&prose.trace),
        })
    }
}

/// 形态 A 的单次修改 Node（B 不注册它，因此其调用计数可作为零调用对照）。
struct Revise;

impl Node for Revise {
    type Input = (Prose, Diagnosis, WritingBoundary);
    type Output = Prose;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Prose, BodyError> {
        let (prose, _, _) = query.get();
        prose.trace.note(|calls| &calls.revise, "revise");
        Ok(Prose {
            score: prose.score + 1,
            trace: Rc::clone(&prose.trace),
        })
    }
}

/// 两种 Definition 共用的**同一批**业务 Node。
///
/// 全部是无状态结构体：复用 Node 类型与协议，不在 Node 里携带执行状态；观察记录随业务数据
/// （`WritingBoundary` → `Prose` → `Diagnosis`）流动。
struct Nodes {
    basis: MakeBasis,
    write: WriteProse,
    diagnose: Diagnose,
    stop: StopIfAcceptable,
    choose: ChooseRevision,
    reduce: Reduce,
    supplement: Supplement,
    rewrite: Rewrite,
    revise: Revise,
}

impl Nodes {
    fn new() -> Self {
        Self {
            basis: MakeBasis,
            write: WriteProse,
            diagnose: Diagnose,
            stop: StopIfAcceptable,
            choose: ChooseRevision,
            reduce: Reduce,
            supplement: Supplement,
            rewrite: Rewrite,
            revise: Revise,
        }
    }
}

// ------------------------------------------------------------------ 两种 Definition

/// 形态 A：单次诊断修改（基线 §10.10 的理想 Public API A）。
async fn scheme_a(runtime: &Runtime, nodes: &Nodes, input: SesInput) -> Result<Prose, RunError> {
    runtime
        .execute(
            |flow: &mut Flow<'_>, (characters, facts, task, boundary)| {
                let basis =
                    flow.then_with_output(&nodes.basis, (characters, facts, task, boundary));
                let draft = flow.chain(|writing| {
                    let plan = writing.then_with_output(make_plan, (characters, facts, task));
                    writing.then_with_output(&nodes.write, (plan, characters, facts, boundary))
                });
                let diagnosis = flow.then_with_output(&nodes.diagnose, (draft, basis));
                let out_refs = flow.then_with_output(&nodes.revise, (draft, diagnosis, boundary));
                flow.output_with(out_refs);
            },
            input,
        )
        .await
}

/// 形态 B：Iter ＋ Choose（基线 §10.10 的理想 Public API B）。
///
/// `basis` 与 `draft` 都在 Iter 之外；每轮依次诊断 → 控制 adapter → 策略 → 策略分支修改。
async fn scheme_b(
    runtime: &Runtime,
    nodes: &Nodes,
    input: SesInput,
    limit: usize,
) -> Result<Prose, RunError> {
    runtime
        .execute(
            |flow: &mut Flow<'_>, (characters, facts, task, boundary)| {
                let basis =
                    flow.then_with_output(&nodes.basis, (characters, facts, task, boundary));
                let draft = flow.chain(|writing| {
                    let plan = writing.then_with_output(make_plan, (characters, facts, task));
                    writing.then_with_output(&nodes.write, (plan, characters, facts, boundary))
                });
                let out_refs = flow.iter(draft, limit, |current, round| {
                    let diagnosis = round.then_with_output(&nodes.diagnose, (current, basis));
                    round.then_with_output(&nodes.stop, diagnosis);
                    let action = round.then_with_output(&nodes.choose, diagnosis);
                    round.choose(action, |choice| {
                        choice.case(RevisionAction::Reduce, |branch| {
                            branch.then_with_output(&nodes.reduce, (current, diagnosis, boundary))
                        });
                        choice.case(RevisionAction::Supplement, |branch| {
                            branch.then_with_output(
                                &nodes.supplement,
                                (current, diagnosis, task, boundary),
                            )
                        });
                        choice.case(RevisionAction::Rewrite, |branch| {
                            branch.then_with_output(
                                &nodes.rewrite,
                                (current, diagnosis, characters, facts),
                            )
                        });
                    })
                });
                flow.output_with(out_refs);
            },
            input,
        )
        .await
}

// ------------------------------------------------------------------ 测试辅助

fn ses_input(target: u32, trace: &Rc<Trace>) -> SesInput {
    (
        CharacterState(0),
        BackgroundFacts(0),
        ExpressionTask(target),
        WritingBoundary(Rc::clone(trace)),
    )
}

fn ev(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| (*item).to_string()).collect()
}

fn index_of(events: &[String], needle: &str) -> usize {
    events
        .iter()
        .position(|event| event == needle)
        .unwrap_or_else(|| panic!("event {needle:?} missing from {events:?}"))
}

// ------------------------------------------------------------------ 形态 A

#[test]
fn s10_scheme_a_single_diagnosis_revision_consumes_four_root_inputs_once() {
    let runtime = Runtime::new();
    let nodes = Nodes::new();
    let trace = Trace::new();

    let prose = block_on(scheme_a(&runtime, &nodes, ses_input(2, &trace)))
        .unwrap_or_else(|error| panic!("scheme A must succeed: {error}"));

    assert_eq!(prose.score, 1, "initial draft 0 plus one revision is 1");
    assert_eq!(
        trace.events(),
        ev(&["basis", "write", "diagnose:0", "revise"]),
        "A 的定义顺序就是执行顺序"
    );

    // basis 在 Iter 之外：本次 execute 只产生一次。
    assert_eq!(trace.count(|calls| &calls.basis), 1);
    assert_eq!(trace.count(|calls| &calls.write), 1);
    assert_eq!(trace.count(|calls| &calls.diagnose), 1);
    assert_eq!(trace.count(|calls| &calls.revise), 1);

    // A 不含 Iter／Choose：B 专属的控制 adapter、策略与三类修改 Node 全部零调用。
    assert_eq!(trace.count(|calls| &calls.stop), 0);
    assert_eq!(trace.count(|calls| &calls.choose), 0);
    assert_eq!(trace.count(|calls| &calls.reduce), 0);
    assert_eq!(trace.count(|calls| &calls.supplement), 0);
    assert_eq!(trace.count(|calls| &calls.rewrite), 0);
}

// ------------------------------------------------------------------ 形态 B：Stop 先于 Choose

#[test]
fn s10_scheme_b_stop_precedes_strategy_and_break_silences_revision() {
    let runtime = Runtime::new();
    let nodes = Nodes::new();
    let trace = Trace::new();

    let prose = block_on(scheme_b(&runtime, &nodes, ses_input(2, &trace), 3))
        .unwrap_or_else(|error| panic!("scheme B must break before the round limit: {error}"));

    // IterBreak 交付进入本轮时的 current（2），不是本轮刚产生、尚未成为 current 的 next。
    assert_eq!(prose.score, 2);

    let events = trace.events();
    assert_eq!(
        events,
        ev(&[
            "basis",
            "write",
            "diagnose:0",
            "stop:0",
            "choose:0",
            "reduce",
            "diagnose:1",
            "stop:1",
            "choose:1",
            "supplement",
            "diagnose:2",
            "stop:2",
        ]),
        "每轮固定为 diagnose → stop → choose → 修改动作"
    );

    // Stop 与 Choose 只共享 Diagnosis、没有直接数据依赖：顺序只能由定义顺序保证。
    // （若顺序被交换，事件序列断言与这里的下标比较都会失败。）
    for round in 0..2 {
        assert!(
            index_of(&events, &format!("stop:{round}"))
                < index_of(&events, &format!("choose:{round}")),
            "round {round}: stop must run before the strategy Choose"
        );
    }

    // 第 3 轮 stop:2 发出 IterBreak：策略与三类修改动作都不再执行。
    let after_break = index_of(&events, "stop:2") + 1;
    assert!(
        events[after_break..].is_empty(),
        "IterBreak 之后不得再有业务调用：{:?}",
        &events[after_break..]
    );
    assert_eq!(trace.count(|calls| &calls.stop), 3);
    assert_eq!(trace.count(|calls| &calls.diagnose), 3);
    assert_eq!(
        trace.count(|calls| &calls.choose),
        2,
        "Break 之后策略 Node 零调用"
    );
    assert_eq!(
        trace.count(|calls| &calls.reduce)
            + trace.count(|calls| &calls.supplement)
            + trace.count(|calls| &calls.rewrite),
        2,
        "Break 之后三类修改 Node 零调用"
    );
    assert_eq!(trace.count(|calls| &calls.reduce), 1);
    assert_eq!(trace.count(|calls| &calls.supplement), 1);
    assert_eq!(trace.count(|calls| &calls.rewrite), 0);
    assert_eq!(
        trace.count(|calls| &calls.revise),
        0,
        "A 的单次修改不参与 B"
    );
    assert_eq!(
        trace.count(|calls| &calls.basis),
        1,
        "basis 在 Iter 外只产生一次"
    );
    assert_eq!(trace.count(|calls| &calls.write), 1);
}

// ------------------------------------------------------------------ 形态 B：三类策略／无额外 Judge

#[test]
fn s10_scheme_b_runs_all_three_strategies_without_an_extra_final_judge() {
    let runtime = Runtime::new();
    let nodes = Nodes::new();
    let trace = Trace::new();

    let prose = block_on(scheme_b(&runtime, &nodes, ses_input(4, &trace), 5))
        .unwrap_or_else(|error| panic!("scheme B must break on an acceptable draft: {error}"));

    assert_eq!(prose.score, 4, "five rounds: 0 → 1 → 2 → 3 → 4 (break)");
    assert_eq!(
        trace.events(),
        ev(&[
            "basis",
            "write",
            "diagnose:0",
            "stop:0",
            "choose:0",
            "reduce",
            "diagnose:1",
            "stop:1",
            "choose:1",
            "supplement",
            "diagnose:2",
            "stop:2",
            "choose:2",
            "rewrite",
            "diagnose:3",
            "stop:3",
            "choose:3",
            "reduce",
            "diagnose:4",
            "stop:4",
        ])
    );

    // 三类策略都有样本：score % 3 依次命中 Reduce → Supplement → Rewrite → Reduce。
    assert_eq!(trace.count(|calls| &calls.reduce), 2);
    assert_eq!(trace.count(|calls| &calls.supplement), 1);
    assert_eq!(trace.count(|calls| &calls.rewrite), 1);
    assert_eq!(trace.count(|calls| &calls.choose), 4);
    assert_eq!(trace.count(|calls| &calls.stop), 5);
    assert_eq!(
        trace.count(|calls| &calls.diagnose),
        5,
        "Judge 只在每轮开头执行：Break 轮之后没有额外 Judge"
    );
    assert_eq!(trace.count(|calls| &calls.basis), 1);
}

#[test]
fn s10_scheme_b_round_limit_reports_without_running_an_extra_judge() {
    let runtime = Runtime::new();
    let nodes = Nodes::new();
    let trace = Trace::new();

    match block_on(scheme_b(&runtime, &nodes, ses_input(3, &trace), 3)) {
        Ok(prose) => panic!(
            "the round limit must fail instead of delivering score {}",
            prose.score
        ),
        Err(RunError::IterationLimitReached {
            max_iterations,
            completed_iterations,
        }) => assert_eq!((max_iterations, completed_iterations), (3, 3)),
        Err(other) => panic!("expected IterationLimitReached, got {other}"),
    }

    let events = trace.events();
    assert_eq!(
        events,
        ev(&[
            "basis",
            "write",
            "diagnose:0",
            "stop:0",
            "choose:0",
            "reduce",
            "diagnose:1",
            "stop:1",
            "choose:1",
            "supplement",
            "diagnose:2",
            "stop:2",
            "choose:2",
            "rewrite",
        ]),
        "第 3 轮正常完成仍 Continue；上限不强制成功，也不额外跑一轮"
    );
    assert_eq!(
        trace.count(|calls| &calls.diagnose),
        3,
        "上限判定后不得额外执行一次 Judge"
    );
    assert!(
        !events.iter().any(|event| event == "diagnose:3"),
        "第 3 轮产生的 next（score 3）没有成为 current，也没有被再次诊断"
    );
    assert_eq!(trace.count(|calls| &calls.stop), 3);
    assert_eq!(trace.count(|calls| &calls.choose), 3);
    assert_eq!(trace.count(|calls| &calls.reduce), 1);
    assert_eq!(trace.count(|calls| &calls.supplement), 1);
    assert_eq!(trace.count(|calls| &calls.rewrite), 1);
    assert_eq!(trace.count(|calls| &calls.basis), 1);
}

// ------------------------------------------------------------------ 两形态复用同一批 Node

#[test]
fn s10_both_schemes_reuse_one_business_node_batch_with_one_basis_per_execute() {
    let runtime = Runtime::new();
    let nodes = Nodes::new();
    let trace = Trace::new();

    let a = block_on(scheme_a(&runtime, &nodes, ses_input(2, &trace)))
        .unwrap_or_else(|error| panic!("scheme A must succeed: {error}"));
    assert_eq!(a.score, 1);
    assert_eq!(
        trace.count(|calls| &calls.basis),
        1,
        "A：每次 execute 只产生一次 basis"
    );
    let after_a = trace.events().len();

    // 同一批业务 Node 直接用于第二种 Definition：只改 Definition，不改 Node 类型／协议。
    let b = block_on(scheme_b(&runtime, &nodes, ses_input(2, &trace), 3))
        .unwrap_or_else(|error| panic!("scheme B must break before the round limit: {error}"));
    assert_eq!(b.score, 2);
    assert_eq!(
        trace.count(|calls| &calls.basis),
        2,
        "B：第二次 execute 仍只产生一次 basis（增量恒为 1）"
    );

    let events = trace.events();
    assert_eq!(
        &events[after_a..],
        ev(&[
            "basis",
            "write",
            "diagnose:0",
            "stop:0",
            "choose:0",
            "reduce",
            "diagnose:1",
            "stop:1",
            "choose:1",
            "supplement",
            "diagnose:2",
            "stop:2",
        ])
        .as_slice(),
        "B 的事件段与独立样本逐项一致"
    );
    assert_eq!(
        trace.count(|calls| &calls.revise),
        1,
        "A 的修改 Node 只服务 A"
    );
}
