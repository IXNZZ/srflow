//! WE-T06 §4 S08／S09（X11／X12／X18 与 X21 的外部可观察部分）：六操作完整组合与
//! 控制／失败传播矩阵的正式集成样本。
//!
//! 归属：本文件由 WE-T06 W04 的 S08／S09 分片独占，只用 crate 根导出的新公开 API
//! （`Runtime`／`Flow`／`Ref`／`Query`／`Node`／`Data`／`Choice`／`RunError`／…），
//! 不引用 `srflow::legacy`，也不使用任何内部 `#[cfg(test)]` 钩子。
//!
//! 判据全部针对真实执行的可观察事实：
//! - 业务 Node 的调用顺序与次数（[`assert_events`] 逐项相等，不是“六类 Step 已登记”）；
//! - 每项／每轮的调用计数与产物值；
//! - 错误分类、冻结字段与动态原因对象（`downcast_ref` 到具体类型）；
//! - 故障后“后续业务零调用”（[`count`]）。
//!
//! 未覆盖（见报告 uncovered）：失败退出叠加清理故障 → `RunError::Runtime` 的组合、
//! `NoMatchingCase` 叠加清理故障，以及任何需要内部钩子的清理诊断样本——这些由
//! WE-T06 的见证／Each finish 工作项覆盖，不在本文件范围。

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use futures::executor::block_on;
use srflow::{BodyError, ControlSignal, Data, Flow, Query, Ref, RunError, Runtime};

// ---------------------------------------------------------------- 观察设施

thread_local! {
    /// 业务 Node 调用事件日志（每次调用一条，含调用方给定的标签）。
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    /// 每个命名计数器（依调用序编号，用于区分 attempt／item／round）。
    static COUNTERS: RefCell<HashMap<String, u32>> = RefCell::new(HashMap::new());
}

fn reset() {
    EVENTS.with(|events| events.borrow_mut().clear());
    COUNTERS.with(|counters| counters.borrow_mut().clear());
}

fn log(tag: impl Into<String>) {
    EVENTS.with(|events| events.borrow_mut().push(tag.into()));
}

fn events() -> Vec<String> {
    EVENTS.with(|events| events.borrow().clone())
}

/// 命名计数器的下一个值（从 1 开始）。
fn bump(name: &str) -> u32 {
    COUNTERS.with(|counters| {
        let mut counters = counters.borrow_mut();
        let value = counters.entry(name.to_owned()).or_insert(0);
        *value += 1;
        *value
    })
}

/// 精确匹配该标签的事件条数。
fn count(tag: &str) -> usize {
    EVENTS.with(|events| events.borrow().iter().filter(|e| e.as_str() == tag).count())
}

/// 逐项断言业务 Node 的真实调用序列。
fn assert_events(expected: &[&str]) {
    let actual = events();
    let expected: Vec<String> = expected.iter().map(|tag| (*tag).to_owned()).collect();
    assert_eq!(actual, expected, "business node call sequence");
}

// ---------------------------------------------------------------- 业务类型

#[derive(Data)]
struct Seed(u32);
#[derive(Data)]
struct Key(u8);
#[derive(Data)]
struct Candidate(u32);
#[derive(Data)]
struct Verdict(u32);
#[derive(Data)]
struct Amount(u32);
#[derive(Data)]
struct Label(u32);
#[derive(Data)]
struct Merged(u32);
#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Score(u32);
#[derive(Data)]
struct Summary(u32);
#[derive(Data)]
struct Draft(String);
#[derive(Data)]
struct Context(u32);

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// 明确的 Retry 动态原因：携带值，不被字符串化重建。
#[derive(Debug)]
struct NotReady {
    value: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not ready ({})", self.value)
    }
}

impl Error for NotReady {}

/// 明确的 Failure 动态原因：由 `downcast_ref` 核对原对象身份。
#[derive(Debug)]
struct TooSmall {
    value: u32,
}

impl fmt::Display for TooSmall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "too small ({})", self.value)
    }
}

impl Error for TooSmall {}

// ================================================================ S08：一次真实执行中的六类操作

// ---- then／retry：Seed → Key，以及受预算约束的 attempt

async fn key_of(query: Query<&Seed>) -> Result<Key, BodyError> {
    log("key_of");
    Ok(Key(query.get().0 as u8))
}

/// 每次 attempt 产生一个新的候选值（同时记下 attempt 序号）。
async fn generate(query: Query<&Seed>) -> Result<Candidate, BodyError> {
    let attempt = bump("generate");
    log(format!("generate#{attempt}"));
    Ok(Candidate(query.get().0 * 10 + attempt))
}

/// 第一次判定要求 Retry，第二次正常完成。
async fn judge(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    let value = query.get().0;
    if value < 22 {
        log("judge:retry");
        Err(BodyError::retry(NotReady { value }))
    } else {
        log("judge:ok");
        Ok(Verdict(value * 10))
    }
}

// ---- choose 的三个分支（读 Retry 的产物）

async fn double(query: Query<&Verdict>) -> Result<Amount, BodyError> {
    log("double");
    Ok(Amount(query.get().0 * 2))
}

async fn triple(query: Query<&Verdict>) -> Result<Amount, BodyError> {
    log("triple");
    Ok(Amount(query.get().0 * 3))
}

async fn fallback(query: Query<&Verdict>) -> Result<Amount, BodyError> {
    log("fallback");
    Ok(Amount(query.get().0 + 100))
}

// ---- chain：child 边界内的标签与合并

async fn label_at_root(query: Query<&Seed>) -> Result<Label, BodyError> {
    log("label:root");
    Ok(Label(query.get().0 * 100))
}

async fn merge(query: Query<(&Amount, &Verdict, &Label)>) -> Result<Merged, BodyError> {
    let (amount, verdict, label) = query.get();
    log("merge");
    Ok(Merged(amount.0 + verdict.0 + label.0))
}

// ---- each：每项的多输出与更深一层 chain 捕获

async fn score(query: Query<&Item>) -> Result<Score, BodyError> {
    let item = query.get().0;
    log(format!("score#{item}"));
    Ok(Score(item * 10))
}

/// item 内 chain 的深层捕获节点：读 Root 祖先 `Seed`。
async fn label_in_item(query: Query<&Seed>) -> Result<Label, BodyError> {
    log("label:item");
    Ok(Label(query.get().0 * 100))
}

async fn summary(query: Query<(&Item, &Score, &Label)>) -> Result<Summary, BodyError> {
    let (item, score, label) = query.get();
    log(format!("summary#{}", item.0));
    Ok(Summary(score.0 + label.0 + item.0))
}

// ---- iter：一轮 Continue、一轮 Break

async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    log("revise");
    let Draft(text) = query.get();
    Ok(Draft(format!("{text}+")))
}

async fn bump_context(query: Query<&Context>) -> Result<Context, BodyError> {
    log("bump");
    Ok(Context(query.get().0 + 1))
}

async fn round_judge(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let round = query.get().0;
    if round >= 1 {
        log(format!("round_judge#{round}:break"));
        Err(BodyError::iter_break())
    } else {
        log(format!("round_judge#{round}:continue"));
        Ok(Verdict(round))
    }
}

/// S08（X11）：一次真实执行同时包含 then／chain／each／choose／retry／iter，
/// 深层 capture（item body 的 chain 读 Root `Seed`）、多输出（chain 两槽、each 两槽、
/// Root 八槽），一次分支选择、一次 Retry、一次 Continue 后 Break。
#[test]
fn s08_six_operations_compose_in_one_execution() {
    reset();
    let runtime = Runtime::new();
    let output =
        block_on(runtime.execute::<_, (
            Verdict,
            Amount,
            Merged,
            Label,
            Vec<Score>,
            Vec<Summary>,
            Draft,
            Context,
        ), _>(
            |flow: &mut Flow<'_>,
             (seed, items, draft, context): (
                Ref<Seed>,
                Ref<Vec<Item>>,
                Ref<Draft>,
                Ref<Context>,
            )| {
                // 1) then：selector 键。
                let key = flow.then_with_output(&key_of, seed);
                // 2) retry：完整 attempt 重启一次后成功。
                let verdict: Ref<Verdict> = flow.retry_output(3, |attempt| {
                    let candidate = attempt.then_with_output(&generate, seed);
                    let out_refs = attempt.then_with_output(&judge, candidate);
                    attempt.output_with(out_refs);
                });
                // 3) choose：Key(2) 命中 triple，其余分支一个都不执行。
                let amount = flow.choose_with_output(key, |choice| {
                    choice.case(Key(1), |branch| {
                        let out_refs = branch.then_with_output(&double, verdict);
                        branch.output_with(out_refs);
                    });
                    choice.case(Key(2), |branch| {
                        let out_refs = branch.then_with_output(&triple, verdict);
                        branch.output_with(out_refs);
                    });
                    choice.otherwise(|branch| {
                        let out_refs = branch.then_with_output(&fallback, verdict);
                        branch.output_with(out_refs);
                    });
                });
                // 4) chain：child 边界 + 两槽输出。
                let (merged, label): (Ref<Merged>, Ref<Label>) = flow.chain_output(|sub| {
                    let label = sub.then_with_output(&label_at_root, seed);
                    let merged = sub.then_with_output(&merge, (amount, verdict, label));
                    let out_refs = (merged, label);
                    sub.output_with(out_refs);
                });
                // 5) each：每项两个输出槽，item body 内再一层 chain（深层捕获 Root `Seed`）。
                let (scores, summaries): (Ref<Vec<Score>>, Ref<Vec<Summary>>) =
                    flow.each_output(items, |each| {
                        let item = each.ref_of::<Item>();

                        let score = each.then_with_output(&score, item);
                        let summary: Ref<Summary> = each.chain_output(|sub| {
                            let label = sub.then_with_output(&label_in_item, seed);
                            let out_refs = sub.then_with_output(&summary, (item, score, label));
                            sub.output_with(out_refs);
                        });
                        let out_refs = (score, summary);
                        each.output_with(out_refs);
                    });
                // 6) iter：第一轮 Continue，第二轮 Break（本轮 next 已产生但不交付）。
                let (draft, context) = flow.iter(
                    (draft, context),
                    3,
                    |(current_draft, current_context), round| {
                        let next_draft = round.then_with_output(&revise, current_draft);
                        let next_context = round.then_with_output(&bump_context, current_context);
                        round.then_with_output(&round_judge, current_context);
                        (next_draft, next_context)
                    },
                );
                let out_refs = (
                    verdict, amount, merged, label, scores, summaries, draft, context,
                );
                flow.output_with(out_refs);
            },
            (
                Seed(2),
                vec![Item(1), Item(2), Item(3)],
                Draft(String::from("s0")),
                Context(0),
            ),
        ))
        .expect("the six-operation definition runs end to end");

    let (verdict, amount, merged, label, scores, summaries, draft, context) = output;
    // 产物值：attempt 1 → 21（Retry），attempt 2 → 22 → Verdict(220)。
    assert_eq!(verdict.0, 220, "the successful attempt verdict");
    assert_eq!(amount.0, 660, "Key(2) selected triple: 220 * 3");
    assert_eq!(label.0, 200, "chain child label: 2 * 100");
    assert_eq!(merged.0, 1080, "merge: 660 + 220 + 200");
    assert_eq!(
        scores.iter().map(|score| score.0).collect::<Vec<_>>(),
        vec![10, 20, 30],
        "each slot 1 aggregates in input order"
    );
    assert_eq!(
        summaries.iter().map(|s| s.0).collect::<Vec<_>>(),
        vec![211, 222, 233],
        "each slot 2: score + label + item"
    );
    assert_eq!(draft.0.as_str(), "s0+", "round-2 current, not its next");
    assert_eq!(context.0, 1, "round-2 current context");

    // 事件序列逐项：attempt 重启、只运行选中的分支、item 顺序、round 顺序。
    assert_events(&[
        "key_of",
        "generate#1",
        "judge:retry",
        "generate#2",
        "judge:ok",
        "triple",
        "label:root",
        "merge",
        "score#1",
        "label:item",
        "summary#1",
        "score#2",
        "label:item",
        "summary#2",
        "score#3",
        "label:item",
        "summary#3",
        "revise",
        "bump",
        "round_judge#0:continue",
        "revise",
        "bump",
        "round_judge#1:break",
    ]);

    // scope／owner 事实：未选分支零调用；每个 item 的 body 恰好执行一次；
    // 每轮 body 执行次数与 round 判定次数一致。
    assert_eq!(
        count("double"),
        0,
        "the unselected case branch must not run"
    );
    assert_eq!(count("fallback"), 0, "the otherwise branch must not run");
    assert_eq!(count("triple"), 1, "the selected branch runs exactly once");
    assert_eq!(count("generate#3"), 0, "no third attempt");
    for item in [1, 2, 3] {
        assert_eq!(
            count(&format!("score#{item}")),
            1,
            "item body runs once per item"
        );
        assert_eq!(
            count(&format!("summary#{item}")),
            1,
            "item body chain runs once per item"
        );
    }
    assert_eq!(count("score#4"), 0, "no fourth item");
    assert_eq!(count("label:item"), 3, "one deep capture per item");
    assert_eq!(count("revise"), 2, "one revise per round");
    assert_eq!(count("bump"), 2, "one bump per round");
    assert_eq!(count("round_judge#0:continue"), 1, "first round continues");
    assert_eq!(count("round_judge#1:break"), 1, "second round breaks");
    assert_eq!(count("round_judge#2:break"), 0, "no third round");
}

// ================================================================ S09 主路径

/// 第二轮到达 `Context(1)` 时 Break。
async fn break_at_one(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let value = query.get().0;
    if value >= 1 {
        log("break:break");
        Err(BodyError::iter_break())
    } else {
        log("break:continue");
        Ok(Verdict(value))
    }
}

/// S09 主路径 A（X12）：IterBreak 由最近的 Iter 捕获，交付**进入本轮时的 current**，
/// 即使本轮已产生了 next（`a++`／2）也不以它代替。
#[test]
fn s09_path_a_iter_break_delivers_current_not_next() {
    reset();
    let runtime = Runtime::new();
    let (draft, context) = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow, (draft, context)| {
            let out_refs = flow.iter(
                (draft, context),
                4,
                |(current_draft, current_context), round| {
                    let next_draft = round.then_with_output(&revise, current_draft);
                    let next_context = round.then_with_output(&bump_context, current_context);
                    round.then_with_output(&break_at_one, current_context);
                    (next_draft, next_context)
                },
            );
            flow.output_with(out_refs);
        },
        (Draft(String::from("a")), Context(0)),
    ))
    .expect("the iter breaks on the second round");

    assert_eq!(
        (draft.0.as_str(), context.0),
        ("a+", 1),
        "Break delivers the round-2 current, not its already produced next"
    );
    assert_events(&[
        "revise",
        "bump",
        "break:continue",
        "revise",
        "bump",
        "break:break",
    ]);
    assert_eq!(count("revise"), 2);
    assert_eq!(count("bump"), 2);
    assert_eq!(count("break:continue"), 1);
    assert_eq!(count("break:break"), 1);
}

// ---- 路径 B：Round 内的内层 Retry 重启完整 attempt

async fn gen_in_round(query: Query<&Context>) -> Result<Candidate, BodyError> {
    let call = bump("gen_in_round");
    log(format!("gen_in_round#{call}"));
    Ok(Candidate(query.get().0 * 10 + call))
}

async fn judge_in_round(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    let call = bump("judge_in_round");
    if call == 1 {
        log(format!("judge_in_round#{call}:retry"));
        Err(BodyError::retry(NotReady {
            value: query.get().0,
        }))
    } else {
        log(format!("judge_in_round#{call}:ok"));
        Ok(Verdict(query.get().0 * 10))
    }
}

async fn consume(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("consume");
    Ok(Summary(query.get().0))
}

/// S09 主路径 B（X12）：Iter 的 Round 内层 Retry 捕获 Retry 信号并重启**完整 attempt**
/// （两层 `gen_in_round` 后才 `consume`），Round 与 Iter 都不被终止。
#[test]
fn s09_path_b_inner_retry_restarts_complete_attempt_in_round() {
    reset();
    let runtime = Runtime::new();
    let (draft, context) = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow, (draft, context)| {
            let out_refs = flow.iter(
                (draft, context),
                3,
                |(current_draft, current_context), round| {
                    let verdict: Ref<Verdict> = round.retry_output(1, |attempt| {
                        let candidate = attempt.then_with_output(&gen_in_round, current_context);
                        let out_refs = attempt.then_with_output(&judge_in_round, candidate);
                        attempt.output_with(out_refs);
                    });
                    round.then_with_output(&consume, verdict);
                    let next_draft = round.then_with_output(&revise, current_draft);
                    let next_context = round.then_with_output(&bump_context, current_context);
                    round.then_with_output(&break_at_one, current_context);
                    (next_draft, next_context)
                },
            );
            flow.output_with(out_refs);
        },
        (Draft(String::from("b")), Context(0)),
    ))
    .expect("the iter only breaks on its second round");

    assert_eq!((draft.0.as_str(), context.0), ("b+", 1));
    assert_events(&[
        "gen_in_round#1",
        "judge_in_round#1:retry",
        "gen_in_round#2",
        "judge_in_round#2:ok",
        "consume",
        "revise",
        "bump",
        "break:continue",
        "gen_in_round#3",
        "judge_in_round#3:ok",
        "consume",
        "revise",
        "bump",
        "break:break",
    ]);
    assert_eq!(
        count("judge_in_round#1:retry"),
        1,
        "exactly one retry request"
    );
    assert_eq!(count("consume"), 2, "one consumed verdict per round");
    assert_eq!(count("gen_in_round#3"), 1);
    assert_eq!(count("gen_in_round#4"), 0);
    assert_eq!(count("revise"), 2);
}

// ---- 路径 C：普通 Failure

async fn first_ok(query: Query<&Seed>) -> Result<Verdict, BodyError> {
    log("mark_first");
    Ok(Verdict(query.get().0 + 1))
}

async fn fail_here(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("fail_here");
    Err(BodyError::fail(TooSmall {
        value: query.get().0,
    }))
}

async fn after_failure(query: Query<&Summary>) -> Result<Amount, BodyError> {
    log("after_failure");
    Ok(Amount(query.get().0))
}

/// S09 主路径 C（X12／X18）：普通 Failure 立即终止路径，保留原动态对象；后续业务零调用。
#[test]
fn s09_path_c_failure_terminates_and_stops_followups() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, Amount, _>(
        |flow, seed| {
            let first = flow.then_with_output(&first_ok, seed);
            let failed = flow.then_with_output(&fail_here, first);
            let out_refs = flow.then_with_output(&after_failure, failed);
            flow.output_with(out_refs);
        },
        Seed(1),
    )) {
        Ok(_) => panic!("a business failure must not produce a normal output"),
        Err(error) => error,
    };

    let RunError::Body(failure) = error else {
        panic!("expected RunError::Body, got {error}");
    };
    let reason = failure
        .source()
        .expect("the dynamic failure reason stays reachable");
    let preserved = reason
        .downcast_ref::<TooSmall>()
        .expect("the original dynamic error object is preserved");
    assert_eq!(preserved.value, 2, "first_ok(Seed(1)) produced Verdict(2)");
    assert!(
        failure.cleanup_source().is_none(),
        "no cleanup fault is installed in this sample"
    );

    assert_events(&["mark_first", "fail_here"]);
    assert_eq!(count("after_failure"), 0, "no business after the failure");
}

// ---- 路径 D：Retry 预算耗尽

async fn always_generate(query: Query<&Seed>) -> Result<Candidate, BodyError> {
    let call = bump("always_generate");
    log(format!("always_generate#{call}"));
    Ok(Candidate(query.get().0 * 10 + call))
}

async fn always_retry(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    let call = bump("always_retry");
    log(format!("always_retry#{call}"));
    Err(BodyError::retry(NotReady {
        value: query.get().0,
    }))
}

async fn after_retry(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("after_retry");
    Ok(Summary(query.get().0))
}

/// S09 主路径 D（X18）：Retry 预算耗尽 → `RetryExhausted` 保留配置、实际 attempts 与
/// 最后一次动态原因；无第三次 attempt，也不交付最后候选，后续业务零调用。
#[test]
fn s09_path_d_retry_exhaustion_keeps_fields_and_last_reason() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let verdict: Ref<Verdict> = flow.retry_output(1, |attempt| {
                let candidate = attempt.then_with_output(&always_generate, seed);
                let out_refs = attempt.then_with_output(&always_retry, candidate);
                attempt.output_with(out_refs);
            });
            let out_refs = flow.then_with_output(&after_retry, verdict);
            flow.output_with(out_refs);
        },
        Seed(5),
    )) {
        Ok(_) => panic!("budget exhaustion must not deliver a normal output"),
        Err(error) => error,
    };

    let RunError::RetryExhausted {
        max_retries,
        attempts,
        last_error,
    } = error
    else {
        panic!("expected RunError::RetryExhausted, got {error}");
    };
    assert_eq!(max_retries, 1, "configured retry budget");
    assert_eq!(attempts, 2, "N + 1 attempts were actually executed");
    let reason = last_error
        .source()
        .expect("the last dynamic reason stays reachable");
    let preserved = reason
        .downcast_ref::<NotReady>()
        .expect("the last dynamic reason object is preserved");
    assert_eq!(
        preserved.value, 52,
        "the second attempt produced Candidate(52)"
    );

    assert_events(&[
        "always_generate#1",
        "always_retry#1",
        "always_generate#2",
        "always_retry#2",
    ]);
    assert_eq!(count("always_generate#3"), 0, "no third attempt");
    assert_eq!(
        count("after_retry"),
        0,
        "no business after the budget terminal"
    );
}

// ================================================================ S09 补充传播表

// ---- Chain 穿透

async fn chain_probe(query: Query<&Seed>) -> Result<Verdict, BodyError> {
    log("chain_probe");
    Ok(Verdict(query.get().0))
}

async fn chain_break(_query: Query<&Verdict>) -> Result<Verdict, BodyError> {
    log("chain_break");
    Err(BodyError::iter_break())
}

async fn chain_fail(query: Query<&Verdict>) -> Result<Verdict, BodyError> {
    log("chain_fail");
    Err(BodyError::fail(TooSmall {
        value: query.get().0,
    }))
}

async fn after_chain(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("after_chain");
    Ok(Summary(query.get().0))
}

/// S09 补充表（X12）：Chain 不吞 Control／Failure；child 内的信号跨 child 边界
/// 原样到达 Root，chain 之后的业务零调用。
#[test]
fn s09_propagation_chain_control_and_failure_reach_root_unchanged() {
    reset();
    let runtime = Runtime::new();

    // (a) child 内的 IterBreak：无匹配边界的 Control → UnhandledControl。
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let _before = flow.then_with_output(&chain_probe, seed);
            let inside: Ref<Verdict> = flow.chain_output(|sub| {
                let probe = sub.then_with_output(&chain_probe, seed);
                let out_refs = sub.then_with_output(&chain_break, probe);
                sub.output_with(out_refs);
            });
            let out_refs = flow.then_with_output(&after_chain, inside);
            flow.output_with(out_refs);
        },
        Seed(1),
    )) {
        Ok(_) => panic!("an unmatched control signal must escape the Root"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            RunError::UnhandledControl(ControlSignal::IterBreak(_))
        ),
        "expected UnhandledControl(IterBreak), got {error}"
    );
    assert_events(&["chain_probe", "chain_probe", "chain_break"]);
    assert_eq!(
        count("after_chain"),
        0,
        "no business after the escaping control"
    );

    // (b) child 内的 Failure：原对象跨 child 边界保留。
    reset();
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let inside: Ref<Verdict> = flow.chain_output(|sub| {
                let probe = sub.then_with_output(&chain_probe, seed);
                let out_refs = sub.then_with_output(&chain_fail, probe);
                sub.output_with(out_refs);
            });
            let out_refs = flow.then_with_output(&after_chain, inside);
            flow.output_with(out_refs);
        },
        Seed(9),
    )) {
        Ok(_) => panic!("a business failure must escape the Root"),
        Err(error) => error,
    };
    let RunError::Body(failure) = error else {
        panic!("expected RunError::Body, got {error}");
    };
    let preserved = failure
        .source()
        .expect("the dynamic failure reason stays reachable")
        .downcast_ref::<TooSmall>()
        .expect("the original dynamic error object is preserved")
        .value;
    assert_eq!(
        preserved, 9,
        "the dynamic payload crossed the chain boundary"
    );
    assert_events(&["chain_probe", "chain_fail"]);
    assert_eq!(count("after_chain"), 0);
}

// ---- Choose 穿透

async fn selector_from_seed(query: Query<&Seed>) -> Result<Key, BodyError> {
    let value = query.get().0;
    log(format!("selector#{value}"));
    Ok(Key(value as u8))
}

async fn branch_control(_query: Query<&Seed>) -> Result<Amount, BodyError> {
    log("branch:control");
    Err(BodyError::iter_break())
}

async fn branch_fail(query: Query<&Seed>) -> Result<Amount, BodyError> {
    log("branch:fail");
    Err(BodyError::fail(TooSmall {
        value: query.get().0,
    }))
}

async fn branch_other(query: Query<&Seed>) -> Result<Amount, BodyError> {
    log("branch:other");
    Ok(Amount(query.get().0))
}

async fn branch_fallback(_query: Query<&Seed>) -> Result<Amount, BodyError> {
    log("branch:fallback");
    Ok(Amount(0))
}

async fn after_choose(query: Query<&Amount>) -> Result<Summary, BodyError> {
    log("after_choose");
    Ok(Summary(query.get().0))
}

/// S09 补充表（X12）：Choose 不吞信号也不因失败改走 otherwise；未选分支一个 Node 都不执行。
#[test]
fn s09_propagation_choose_control_and_failure_do_not_run_other_branches() {
    reset();
    let runtime = Runtime::new();

    // (a) 选中分支发出 IterBreak：跨 Choose 边界到 Root → UnhandledControl。
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let selector = flow.then_with_output(&selector_from_seed, seed);
            let picked: Ref<Amount> = flow.choose_with_output(selector, |choice| {
                choice.case(Key(1), |branch| {
                    let out_refs = branch.then_with_output(&branch_control, seed);
                    branch.output_with(out_refs);
                });
                choice.case(Key(2), |branch| {
                    let out_refs = branch.then_with_output(&branch_other, seed);
                    branch.output_with(out_refs);
                });
                choice.otherwise(|branch| {
                    let out_refs = branch.then_with_output(&branch_fallback, seed);
                    branch.output_with(out_refs);
                });
            });
            let out_refs = flow.then_with_output(&after_choose, picked);
            flow.output_with(out_refs);
        },
        Seed(1),
    )) {
        Ok(_) => panic!("an unmatched control signal must escape the Root"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            RunError::UnhandledControl(ControlSignal::IterBreak(_))
        ),
        "expected UnhandledControl(IterBreak), got {error}"
    );
    assert_events(&["selector#1", "branch:control"]);
    assert_eq!(count("branch:other"), 0, "unselected case branch");
    assert_eq!(
        count("branch:fallback"),
        0,
        "otherwise is not a fallback for controls"
    );
    assert_eq!(count("after_choose"), 0);

    // (b) 选中分支 Failure：不回退 otherwise；原对象保留。
    reset();
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let selector = flow.then_with_output(&selector_from_seed, seed);
            let picked: Ref<Amount> = flow.choose_with_output(selector, |choice| {
                choice.case(Key(1), |branch| {
                    let out_refs = branch.then_with_output(&branch_fail, seed);
                    branch.output_with(out_refs);
                });
                choice.case(Key(2), |branch| {
                    let out_refs = branch.then_with_output(&branch_other, seed);
                    branch.output_with(out_refs);
                });
                choice.otherwise(|branch| {
                    let out_refs = branch.then_with_output(&branch_fallback, seed);
                    branch.output_with(out_refs);
                });
            });
            let out_refs = flow.then_with_output(&after_choose, picked);
            flow.output_with(out_refs);
        },
        Seed(1),
    )) {
        Ok(_) => panic!("a business failure must not fall back to otherwise"),
        Err(error) => error,
    };
    let RunError::Body(failure) = error else {
        panic!("expected RunError::Body, got {error}");
    };
    let preserved = failure
        .source()
        .expect("the dynamic failure reason stays reachable")
        .downcast_ref::<TooSmall>()
        .expect("the original dynamic error object is preserved")
        .value;
    assert_eq!(preserved, 1);
    assert_events(&["selector#1", "branch:fail"]);
    assert_eq!(count("branch:other"), 0);
    assert_eq!(
        count("branch:fallback"),
        0,
        "failure does not select otherwise"
    );
    assert_eq!(count("after_choose"), 0);
}

// ---- Each 穿透

async fn item_checked(query: Query<&Item>) -> Result<Score, BodyError> {
    let value = query.get().0;
    log(format!("item#{value}"));
    if value < 2 {
        Err(BodyError::fail(TooSmall { value }))
    } else {
        Ok(Score(value * 10))
    }
}

async fn item_control(query: Query<&Item>) -> Result<Score, BodyError> {
    let value = query.get().0;
    log(format!("item#{value}"));
    if value >= 2 {
        Err(BodyError::iter_break())
    } else {
        Ok(Score(value * 10))
    }
}

/// S09 补充表（X12）／X21：Each 不吞信号；Failure 与向外传播的 Control 都停止**剩余 item**，
/// 并且不产生部分正常集合（Root 返回错误而不是聚合 Vec）。
#[test]
fn s09_propagation_each_stops_remaining_items() {
    reset();
    let runtime = Runtime::new();

    // (a) 第二项 Failure：第三项不再执行。
    let error = match block_on(runtime.execute::<_, Vec<Score>, _>(
        |flow, items| {
            let out_refs: Ref<Vec<Score>> = flow.each_output(items, |each| {
                let item = each.ref_of::<Item>();

                let out_refs = each.then_with_output(&item_checked, item);
                each.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(2), Item(1), Item(3)],
    )) {
        Ok(_) => panic!("an item failure must escape Each"),
        Err(error) => error,
    };
    let RunError::Body(failure) = error else {
        panic!("expected RunError::Body, got {error}");
    };
    let preserved = failure
        .source()
        .expect("the dynamic failure reason stays reachable")
        .downcast_ref::<TooSmall>()
        .expect("the original dynamic error object is preserved")
        .value;
    assert_eq!(preserved, 1, "the failing item is the second one");
    assert_events(&["item#2", "item#1"]);
    assert_eq!(count("item#3"), 0, "the third item does not run");

    // (b) 第二项向外传播的 IterBreak：第三项不再执行，且无部分聚合。
    reset();
    let error = match block_on(runtime.execute::<_, Vec<Score>, _>(
        |flow, items| {
            let out_refs: Ref<Vec<Score>> = flow.each_output(items, |each| {
                let item = each.ref_of::<Item>();

                let out_refs = each.then_with_output(&item_control, item);
                each.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2), Item(3)],
    )) {
        Ok(_) => panic!("an unmatched control signal must escape Each"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            RunError::UnhandledControl(ControlSignal::IterBreak(_))
        ),
        "expected UnhandledControl(IterBreak), got {error}"
    );
    assert_events(&["item#1", "item#2"]);
    assert_eq!(count("item#3"), 0, "the third item does not run");
}

// ---- 同类最近捕获：Retry → Retry

async fn outer_probe(query: Query<&Seed>) -> Result<Verdict, BodyError> {
    log("outer_probe");
    Ok(Verdict(query.get().0 + 1))
}

async fn nested_generate(query: Query<&Verdict>) -> Result<Candidate, BodyError> {
    let call = bump("nested_generate");
    log(format!("nested_generate#{call}"));
    Ok(Candidate(query.get().0 * 10 + call))
}

async fn nested_judge(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    let call = bump("nested_judge");
    if call == 1 {
        log(format!("nested_judge#{call}:retry"));
        Err(BodyError::retry(NotReady {
            value: query.get().0,
        }))
    } else {
        log(format!("nested_judge#{call}:ok"));
        Ok(Verdict(query.get().0 * 10))
    }
}

async fn after_outer(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("after_outer");
    Ok(Summary(query.get().0))
}

/// S09 补充表（X12）：Retry 内含 Retry —— 内层先捕获，外层 attempt 不重启（外层业务
/// 只执行一次），内层完整 attempt 重启后正常交付。
#[test]
fn s09_same_class_nearest_capture_retry_in_retry() {
    reset();
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let outer: Ref<Verdict> = flow.retry_output(3, |attempt| {
                let probe = attempt.then_with_output(&outer_probe, seed);
                let t04_out_refs: Ref<Verdict> = attempt.retry_output(1, |inner| {
                    let candidate = inner.then_with_output(&nested_generate, probe);
                    let out_refs = inner.then_with_output(&nested_judge, candidate);
                    inner.output_with(out_refs);
                });
                attempt.output_with(t04_out_refs);
            });
            let out_refs = flow.then_with_output(&after_outer, outer);
            flow.output_with(out_refs);
        },
        Seed(1),
    ))
    .expect("the inner retry recovers within the first outer attempt");

    assert_eq!(delivered.0, 220, "nested_judge#2 → Verdict(22 * 10)");
    assert_events(&[
        "outer_probe",
        "nested_generate#1",
        "nested_judge#1:retry",
        "nested_generate#2",
        "nested_judge#2:ok",
        "after_outer",
    ]);
    assert_eq!(
        count("outer_probe"),
        1,
        "the outer attempt was not restarted"
    );
    assert_eq!(count("nested_generate#3"), 0);
}

// ---- 同类最近捕获：Iter → Iter

async fn inner_advance(query: Query<&Context>) -> Result<Context, BodyError> {
    let value = query.get().0;
    log(format!("inner_advance#{value}"));
    Ok(Context(value + 1))
}

async fn inner_round_guard(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let value = query.get().0;
    if value >= 1 {
        log("inner_round:break");
        Err(BodyError::iter_break())
    } else {
        log("inner_round:continue");
        Ok(Verdict(value))
    }
}

async fn outer_round_probe(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let value = query.get().0;
    log(format!("outer_probe#{value}"));
    Ok(Verdict(value))
}

async fn outer_round_guard(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let value = query.get().0;
    if value >= 1 {
        log("outer_round:break");
        Err(BodyError::iter_break())
    } else {
        log("outer_round:continue");
        Ok(Verdict(value))
    }
}

/// S09 补充表（X12）：Iter 内含 Iter —— 内层 IterBreak 由内层捕获并交付**内层** current
/// （不是本轮已产生的 next），外层该轮正常完成并继续；外层自己的 Break 交付外层 current。
#[test]
fn s09_same_class_nearest_capture_iter_in_iter() {
    reset();
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute::<_, Context, _>(
        |flow, context| {
            let out_refs = flow.iter(context, 3, |current, round| {
                let inner = round.iter(current, 3, |inner_current, inner_round| {
                    let next = inner_round.then_with_output(&inner_advance, inner_current);
                    inner_round.then_with_output(&inner_round_guard, inner_current);
                    next
                });
                round.then_with_output(&outer_round_probe, inner);
                let next = round.then_with_output(&bump_context, current);
                round.then_with_output(&outer_round_guard, current);
                next
            });
            flow.output_with(out_refs);
        },
        Context(0),
    ))
    .expect("the outer iter breaks on its second round");

    assert_eq!(
        delivered.0, 1,
        "the outer iter delivers its round-2 current (Context(1))"
    );
    assert_events(&[
        "inner_advance#0",
        "inner_round:continue",
        "inner_advance#1",
        "inner_round:break",
        "outer_probe#1",
        "bump",
        "outer_round:continue",
        "inner_advance#1",
        "inner_round:break",
        "outer_probe#1",
        "bump",
        "outer_round:break",
    ]);
    // 内层交付的是 current（1）而不是内层本轮已产生的 next（2）。
    assert_eq!(
        count("outer_probe#1"),
        2,
        "outer saw the inner current twice"
    );
    assert_eq!(
        count("outer_probe#2"),
        0,
        "the inner next (2) was never delivered"
    );
    // 内层 Break 没有终止外层：外层跑满两轮才由自己的 Break 收尾。
    assert_eq!(count("inner_round:break"), 2);
    assert_eq!(count("inner_round:continue"), 1);
    assert_eq!(count("outer_round:continue"), 1);
    assert_eq!(count("outer_round:break"), 1);
    assert_eq!(count("inner_advance#0"), 1);
    assert_eq!(count("inner_advance#1"), 2);
}

// ---- Iter 中 Retry：重启完整 attempt，内层 Iter 从该 attempt 原始 state 开始

async fn gate(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let call = bump("gate");
    log(format!("gate#{call}"));
    if call == 1 {
        Err(BodyError::retry(NotReady {
            value: query.get().0,
        }))
    } else {
        Ok(Verdict(query.get().0))
    }
}

async fn round_break_at_one(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let value = query.get().0;
    if value >= 1 {
        log("round:break");
        Err(BodyError::iter_break())
    } else {
        log("round:continue");
        Ok(Verdict(value))
    }
}

/// S09 补充表（X12）：Iter 内的 Retry 由外层 Retry 捕获 → **完整 attempt 重启**，其下 Iter
/// 从该 attempt 的**原始 state** 重新开始（若沿用失败状态会得到 `q++`／2）。
#[test]
fn s09_retry_inside_iter_restarts_attempt_and_iter_state() {
    reset();
    let runtime = Runtime::new();
    let (draft, context) = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow, (draft, context)| {
            let out_refs: (Ref<Draft>, Ref<Context>) = flow.retry_output(1, |attempt| {
                let out_refs = attempt.iter(
                    (draft, context),
                    3,
                    |(current_draft, current_context), round| {
                        round.then_with_output(&gate, current_context);
                        let next_draft = round.then_with_output(&revise, current_draft);
                        let next_context = round.then_with_output(&bump_context, current_context);
                        round.then_with_output(&round_break_at_one, current_context);
                        (next_draft, next_context)
                    },
                );
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        (Draft(String::from("q")), Context(0)),
    ))
    .expect("the second outer attempt succeeds");

    assert_eq!(
        (draft.0.as_str(), context.0),
        ("q+", 1),
        "the inner iter restarted from the attempt's original state"
    );
    assert_events(&[
        "gate#1",
        "gate#2",
        "revise",
        "bump",
        "round:continue",
        "gate#3",
        "revise",
        "bump",
        "round:break",
    ]);
    assert_eq!(count("gate#4"), 0, "no third outer attempt");
    assert_eq!(
        count("revise"),
        2,
        "one round completed per attempt-2 round"
    );
    assert_eq!(count("bump"), 2);
}

// ---- Retry 中 IterBreak：不被 Retry 捕获，由外层 Iter 交付 current

async fn gen_in_retry(query: Query<&Context>) -> Result<Candidate, BodyError> {
    let call = bump("gen_in_retry");
    log(format!("gen_in_retry#{call}"));
    Ok(Candidate(query.get().0 * 10 + call))
}

async fn break_in_retry(_query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    log("break_in_retry");
    Err(BodyError::iter_break())
}

async fn after_retry_in_iter(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("after_retry_in_iter");
    Ok(Summary(query.get().0))
}

/// S09 补充表（X12）：Retry 内的 IterBreak 不被 Retry 捕获（attempt 只执行一次），
/// 由外层 Iter 捕获并交付该轮 current；该轮后续步骤与后续 round 都不执行。
#[test]
fn s09_retry_does_not_capture_iter_break_and_iter_delivers_current() {
    reset();
    let runtime = Runtime::new();
    let delivered = block_on(runtime.execute::<_, Context, _>(
        |flow, context| {
            let out_refs = flow.iter(context, 3, |current, round| {
                let verdict: Ref<Verdict> = round.retry_output(2, |attempt| {
                    let candidate = attempt.then_with_output(&gen_in_retry, current);
                    let out_refs = attempt.then_with_output(&break_in_retry, candidate);
                    attempt.output_with(out_refs);
                });
                round.then_with_output(&after_retry_in_iter, verdict);
                round.then_with_output(&bump_context, current)
            });
            flow.output_with(out_refs);
        },
        Context(0),
    ))
    .expect("the enclosing iter catches the iter break");

    assert_eq!(
        delivered.0, 0,
        "the iter delivers the round-1 current (Context(0))"
    );
    assert_events(&["gen_in_retry#1", "break_in_retry"]);
    assert_eq!(
        count("gen_in_retry#2"),
        0,
        "IterBreak is not a retry request: no second attempt"
    );
    assert_eq!(
        count("after_retry_in_iter"),
        0,
        "round-1 steps after the retry"
    );
    assert_eq!(count("bump"), 0, "the round's next was never used");
}

// ================================================================ S09 terminal 分类

async fn limit_judge(query: Query<&Context>) -> Result<Verdict, BodyError> {
    let value = query.get().0;
    log(format!("limit_judge#{value}"));
    Ok(Verdict(value))
}

async fn after_iter_limit(query: Query<&Context>) -> Result<Summary, BodyError> {
    log("after_iter_limit");
    Ok(Summary(query.get().0))
}

/// S09 terminal（X18）：轮次上限 → `IterationLimitReached`；第 N 轮不再额外判定一次 Judge、
/// 不强制成功、不把最后状态当结果；后续业务零调用。
#[test]
fn s09_terminal_iteration_limit_reached() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, context| {
            let state = flow.iter(context, 2, |current, round| {
                round.then_with_output(&limit_judge, current);
                round.then_with_output(&bump_context, current)
            });
            let out_refs = flow.then_with_output(&after_iter_limit, state);
            flow.output_with(out_refs);
        },
        Context(0),
    )) {
        Ok(_) => panic!("the round limit must terminate with an error"),
        Err(error) => error,
    };

    let RunError::IterationLimitReached {
        max_iterations,
        completed_iterations,
    } = error
    else {
        panic!("expected RunError::IterationLimitReached, got {error}");
    };
    assert_eq!(max_iterations, 2);
    assert_eq!(completed_iterations, 2);
    assert_events(&["limit_judge#0", "bump", "limit_judge#1", "bump"]);
    assert_eq!(
        count("limit_judge#2"),
        0,
        "no extra judgement after the limit"
    );
    assert_eq!(count("after_iter_limit"), 0);
}

async fn ask_retry(query: Query<&Seed>) -> Result<Verdict, BodyError> {
    log("ask_retry");
    Err(BodyError::retry(NotReady {
        value: query.get().0,
    }))
}

async fn after_unhandled(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("after_unhandled");
    Ok(Summary(query.get().0))
}

/// S09 terminal（X18）：Root 没有匹配边界时，Retry 信号形成 `UnhandledControl(Retry)`，
/// 原动态原因保留；后续业务零调用。
#[test]
fn s09_terminal_unhandled_retry_without_boundary() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let verdict = flow.then_with_output(&ask_retry, seed);
            let out_refs = flow.then_with_output(&after_unhandled, verdict);
            flow.output_with(out_refs);
        },
        Seed(7),
    )) {
        Ok(_) => panic!("an unmatched retry signal must escape the Root"),
        Err(error) => error,
    };

    let RunError::UnhandledControl(ControlSignal::Retry(reason)) = error else {
        panic!("expected UnhandledControl(Retry), got {error}");
    };
    let preserved = reason
        .source()
        .expect("the dynamic retry reason stays reachable")
        .downcast_ref::<NotReady>()
        .expect("the retry reason object is preserved");
    assert_eq!(preserved.value, 7);

    assert_events(&["ask_retry"]);
    assert_eq!(count("after_unhandled"), 0);
}

/// S09 terminal（X18）：Choose 无匹配且无 otherwise → `NoMatchingCase`；任何分支与后续业务
/// 零调用。
#[test]
fn s09_terminal_no_matching_case() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, Summary, _>(
        |flow, seed| {
            let selector = flow.then_with_output(&selector_from_seed, seed);
            let picked: Ref<Amount> = flow.choose_with_output(selector, |choice| {
                choice.case(Key(1), |branch| {
                    let out_refs = branch.then_with_output(&branch_other, seed);
                    branch.output_with(out_refs);
                });
                choice.case(Key(2), |branch| {
                    let out_refs = branch.then_with_output(&branch_fallback, seed);
                    branch.output_with(out_refs);
                });
            });
            let out_refs = flow.then_with_output(&after_choose, picked);
            flow.output_with(out_refs);
        },
        Seed(3),
    )) {
        Ok(_) => panic!("a missing case must terminate with an error"),
        Err(error) => error,
    };

    assert!(
        matches!(error, RunError::NoMatchingCase),
        "expected RunError::NoMatchingCase, got {error}"
    );
    assert_events(&["selector#3"]);
    assert_eq!(count("branch:other"), 0);
    assert_eq!(count("branch:fallback"), 0);
    assert_eq!(count("after_choose"), 0);
}

// ---- Definition 错误：业务零

async fn never_runs(query: Query<&Context>) -> Result<Context, BodyError> {
    log("never_runs");
    Ok(Context(query.get().0))
}

async fn never_runs_seed(query: Query<&Seed>) -> Result<Verdict, BodyError> {
    log("never_runs_seed");
    Ok(Verdict(query.get().0))
}

/// S09 terminal（X18）：`Iter` 预算 0 → Definition 阶段拒绝；构建 closure 自己的副作用
/// 已发生（SPEC §4），但任何业务 Node 都零调用。
#[test]
fn s09_definition_error_iter_zero_rounds_runs_zero_business() {
    reset();
    let runtime = Runtime::new();
    let defined = Cell::new(0);
    let error = match block_on(runtime.execute::<_, Context, _>(
        |flow, context| {
            defined.set(defined.get() + 1);
            let out_refs = flow.iter(context, 0, |current, round| {
                round.then_with_output(&never_runs, current)
            });
            flow.output_with(out_refs);
        },
        Context(1),
    )) {
        Ok(_) => panic!("max_iterations = 0 must be rejected at build time"),
        Err(error) => error,
    };

    let RunError::Definition(build) = error else {
        panic!("expected RunError::Definition, got {error}");
    };
    assert_eq!(
        defined.get(),
        1,
        "the definition closure runs before validation"
    );
    assert!(
        build.to_string().contains("at least one round"),
        "build diagnosis: {build}"
    );
    assert!(
        events().is_empty(),
        "no business node starts: {:?}",
        events()
    );
}

/// S09 terminal（X18）：`Retry` 的 `usize::MAX` 预算无法表达 N+1 → Definition 阶段拒绝；
/// 业务零调用。
#[test]
fn s09_definition_error_retry_max_budget_runs_zero_business() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, Verdict, _>(
        |flow, seed| {
            let out_refs: Ref<Verdict> = flow.retry_output(usize::MAX, |attempt| {
                let out_refs = attempt.then_with_output(&never_runs_seed, seed);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Seed(1),
    )) {
        Ok(_) => panic!("max_retries = usize::MAX must be rejected at build time"),
        Err(error) => error,
    };

    let RunError::Definition(build) = error else {
        panic!("expected RunError::Definition, got {error}");
    };
    assert!(
        build.to_string().contains("usize::MAX"),
        "build diagnosis: {build}"
    );
    assert!(
        events().is_empty(),
        "no business node starts: {:?}",
        events()
    );
}

/// X13（外部可观察部分，X18 分类）：Root 输出直接重复同一逻辑 Ref → Definition 阶段拒绝
/// （BuildError），任何业务 Node 都不开始执行。
#[test]
fn s09_root_duplicate_output_rejected_before_any_business() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, (Verdict, Verdict), _>(
        |flow, seed| {
            let value = flow.then_with_output(&x21_first, seed);
            let out_refs = (value, value);
            flow.output_with(out_refs);
        },
        Seed(1),
    )) {
        Ok(_) => panic!("a duplicated root output must be rejected"),
        Err(error) => error,
    };

    let RunError::Definition(build) = error else {
        panic!("expected RunError::Definition, got {error}");
    };
    assert!(
        build.to_string().contains("more than once"),
        "build diagnosis: {build}"
    );
    assert!(
        events().is_empty(),
        "no business node starts: {:?}",
        events()
    );
}

// ================================================================ X21：故障时业务停止

async fn x21_first(query: Query<&Seed>) -> Result<Verdict, BodyError> {
    log("x21_first");
    Ok(Verdict(query.get().0 + 1))
}

async fn x21_fail(query: Query<&Verdict>) -> Result<Summary, BodyError> {
    log("x21_fail");
    Err(BodyError::fail(TooSmall {
        value: query.get().0,
    }))
}

async fn x21_after(query: Query<&Summary>) -> Result<Amount, BodyError> {
    log("x21_after");
    Ok(Amount(query.get().0))
}

/// X21（外部可观察部分）：故障发生时业务停止，后续业务**零调用**（逐 Node 计数断言）。
/// item 层级的“停止剩余项”由 [`s09_propagation_each_stops_remaining_items`] 覆盖。
#[test]
fn x21_fault_stops_subsequent_business_with_zero_calls() {
    reset();
    let runtime = Runtime::new();
    let error = match block_on(runtime.execute::<_, Amount, _>(
        |flow, seed| {
            let first = flow.then_with_output(&x21_first, seed);
            let failed = flow.then_with_output(&x21_fail, first);
            let out_refs = flow.then_with_output(&x21_after, failed);
            flow.output_with(out_refs);
        },
        Seed(1),
    )) {
        Ok(_) => panic!("the failing node must terminate the path"),
        Err(error) => error,
    };

    let RunError::Body(failure) = error else {
        panic!("expected RunError::Body, got {error}");
    };
    let preserved = failure
        .source()
        .expect("the dynamic failure reason stays reachable")
        .downcast_ref::<TooSmall>()
        .expect("the original dynamic error object is preserved")
        .value;
    assert_eq!(preserved, 2);
    assert_events(&["x21_first", "x21_fail"]);
    assert_eq!(count("x21_first"), 1);
    assert_eq!(count("x21_fail"), 1);
    assert_eq!(count("x21_after"), 0, "zero calls after the fault");
}
