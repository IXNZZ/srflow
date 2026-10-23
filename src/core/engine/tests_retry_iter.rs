//! WE-T04 定向样本：Retry／Iter 控制器与成组 Promote 的 E 矩阵证据（基础批）。
//!
//! 全部用例只通过公开入口 `Runtime::new().execute(...)` 运行；观察点使用本模块 cfg(test)
//! 通道（控制器／Round 记录、成组提交现场）与 Drop 事件，不直接触碰内部结构。

use crate::Data;
use crate::core::engine::boundary::{
    install_iter_delivery_cleanup_fault, install_retry_round_cleanup_fault,
};
use crate::core::engine::definition::{PositionSource, build_definition};
use crate::core::engine::error::{BodyError, BuildErrorDetail, RunError, Visibility};
use crate::core::engine::flow::Flow;
use crate::core::engine::observe;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use futures::executor::block_on;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Data)]
struct Prompt(u32);
#[derive(Data)]
struct Candidate(u32);
#[derive(Data)]
struct Report(u32);
#[derive(Data)]
struct Draft(String);
#[derive(Data)]
struct Context(u32);

/// 带 Drop 事件的业务值：证明 attempt 自产值在下一轮之前已释放。
#[derive(Data)]
struct Tracked {
    log: Rc<RefCell<Vec<String>>>,
    tag: &'static str,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.log.borrow_mut().push(format!("drop:{}", self.tag));
    }
}

/// Root 输入：以 `Rc` 承载共享日志，用于证明各 attempt 复用同一原始依赖对象。
#[derive(Data)]
struct Seed {
    log: Rc<RefCell<Vec<String>>>,
}

/// 业务重试理由：携带动态原因，供 E10 的 source chain 检查。
#[derive(Debug)]
struct NotReady(u32);

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate {} is not ready", self.0)
    }
}

impl Error for NotReady {}

fn log_of(seed: &Seed) -> Rc<RefCell<Vec<String>>> {
    Rc::clone(&seed.log)
}

/// 尝试计数器：随每次 attempt 递增，使每轮产生不同的候选值。
#[derive(Data)]
struct Attempts(Rc<std::cell::Cell<u32>>);

/// 生成候选：固定推进一格。
async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    Ok(Candidate(query.get().0 + 1))
}

/// 生成候选：每次 attempt 递增计数并产生新值（E07／E09 样本使用）。
async fn generate_counted(query: Query<(&Prompt, &Attempts)>) -> Result<Candidate, BodyError> {
    let (prompt, attempts) = query.get();
    let attempt = attempts.0.get() + 1;
    attempts.0.set(attempt);
    Ok(Candidate(prompt.0 + attempt))
}

/// 把 attempt 自产值登记为可观察对象（Drop 时记事件）。
async fn track(query: Query<&Seed>) -> Result<Tracked, BodyError> {
    let log = log_of(query.get());
    log.borrow_mut().push(String::from("run:attempt"));
    Ok(Tracked {
        log,
        tag: "attempt",
    })
}

/// 评审：value < 3 时要求重试，否则给出报告。
async fn judge_retrying(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let value = query.get().0;
    if value < 3 {
        Err(BodyError::retry(NotReady(value)))
    } else {
        Ok(Report(value * 10))
    }
}

/// 评审：永远要求重试。
async fn judge_always_retrying(query: Query<&Candidate>) -> Result<Report, BodyError> {
    Err(BodyError::retry(NotReady(query.get().0)))
}

/// 评审：第二轮起发出 IterBreak。
async fn judge_until_break(query: Query<&Context>) -> Result<Report, BodyError> {
    let context = query.get();
    if context.0 >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Report(context.0))
    }
}

/// 评审：发出 IterBreak（Retry 不得捕获）。
async fn judge_break(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let _ = query.get();
    Err(BodyError::iter_break())
}

/// 评审：永远 Continue。
async fn judge_continue(query: Query<&Context>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0))
}

/// 每轮把 draft 追加一个字符。
async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    Ok(Draft(format!("{}+", query.get().0)))
}

/// 每轮把 context 加一。
async fn bump(query: Query<&Context>) -> Result<Context, BodyError> {
    Ok(Context(query.get().0 + 1))
}

fn reset_observations() {
    observe::reset();
}

// E10：预算耗尽保留 max_retries／实际 attempts／最后动态原因，且不交付候选。
#[test]
fn e10_retry_exhausted_keeps_fields_and_last_dynamic_reason() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(1, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                attempt.then_with_output(&judge_always_retrying, candidate)
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    let error = outcome.err().expect("budget exhaustion fails");
    match &error {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!((*max_retries, *attempts), (1, 2));
            let reason = last_error.source().expect("dynamic reason reachable");
            assert!(reason.to_string().contains("not ready"));
        }
        other => panic!("expected RetryExhausted, got {other}"),
    }
    reset_observations();
}

// E07／E09：各 attempt 复用同一原始依赖，且上一轮自产值在下一轮 Node 之前释放。
#[test]
fn e07_e09_attempts_reuse_original_dependency_and_clean_the_previous_one() {
    reset_observations();
    let runtime = Runtime::new();
    let log = Rc::new(RefCell::new(Vec::new()));
    let seed = Seed {
        log: Rc::clone(&log),
    };
    let report = block_on(runtime.execute::<_, (Report, Tracked), _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>,
         (seed, prompt, attempts): (Ref<Seed>, Ref<Prompt>, Ref<Attempts>)| {
            let out_refs = flow.retry(3, |attempt| {
                let tracked = attempt.then_with_output(&track, seed);
                let candidate = attempt.then_with_output(&generate_counted, (prompt, attempts));
                let report = attempt.then_with_output(&judge_retrying, candidate);
                (report, tracked)
            });
            flow.output_with(out_refs);
        },
        (seed, Prompt(0), Attempts(Rc::new(std::cell::Cell::new(0)))),
    ));
    let (report, _tracked) = report.expect("third attempt succeeds");
    assert_eq!(report.0, 30);
    let events = log.borrow().clone();
    let runs = events
        .iter()
        .filter(|event| *event == "run:attempt")
        .count();
    assert_eq!(runs, 3, "one attempt node per executed attempt: {events:?}");
    // 前两轮的 Tracked 必须在第三轮运行前释放：事件顺序为 run,drop,run,drop,run。
    assert_eq!(
        &events[..5],
        [
            "run:attempt",
            "drop:attempt",
            "run:attempt",
            "drop:attempt",
            "run:attempt"
        ],
        "events: {events:?}"
    );
    reset_observations();
}

// E13：Break 交付进入该轮时的 current，而不是本轮已产生的 next。
#[test]
fn e13_iter_break_delivers_the_entry_current_of_that_round() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, (draft, context)| {
            let out_refs = flow.iter(
                (draft, context),
                5,
                |(current_draft, current_context), round| {
                    round.then_with_output(&judge_until_break, current_context);
                    let next_draft = round.then_with_output(&revise, current_draft);
                    let next_context = round.then_with_output(&bump, current_context);
                    (next_draft, next_context)
                },
            );
            flow.output_with(out_refs);
        },
        (Draft(String::from("s0")), Context(0)),
    ));
    let (draft, context) = outcome.expect("break delivers the current state");
    assert_eq!((draft.0.as_str(), context.0), ("s0+", 1));
    reset_observations();
}

// E14／E17：第一轮 Break 合法；current 原样返回（same-target）不重绑、不丢失。
#[test]
fn e14_e17_first_round_break_returns_the_original_state_unchanged() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, (draft, context)| {
            let out_refs = flow.iter(
                (draft, context),
                1,
                |(current_draft, current_context), round| {
                    round.then_with_output(&judge_until_break, current_context);
                    // same-target：一个槽保持 current 不变，另一个槽推进。
                    let _next_draft = round.then_with_output(&revise, current_draft);
                    (current_draft, current_context)
                },
            );
            flow.output_with(out_refs);
        },
        (Draft(String::from("keep")), Context(9)),
    ));
    let (draft, context) = outcome.expect("first round break is legal");
    assert_eq!((draft.0.as_str(), context.0), ("keep", 9));
    reset_observations();
}

// E15：第 N 轮 Normal 完成整组 Promote 后返回 IterationLimitReached，不交付最后状态。
#[test]
fn e15_round_limit_promotes_the_last_round_then_reports_completed_iterations() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, (draft, context)| {
            let out_refs = flow.iter(
                (draft, context),
                2,
                |(current_draft, current_context), round| {
                    round.then_with_output(&judge_continue, current_context);
                    let next_draft = round.then_with_output(&revise, current_draft);
                    let next_context = round.then_with_output(&bump, current_context);
                    (next_draft, next_context)
                },
            );
            flow.output_with(out_refs);
        },
        (Draft(String::from("r")), Context(0)),
    ));
    let error = outcome.err().expect("round limit fails");
    match &error {
        RunError::IterationLimitReached {
            max_iterations,
            completed_iterations,
        } => assert_eq!((*max_iterations, *completed_iterations), (2, 2)),
        other => panic!("expected IterationLimitReached, got {other}"),
    }
    let rounds = observe::take_controller_rounds();
    let promoted: Vec<usize> = rounds
        .iter()
        .filter(|record| record.kind == "iter" && record.at == "round-promoted")
        .map(|record| record.index)
        .collect();
    assert_eq!(promoted, vec![1, 2], "each round promotes: {rounds:?}");
    let exhausted = rounds
        .iter()
        .any(|record| record.at == "budget-exhausted" && record.index == 2);
    assert!(exhausted, "the Nth round is the exhausting one: {rounds:?}");
    reset_observations();
}

// E12：Retry 不捕获 IterBreak；未匹配信号到 Root 成为 UnhandledControl。
#[test]
fn e12_retry_does_not_capture_iter_break() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Candidate, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(3, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                let _ = attempt.then_with_output(&judge_break, candidate);
                candidate
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    let error = outcome.err().expect("IterBreak leaves the retry boundary");
    assert!(
        matches!(error, RunError::UnhandledControl(_)),
        "expected UnhandledControl, got {error}"
    );
    reset_observations();
}

// E02：Retry 的 usize::MAX 预算在构建期拒绝（不建立 Root、不调用 Node）。
#[test]
fn e02_retry_budget_max_is_rejected_at_build_time() {
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Candidate, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(usize::MAX, |attempt| {
                attempt.then_with_output(&generate, prompt)
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    assert!(matches!(outcome, Err(RunError::Definition(_))));
}

// E02：Iter 的 0 轮预算在构建期拒绝。
#[test]
fn e02_iter_zero_rounds_is_rejected_at_build_time() {
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Context, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, context: Ref<Context>| {
            let out_refs = flow.iter(context, 0, |current, round| {
                let _ = round.then_with_output(&judge_continue, current);
                current
            });
            flow.output_with(out_refs);
        },
        Context(0),
    ));
    assert!(matches!(outcome, Err(RunError::Definition(_))));
}

// ---------------------------------------------------------------- R01 formal current 静态可见性

// 计数一次业务 Node 调用：Definition 负例下必须保持 0；合法样本下证明实际执行。
// 用线程局部计数（每个 `#[test]` 各自线程），避免并行用例互相污染。
thread_local! {
    static ESCAPE_NODE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn escape_calls() -> usize {
    ESCAPE_NODE_CALLS.with(std::cell::Cell::get)
}

fn reset_escape_calls() {
    ESCAPE_NODE_CALLS.with(|calls| calls.set(0));
}

fn bump_escape_calls() {
    ESCAPE_NODE_CALLS.with(|calls| calls.set(calls.get() + 1));
}

/// 读入单个 `u32` 并以 `IterBreak` 结束（用于把 formal current 送进业务路径）。
async fn count_and_break(_: Query<&u32>) -> Result<(), BodyError> {
    bump_escape_calls();
    Err(BodyError::iter_break())
}

/// 消费单个 `u32` 并原样返回（用于 parent／sibling 使用点）。
async fn consume_u32(query: Query<&u32>) -> Result<u32, BodyError> {
    bump_escape_calls();
    Ok(*query.get())
}

/// 只读取两个 `u32` 位置（用于强制 round 捕获祖先 Ref）。
async fn read_pair(_: Query<(&u32, &u32)>) -> Result<(), BodyError> {
    Ok(())
}

/// 断言两个 `u32` 位置相等（用于“round 捕获 initial”正例）。
async fn assert_same(query: Query<(&u32, &u32)>) -> Result<(), BodyError> {
    let (left, right) = query.get();
    assert_eq!(*left, *right, "current equals the captured initial");
    Ok(())
}

/// 以 `IterBreak` 结束并断言两个输入相等（共享 initial 正例）。
async fn break_pair(query: Query<(&u32, &u32)>) -> Result<(), BodyError> {
    let (left, right) = query.get();
    assert_eq!((*left, *right), (11, 11));
    Err(BodyError::iter_break())
}

/// 以 `IterBreak` 结束且不断言输入（distinct 槽样本）。
async fn break_two(_: Query<(&u32, &u32)>) -> Result<(), BodyError> {
    Err(BodyError::iter_break())
}

// R01：formal current 不得作为 Root 输出外泄；Definition 拒绝且零业务 Node。
#[test]
fn r01_iter_formal_current_root_output_leak_is_rejected() {
    reset_observations();
    reset_escape_calls();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, initial: Ref<u32>| {
            let mut escaped: Option<Ref<u32>> = None;
            let _output = flow.iter(initial, 1, |current, round| {
                escaped = Some(current);
                round.then_with_output(&count_and_break, current);
                current
            });
            let out_refs = escaped.expect("captured formal current");
            flow.output_with(out_refs);
        },
        11u32,
    ));
    let error = outcome.expect_err("leaked formal current must be rejected");
    match &error {
        RunError::Definition(build) => assert!(
            matches!(
                build.detail,
                BuildErrorDetail::OutOfScope {
                    relation: Visibility::ChildLocal,
                    ..
                }
            ),
            "root output leak must be OutOfScope(ChildLocal): {build}"
        ),
        other => panic!("expected Definition, got {other}"),
    }
    assert_eq!(
        escape_calls(),
        0,
        "an invalid Definition must not run any business Node"
    );
    reset_observations();
}

// R01：formal current 不得被 parent Body 的 step 使用；Definition 拒绝且零业务 Node。
#[test]
fn r01_iter_formal_current_parent_step_use_is_rejected() {
    reset_observations();
    reset_escape_calls();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, initial: Ref<u32>| {
            let out_refs = flow.chain(|sub| {
                let mut escaped: Option<Ref<u32>> = None;
                let _output = sub.iter(initial, 1, |current, round| {
                    escaped = Some(current);
                    round.then_with_output(&count_and_break, current);
                    current
                });
                sub.then_with_output(&consume_u32, escaped.expect("captured formal current"))
            });
            flow.output_with(out_refs);
        },
        11u32,
    ));
    let error = outcome.expect_err("parent use of formal current must be rejected");
    match &error {
        RunError::Definition(build) => assert!(
            matches!(
                build.detail,
                BuildErrorDetail::OutOfScope {
                    relation: Visibility::ChildLocal,
                    ..
                }
            ),
            "parent use of formal current must be OutOfScope(ChildLocal): {build}"
        ),
        other => panic!("expected Definition, got {other}"),
    }
    assert_eq!(
        escape_calls(),
        0,
        "an invalid Definition must not run any business Node"
    );
    reset_observations();
}

// R01：formal current 不得被 sibling Body 使用；Definition 拒绝且零业务 Node。
#[test]
fn r01_iter_formal_current_sibling_use_is_rejected() {
    reset_observations();
    reset_escape_calls();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, initial: Ref<u32>| {
            let mut escaped: Option<Ref<u32>> = None;
            let _first = flow.chain(|sub| {
                sub.iter(initial, 1, |current, round| {
                    escaped = Some(current);
                    round.then_with_output(&count_and_break, current);
                    current
                })
            });
            let sibling_local = escaped.expect("captured formal current");
            let out_refs = flow.chain(move |sub| sub.then_with_output(&consume_u32, sibling_local));
            flow.output_with(out_refs);
        },
        11u32,
    ));
    let error = outcome.expect_err("sibling use of formal current must be rejected");
    match &error {
        RunError::Definition(build) => assert!(
            matches!(
                build.detail,
                BuildErrorDetail::OutOfScope {
                    relation: Visibility::Sibling,
                    ..
                }
            ),
            "sibling use of formal current must be OutOfScope(Sibling): {build}"
        ),
        other => panic!("expected Definition, got {other}"),
    }
    assert_eq!(
        escape_calls(),
        0,
        "an invalid Definition must not run any business Node"
    );
    reset_observations();
}

// R01 正例：round 闭包合法捕获祖先 Ref（非 formal current）并交付 current。
#[test]
fn r01_iter_round_captures_ancestor_ref_legally() {
    reset_observations();
    reset_escape_calls();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, (initial, external): (Ref<u32>, Ref<u32>)| {
            let out_refs = flow.iter(initial, 2, |current, round| {
                round.then_with_output(&read_pair, (current, external));
                round.then_with_output(&count_and_break, current);
                current
            });
            flow.output_with(out_refs);
        },
        (11u32, 7u32),
    ));
    assert_eq!(
        outcome.expect("a legal deep capture in the round must build and run"),
        11
    );
    assert_eq!(escape_calls(), 1, "the round body really runs once");
    reset_observations();
}

// ---------------------------------------------------------------- R02 initial 与词法依赖去重

// R02 正例：`iter((x, x), ..)` 重复 initial 合法；两个 state 槽各自保独立身份。
#[test]
fn r02_iter_duplicate_initial_is_deduplicated_but_slots_stay_distinct() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, initial: Ref<u32>| {
            let output = flow.iter((initial, initial), 1, |(left, right), round| {
                round.then_with_output(&break_pair, (left, right));
                (left, right)
            });
            let out_refs = output.0;
            flow.output_with(out_refs);
        },
        11u32,
    ));
    assert_eq!(
        outcome.expect("duplicate initial is a legal shared-state alias"),
        11
    );

    // 逐槽身份：Definition 为两个 state 槽各登记一个 IterCurrent，同属一个 round Body。
    let definition = build_definition::<u32, u32, _>(|flow: &mut Flow<'_>, initial: Ref<u32>| {
        let output = flow.iter((initial, initial), 1, |(left, right), round| {
            round.then_with_output(&break_pair, (left, right));
            (left, right)
        });
        let out_refs = output.0;
        flow.output_with(out_refs);
    })
    .expect("legal shared-initial definition builds");
    let currents: Vec<_> = definition
        .positions()
        .iter()
        .filter(|record| matches!(record.source(), PositionSource::IterCurrent { .. }))
        .collect();
    assert_eq!(
        currents.len(),
        2,
        "each state slot keeps an independent formal current"
    );
    assert_ne!(
        currents[0].handle().position,
        currents[1].handle().position,
        "the two state slots must not collapse to one logical position"
    );
    assert_eq!(
        currents[0].owner(),
        currents[1].owner(),
        "both formal current slots belong to the same round Body"
    );
    reset_observations();
}

// R02 正例：round 同时使用 current 并捕获 initial（普通词法依赖与 initial 重叠）合法。
#[test]
fn r02_iter_round_may_capture_initial_alongside_current() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, u32, _>(
        |flow: &mut Flow<'_>, initial: Ref<u32>| {
            let out_refs = flow.iter(initial, 1, |current, round| {
                round.then_with_output(&assert_same, (current, initial));
                round.then_with_output(&count_and_break, current);
                current
            });
            flow.output_with(out_refs);
        },
        11u32,
    ));
    assert_eq!(
        outcome.expect("capturing the initial state inside the round is legal"),
        11
    );
    reset_observations();
}

// R02 正例：两个 distinct 但同类型的 initial 槽不得按类型或 DataId 合并（逐槽保留身份）。
#[test]
fn r02_iter_distinct_same_type_initial_slots_are_not_merged() {
    reset_observations();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (u32, u32), _>(
        |flow: &mut Flow<'_>, (left, right): (Ref<u32>, Ref<u32>)| {
            let out_refs = flow.iter((left, right), 1, |(current_left, current_right), round| {
                round.then_with_output(&break_two, (current_left, current_right));
                (current_left, current_right)
            });
            flow.output_with(out_refs);
        },
        (3u32, 5u32),
    ));
    assert_eq!(
        outcome.expect("distinct same-type slots stay independent"),
        (3, 5)
    );
    reset_observations();
}

// ---------------------------------------------------------------- E27 真实控制退出故障

/// E27(a) attempt 计数：故障后不得再执行新 attempt。
static E27A_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

/// 推进候选并计数一次 attempt。
async fn counted_generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    E27A_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    Ok(Candidate(query.get().0 + 1))
}

/// 始终发出 IterBreak（E27(b) 交付路径样本）。
async fn judge_break_ctx(query: Query<&Context>) -> Result<Report, BodyError> {
    let _ = query.get();
    Err(BodyError::iter_break())
}

/// 真实清理故障的统一诊断文本（ScopeError::Invariant）。
const CLEANUP_INVARIANT: &str =
    "internal invariant violated: owned entry must exist until its scope closes";

/// E27(b) 后续业务计数：故障后不得再跑任何业务 Node。
static E27B_BUSINESS: AtomicUsize = AtomicUsize::new(0);

/// 与 [`revise`] 等价并计数一次业务调用（E27(b) 样本使用）。
async fn counted_revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    E27B_BUSINESS.fetch_add(1, Ordering::SeqCst);
    revise(query).await
}

// E27(a)：Retry 匹配 Retry 信号的 round 收口真实清理故障 → Runtime；不开新 attempt／不预算收口，
// round 真实未关闭、责任保留，首框架与首清理诊断分别保留。
#[test]
fn e27a_retry_round_cleanup_fault_is_runtime_and_stops_attempts() {
    reset_observations();
    E27A_ATTEMPTS.store(0, Ordering::SeqCst);
    install_retry_round_cleanup_fault();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(3, |attempt| {
                let candidate = attempt.then_with_output(&counted_generate, prompt);
                attempt.then_with_output(&judge_always_retrying, candidate)
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    let error = outcome.err().expect("a real cleanup failure is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(runtime_error.note(), "retry attempt cleanup failed");
    let framework =
        Error::source(runtime_error).expect("the first framework diagnostic is preserved");
    assert_eq!(
        framework.to_string(),
        CLEANUP_INVARIANT,
        "the framework diagnostic is the exact ScopeError, not a placeholder string"
    );
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(
        cleanup.to_string(),
        CLEANUP_INVARIANT,
        "the first cleanup diagnostic keeps its exact text"
    );
    assert_eq!(
        E27A_ATTEMPTS.load(Ordering::SeqCst),
        1,
        "no further attempt may run after the cleanup fault"
    );

    let rounds = observe::take_controller_rounds();
    let retry_ats: Vec<&str> = rounds
        .iter()
        .filter(|record| record.kind == "retry")
        .map(|record| record.at)
        .collect();
    assert_eq!(
        retry_ats,
        ["controller-created", "round-created", "round-caught"],
        "no new attempt and no budget close: {rounds:?}"
    );
    let caught = rounds
        .iter()
        .find(|record| record.kind == "retry" && record.at == "round-caught")
        .expect("the matched round is recorded");
    assert_ne!(
        caught.caller, caught.controller,
        "caller and controller are distinct scopes"
    );
    let round_scope = caught.round.clone().expect("the round Scope is recorded");
    let controller_scope = caught.controller.clone();
    assert_ne!(
        round_scope, controller_scope,
        "the round is the controller's child"
    );
    let root_scope = observe::take_frames()
        .into_iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .expect("the root frame is recorded")
        .scope;

    // F02：失败时点的责任快照 —— 失败的 round 仍由本 guard 负责退出，未被解除责任。
    let guard_failures = crate::core::test_support::take_guard_failures();
    let round_failure = guard_failures
        .iter()
        .find(|record| record.frame_scope.as_ref() == Some(&round_scope))
        .expect("the round guard's failure exit is recorded");
    assert_eq!(
        round_failure.responsible,
        vec![round_scope.clone()],
        "the failed round scope is NOT released: {guard_failures:?}"
    );

    // F01：首清理诊断的真实 Scope 身份 —— 首条为 round；后续 controller／root 只登记不覆盖。
    let cleanup_failures = crate::core::test_support::take_cleanup_failures();
    let recorded: Vec<_> = cleanup_failures
        .iter()
        .map(|record| (record.scope.clone(), record.stored_first))
        .collect();
    assert_eq!(
        recorded,
        vec![
            (round_scope.clone(), true),
            (controller_scope.clone(), false),
            (root_scope.clone(), false),
        ],
        "the first cleanup (round) is stored; later scopes only register: {cleanup_failures:?}"
    );
    match &cleanup_failures[0].error {
        crate::core::internal_error::ScopeError::Invariant { violated } => assert_eq!(
            *violated, "owned entry must exist until its scope closes",
            "the stored first diagnostic is the real cleanup ScopeError"
        ),
        other => panic!("expected the real Invariant cleanup diagnostic, got {other:?}"),
    }
    assert_eq!(
        cleanup.to_string(),
        cleanup_failures[0].error.to_string(),
        "the public cleanup_source is exactly the first registered diagnostic"
    );

    // F02：清理由合法父层接管 —— round 自身清理失败后，controller 层执行了整棵子树的收口尝试。
    let guard_cleanups = crate::core::test_support::take_guard_cleanups();
    assert!(
        guard_cleanups.iter().any(|record| {
            record.frame_scope.as_ref() == Some(&round_scope)
                && record.scope == round_scope
                && !record.ok
        }),
        "the round's own teardown ran and really failed: {guard_cleanups:?}"
    );
    assert!(
        guard_cleanups.iter().any(|record| {
            record.frame_scope.as_ref() == Some(&controller_scope)
                && record.scope == controller_scope
                && !record.ok
        }),
        "the legal parent layer (controller) took over the failed subtree teardown: {guard_cleanups:?}"
    );

    // F02：失败时点的真实存活快照 —— 被破坏的 owned entry 仍登记在本 round 名下（owner=round），
    // 但 Container 实例已不存在（alive=false）：Guard 收口责任与 Container 物理 owner 分离。
    let snapshots = crate::core::test_support::take_controller_snapshots();
    let round_before = snapshots
        .iter()
        .find(|snapshot| snapshot.phase == "retry-cleanup-before" && snapshot.role == "round")
        .expect("the round's cleanup-time snapshot is recorded");
    assert_eq!(round_before.scope, round_scope);
    let dead: Vec<_> = round_before
        .owned_alive
        .iter()
        .filter(|(_, alive, _)| !*alive)
        .collect();
    assert_eq!(
        dead.len(),
        1,
        "exactly the injected entry is dead: {:?}",
        round_before.owned_alive
    );
    assert_eq!(
        dead[0].2.as_ref(),
        Some(&round_scope),
        "the dead entry is still owned by the failed round (Guard 收口责任)"
    );

    let states = observe::take_scope_states();
    let failed = states
        .iter()
        .find(|record| record.at == "control-cleanup-failed")
        .expect("the failing cleanup scope is observed");
    assert_eq!(
        failed.scope, round_scope,
        "the failing scope is the retry round"
    );
    assert_ne!(
        failed.state,
        crate::core::scope::ScopeState::Closed,
        "the round is really left unclosed"
    );
    reset_observations();
}

// E27(b)：Iter 匹配 IterBreak 的交付路径收口清理故障 → Runtime；current 不交付、两跳无半导出、
// 责任收拢，首框架与首清理诊断分别保留。
#[test]
fn e27b_iter_break_delivery_cleanup_fault_does_not_deliver_current() {
    reset_observations();
    E27B_BUSINESS.store(0, Ordering::SeqCst);
    install_iter_delivery_cleanup_fault();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut Flow<'_>, (draft, context): (Ref<Draft>, Ref<Context>)| {
            let out_refs = flow.iter(
                (draft, context),
                3,
                |(current_draft, current_context), round| {
                    // 先产生本轮 next（未接受），再发出 IterBreak。
                    let next_draft = round.then_with_output(&counted_revise, current_draft);
                    let next_context = round.then_with_output(&bump, current_context);
                    round.then_with_output(&judge_break_ctx, current_context);
                    (next_draft, next_context)
                },
            );
            flow.output_with(out_refs);
        },
        (Draft(String::from("s0")), Context(0)),
    ));
    let error = outcome
        .err()
        .expect("a delivery cleanup failure is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(runtime_error.note(), "iter output handover rejected");
    let framework =
        Error::source(runtime_error).expect("the first framework diagnostic is preserved");
    assert_eq!(
        framework.to_string(),
        CLEANUP_INVARIANT,
        "the framework diagnostic is the exact ScopeError"
    );
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(
        cleanup.to_string(),
        CLEANUP_INVARIANT,
        "the first cleanup diagnostic keeps its exact text"
    );

    let rounds = observe::take_controller_rounds();
    let caught = rounds
        .iter()
        .find(|record| record.kind == "iter" && record.at == "round-caught")
        .expect("the matched iter round is recorded");
    let round_scope = caught.round.clone().expect("the round Scope is recorded");
    let controller_scope = caught.controller.clone();

    // F01：首清理诊断的真实 Scope 身份 = round；后续 controller／root 只登记不覆盖。
    let cleanup_failures = crate::core::test_support::take_cleanup_failures();
    assert_eq!(
        cleanup_failures[0].scope, round_scope,
        "the first cleanup diagnostic is the round, at its real scope: {cleanup_failures:?}"
    );
    assert!(
        cleanup_failures[0].stored_first,
        "the round's cleanup diagnostic is the stored first"
    );
    assert!(
        cleanup_failures
            .iter()
            .skip(1)
            .all(|record| !record.stored_first),
        "later cleanup failures never overwrite the first: {cleanup_failures:?}"
    );
    assert_eq!(
        runtime_error
            .cleanup_source()
            .expect("the first cleanup diagnostic is preserved")
            .to_string(),
        cleanup_failures[0].error.to_string(),
        "the public cleanup_source is exactly the first registered diagnostic"
    );
    assert_eq!(
        cleanup_failures.len(),
        3,
        "round, controller and root all fail cleanup: {cleanup_failures:?}"
    );

    // F02：错误前后的真实快照 —— 区分 Guard 收口责任（owner 登记）与 Container 物理 owner。
    let snapshots = crate::core::test_support::take_controller_snapshots();
    let find = |phase: &str, role: &str| {
        snapshots
            .iter()
            .find(|snapshot| snapshot.phase == phase && snapshot.role == role)
            .unwrap_or_else(|| panic!("missing snapshot {phase}/{role}: {snapshots:?}"))
    };
    let round_before = find("delivery-before", "round");
    let round_failed = find("delivery-failed", "round");
    let controller_before = find("delivery-before", "controller");
    let controller_failed = find("delivery-failed", "controller");

    // round：本轮已产生但未交付的 next 仍登记在本 round 名下（owner=round），但其中
    // 一个 owned entry 的 Container 实例已被故障销毁（alive=false）——责任登记与物理存活分离。
    assert_eq!(round_before.scope, round_scope);
    assert_eq!(round_before.owned.len(), 3);
    let dead: Vec<_> = round_before
        .owned_alive
        .iter()
        .filter(|(_, alive, _)| !*alive)
        .collect();
    assert_eq!(
        dead.len(),
        1,
        "exactly the injected entry is dead: {:?}",
        round_before.owned_alive
    );
    assert_eq!(
        dead[0].2.as_ref(),
        Some(&round_scope),
        "the dead entry is still owned by the round (Guard 收口责任)"
    );
    assert_eq!(
        round_before.state,
        Some(crate::core::scope::ScopeState::Active)
    );

    // 失败后 round 未被 Closed，owned 集合未被半销毁（整组预检拒绝，无半导出）。
    assert_ne!(
        round_failed.state,
        Some(crate::core::scope::ScopeState::Closed),
        "the round is left unclosed after the failed delivery"
    );
    assert_eq!(
        round_failed.owned, round_before.owned,
        "no partial owned destruction / half export"
    );
    assert_eq!(round_failed.scope, round_scope);

    // controller 目的绑定、owner、pending 在错误前后完全不变：Guard 收口责任未泄漏到 Container。
    assert_eq!(controller_before.scope, controller_scope);
    assert_eq!(
        controller_before.refs, controller_failed.refs,
        "controller purpose bindings are unchanged"
    );
    assert_eq!(controller_before.owned, controller_failed.owned);
    assert_eq!(controller_before.owned_alive, controller_failed.owned_alive);
    assert_eq!(controller_before.state, controller_failed.state);
    assert_eq!(controller_before.pending, controller_failed.pending);
    assert_eq!(
        controller_before.pending,
        Some(Vec::new()),
        "controller pending recycle list is the real (empty) one, not a missing snapshot"
    );

    assert_eq!(
        E27B_BUSINESS.load(Ordering::SeqCst),
        1,
        "no further business node runs after the delivery fault"
    );

    let deliveries = observe::take_controller_deliveries();
    assert!(
        !deliveries.iter().any(|record| record.kind == "iter"),
        "neither hop (round-to-controller / controller-to-caller) may half-export: {deliveries:?}"
    );
    reset_observations();
}
