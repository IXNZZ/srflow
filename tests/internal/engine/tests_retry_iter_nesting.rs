//! WE-T04 定向样本（嵌套与终止批）。
//!
//! 覆盖 E24（真实嵌套捕获与跨类型传播）、E11（不驱动自动 Retry）、E25（预算错误沿
//! Chain／其他控制器终端传播并保留冻结字段）、E29（Pending 取消）与 E26（耗尽叠加真实
//! 清理故障）。全部经公开入口 `Runtime::new().execute(...)`；观察点只用本模块的
//! cfg(test) 通道（控制器／Round 记录）与 Drop 事件，不直接触碰内部结构。
use crate::Data;
use crate::core::engine::boundary::observe::{
    install_boundary_cleanup_fault, install_control_cleanup_fault,
};
use crate::core::engine::error::{BodyError, ControlSignal, RunError};
use crate::core::engine::observe;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::runtime::observe::install_root_cleanup_faults;
use crate::core::engine::shape::{Query, Ref};
use futures::executor::block_on;
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

/// 只在断言/转换中被引用（构造点随历史用例删除）：保留类型以维持断言可执行。
#[allow(dead_code)]
#[derive(Debug)]
struct Rejected(&'static str);

// ---------------------------------------------------------------- 业务类型

#[derive(Data)]
struct Prompt(u32);
#[derive(Data, Debug)]
struct Candidate(u32);
// `Report` 只作为控制 Node 的声明输出（其值不参与断言）；`Filler` 是刻意不读取的
// 可被销毁 Root 输入——两者的载荷按设计不被读取。
#[derive(Data)]
struct Report(#[allow(dead_code)] u32);
#[derive(Data)]
struct Context(u32);

/// 会被 `install_root_cleanup_faults` 提前销毁的 Root 输入（业务绝不读取它）。
#[derive(Data)]
struct Filler(#[allow(dead_code)] u32);

/// 业务重试理由：携带动态原因，供 last_error 源链检查。
#[derive(Debug)]
struct NotReady(u32);

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate {} is not ready", self.0)
    }
}

impl Error for NotReady {}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rejected: {}", self.0)
    }
}

impl Error for Rejected {}

/// 带 Drop 事件的业务值（借用 Future 与各层临时值的释放观察）。
#[derive(Data)]
struct Tracked {
    label: &'static str,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.log.borrow_mut().push(format!("drop:{}", self.label));
    }
}

/// Root 输入：以 `Rc` 承载共享日志；Drop 记录一次释放事件。
#[derive(Data)]
struct Seed {
    value: u32,
    log: Rc<RefCell<Vec<String>>>,
}

impl Drop for Seed {
    fn drop(&mut self) {
        self.log.borrow_mut().push(String::from("drop:seed"));
    }
}

// ---------------------------------------------------------------- 业务 Node

/// Prompt → Candidate：每次 attempt 产生新候选值。
async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    Ok(Candidate(query.get().0 + 1))
}

/// Context → Candidate：嵌套 Retry 的候选来源（读祖先 Ref）。
async fn generate_from_context(query: Query<&Context>) -> Result<Candidate, BodyError> {
    Ok(Candidate(query.get().0 + 1))
}

/// 永远要求重试：用于运行期预算耗尽路径。
async fn judge_always_retrying(query: Query<&Candidate>) -> Result<Report, BodyError> {
    Err(BodyError::retry(NotReady(query.get().0)))
}

/// 第二轮起发出 IterBreak：交付进入本轮时的 current。
async fn judge_break_on_context(query: Query<&Context>) -> Result<Report, BodyError> {
    let context = query.get();
    if context.0 >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Report(context.0))
    }
}

/// 发出 IterBreak（Retry 不得捕获，E12 样本）。
async fn breaking(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let _ = query.get();
    Err(BodyError::iter_break())
}

/// 不可恢复业务 Failure（E11 样本）。
async fn judge_failing(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let _ = query.get();
    Err(BodyError::fail(Rejected("business refusal")))
}

/// Context → next Context；计数以证明上限／失败后不再推进。
async fn bump(query: Query<&Context>) -> Result<Context, BodyError> {
    BUMP_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(Context(query.get().0 + 1))
}

/// Prompt → 与任何 case 都不相等的 selector（无匹配样本使用）。
async fn to_selector(query: Query<&Prompt>) -> Result<u32, BodyError> {
    let _ = query.get();
    Ok(1)
}

thread_local! {
    /// 迭代推进 Node 的实际调用次数（上限／失败样本使用；thread_local 隔离平行用例）。
    static BUMP_CALLS: Cell<usize> = const { Cell::new(0) };
    /// Pending 后续 Node 的实际调用次数（取消样本使用）。
    static AFTER_CALLS: Cell<usize> = const { Cell::new(0) };
}

fn bump_calls() -> usize {
    BUMP_CALLS.with(Cell::get)
}

fn after_calls() -> usize {
    AFTER_CALLS.with(Cell::get)
}

fn reset_counters() {
    BUMP_CALLS.with(|calls| calls.set(0));
    AFTER_CALLS.with(|calls| calls.set(0));
}

// ---------------------------------------------------------------- 取消设施

/// 首次 poll 返回 Pending 并自唤醒；Drop 记录一次事件（借用 Future 的生命周期观察）。
struct PendingWithGuard {
    yielded: bool,
    _log: Rc<RefCell<Vec<String>>>,
    _guard: Tracked,
}

impl Future for PendingWithGuard {
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

/// 产生并登记一个本层业务临时值（Drop 可观察）。
async fn track_temp(query: Query<&Seed>) -> Result<Tracked, BodyError> {
    let seed = query.get();
    let _ = seed.value;
    seed.log.borrow_mut().push(String::from("run:temp"));
    Ok(Tracked {
        label: "temp",
        log: Rc::clone(&seed.log),
    })
}

/// 进入真实 Pending 的 Node；借用 Future 自带 Drop 观察。
async fn pending_guard_node(query: Query<&Seed>) -> Result<Candidate, BodyError> {
    let log = Rc::clone(&query.get().log);
    PendingWithGuard {
        yielded: false,
        _log: Rc::clone(&log),
        _guard: Tracked {
            label: "pending-future",
            log,
        },
    }
    .await;
    Ok(Candidate(1))
}

/// Pending 之后的“下一个 Node”：取消后必须一次都不执行。
async fn after_pending(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let _ = query.get();
    AFTER_CALLS.with(|calls| calls.set(calls.get() + 1));
    Ok(Report(0))
}

// ---------------------------------------------------------------- 观察工具

/// 取出全部控制器／Round 记录并按 `at` 序列收尾（调用方已 reset）。
fn retry_ats(rounds: &[observe::ControllerRoundRecord]) -> Vec<&'static str> {
    rounds
        .iter()
        .filter(|record| record.kind == "retry")
        .map(|record| record.at)
        .collect()
}

fn iter_ats(rounds: &[observe::ControllerRoundRecord]) -> Vec<&'static str> {
    rounds
        .iter()
        .filter(|record| record.kind == "iter")
        .map(|record| record.at)
        .collect()
}

// ---------------------------------------------------------------- E24 真实嵌套捕获

// E24：Retry 内含 Iter —— 内层 IterBreak 在内层 Iter 边界被捕获并交付 current，
// 外层 Retry 的该次 attempt 正常完成、不重试（无第二次 attempt）。
#[test]
fn e24_retry_wrapping_iter_handles_break_at_the_inner_boundary() {
    observe::reset();
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Context, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, context: Ref<Context>| {
            let out_refs: Ref<Context> = flow.retry_output(2, |attempt| {
                let out_refs = attempt.iter_output(context, 5, |round| {
                    let current = round.current::<Context>();
                    round.then_with_output(&judge_break_on_context, current);
                    let next = round.then_with_output(&bump, current);
                    round.next_with(next);
                });
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Context(0),
    ));
    let context = outcome.expect("the inner iter handles the break and delivers current");
    assert_eq!(
        context.0, 1,
        "round 2 delivers the entry current (Context(1))"
    );

    let rounds = observe::take_controller_rounds();
    assert_eq!(
        retry_ats(&rounds),
        ["controller-created", "round-created", "round-normal"],
        "the retry attempt succeeds exactly once (no retry): {rounds:?}"
    );
    assert_eq!(
        iter_ats(&rounds),
        [
            "controller-created",
            "round-created",
            "round-promoted",
            "round-created",
            "round-caught",
        ],
        "the break is caught at round 2 inside the iter: {rounds:?}"
    );
    assert!(
        rounds
            .iter()
            .all(|record| record.at != "budget-exhausted" && record.at != "round-propagated"),
        "no budget exhaustion and no escape of the break: {rounds:?}"
    );
    observe::reset();
}

// E24：Iter 内含 Retry —— 内层 Retry 预算耗尽产生的预算终止不是控制信号；外层 Iter
// 把它当作普通 Round 失败（round-failed）上传为 terminal，绝不当 IterBreak 交付 current。
#[test]
fn e24_iter_wrapping_retry_propagates_inner_exhaustion_as_terminal() {
    observe::reset();
    reset_counters();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Context, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, context: Ref<Context>| {
            let out_refs = flow.iter_output(context, 5, |round| {
                let current = round.current::<Context>();
                round.retry(1, |attempt| {
                    let candidate = attempt.then_with_output(&generate_from_context, current);
                    let out_refs = attempt.then_with_output(&judge_always_retrying, candidate);
                    attempt.output_with(out_refs);
                });
                let next = round.then_with_output(&bump, current);
                round.next_with(next);
            });
            flow.output_with(out_refs);
        },
        Context(0),
    ));
    let error = outcome
        .err()
        .expect("the inner retry exhaustion is terminal");
    match &error {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!(
                (*max_retries, *attempts),
                (1, 2),
                "the inner retry's frozen budget fields survive the outer iter"
            );
            let reason = Error::source(last_error).expect("dynamic reason reachable");
            assert!(
                reason.downcast_ref::<NotReady>().is_some(),
                "the frozen last_error still points at the original reason"
            );
        }
        other => panic!("expected RetryExhausted, got {other}"),
    }

    let rounds = observe::take_controller_rounds();
    assert_eq!(
        iter_ats(&rounds),
        ["controller-created", "round-created", "round-failed"],
        "the outer iter treats the budget error as a terminal round failure: {rounds:?}"
    );
    assert!(
        rounds.iter().all(|record| record.kind != "iter"
            || (record.at != "round-caught"
                && record.at != "round-promoted"
                && record.at != "budget-exhausted")),
        "the iter never treats the budget error as IterBreak nor promotes: {rounds:?}"
    );
    assert_eq!(
        retry_ats(&rounds),
        [
            "controller-created",
            "round-created",
            "round-caught",
            "round-created",
            "round-caught",
            "budget-exhausted",
        ],
        "the inner retry exhausts after two attempts: {rounds:?}"
    );
    assert_eq!(
        bump_calls(),
        0,
        "the next Node after the failed retry never runs"
    );
    observe::reset();
}

// E12：IterBreak 从 Choose 分支内的 Retry 跨 Choose 边界传播，仍在 Root 形成
// UnhandledControl；Retry 只 round-propagated，不重试。
#[test]
fn e12_iter_break_crosses_choose_to_root() {
    observe::reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, prompt: Ref<Prompt>| {
            let selector = flow.then_with_output(&to_selector, prompt);
            let out_refs: Ref<Report> = flow.choose_with_output(selector, |choice| {
                choice.case(1u32, |branch| {
                    let out_refs: Ref<Report> = branch.retry_output(2, |attempt| {
                        let candidate = attempt.then_with_output(&generate, prompt);
                        let out_refs = attempt.then_with_output(&breaking, candidate);
                        attempt.output_with(out_refs);
                    });
                    branch.output_with(out_refs);
                });
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    let error = outcome.err().expect("the iter break reaches the root");
    assert!(
        matches!(
            &error,
            RunError::UnhandledControl(ControlSignal::IterBreak(_))
        ),
        "expected UnhandledControl(IterBreak), got {error}"
    );
    let rounds = observe::take_controller_rounds();
    assert_eq!(
        retry_ats(&rounds),
        ["controller-created", "round-created", "round-propagated"],
        "the nested retry propagates the break: {rounds:?}"
    );
    observe::reset();
}

// E25：Retry 预算错误沿 Choose 分支边界传播为 terminal；冻结字段保留（选择信号未
// 被 Choose 捕获为控制，也没有伪造业务错误）。
#[test]
fn e25_budget_error_propagates_through_choose_as_terminal() {
    observe::reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, prompt: Ref<Prompt>| {
            let selector = flow.then_with_output(&to_selector, prompt);
            let out_refs: Ref<Report> = flow.choose_with_output(selector, |choice| {
                choice.case(1u32, |branch| {
                    let out_refs: Ref<Report> = branch.retry_output(1, |inner| {
                        let candidate = inner.then_with_output(&generate, prompt);
                        let out_refs = inner.then_with_output(&judge_always_retrying, candidate);
                        inner.output_with(out_refs);
                    });
                    branch.output_with(out_refs);
                });
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    let error = outcome.err().expect("the budget error is terminal");
    match &error {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!((*max_retries, *attempts), (1, 2));
            let reason = Error::source(last_error).expect("dynamic reason reachable");
            assert!(reason.downcast_ref::<NotReady>().is_some());
        }
        other => panic!("expected RetryExhausted, got {other}"),
    }

    let selections = observe::take_choose_selections();
    assert_eq!(selections.len(), 1, "one choose decision: {selections:?}");
    assert_eq!(
        selections[0].selected,
        Some(0),
        "the matching case actually ran: {selections:?}"
    );
    let rounds = observe::take_controller_rounds();
    assert_eq!(
        retry_ats(&rounds),
        [
            "controller-created",
            "round-created",
            "round-caught",
            "round-created",
            "round-caught",
            "budget-exhausted",
        ],
        "the nested retry exhausts and the error crosses the choose branch: {rounds:?}"
    );
    observe::reset();
}

// ---------------------------------------------------------------- E11 不驱动自动 Retry

// E11：attempt 内返回不可恢复业务 Failure → RunError::Body；只执行一次 attempt，后续不再重试。
#[test]
fn e11_business_failure_does_not_drive_retry() {
    observe::reset();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: Ref<Report> = flow.retry_output(3, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                let out_refs = attempt.then_with_output(&judge_failing, candidate);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    let error = outcome.err().expect("a business failure is terminal");
    let RunError::Body(failure) = &error else {
        panic!("expected Body, got {error}");
    };
    let business = Error::source(failure).expect("original business object reachable");
    assert!(
        business.downcast_ref::<Rejected>().is_some(),
        "the original Failure object is preserved"
    );
    assert!(
        failure.cleanup_source().is_none(),
        "no cleanup fault was installed for this sample"
    );

    let rounds = observe::take_controller_rounds();
    assert_eq!(
        retry_ats(&rounds),
        ["controller-created", "round-created", "round-failed"],
        "exactly one attempt runs and no retry is driven: {rounds:?}"
    );
    observe::reset();
}

// ---------------------------------------------------------------- E29 Pending 取消

// E29：Retry 内含真实 Pending —— 首次 poll 返回 Pending 后整体取消；借用 Future 先 Drop，
// 各层业务实例各释放一次，未 poll 的后续 Node 不执行。
#[test]
fn e29_retry_nested_pending_cancel_releases_every_layer_once() {
    observe::reset();
    reset_counters();
    let log = Rc::new(RefCell::new(Vec::new()));
    let runtime = Runtime::new();
    let mut future = Box::pin(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, seed: Ref<Seed>| {
            let out_refs: Ref<Report> = flow.retry_output(3, |attempt| {
                let _temp = attempt.then_with_output(&track_temp, seed);
                let pending = attempt.then_with_output(&pending_guard_node, seed);
                let out_refs = attempt.then_with_output(&after_pending, pending);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Seed {
            value: 1,
            log: Rc::clone(&log),
        },
    ));

    let waker = futures::task::noop_waker();
    let mut cx = TaskContext::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
    assert_eq!(
        after_calls(),
        0,
        "the node after the pending one is not polled yet"
    );
    drop(future);

    assert_eq!(
        log.borrow().clone(),
        vec![
            "run:temp".to_string(),
            "drop:pending-future".to_string(),
            "drop:temp".to_string(),
            "drop:seed".to_string(),
        ],
        "borrowed future ends first, then the round temporary, then the root input (each once)"
    );
    assert_eq!(after_calls(), 0, "cancellation runs no later node");
    observe::reset();
}

// ---------------------------------------------------------------- E26 耗尽叠加清理故障

/// 真实清理故障的统一诊断文本（`ScopeError::Invariant`）。
const CLEANUP_INVARIANT: &str =
    "internal invariant violated: owned entry must exist until its scope closes";

/// 真实 Root 帧的 Scope 身份（只读观察）。
fn root_frame_scope() -> crate::core::identity::ScopeId {
    observe::take_frames()
        .into_iter()
        .find(|frame| frame.kind == "root" && frame.phase == "enter")
        .expect("the root frame is recorded")
        .scope
}

/// 取出本样本唯一 Chain 的真实 child Scope。
fn chain_child_scope() -> crate::core::identity::ScopeId {
    observe::take_chain_creations()
        .into_iter()
        .next()
        .expect("the chain child is recorded")
        .child
}

// E26：Retry 实际耗尽 + 真实 Root 清理故障 → SPEC §12 的 RunError::Runtime；其 source 下转型
// 回 RunError::RetryExhausted（字段完整、last_error 源链保留），cleanup_source 有文本。
#[test]
fn e26_retry_exhaustion_with_cleanup_fault_becomes_runtime() {
    observe::reset();
    install_root_cleanup_faults(1); // 销毁第一个 Root 输入 Filler；业务只读 Prompt。
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>,
         (_filler, prompt): (Ref<Filler>, Ref<Prompt>)| {
            let out_refs: Ref<Report> = flow.retry_output(1, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                let out_refs = attempt.then_with_output(&judge_always_retrying, candidate);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        (Filler(0), Prompt(0)),
    ));
    let error = outcome.err().expect("exhaustion is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "budget exhaustion ended with a cleanup failure"
    );
    let source = Error::source(runtime_error).expect("the original budget classification survives");
    let frozen = source
        .downcast_ref::<RunError>()
        .expect("source is the frozen RunError");
    match frozen {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!((*max_retries, *attempts), (1, 2));
            let reason = Error::source(last_error).expect("dynamic reason reachable");
            assert!(reason.downcast_ref::<NotReady>().is_some());
        }
        other => panic!("expected the frozen RetryExhausted, got {other}"),
    }
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(
        cleanup.to_string(),
        CLEANUP_INVARIANT,
        "the first cleanup diagnostic keeps its exact text, not a placeholder"
    );
    let root_scope = root_frame_scope();
    let cleanup_failures = crate::core::test_support::take_cleanup_failures();
    assert_eq!(
        cleanup_failures.len(),
        1,
        "the failing Root is the single cleanup scope here: {cleanup_failures:?}"
    );
    assert_eq!(
        cleanup_failures[0].scope, root_scope,
        "the first cleanup diagnostic is at the real RootScope"
    );
    assert!(cleanup_failures[0].stored_first);
    match &cleanup_failures[0].error {
        crate::core::internal_error::ScopeError::Invariant { violated } => assert_eq!(
            *violated, "owned entry must exist until its scope closes",
            "the stored diagnostic is the real cleanup ScopeError"
        ),
        other => panic!("expected the real Invariant cleanup diagnostic, got {other:?}"),
    }
    assert_eq!(
        cleanup.to_string(),
        cleanup_failures[0].error.to_string(),
        "the public cleanup_source is exactly the first registered diagnostic"
    );
    observe::reset();
}

// E26（F01 后项不覆盖）：RetryExhausted 是原终止原因；先发生的子 Scope 清理失败是“首清理诊断”，
// 其后的 Root 清理失败只登记、不得覆盖第一条。
#[test]
fn e26_retry_exhaustion_keeps_first_cleanup_over_later_scopes() {
    observe::reset();
    install_boundary_cleanup_fault();
    install_root_cleanup_faults(1);
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>,
         (_filler, prompt): (Ref<Filler>, Ref<Prompt>)| {
            let out_refs: Ref<Report> = flow.chain_output(|sub| {
                let _produced = sub.then_with_output(&generate, prompt);
                let t04_out_refs: Ref<Report> = sub.retry_output(1, |inner| {
                    let candidate = inner.then_with_output(&generate, prompt);
                    let out_refs = inner.then_with_output(&judge_always_retrying, candidate);
                    inner.output_with(out_refs);
                });
                sub.output_with(t04_out_refs);
            });
            flow.output_with(out_refs);
        },
        (Filler(0), Prompt(0)),
    ));
    let error = outcome.err().expect("the budget error is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(
        runtime_error.note(),
        "budget exhaustion ended with a cleanup failure"
    );
    let source = Error::source(runtime_error).expect("the budget classification survives");
    let frozen = source
        .downcast_ref::<RunError>()
        .expect("source is the frozen RunError");
    match frozen {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!((*max_retries, *attempts), (1, 2));
            let reason = Error::source(last_error).expect("dynamic reason reachable");
            assert!(reason.downcast_ref::<NotReady>().is_some());
        }
        other => panic!("expected the frozen RetryExhausted, got {other}"),
    }
    let cleanup = runtime_error
        .cleanup_source()
        .expect("the first cleanup diagnostic is preserved");
    assert_eq!(cleanup.to_string(), CLEANUP_INVARIANT);
    let chain_scope = chain_child_scope();
    let root_scope = root_frame_scope();
    let cleanup_failures = crate::core::test_support::take_cleanup_failures();
    assert_eq!(
        cleanup_failures
            .iter()
            .map(|record| (record.scope.clone(), record.stored_first))
            .collect::<Vec<_>>(),
        vec![(chain_scope, true), (root_scope, false)],
        "the child's cleanup is first and stored; the later Root failure only registers: {cleanup_failures:?}"
    );
    assert_eq!(cleanup.to_string(), cleanup_failures[0].error.to_string());
    observe::reset();
}

// E27（F01 第二条 Control 故障）：Chain 控制退出经 `exit_control_boundary`，其清理故障 → Runtime；
// 首清理诊断是真实 Chain child，后续 round／controller／root 只登记不覆盖；不进入新 attempt。
#[test]
fn e27c_chain_control_cleanup_fault_keeps_first_diagnostic() {
    observe::reset();
    install_control_cleanup_fault();
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut crate::core::engine::flow::Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: Ref<Report> = flow.retry_output(3, |attempt| {
                let t04_out_refs: Ref<Report> = attempt.chain_output(|sub| {
                    let candidate = sub.then_with_output(&generate, prompt);
                    let out_refs = sub.then_with_output(&breaking, candidate);
                    sub.output_with(out_refs);
                });
                attempt.output_with(t04_out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ));
    let error = outcome.err().expect("the cleanup fault is terminal");
    let RunError::Runtime(runtime_error) = &error else {
        panic!("expected Runtime, got {error}");
    };
    assert_eq!(runtime_error.note(), "control exit cleanup failed");
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
    assert_eq!(cleanup.to_string(), CLEANUP_INVARIANT);
    let chain_scope = chain_child_scope();
    let cleanup_failures = crate::core::test_support::take_cleanup_failures();
    assert_eq!(
        cleanup_failures[0].scope, chain_scope,
        "the first cleanup diagnostic is the chain child, at its real scope: {cleanup_failures:?}"
    );
    assert!(cleanup_failures[0].stored_first);
    assert!(
        cleanup_failures
            .iter()
            .skip(1)
            .all(|record| !record.stored_first),
        "later round／controller／root cleanup failures never overwrite the first: {cleanup_failures:?}"
    );
    assert_eq!(cleanup.to_string(), cleanup_failures[0].error.to_string());
    let rounds = observe::take_controller_rounds();
    assert_eq!(
        retry_ats(&rounds),
        ["controller-created", "round-created", "round-failed"],
        "the control cleanup fault is terminal, no further attempt: {rounds:?}"
    );
    observe::reset();
}
