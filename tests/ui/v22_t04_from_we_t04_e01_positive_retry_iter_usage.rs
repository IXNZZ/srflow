//! T03 迁移自 `we_t04_e01_positive_retry_iter_usage.rs`（WE-T04 E01 正例）：冻结的
//! `retry`／`iter` 用法在**独立消费者**中真实运行。新契约下 Root 输出改由
//! [`Flow::output_with`] 声明（定义闭包固定返回 `()`），child 显式连接更名为
//! `then_with_output`；原断言、计数与运行语义保持不变。
//!
//! 冻结形态（任务书 §1、SPEC §10／§11）：
//!
//! ```text
//! flow.retry(3, |attempt| { … attempt.then_with_output(…) … })         // attempt 是完整 &mut Flow<'n>
//! flow.iter(state, 5, |state, round| { … round.then_with_output(…) … }) // round 是完整 &mut Flow<'n>
//! ```
//!
//! 覆盖：
//!
//! 1. Retry 2 元 flat tuple 输出（`(Report, Candidate)`），`Retry／Retry／Normal` 成功一次交付；
//! 2. Iter 2 元 flat tuple State，第二轮 `IterBreak` 交付**进入本轮时的 current**；
//! 3. 嵌套：attempt 内 `chain` + 内层 `choose`；Fragment（普通函数返回闭包）作为 retry 定义；
//!    公开方法名不按 arity 分类。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t04-positive-ok`。

use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};
use std::cell::Cell;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::task::{Context as TaskContext, Poll, Waker};

/// 最小同步执行器：`Runtime::execute` 的 Future 在首次 poll 即就绪，故用 noop Waker 轮询即可；
/// 由此夹具不依赖 `futures`，与 `ui_check.py` 的 rustc 装配一致。
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = TaskContext::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[derive(Data)]
struct Prompt(u32);
#[derive(Data)]
struct Candidate(u32);
#[derive(Data)]
struct Report(u32);
#[derive(Data)]
struct Draft(u32);
#[derive(Data)]
struct Context(u32);
#[derive(Data, PartialEq)]
struct Route(u32);

thread_local! {
    /// 每次 attempt 的候选序号：真实生成节点随尝试取样。
    static ATTEMPT: Cell<u32> = const { Cell::new(0) };
}

/// 动态重试理由：携带可被 `source` 取回的原因，不被字符串化重建。
#[derive(Debug)]
struct NotReady(u32);

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate {} is not ready", self.0)
    }
}

impl Error for NotReady {}

/// 每次 attempt 产生新的候选值。
async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    let base = query.get().0;
    let attempt = ATTEMPT.with(|counter| {
        let value = counter.get() + 1;
        counter.set(value);
        value
    });
    Ok(Candidate(base + attempt))
}

/// 评审：候选未达阈值则 Retry，否则 Normal。
async fn judge_retrying(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let value = query.get().0;
    if value < 3 {
        Err(BodyError::retry(NotReady(value)))
    } else {
        Ok(Report(value * 10))
    }
}

/// 每轮推进 draft。
async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    Ok(Draft(query.get().0 + 1))
}

/// 每轮推进 context。
async fn bump(query: Query<&Context>) -> Result<Context, BodyError> {
    Ok(Context(query.get().0 + 1))
}

/// 第二轮返回 `IterBreak`：交付进入该轮时的 current。
async fn judge_until_break(query: Query<&Context>) -> Result<Report, BodyError> {
    let value = query.get().0;
    if value >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Report(value))
    }
}

async fn route_of(query: Query<&Prompt>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 2))
}

async fn pick_positive(query: Query<&Candidate>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0))
}

async fn pick_negative(_query: Query<&Candidate>) -> Result<Report, BodyError> {
    Ok(Report(0))
}

/// Fragment：普通函数返回的闭包可直接作为 `retry` 的定义（不引入内部 Signature 类型）。
fn retry_fragment<'n>(
    prompt: Ref<Prompt>,
) -> impl FnOnce(&mut Flow<'n>) + 'n {
    move |attempt| {
        let candidate = attempt.then_with_output(&generate, prompt);
        let declared = (attempt.then_with_output(&judge_retrying, candidate), candidate);
        attempt.output_with(declared);
    }
}

fn main() {
    let runtime = Runtime::new();

    // 1) 冻结 retry 用法：2 元输出，Retry／Retry／Normal 成功只交付一次。
    ATTEMPT.with(|counter| counter.set(0));
    let (report, candidate) = block_on(runtime.execute::<_, (Report, Candidate), _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: (Ref<Report>, Ref<Candidate>) = flow.retry_output(3, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                let declared = (attempt.then_with_output(&judge_retrying, candidate), candidate);
                attempt.output_with(declared);
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ))
    .expect("frozen retry run");
    assert_eq!(
        (report.0, candidate.0),
        (30, 3),
        "three attempts, success delivered once"
    );

    // 2) Fragment 作为 retry 定义。
    ATTEMPT.with(|counter| counter.set(0));
    let (report, candidate) = block_on(runtime.execute::<_, (Report, Candidate), _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: (Ref<Report>, Ref<Candidate>) = flow.retry_output(3, retry_fragment(prompt));
            flow.output_with(out_refs);
        },
        Prompt(10),
    ))
    .expect("fragment retry run");
    assert_eq!(
        (report.0, candidate.0),
        (110, 11),
        "fragment-defined retry attempts reuse the original dependency"
    );

    // 3) 冻结 iter 用法：2 元 State，第二轮 Break 交付进入本轮时的 current。
    let (draft, context) = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut Flow<'_>, (draft, context)| {
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
        (Draft(0), Context(0)),
    ))
    .expect("frozen iter run");
    assert_eq!(
        (draft.0, context.0),
        (1, 1),
        "IterBreak delivers the entry current of that round"
    );

    // 4) 嵌套：attempt 内 chain + 内层 choose（未选分支不执行）。
    ATTEMPT.with(|counter| counter.set(0));
    let (report, candidate) = block_on(runtime.execute::<_, (Report, Candidate), _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs: (Ref<Report>, Ref<Candidate>) = flow.retry_output(2, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                let _ = attempt.then_with_output(&judge_retrying, candidate);
                let selector = attempt.then_with_output(route_of, prompt);
                let selected = attempt.choose(selector, |choice| {
                    choice.case(Route(0), |branch| {
                        branch.then_with_output(pick_positive, candidate)
                    });
                    choice.otherwise(|branch| branch.then_with_output(pick_negative, candidate));
                });
                // 内层 chain：把 attempt 域的选择结果声明为该 child 的输出。
                let selected_refs: Ref<Report> = attempt.chain_output(|inner| {
                    inner.output_with(selected);
                });
                let attempt_refs = (selected_refs, candidate);
                attempt.output_with(attempt_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ))
    .expect("nested retry + chain + choose run");
    assert_eq!(
        (report.0, candidate.0),
        (3, 3),
        "nested chain/choose inside a retry attempt"
    );

    println!("we_t04-positive-ok");
}
