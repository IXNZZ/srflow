//! Retry／Iter 运行期示例：预算重试、状态推进与 IterBreak 交付。
//!
//! 运行：`cargo run --offline --example retry_iter_runtime`
//!
//! 覆盖三条真实路径：
//! - `Flow::retry`：judge 依次 Retry／Retry／Normal，成功一次交付；预算耗尽返回
//!   `RunError::RetryExhausted`（字段为配置次数、实际尝试次数与最后动态原因）。
//! - `Flow::iter`：第二轮 judge 返回 `IterBreak`，交付**进入本轮时的 current**（不是本轮
//!   已产生但未接受的 next）。
//! - `Flow::iter`：第 N 轮仍 Continue 时返回 `RunError::IterationLimitReached`。

use futures::executor::block_on;
use srflow::Ref;
use srflow::{BodyError, Data, Flow, Query, RunError, Runtime};
use std::error::Error;
use std::fmt;

thread_local! {
    /// 每次 attempt 的候选序号（真实生成节点会随尝试取样，这里用计数器等价表达）。
    static ATTEMPT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// 生成节点：每次 attempt 产生新的候选值。
async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    let base = query.get().0;
    let attempt = ATTEMPT.with(|counter| {
        let value = counter.get() + 1;
        counter.set(value);
        value
    });
    Ok(Candidate(base + attempt))
}

/// 评审节点：按剩余重试次数决定 Retry 还是 Normal。
async fn judge_retrying(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let value = query.get().0;
    if value < 3 {
        Err(BodyError::retry(NotReady { value }))
    } else {
        Ok(Report(value * 10))
    }
}

/// 评审节点：始终要求重试（用于预算耗尽路径）。
async fn judge_always_retrying(query: Query<&Candidate>) -> Result<Report, BodyError> {
    Err(BodyError::retry(NotReady {
        value: query.get().0,
    }))
}

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

/// 明确重试理由：携带动态原因，不被字符串化重建。
#[derive(Debug)]
struct NotReady {
    value: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate {} is not ready", self.value)
    }
}

impl Error for NotReady {}

/// 每轮把 draft 追加一个字符。
async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    let Draft(text) = query.get();
    Ok(Draft(format!("{text}+")))
}

/// 每轮把 context 计数加一。
async fn bump(query: Query<&Context>) -> Result<Context, BodyError> {
    Ok(Context(query.get().0 + 1))
}

/// 第二轮返回 `IterBreak`：交付进入该轮时的 current。
async fn judge_until_break(query: Query<&Context>) -> Result<Report, BodyError> {
    let context = query.get();
    if context.0 >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Report(context.0))
    }
}

/// 永远 Continue：用于轮次上限路径。
async fn judge_always_continue(query: Query<&Context>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0))
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = Runtime::new();

    // 1) Retry：Retry／Retry／Normal —— 成功只交付一次。
    let report = block_on(runtime.execute::<_, (Report, Candidate), _>(
        |flow: &mut Flow<'_>, prompt: srflow::Ref<Prompt>| {
            let out_refs: (Ref<Report>, Ref<Candidate>) = flow.retry_output(3, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                let out_refs = (
                    attempt.then_with_output(&judge_retrying, candidate),
                    candidate,
                );
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ))?;
    let (report, candidate) = report;
    assert_eq!((report.0, candidate.0), (30, 3));
    println!(
        "retry success: report={} candidate={}",
        report.0, candidate.0
    );

    // 2) Retry 预算耗尽：最后原因与字段完整保留。
    let error = match block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: srflow::Ref<Prompt>| {
            let out_refs: Ref<Report> = flow.retry_output(1, |attempt| {
                let candidate = attempt.then_with_output(&generate, prompt);
                let out_refs = attempt.then_with_output(&judge_always_retrying, candidate);
                attempt.output_with(out_refs);
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    )) {
        Ok(_) => panic!("budget exhaustion must fail"),
        Err(error) => error,
    };
    match &error {
        RunError::RetryExhausted {
            max_retries,
            attempts,
            last_error,
        } => {
            assert_eq!((*max_retries, *attempts), (1, 2));
            let reason = last_error
                .source()
                .expect("the dynamic retry reason stays reachable");
            assert!(reason.to_string().contains("not ready"));
            println!("retry exhausted: attempts={attempts} reason={reason}");
        }
        other => panic!("expected RetryExhausted, got {other}"),
    }

    // 3) Iter：第二轮 Break —— 交付进入本轮时的 current（draft 一个加号、context 为 1）。
    let (draft, context) = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut Flow<'_>, (draft, context)| {
            let out_refs = flow.iter_output((draft, context), 5, |round| {
                let (current_draft, current_context) = round.current::<(Draft, Context)>();
                round.then_with_output(&judge_until_break, current_context);
                let next_draft = round.then_with_output(&revise, current_draft);
                let next_context = round.then_with_output(&bump, current_context);
                round.next_with((next_draft, next_context));
            });
            flow.output_with(out_refs);
        },
        (Draft(String::from("s0")), Context(0)),
    ))?;
    assert_eq!((draft.0.as_str(), context.0), ("s0+", 1));
    println!("iter break: draft={} context={}", draft.0, context.0);

    // 4) Iter 轮次上限：第 N 轮仍 Continue。
    let error = match block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut Flow<'_>, (draft, context)| {
            let out_refs = flow.iter_output((draft, context), 2, |round| {
                let (current_draft, current_context) = round.current::<(Draft, Context)>();
                round.then_with_output(&judge_always_continue, current_context);
                let next_draft = round.then_with_output(&revise, current_draft);
                let next_context = round.then_with_output(&bump, current_context);
                round.next_with((next_draft, next_context));
            });
            flow.output_with(out_refs);
        },
        (Draft(String::from("x")), Context(0)),
    )) {
        Ok(_) => panic!("round limit must fail"),
        Err(error) => error,
    };
    match &error {
        RunError::IterationLimitReached {
            max_iterations,
            completed_iterations,
        } => {
            assert_eq!((*max_iterations, *completed_iterations), (2, 2));
            println!("iter limit: completed={completed_iterations}");
        }
        other => panic!("expected IterationLimitReached, got {other}"),
    }

    Ok(())
}
