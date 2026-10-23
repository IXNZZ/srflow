//! V22-T03 夹具迁移（源自 WE_T06 `we_t06_p06_retry.rs`）：Retry（SPEC §10）。
//!
//! 原目的（X24/S06／C13／C15）：`max_retries` 语义（首次 attempt 外最多再 N 次）、重试单位是
//! 完整 attempt body 且复用原始 ancestor 依赖、成功后只交付一次，以及 `max_retries = 0` 仍执行
//! 首次 attempt；断言 `RetryExhausted{max_retries:1, attempts:2}` 且最后动态原因经 `Error::source` 可达。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)`（含 attempt 闭包内两次调用）改为 `then_with_output`；
//! 只有 Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out = Report`。Retry 的 attempt
//! 协议与错误断言不变。

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Prompt(u32);

#[derive(Data)]
struct Candidate(u32);

#[derive(Data)]
struct Report(u32);

/// attempt 序号（每次 generate 取样一次）。
static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
/// generate 的真实调用次数（完整 attempt 重启的证据）。
static GENERATED: AtomicUsize = AtomicUsize::new(0);

/// 动态 Retry 原因：保留动态对象，不被字符串化重建。
#[derive(Debug)]
struct NotReady {
    candidate: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "candidate {} is not ready", self.candidate)
    }
}

impl Error for NotReady {}

async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    GENERATED.fetch_add(1, Ordering::SeqCst);
    let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
    Ok(Candidate(query.get().0 + attempt as u32))
}

/// 前两次 attempt 要求重试，第三次成功。
async fn judge(query: Query<&Candidate>) -> Result<Report, BodyError> {
    let candidate = query.get().0;
    if candidate < 3 {
        Err(BodyError::retry(NotReady { candidate }))
    } else {
        Ok(Report(candidate * 10))
    }
}

async fn always_retry(query: Query<&Candidate>) -> Result<Report, BodyError> {
    Err(BodyError::retry(NotReady {
        candidate: query.get().0,
    }))
}

fn main() {
    let runtime = Runtime::new();

    // 1) 两次 Retry 后成功：完整 attempt 重启，只交付一次。
    ATTEMPTS.store(0, Ordering::SeqCst);
    GENERATED.store(0, Ordering::SeqCst);
    let report = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(3, |attempt| {
                let candidate = attempt.then_with_output(generate, prompt);
                attempt.then_with_output(judge, candidate)
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ))
    .expect("retry succeeds within budget");
    assert_eq!(report.0, 30, "第三次 attempt 的候选 3 交付");
    assert_eq!(GENERATED.load(Ordering::SeqCst), 3, "完整 attempt 重启三次");

    // 2) max_retries = 0：仍执行首次 attempt（成功路径）。
    ATTEMPTS.store(9, Ordering::SeqCst);
    GENERATED.store(0, Ordering::SeqCst);
    let zero = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(0, |attempt| {
                let candidate = attempt.then_with_output(generate, prompt);
                attempt.then_with_output(judge, candidate)
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ))
    .expect("max_retries = 0 still runs the first attempt");
    assert_eq!(zero.0, 100, "候选 10 直接成功");
    assert_eq!(GENERATED.load(Ordering::SeqCst), 1);

    // 3) 预算耗尽：字段保留配置次数、实际尝试次数与最后动态原因。
    ATTEMPTS.store(0, Ordering::SeqCst);
    let error = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, prompt: Ref<Prompt>| {
            let out_refs = flow.retry(1, |attempt| {
                let candidate = attempt.then_with_output(generate, prompt);
                attempt.then_with_output(always_retry, candidate)
            });
            flow.output_with(out_refs);
        },
        Prompt(0),
    ))
    .err()
    .expect("budget exhaustion fails");
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
            assert!(reason.to_string().contains("not ready"), "reason={reason}");
        }
        other => panic!("expected RetryExhausted, got {other:?}"),
    }

    println!("we_t06 p06 report={} zero={}", report.0, zero.0);
    println!("we_t06-positive-ok");
}
