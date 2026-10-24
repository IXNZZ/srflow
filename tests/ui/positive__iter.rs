//! V22-T03 夹具迁移（源自 WE_T06 `we_t06_p07_iter.rs`）：Iter（SPEC §11）。
//!
//! 原目的（X24/S07／C13／C15）：flat tuple StateShape 的整轮推进、最近 Iter 捕获 `IterBreak`
//! 后交付**进入本轮时的 current**（而不是本轮已产生的 next），以及第 N 轮仍 Continue 时返回
//! `RunError::IterationLimitReached{2,2}`（不额外执行一次判断）。
//!
//! 迁移手法：旧两参 `round.then(node, refs)` 改为 `then_with_output`；只有 Root 闭包改为固定
//! 返回 `()` 并经 `output_with` 声明 owned `Out = (Draft, Context)`。Iter 的 StateShape 契约、
//! Break 交付语义与计数断言不变。
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Draft(String);

#[derive(Data)]
struct Context(u32);

#[derive(Data)]
struct Report(u32);

static JUDGED: AtomicUsize = AtomicUsize::new(0);

/// 每轮把 draft 追加一个加号（产生 next）。
async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    Ok(Draft(format!("{}+", query.get().0)))
}

/// 每轮把 context 计数加一（产生 next）。
async fn bump(query: Query<&Context>) -> Result<Context, BodyError> {
    Ok(Context(query.get().0 + 1))
}

/// 进入第二轮时返回 `IterBreak`：交付进入该轮时的 current。
async fn judge_until_break(query: Query<&Context>) -> Result<Report, BodyError> {
    JUDGED.fetch_add(1, Ordering::SeqCst);
    let context = query.get().0;
    if context >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(Report(context))
    }
}

/// 永远 Continue：用于轮次上限路径。
async fn judge_always_continue(query: Query<&Context>) -> Result<Report, BodyError> {
    JUDGED.fetch_add(1, Ordering::SeqCst);
    Ok(Report(query.get().0))
}

fn main() {
    let runtime = Runtime::new();

    // 1) 第二轮 Break：交付进入该轮的 current（draft 一个加号、context = 1）。
    JUDGED.store(0, Ordering::SeqCst);
    let (draft, context) = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut Flow<'_>, (draft, context): (Ref<Draft>, Ref<Context>)| {
            let out_refs = flow.iter_output((draft, context), 5, |round| {
                let (current_draft, current_context) = round.current::<(Draft, Context)>();
                round.then_with_output(judge_until_break, current_context);
                let next_draft = round.then_with_output(revise, current_draft);
                let next_context = round.then_with_output(bump, current_context);
                round.next_with((next_draft, next_context));
            });
            flow.output_with(out_refs);
        },
        (Draft(String::from("s0")), Context(0)),
    ))
    .expect("iter break runs");
    assert_eq!((draft.0.as_str(), context.0), ("s0+", 1));
    assert_eq!(JUDGED.load(Ordering::SeqCst), 2, "两轮各判断一次");

    // 2) 轮次上限：第 N 轮仍 Continue，不额外执行一次判断、不交付最后 next。
    JUDGED.store(0, Ordering::SeqCst);
    let error = block_on(runtime.execute::<_, (Draft, Context), _>(
        |flow: &mut Flow<'_>, (draft, context): (Ref<Draft>, Ref<Context>)| {
            let out_refs = flow.iter_output((draft, context), 2, |round| {
                let (current_draft, current_context) = round.current::<(Draft, Context)>();
                round.then_with_output(judge_always_continue, current_context);
                let next_draft = round.then_with_output(revise, current_draft);
                let next_context = round.then_with_output(bump, current_context);
                round.next_with((next_draft, next_context));
            });
            flow.output_with(out_refs);
        },
        (Draft(String::from("x")), Context(0)),
    ))
    .err()
    .expect("round limit must fail");
    match &error {
        RunError::IterationLimitReached {
            max_iterations,
            completed_iterations,
        } => assert_eq!((*max_iterations, *completed_iterations), (2, 2)),
        other => panic!("expected IterationLimitReached, got {other:?}"),
    }
    assert_eq!(
        JUDGED.load(Ordering::SeqCst),
        2,
        "两轮恰好两次判断，不额外一次"
    );

    println!("we_t06 p07 draft={} context={}", draft.0, context.0);
    println!("we_t06-positive-ok");
}
