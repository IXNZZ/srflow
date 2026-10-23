//! V22-T03 夹具迁移（源自 WE_T06 `we_t06_p08_six_ops_composition.rs`）：六操作完整组合（SPEC §12）。
//!
//! 原目的（X24/S08／C13／C15）：在**一次真实执行**中同时包含 `then`／`chain`／`each`／`choose`／
//! `retry`／`iter`，并有真实的重试（非零）、逐项聚合（多输出）与 IterBreak；断言 `picked=81`、
//! `marks=[30,40]`、`draft="d3+"`、`context=81`、`attempts=2`、mark 调用 2 次、Break 1 次。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)`（含 chain／retry／choose／each／iter 内部）
//! 全部改为 `then_with_output`；只有 Root 闭包改为固定返回 `()`，原四元组尾表达式求值一次后
//! 交给 `output_with`，并显式写 `execute::<_, (Amount, Vec<Mark>, Draft, Report), _>`。

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};

#[derive(Data)]
struct Seed {
    value: u32,
}

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Report(u32);

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Mark(u32);

#[derive(Data)]
struct Draft(String);

/// Choose 键：只声明 `Data` + 手写 `PartialEq`。
#[derive(Data)]
struct Route(u32);

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[derive(Debug)]
struct NotReady {
    value: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "value {} is not ready", self.value)
    }
}

impl Error for NotReady {}

static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static MARKED: AtomicUsize = AtomicUsize::new(0);
static BROKEN: AtomicUsize = AtomicUsize::new(0);

async fn amount_of(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value))
}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

async fn plus_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

/// 第一次 attempt 要求重试，第二次成功。
async fn judge_retry_once(query: Query<&Amount>) -> Result<Report, BodyError> {
    let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
    let value = query.get().0;
    if attempt == 1 {
        Err(BodyError::retry(NotReady { value }))
    } else {
        Ok(Report(value * 10))
    }
}

async fn route_of(query: Query<&Report>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 2))
}

async fn label_even(query: Query<&Report>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn label_odd(query: Query<&Report>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 2))
}

async fn items_of(query: Query<&Seed>) -> Result<Vec<Item>, BodyError> {
    let value = query.get().value;
    Ok(vec![Item(value), Item(value + 1)])
}

async fn mark_of(query: Query<&Item>) -> Result<Mark, BodyError> {
    MARKED.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().0 * 10))
}

async fn draft_of(query: Query<&Seed>) -> Result<Draft, BodyError> {
    Ok(Draft(format!("d{}", query.get().value)))
}

async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    Ok(Draft(format!("{}+", query.get().0)))
}

async fn bump(query: Query<&Report>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0 + 1))
}

/// 第二轮（draft 已含 `+`）返回 `IterBreak`，交付进入该轮的 current。
async fn judge_break(query: Query<&Draft>) -> Result<Report, BodyError> {
    let draft = &query.get().0;
    if draft.contains('+') {
        BROKEN.fetch_add(1, Ordering::SeqCst);
        Err(BodyError::iter_break())
    } else {
        Ok(Report(draft.len() as u32))
    }
}

fn main() {
    let runtime = Runtime::new();
    ATTEMPTS.store(0, Ordering::SeqCst);
    MARKED.store(0, Ordering::SeqCst);
    BROKEN.store(0, Ordering::SeqCst);

    let output = block_on(runtime.execute::<_, (Amount, Vec<Mark>, Draft, Report), _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            // then
            let amount = flow.then_with_output(amount_of, seed);
            // chain（祖先捕获 + child 边界）
            let grown = flow.chain(|sub| {
                let doubled = sub.then_with_output(double, amount);
                sub.then_with_output(plus_one, doubled)
            });
            // retry（真实重启一次）
            let report = flow.retry(2, |attempt| {
                let candidate = attempt.then_with_output(plus_one, grown);
                attempt.then_with_output(judge_retry_once, candidate)
            });
            // choose（由上游选择键决定，只跑一支）
            let selector = flow.then_with_output(route_of, report);
            let picked = flow.choose(selector, |choice| {
                choice.case(Route(0), |branch| branch.then_with_output(label_even, report));
                choice.otherwise(|branch| branch.then_with_output(label_odd, report));
            });
            // each（逐项聚合到 Vec）
            let items = flow.then_with_output(items_of, seed);
            let marks = flow.each(items, |item, each| each.then_with_output(mark_of, item));
            // iter（第二轮 Break 交付 current）
            let initial_draft = flow.then_with_output(draft_of, seed);
            let (draft, context) = flow.iter(
                (initial_draft, report),
                4,
                |(current_draft, current_context), round| {
                    round.then_with_output(judge_break, current_draft);
                    let next_draft = round.then_with_output(revise, current_draft);
                    let next_context = round.then_with_output(bump, current_context);
                    (next_draft, next_context)
                },
            );
            let out_refs: (Ref<Amount>, Ref<Vec<Mark>>, Ref<Draft>, Ref<Report>) =
                (picked, marks, draft, context);
            flow.output_with(out_refs);
        },
        Seed { value: 3 },
    ))
    .expect("six-operation composition runs");

    let (picked, marks, draft, context) = output;
    assert_eq!(picked.0, 81, "retry 后 report=80，case Route(0) 命中 +1");
    assert_eq!(
        marks.iter().map(|mark| mark.0).collect::<Vec<_>>(),
        vec![30, 40],
        "each 按输入顺序逐项聚合"
    );
    assert_eq!(draft.0, "d3+", "Break 交付进入该轮的 current，而不是本轮 next");
    assert_eq!(context.0, 81, "同一轮 current 的 context 未被本轮 next 覆盖");
    assert_eq!(ATTEMPTS.load(Ordering::SeqCst), 2, "至少一次真实重试");
    assert_eq!(MARKED.load(Ordering::SeqCst), 2, "each body 每项各一次");
    assert_eq!(BROKEN.load(Ordering::SeqCst), 1, "恰好一次 IterBreak");

    println!(
        "we_t06 p08 picked={} marks={:?} draft={} context={}",
        picked.0,
        marks.iter().map(|mark| mark.0).collect::<Vec<_>>(),
        draft.0,
        context.0
    );
    println!("we_t06-positive-ok");
}
