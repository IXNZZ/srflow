//! WE-T06 §8 X23：完整组合取消与执行隔离（A 阶段，旧上层仍在位）。
//!
//! 只用新公开 API（`Runtime`／`Flow`／`Ref`／`Query`／`Data`…），不引用 `srflow::legacy`，
//! 不引入新依赖，不使用 `unsafe`。每个样本都经 `Runtime::new().execute(...)` 建立真实
//! Definition，在**真实 `Pending`**（节点 `Future` 尚未 ready，`poll` 返回
//! `Poll::Pending`）处 drop 外层 `execute` Future 完成取消；Pending 原语是自写的
//! [`PendingGate`]（首次 poll 返回 Pending 并自唤醒），不使用睡眠、也不用人造 panic。
//!
//! 样本与判据：
//! - `x23a`：未 poll 的 `execute` Future 直接 drop —— owned 输入随 Future 销毁且各恰一次，
//!   零业务调用；
//! - `x23b`：Each 多输出进行到中途（前两项已收集、第三项 Score 已产生未收集）时取消 ——
//!   借用 Future 先结束、部分成果／输入逐实例 Drop 恰一次、取消后业务调用计数不再增长、
//!   再跑一个无关执行后被取消样本的日志保持冻结（无 detached child）；
//! - `x23c`：Retry 进行到第二个 attempt 时取消 —— 同上，并覆盖对 `Prompt` 的借用先结束；
//! - `x23d`：取消一次 execute 不污染另一次 execute —— 同一 Definition 的第二次完整值；
//! - `x23e`：Retry Definition 在取消后一次完整跑通（第二个 attempt 的自唤醒 Pending 恢复）。
//!
//! 「输入借用先结束才能移动／销毁来源」的两类证据：
//! 1. 事件顺序：持有借用的节点 Future（`drop:pending-gate`）先于它借用的来源
//!    （`drop:item:*` / `drop:prompt`）与部分成果被销毁；
//! 2. 借用检查下的编译事实：节点在 `await` 之后仍经同一 `&Item`／`&Prompt` 读取输入
//!    （`pending:resume:*`），rustc 因此强制该借用存活到挂起点之后——借用不可能在来源
//!    销毁后才结束。

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll, Waker};

// ---------------------------------------------------------------- 观察设施

/// 析构／调用事件日志（单线程执行的共享记录，不引入依赖）。
type Log = Rc<RefCell<Vec<String>>>;

fn log_new() -> Log {
    Rc::new(RefCell::new(Vec::new()))
}

fn record(log: &Log, event: impl Into<String>) {
    log.borrow_mut().push(event.into());
}

fn events(log: &Log) -> Vec<String> {
    log.borrow().clone()
}

/// 某事件的出现次数（逐实例 Drop 恰一次的证据）。
fn occurrences(log: &Log, needle: &str) -> usize {
    log.borrow()
        .iter()
        .filter(|event| event.as_str() == needle)
        .count()
}

/// 某事件第一次出现的下标（顺序证据：借用先结束才能销毁来源）。
fn position(log: &Log, needle: &str) -> usize {
    let current = log.borrow();
    current
        .iter()
        .position(|event| event.as_str() == needle)
        .unwrap_or_else(|| panic!("missing event `{needle}`: {current:?}"))
}

thread_local! {
    /// 业务节点真实调用次数（「取消后零增长」的判据）。
    static BODY_CALLS: Cell<u32> = const { Cell::new(0) };
}

fn body_calls() -> u32 {
    BODY_CALLS.with(Cell::get)
}

fn reset_body_calls() {
    BODY_CALLS.with(|calls| calls.set(0));
}

/// 真实 Pending 原语：首次 poll 返回 `Poll::Pending` 并自唤醒，之后 poll 到 Ready。
///
/// 取消样本用 noop waker 只 poll 外层 Future 一次，因此节点一直停在真实挂起；隔离样本用
/// `block_on` 再次驱动时自然恢复（自唤醒保证恢复不依赖时间或外部事件）。
struct PendingGate {
    log: Log,
    yielded: Cell<bool>,
}

impl PendingGate {
    fn new(log: &Log) -> Self {
        Self {
            log: Rc::clone(log),
            yielded: Cell::new(false),
        }
    }
}

impl Future for PendingGate {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        if self.yielded.replace(true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl Drop for PendingGate {
    fn drop(&mut self) {
        record(&self.log, "drop:pending-gate");
    }
}

// ---------------------------------------------------------------- Each 样本的业务类型

#[derive(Data)]
struct Item {
    value: u32,
    log: Log,
}

impl Item {
    fn new(value: u32, log: &Log) -> Self {
        Self {
            value,
            log: Rc::clone(log),
        }
    }
}

impl Drop for Item {
    fn drop(&mut self) {
        record(&self.log, format!("drop:item:{}", self.value));
    }
}

#[derive(Data)]
struct Score {
    value: u32,
    log: Log,
}

impl Drop for Score {
    fn drop(&mut self) {
        record(&self.log, format!("drop:score:{}", self.value));
    }
}

#[derive(Data)]
struct Report {
    value: u32,
    log: Log,
}

impl Drop for Report {
    fn drop(&mut self) {
        record(&self.log, format!("drop:report:{}", self.value));
    }
}

/// 每项产生一个 owned `Score`；`value == 3` 的项先产生 `Score`，再在**持有 `&Item` 借用**
/// 时进入真实 Pending，恢复后仍经同一借用读取输入（跨 await 的真实借用）。
async fn score_of(query: Query<&Item>) -> Result<Score, BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    let item = query.get();
    record(&item.log, format!("call:score:{}", item.value));
    let score = Score {
        value: item.value * 10,
        log: Rc::clone(&item.log),
    };
    if item.value == 3 {
        record(&item.log, "pending:enter");
        PendingGate::new(&item.log).await;
        // 跨 await 使用同一 `&Item`：rustc 强制该借用存活到挂起点之后。
        record(&item.log, format!("pending:resume:{}", item.value));
    }
    Ok(score)
}

/// 依赖 `(item, score)` 产生异构 `Report`。
async fn report_of(query: Query<(&Item, &Score)>) -> Result<Report, BodyError> {
    BODY_CALLS.with(|calls| calls.set(calls.get() + 1));
    let (item, score) = query.get();
    record(&item.log, format!("call:report:{}", item.value));
    Ok(Report {
        value: score.value + item.value,
        log: Rc::clone(&item.log),
    })
}

/// Each 多输出 Definition：逐项形成 `Ref<Vec<Score>>`／`Ref<Vec<Report>>`。
fn score_and_report<'n>(
    flow: &mut Flow<'n>,
    items: Ref<Vec<Item>>,
) -> (Ref<Vec<Score>>, Ref<Vec<Report>>) {
    flow.each(items, |item, each| {
        let score = each.then(&score_of, item);
        let report = each.then(&report_of, (item, score));
        (score, report)
    })
}

// ---------------------------------------------------------------- Retry 样本的业务类型

thread_local! {
    static ATTEMPT: Cell<u32> = const { Cell::new(0) };
    static GEN_CALLS: Cell<u32> = const { Cell::new(0) };
    static JUDGE_CALLS: Cell<u32> = const { Cell::new(0) };
}

#[derive(Data)]
struct Prompt {
    value: u32,
    log: Log,
}

impl Prompt {
    fn new(value: u32, log: &Log) -> Self {
        Self {
            value,
            log: Rc::clone(log),
        }
    }
}

impl Drop for Prompt {
    fn drop(&mut self) {
        record(&self.log, "drop:prompt");
    }
}

#[derive(Data)]
struct Candidate {
    attempt: u32,
    log: Log,
}

impl Drop for Candidate {
    fn drop(&mut self) {
        record(&self.log, format!("drop:candidate:{}", self.attempt));
    }
}

#[derive(Data)]
struct Verdict {
    attempt: u32,
    log: Log,
}

impl Drop for Verdict {
    fn drop(&mut self) {
        record(&self.log, format!("drop:verdict:{}", self.attempt));
    }
}

/// 明确重试理由：携带动态原因，不被字符串化重建。
#[derive(Debug)]
struct NotReady {
    attempt: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "attempt {} is not ready", self.attempt)
    }
}

impl Error for NotReady {}

/// 每次 attempt 产生新的 owned `Candidate`；第二个 attempt 在持有 `&Prompt` 借用时进入
/// 真实 Pending，恢复后仍经借用读取输入。
async fn generate(query: Query<&Prompt>) -> Result<Candidate, BodyError> {
    let attempt = ATTEMPT.with(|counter| {
        let next = counter.get() + 1;
        counter.set(next);
        next
    });
    GEN_CALLS.with(|calls| calls.set(calls.get() + 1));
    let prompt = query.get();
    record(&prompt.log, format!("generate:{attempt}"));
    let candidate = Candidate {
        attempt,
        log: Rc::clone(&prompt.log),
    };
    if attempt == 2 {
        record(&prompt.log, "pending:enter");
        PendingGate::new(&prompt.log).await;
        record(&prompt.log, format!("pending:resume:{}", prompt.value));
    }
    Ok(candidate)
}

/// 前两次要求重试；第三次交付 `Verdict`（取消样本永不到达该分支）。
async fn judge(query: Query<&Candidate>) -> Result<Verdict, BodyError> {
    JUDGE_CALLS.with(|calls| calls.set(calls.get() + 1));
    let candidate = query.get();
    record(&candidate.log, format!("judge:{}", candidate.attempt));
    if candidate.attempt < 3 {
        Err(BodyError::retry(NotReady {
            attempt: candidate.attempt,
        }))
    } else {
        Ok(Verdict {
            attempt: candidate.attempt,
            log: Rc::clone(&candidate.log),
        })
    }
}

/// Retry Definition：预算 4 次机会，第三个 attempt 成功交付 `(Verdict, Candidate)`。
fn retry_definition<'n>(
    flow: &mut Flow<'n>,
    prompt: Ref<Prompt>,
) -> (Ref<Verdict>, Ref<Candidate>) {
    flow.retry(4, |attempt| {
        let candidate = attempt.then(&generate, prompt);
        (attempt.then(&judge, candidate), candidate)
    })
}

// ---------------------------------------------------------------- 样本

/// X23a：未 poll 的 `execute` Future 直接 drop —— 不建立 Definition、不执行任何业务节点，
/// owned 输入随 Future 一同销毁且各恰一次。
#[test]
fn x23a_unpolled_execute_future_drops_owned_inputs_exactly_once() {
    let runtime = Runtime::new();
    let log = log_new();
    reset_body_calls();

    let future = runtime.execute(
        score_and_report,
        vec![Item::new(1, &log), Item::new(2, &log)],
    );
    // 从未被 poll：零业务调用、零事件。
    assert_eq!(
        body_calls(),
        0,
        "an unpolled execution runs no business node"
    );
    assert!(
        events(&log).is_empty(),
        "an unpolled execution records nothing: {:?}",
        events(&log)
    );

    drop(future);

    assert_eq!(
        occurrences(&log, "drop:item:1"),
        1,
        "the first owned input drops exactly once: {:?}",
        events(&log)
    );
    assert_eq!(
        occurrences(&log, "drop:item:2"),
        1,
        "the second owned input drops exactly once: {:?}",
        events(&log)
    );
    assert_eq!(
        events(&log).len(),
        2,
        "no extra instance is created or leaked: {:?}",
        events(&log)
    );
    assert_eq!(body_calls(), 0, "still no business node ran");
}

/// X23b：Each 多输出进行到中途时在真实 Pending 点取消。前两项的 `Score`／`Report` 已被
/// collector 收集，第三项的 `Score` 已产生但未收集。
#[test]
fn x23b_cancel_at_a_real_pending_point_ends_the_borrow_first_and_drops_each_instance_once() {
    let runtime = Runtime::new();
    let log = log_new();
    reset_body_calls();

    let mut future = Box::pin(runtime.execute(
        score_and_report,
        vec![Item::new(1, &log), Item::new(2, &log), Item::new(3, &log)],
    ));

    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    assert!(
        matches!(future.as_mut().poll(&mut cx), Poll::Pending),
        "the third item suspends on a real pending point"
    );

    // 挂起现场：前两项已收集，第三项 Score 已产生、Report 未开始；借用的输入仍在；无 Drop。
    assert_eq!(
        events(&log),
        [
            "call:score:1",
            "call:report:1",
            "call:score:2",
            "call:report:2",
            "call:score:3",
            "pending:enter",
        ],
        "partial progress at the pending window"
    );
    let calls_at_pending = body_calls();
    assert_eq!(
        calls_at_pending, 5,
        "3 score nodes + the first two reports ran before the suspension"
    );

    // 取消：drop 拥有 Context 的外层 Future。
    drop(future);

    let frozen = events(&log);
    // 借用 Future（PendingGate）先结束；此后来源与部分成果才被销毁。
    let gate = position(&log, "drop:pending-gate");
    for needle in [
        "drop:score:30",
        "drop:score:10",
        "drop:score:20",
        "drop:report:11",
        "drop:report:22",
        "drop:item:1",
        "drop:item:2",
        "drop:item:3",
    ] {
        assert!(
            gate < position(&log, needle),
            "the borrow ends before `{needle}` is destroyed: {frozen:?}"
        );
    }
    for needle in [
        "drop:pending-gate",
        "drop:score:30",
        "drop:score:10",
        "drop:score:20",
        "drop:report:11",
        "drop:report:22",
        "drop:item:1",
        "drop:item:2",
        "drop:item:3",
    ] {
        assert_eq!(
            occurrences(&log, needle),
            1,
            "`{needle}` drops exactly once: {frozen:?}"
        );
    }
    assert_eq!(
        frozen.len(),
        15,
        "6 progress events + 9 drops, no extras: {frozen:?}"
    );

    // 无 detached child：取消后业务调用计数即冻结。
    assert_eq!(
        body_calls(),
        calls_at_pending,
        "no business node runs after cancellation"
    );

    // 再跑一个无关的完整执行：被取消样本的日志保持冻结。
    let other = log_new();
    let (scores, reports) = block_on(runtime.execute(
        score_and_report,
        vec![Item::new(7, &other), Item::new(8, &other)],
    ))
    .expect("an unrelated execution completes");
    assert_eq!(
        scores.iter().map(|score| score.value).collect::<Vec<_>>(),
        [70, 80],
        "the unrelated execution still produces its own values"
    );
    assert_eq!(
        reports
            .iter()
            .map(|report| report.value)
            .collect::<Vec<_>>(),
        [77, 88]
    );
    assert_eq!(
        events(&log),
        frozen,
        "the cancelled sample stays frozen: {:?}",
        events(&log)
    );
}

/// X23c：Retry 进行到第二个 attempt 时取消（第一个 attempt 已重试）。
#[test]
fn x23c_cancel_inside_a_retry_attempt_ends_the_borrow_first_and_stops_detached_work() {
    let runtime = Runtime::new();
    let log = log_new();
    ATTEMPT.with(|counter| counter.set(0));
    GEN_CALLS.with(|calls| calls.set(0));
    JUDGE_CALLS.with(|calls| calls.set(0));

    let mut future = Box::pin(runtime.execute(retry_definition, Prompt::new(5, &log)));
    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    assert!(
        matches!(future.as_mut().poll(&mut cx), Poll::Pending),
        "the second attempt suspends on a real pending point"
    );

    // 第一个 attempt 的 Candidate 在重试推进时即被销毁；第二项在真实 Pending 处持有借用。
    assert_eq!(
        events(&log),
        [
            "generate:1",
            "judge:1",
            "drop:candidate:1",
            "generate:2",
            "pending:enter",
        ],
        "the second attempt is mid-flight at the pending window"
    );
    assert_eq!(GEN_CALLS.with(Cell::get), 2, "two attempts generated");
    assert_eq!(
        JUDGE_CALLS.with(Cell::get),
        1,
        "only the first attempt was judged"
    );

    drop(future);

    let frozen = events(&log);
    let gate = position(&log, "drop:pending-gate");
    assert!(
        gate < position(&log, "drop:prompt"),
        "the borrow of Prompt ends before Prompt is destroyed: {frozen:?}"
    );
    assert!(
        gate < position(&log, "drop:candidate:2"),
        "the borrow ends before the in-flight Candidate: {frozen:?}"
    );
    for needle in [
        "drop:pending-gate",
        "drop:candidate:1",
        "drop:candidate:2",
        "drop:prompt",
    ] {
        assert_eq!(
            occurrences(&log, needle),
            1,
            "`{needle}` drops exactly once: {frozen:?}"
        );
    }
    assert_eq!(
        frozen.len(),
        8,
        "5 progress events + 3 drops, no extras: {frozen:?}"
    );

    // 无 detached child：取消后不再产生新的 attempt 或判断。
    assert_eq!(
        GEN_CALLS.with(Cell::get),
        2,
        "no further attempt after cancellation"
    );
    assert_eq!(
        JUDGE_CALLS.with(Cell::get),
        1,
        "no further judge after cancellation"
    );
    assert_eq!(
        events(&log),
        frozen,
        "cancellation leaves no detached child behind: {:?}",
        events(&log)
    );
}

/// X23d：取消一次 execute 不污染另一次 execute —— 同一 Definition 的第二次执行完整且值正确，
/// 计数增量精确；第二次执行仍保留真实的 Pending→恢复与逐实例 Drop 恰一次。
#[test]
fn x23d_a_cancelled_execute_does_not_pollute_the_next_execute() {
    let runtime = Runtime::new();
    reset_body_calls();

    // 第一次：在真实 Pending 处取消。
    let log_a = log_new();
    let mut future = Box::pin(runtime.execute(
        score_and_report,
        vec![
            Item::new(1, &log_a),
            Item::new(2, &log_a),
            Item::new(3, &log_a),
        ],
    ));
    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    assert!(
        matches!(future.as_mut().poll(&mut cx), Poll::Pending),
        "the first execution suspends"
    );
    drop(future);
    let cancelled = events(&log_a);
    assert_eq!(
        body_calls(),
        5,
        "the cancelled execution stopped at 5 calls"
    );

    // 第二次：同一 Definition 闭包完整执行，第三个项的自唤醒 Pending 正常恢复。
    let log_b = log_new();
    let (scores, reports) = block_on(runtime.execute(
        score_and_report,
        vec![
            Item::new(1, &log_b),
            Item::new(2, &log_b),
            Item::new(3, &log_b),
        ],
    ))
    .expect("the second execution completes");

    assert_eq!(
        scores.iter().map(|score| score.value).collect::<Vec<_>>(),
        [10, 20, 30],
        "the second execution returns the complete value"
    );
    assert_eq!(
        reports
            .iter()
            .map(|report| report.value)
            .collect::<Vec<_>>(),
        [11, 22, 33],
        "the second execution returns the complete value"
    );
    assert_eq!(
        body_calls(),
        11,
        "the second execution adds exactly 6 calls (no residue from the cancelled one)"
    );

    // 第二次执行内部的跨 await 借用与 Pending 恢复真实发生。
    assert_eq!(occurrences(&log_b, "pending:enter"), 1);
    assert_eq!(
        occurrences(&log_b, "pending:resume:3"),
        1,
        "the borrow across the await resumed and read its input: {:?}",
        events(&log_b)
    );
    assert_eq!(occurrences(&log_b, "drop:pending-gate"), 1);
    for needle in ["drop:item:1", "drop:item:2", "drop:item:3"] {
        assert_eq!(
            occurrences(&log_b, needle),
            1,
            "root inputs drop exactly once: {:?}",
            events(&log_b)
        );
    }
    // 交付值仍由调用方持有：尚未 Drop。
    assert_eq!(
        occurrences(&log_b, "drop:score:10"),
        0,
        "returned outputs are owned by the caller: {:?}",
        events(&log_b)
    );

    drop(scores);
    drop(reports);
    for needle in [
        "drop:score:10",
        "drop:score:20",
        "drop:score:30",
        "drop:report:11",
        "drop:report:22",
        "drop:report:33",
    ] {
        assert_eq!(
            occurrences(&log_b, needle),
            1,
            "`{needle}` drops exactly once: {:?}",
            events(&log_b)
        );
    }

    // 被取消的第一次执行完全未受影响。
    assert_eq!(
        events(&log_a),
        cancelled,
        "the cancelled execution stays frozen: {:?}",
        events(&log_a)
    );
}

/// X23e：Retry Definition 在取消后一次完整跑通（第二个 attempt 的 Pending 自唤醒恢复）。
#[test]
fn x23e_a_retry_definition_completes_after_a_cancelled_attempt() {
    let runtime = Runtime::new();

    // 取消一次：停在第二个 attempt 的真实 Pending。
    let log_a = log_new();
    ATTEMPT.with(|counter| counter.set(0));
    let mut future = Box::pin(runtime.execute(retry_definition, Prompt::new(11, &log_a)));
    let waker = Waker::noop();
    let mut cx = TaskContext::from_waker(waker);
    assert!(
        matches!(future.as_mut().poll(&mut cx), Poll::Pending),
        "the cancelled run suspends in its second attempt"
    );
    drop(future);

    // 同一 Definition 再次执行并跑通：attempt 1、2（自唤醒恢复）、3 成功。
    let log_b = log_new();
    ATTEMPT.with(|counter| counter.set(0));
    GEN_CALLS.with(|calls| calls.set(0));
    JUDGE_CALLS.with(|calls| calls.set(0));
    let (verdict, candidate) = block_on(runtime.execute(retry_definition, Prompt::new(11, &log_b)))
        .expect("the retry definition completes after cancellation");

    assert_eq!(
        (verdict.attempt, candidate.attempt),
        (3, 3),
        "the third attempt delivers the verdict and candidate"
    );
    assert_eq!(
        GEN_CALLS.with(Cell::get),
        3,
        "exactly three attempts generated"
    );
    assert_eq!(JUDGE_CALLS.with(Cell::get), 3, "exactly three judges ran");
    assert_eq!(
        occurrences(&log_b, "pending:resume:11"),
        1,
        "the pending attempt resumed through its live borrow: {:?}",
        events(&log_b)
    );
    assert_eq!(occurrences(&log_b, "drop:pending-gate"), 1);
    assert_eq!(occurrences(&log_b, "drop:prompt"), 1);
    assert_eq!(
        occurrences(&log_b, "drop:candidate:1"),
        1,
        "the first candidate drops exactly once: {:?}",
        events(&log_b)
    );
    assert_eq!(
        occurrences(&log_b, "drop:candidate:2"),
        1,
        "the resumed candidate drops exactly once: {:?}",
        events(&log_b)
    );
    assert_eq!(
        occurrences(&log_b, "drop:candidate:3"),
        0,
        "the delivered Candidate is owned by the caller: {:?}",
        events(&log_b)
    );

    drop(candidate);
    drop(verdict);
    assert_eq!(occurrences(&log_b, "drop:candidate:3"), 1);
    assert_eq!(
        occurrences(&log_b, "drop:verdict:3"),
        1,
        "the delivered Verdict drops exactly once: {:?}",
        events(&log_b)
    );

    // 取消样本未被污染。
    assert_eq!(
        occurrences(&log_a, "generate:3"),
        0,
        "the cancelled run never started a third attempt: {:?}",
        events(&log_a)
    );
}
