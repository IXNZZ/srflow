//! T05 迁移自 `v22_t04_from_we_t05_p03_positive_item_body_child_flow.rs`（原 WE-T05 P03 正例，
//! 公开部分）：item body 是**完整 child Flow**，可用 `then`／`chain`／`retry`。
//!
//! 迁移手法：Each 改为 item Body 单参数写法，item 经 `sub.ref_of::<Item>()` 取回；聚合声明用
//! `flow.each_output::<Item, (Ref<Vec<Score>>, Ref<Vec<Report>>, Ref<Vec<Report>>), _>` 显式给出三槽
//! 输出。item body 内的 `chain_output`／`retry_output` 子边界照旧，只把子 Flow 形参改名为 `inner`
//! 以避免与外层 `sub` 混淆；owned 结果仍由当前 Item 拥有并逐槽聚合。原逐槽断言与 judge 计数（3）
//! 不变。
//!
//! SPEC §7 明文：“所有 child Flow 具有 then / chain / each / choose / retry / iter 能力”。本夹具
//! 驱动三条真实路径：
//! 1. `sub.then_with_output(&score_of, item)`：item body 内的普通步骤；
//! 2. `sub.chain_output(|inner| inner.then_with_output(&report_of, (item, score)))`：item body 内的
//!    Chain 子边界；
//! 3. `sub.retry_output(1, …)`：item body 内的 Retry 边界**首先**处理 Retry 信号（SPEC §12），
//!    成功后本项正常继续，不终止 Each。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use std::cell::Cell;
use std::error::Error;
use std::fmt;

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

thread_local! {
    /// judge 调用次数：第 1 次返回 Retry，之后 Normal（真实动态原因不被字符串化重建）。
    static JUDGED: Cell<u32> = const { Cell::new(0) };
}

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Score(u32);
#[derive(Data)]
struct Report(u32);

/// 动态重试理由：带 source 可取回的原因。
#[derive(Debug)]
struct NotReady(u32);

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "report {} is not ready", self.0)
    }
}

impl Error for NotReady {}

async fn score_of(query: Query<&Item>) -> Result<Score, BodyError> {
    Ok(Score(query.get().0 * 10))
}

/// 纯节点：重试 attempt 重新执行它时结果相同（不伪装状态提升）。
async fn report_of(query: Query<(&Item, &Score)>) -> Result<Report, BodyError> {
    let (item, score) = query.get();
    Ok(Report(item.0 * 100 + score.0))
}

/// 第一次调用要求 Retry，之后 Normal：证明 item body 内的 Retry 边界被最近匹配。
async fn judge(query: Query<&Report>) -> Result<Report, BodyError> {
    let report = query.get();
    let seen = JUDGED.with(|counter| {
        let value = counter.get() + 1;
        counter.set(value);
        value
    });
    if seen == 1 {
        Err(BodyError::retry(NotReady(report.0)))
    } else {
        Ok(Report(report.0))
    }
}

fn main() {
    let runtime = Runtime::new();

    JUDGED.with(|counter| counter.set(0));
    let (scores, summaries, checked) = block_on(runtime.execute::<
        _,
        (Vec<Score>, Vec<Report>, Vec<Report>),
        _,
    >(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each_output::<
                Item,
                (Ref<Vec<Score>>, Ref<Vec<Report>>, Ref<Vec<Report>>),
                _,
            >(items, |sub| {
                let item = sub.ref_of::<Item>();
                let score = sub.then_with_output(&score_of, item);
                let summary: Ref<Report> = sub.chain_output(|inner| {
                    let out_refs = inner.then_with_output(&report_of, (item, score));
                    inner.output_with(out_refs);
                });
                let checked: Ref<Report> = sub.retry_output(1, |attempt| {
                    let draft = attempt.then_with_output(&report_of, (item, score));
                    let out_refs = attempt.then_with_output(&judge, draft);
                    attempt.output_with(out_refs);
                });
                sub.output_with((score, summary, checked));
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ))
    .expect("item body with then + chain + retry runs");

    let numbers = |values: &[Score]| values.iter().map(|value| value.0).collect::<Vec<_>>();
    let reports = |values: &[Report]| values.iter().map(|value| value.0).collect::<Vec<_>>();
    assert_eq!(numbers(&scores), vec![10, 20], "item body then step per item");
    // report_of = item * 100 + score：item1 110、item2 220。
    assert_eq!(reports(&summaries), vec![110, 220], "chain child output collected per item");
    assert_eq!(reports(&checked), vec![110, 220], "retry attempt result collected per item");
    assert_eq!(
        JUDGED.with(|counter| counter.get()),
        3,
        "the retry inside the first item consumed one extra attempt; the second item needed none"
    );

    println!("we_t05-positive-ok");
}
