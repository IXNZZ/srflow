//! T03 迁移自 `we_t05_p03_positive_item_body_child_flow.rs`（WE-T05 P03 正例，公开部分）：item
//! body 是**完整 child Flow**，可用 `then`／`chain`／`retry`。新契约下 Root 输出改由
//! [`Flow::output_with`] 声明（定义闭包固定返回 `()`），显式连接更名为 `then_with_output`；
//! 原逐槽断言与 judge 计数（3）不变。
//!
//! SPEC §7 明文：“所有 child Flow 具有 then / chain / each / choose / retry / iter 能力”；
//! 任务书 §1 据此把 `each.chain(|sub| …)` 登记为**允许的超集用法**——但它不得替代 SPEC §8 的
//! 冻结写法充当“冻结用法”样本（冻结样本见 `v22_t03_from_we_t05_p01_frozen_natural_usage.rs`）。
//!
//! 本夹具驱动三条真实路径：
//! 1. `each.then_with_output(&score_of, item)`：item body 内的普通步骤；
//! 2. `each.chain(|sub| sub.then_with_output(&report_of, (item, score)))`：item body 内的 Chain
//!    子边界，其 owned 结果（Chain Export 产物）仍由当前 Item 拥有，可被收集（任务书 §3.3／P14）；
//! 3. `each.retry(1, …)`：item body 内的 Retry 边界**首先**处理 Retry 信号（SPEC §12：
//!    “Control 沿动态调用链寻找最近对应类型边界”），成功后本项正常继续，不终止 Each。
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
    let (scores, summaries, checked) =
        block_on(runtime.execute::<_, (Vec<Score>, Vec<Report>, Vec<Report>), _>(
            |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
                let out_refs = flow.each(items, |item, each| {
                    let score = each.then_with_output(&score_of, item);
                    let summary: Ref<Report> = each.chain_output(|sub|{
                        let out_refs = sub.then_with_output(&report_of, (item, score));
                        sub.output_with(out_refs);
                    });
                    let checked: Ref<Report> = each.retry_output(1, |attempt| {
                        let draft = attempt.then_with_output(&report_of, (item, score));
                        let out_refs = attempt.then_with_output(&judge, draft);
                        attempt.output_with(out_refs);
                    });
                    (score, summary, checked)
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
