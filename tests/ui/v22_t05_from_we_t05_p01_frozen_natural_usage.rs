//! T03 迁移自 `we_t05_p01_frozen_natural_usage.rs`（WE-T05 P01 正例）：冻结 SPEC §8 自然用法在
//! 独立外部消费者中真实运行。新契约下 Root 输出改由 [`Flow::output_with`] 声明（定义闭包固定
//! 返回 `()`），显式连接更名为 `then_with_output`；原断言（total=102、count=3）与运行语义不变。
//!
//! 冻结用法（SPEC §8 逐字原文，任务书 §1）：
//!
//! ```text
//! let (scores, reports) = flow.each(items, |item, each| {
//!     let score = each.then_with_output(&score, (item, rules));
//!     let report = each.then_with_output(&report, (item, score));
//!     (score, report)
//! });
//! ```
//!
//! 本夹具以真实主包 rlib 编译（`--extern srflow=target/debug/libsrflow.rlib`），不装配内部模块、
//! 不使用 `#[path]`，因此是外部消费者视角。覆盖任务书 §8 的 P01：
//!
//! - 集合为 `Ref<Vec<Item>>`（Root 输入的第二参数直接提供 `Vec<Item>`）；
//! - **祖先捕获**：`rules` 是 Root 输入的第二个位置，在 item body 内作为普通 ancestor 输入
//!   复用（SPEC §8：“其 body 使用普通 ancestor capture，没有 shared-input 分类”）；
//! - **函数 Node**（`&report`，函数 item）与**借用 Struct Node**（`&score`，`&ScoreNode`）两种
//!   可注册形式；
//! - Each 的自然返回形状（`Ref<Vec<Score>>`、`Ref<Vec<Report>>`）可交给后续 `then`、后续
//!   `chain`，并作为 Root 输出交付；
//! - `Item` 不含 `Clone`／`Copy`／`Debug`（SPEC §3.1：Data 不要求 Clone／Copy／Send／Sync
//!   或序列化）——集合元素不被隐式 Clone、不被移走。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, Ref, Runtime};

/// 集合元素：非 Clone／Copy／Debug，证明 Each 不要求元素可复制。
#[derive(Data)]
struct Item(String);
#[derive(Data)]
struct Rule(u32);
#[derive(Data)]
struct Score(u32);
#[derive(Data)]
struct Report(u32);
#[derive(Data)]
struct Total(u32);
#[derive(Data)]
struct Count(u32);

/// 借用 Struct Node：以 `&score` 形式注册。
struct ScoreNode {
    delta: u32,
}

impl Node for ScoreNode {
    type Input = (Item, Rule);
    type Output = Score;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Score, BodyError> {
        let (item, rule) = query.get();
        Ok(Score(item.0.len() as u32 * rule.0 + self.delta))
    }
}

/// 函数 Node：以 `&report` 形式注册。
async fn report(query: Query<(&Item, &Score)>) -> Result<Report, BodyError> {
    let (item, score) = query.get();
    Ok(Report(score.0 + item.0.len() as u32))
}

/// 后续 `then`：把整组 Each 成果读成单值。
async fn summarize(query: Query<(&Vec<Score>, &Vec<Report>)>) -> Result<Total, BodyError> {
    let (scores, reports) = query.get();
    let score_sum: u32 = scores.iter().map(|score| score.0).sum();
    let report_sum: u32 = reports.iter().map(|report| report.0).sum();
    Ok(Total(score_sum + report_sum))
}

/// 后续 `chain`：child 只读祖先 Each 成果。
async fn count_scores(query: Query<&Vec<Score>>) -> Result<Count, BodyError> {
    Ok(Count(query.get().len() as u32))
}

fn main() {
    let runtime = Runtime::new();
    let score = ScoreNode { delta: 10 };

    let (total, count) = block_on(runtime.execute::<_, (Total, Count), _>(
        |flow: &mut Flow<'_>, (items, rules)| {
            // 冻结 SPEC §8 原文。
            let out_refs = flow.each_output::<Item, (Ref<Vec<Score>>, Ref<Vec<Report>>), _>(
                items,
                |sub| {
                    let item = sub.ref_of::<Item>();
                    let score = sub.then_with_output(&score, (item, rules));
                    let report = sub.then_with_output(&report, (item, score));
                    sub.output_with((score, report));
                },
            );
            let (scores, reports) = out_refs;
            // 自然返回形状交给后续 then／chain／Root。
            let total = flow.then_with_output(&summarize, (scores, reports));
            let count: Ref<Count> = flow.chain_output(|sub|{
                let out_refs = sub.then_with_output(&count_scores, scores);
                sub.output_with(out_refs);
            });
            flow.output_with((total, count));
        },
        (
            vec![
                Item(String::from("aa")),
                Item(String::from("bb")),
                Item(String::from("cc")),
            ],
            Rule(3),
        ),
    ))
    .expect("frozen SPEC §8 each usage runs");

    // 每项：score = len * rule + delta = 2 * 3 + 10 = 16；report = score + len = 18。
    assert_eq!(total.0, 3 * 16 + 3 * 18, "each scores/reports summarised");
    assert_eq!(count.0, 3, "each result readable through a later chain");
    println!("we_t05-positive-ok");
}
