//! WE-T05 P13 负例（运行期拒绝）：**ancestor Data 不能作为 Each 的收集输出**。
//!
//! SPEC §8：“只有 item 调用链产生且责任可合法转移的 owned 输出能被收集。item 本身、**ancestor
//! Data** 或重复 owned alias 不能被收集器消费”；任务书 §3.3：“实际目标是否完整、是否当前
//! Item-owned、是否与其他槽共享 DataId，由 Consume 全组预检判定；合法 Ref 可读取不代表可消费”。
//!
//! 本夹具真实编译并运行：body 声明 `(mark, rules)`，其中 `rules` 是 Root 输入的 ancestor 位置
//! （在 item body 内是合法**只读**输入，但不是 Item-owned 结果）。形态在类型层面成立，因此
//! 拒绝必须来自框架；由于 SPEC 明文允许“可提前诊断明确非法来源”，本夹具**不预设**拒绝阶段与
//! 公开分类（可能是 Definition 构建期，也可能是 Consume 全组预检的运行期），只断言：必须以
//! `Err` 结束，不返回部分正常集合。实测分类由 `results/ui_check.log` 记录，清单该行
//! `primary`／`allowed`／`target_line` 为空。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Rule(u32);
#[derive(Data)]
struct Mark(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn touch(query: Query<(&Item, &Rule)>) -> Result<Mark, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    let (item, rule) = query.get();
    Ok(Mark(item.0 + rule.0))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    let result = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, (items, rules): (Ref<Vec<Item>>, Ref<Rule>)| {
            flow.each(items, |item, each| {
                let mark = each.then(&touch, (item, rules));
                // 期望拒绝：ancestor Data 出现在被收集的输出位置。
                (mark, rules)
            })
        },
        (vec![Item(1), Item(2)], Rule(7)),
    ));

    let error = result
        .err()
        .expect("ancestor Data as an each output must be rejected");
    println!(
        "we_t05 ancestor-as-output reject after {} body calls: {error}",
        CALLS.load(Ordering::SeqCst)
    );
    println!("we_t05-reject-ok");
}
