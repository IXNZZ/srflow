//! T03 迁移自 `we_t05_p13_duplicate_owned_alias_rejected.rs`（WE-T05 P13／P15 负例，运行期拒绝）：
//! **重复 owned alias 不能在两槽各移动一次**。新契约下 Root 输出改由 [`Flow::output_with`] 声明
//! （定义闭包固定返回 `()`），显式连接更名为 `then_with_output`；原“必须以 `Err` 结束、不返回
//! 部分正常集合”的断言不变。
//!
//! SPEC §8：“item 本身、ancestor Data 或**重复 owned alias** 不能被收集器消费”；任务书 §5.2
//! 第 4 步：“全组拒绝重复被消费 DataId、重复 collector 目的身份；同类型不同 DataId／collectors
//! 合法。重复 Ref 或不同 alias 指向同一 owned Data 都不能物理移动两次”。
//!
//! 本夹具真实编译并运行：body 返回 `(mark, mark)`——同一个 `Ref<Mark>` 出现在两个槽。形态在
//! Rust 类型层面成立（`Ref` 是 Copy），故拒绝必须来自框架的成组预检；本夹具只断言必须以 `Err`
//! 结束、不返回部分正常集合，实测分类由清单 note 记录，清单该行 `primary`／`allowed` 为空。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t05-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Mark(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn touch(query: Query<&Item>) -> Result<Mark, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    let result = block_on(runtime.execute::<_, (Vec<Mark>, Vec<Mark>), _>(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            let out_refs = flow.each(items, |item, each| {
                let mark = each.then_with_output(&touch, item);
                // 期望拒绝：同一个 owned Ref 出现在两个输出槽。
                (mark, mark)
            });
            flow.output_with(out_refs);
        },
        vec![Item(1), Item(2)],
    ));

    let error = result
        .err()
        .expect("the same owned Ref in two slots must be rejected");
    println!(
        "we_t05 duplicate-owned-alias reject after {} body calls: {error}",
        CALLS.load(Ordering::SeqCst)
    );
    println!("we_t05-reject-ok");
}
