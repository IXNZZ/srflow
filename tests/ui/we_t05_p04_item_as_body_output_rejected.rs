//! WE-T05 P04／P13 负例（运行期拒绝）：**formal item 不能作为 Each 的收集输出**。
//!
//! SPEC §8：“item 是集合元素的受限借用，不获得独立 DataId，不被隐式 Clone 或移走。只有 item
//! 调用链产生且责任可合法转移的 owned 输出能被收集。**item 本身**、ancestor Data 或重复 owned
//! alias 不能被收集器消费”；任务书 §3.3：“实际目标是否完整、是否当前 Item-owned、是否与其他槽
//! 共享 DataId，由 Consume 全组预检判定；合法 Ref 可读取不代表可消费”。因此本负例的冻结要求
//! 是**必须以拒绝结束、不返回部分正常集合**；拒绝的公开分类与阶段不属 SPEC 逐字明文
//! （`RunError::Definition` 与 Consume 期 `RunError::Runtime` 都在 §12 的分类集合内）。
//!
//! 本夹具真实编译并运行：item body 声明 `item` 为一个输出槽（`(mark, item)`）。该形态在
//! Rust 类型层面成立（`Ref<Item>` 正是合法的 body 输出形状），因此拒绝必须来自框架而不是
//! rustc 诊断——故清单该行 `primary`／`allowed`／`target_line` 为空，证据是真实运行退出码 0 +
//! stdout 标记 `we_t05-reject-ok`；实测分类与业务调用数打印到日志。
//!
//! **实测记录（2026-10-13，见 README §8）**：当前实现返回
//! `RunError::Runtime(...)`（框架诊断 “each item consume rejected”），且在拒绝前已执行 1 次
//! 业务节点。这与任务书 P04“formal item 泄漏…Definition 拒绝、业务 Node／Root 创建零”的字面
//! 期待不同；实现侧判定（T05Runtime，2026-10-13）为**现行为正确**：P04 的静态要求针对 item
//! 位置**泄漏到 item Body 之外**（parent／Root／sibling／child-local／前向／未选分支），而本
//! 夹具是 item 留在自身 Body 内、只是进入被收集槽，按 SPEC §8 属收集器消费规则，拒绝阶段就是
//! Consume（§4.2：整组 Consume 是 item Body 的最后输出处置，故业务节点先执行是正常的）。
//! 同源对照：`we_t05_p04_foreign_ref_rejected.rs`（跨 Definition）实测 `RunError::Definition`
//! 且业务调用 0，正是静态层那一类。本夹具因此只断言“必须拒绝、无部分正常集合”，并把实测
//! 分类与调用数保留在日志与清单 note 中，不改标为 Definition。
//!
//! 外部消费者无法在定义闭包之外命名 `item` 或 child-local 位置，因此“parent／Root 直接引用
//! item／child-local 位置”在本目录按同源机制登记（见 README §5），不伪造外部入口。
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

    let error = block_on(runtime.execute(
        |flow: &mut srflow::Flow<'_>, items: Ref<Vec<Item>>| {
            flow.each(items, |item, each| {
                let mark = each.then(&touch, item);
                // 期望拒绝：formal item 出现在被收集的输出位置。
                (mark, item)
            })
        },
        vec![Item(1), Item(2)],
    ))
    .err()
    .expect("formal item as an each output must be rejected");

    // 冻结要求只有“不能被收集器消费／不产生部分正常集合”（SPEC §8）；拒绝的公开分类与阶段
    // 由本次真实运行记录（见 results/ui_check_summary.txt 与本行 note），本夹具不预设。
    println!(
        "we_t05 item-as-output reject after {} body calls: {error}",
        CALLS.load(Ordering::SeqCst)
    );
    println!("we_t05-reject-ok");
}
