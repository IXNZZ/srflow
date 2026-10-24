//! T03 迁移自 `we_t05_p02_each_body_output_17_slots.rs`（WE-T05 P02 负例，编译期拒绝）：item body
//! 输出**超过 16 个 flat 槽**。显式连接更名为 `then_with_output`；原拒绝目的与范围不变。
//!
//! SPEC §3.2：“超过 16 个位置和 nested Ref tuple 不属于本版承诺”；SPEC §8 的多输出映射为
//! “每个位置分别形成 `Ref<Vec<对应类型>>`”，因此 17 槽 flat tuple 不是被承诺的 Shape，不实现
//! 形状协议（任务书 §1／§3.2）。本夹具不在 `each` 调用处使用 arity 命名（方法名不按 arity 分类）。
//!
//! 期望：编译期拒绝（主诊断码、唯一锚点行与符号 token 见 T03 清单
//! `manifest_we_t05.tsv` 该行；驱动实跑回填）。
use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct Item(u32);
#[derive(Data)]
struct Mark(u32);

async fn slot(query: Query<&Item>) -> Result<Mark, BodyError> {
    Ok(Mark(query.get().0))
}

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, items: Ref<Vec<Item>>) {
    flow.each(items, |sub| {
        let item = sub.ref_of::<Item>();
        let c01 = sub.then_with_output(&slot, item);
        let c02 = sub.then_with_output(&slot, item);
        let c03 = sub.then_with_output(&slot, item);
        let c04 = sub.then_with_output(&slot, item);
        let c05 = sub.then_with_output(&slot, item);
        let c06 = sub.then_with_output(&slot, item);
        let c07 = sub.then_with_output(&slot, item);
        let c08 = sub.then_with_output(&slot, item);
        let c09 = sub.then_with_output(&slot, item);
        let c10 = sub.then_with_output(&slot, item);
        let c11 = sub.then_with_output(&slot, item);
        let c12 = sub.then_with_output(&slot, item);
        let c13 = sub.then_with_output(&slot, item);
        let c14 = sub.then_with_output(&slot, item);
        let c15 = sub.then_with_output(&slot, item);
        let c16 = sub.then_with_output(&slot, item);
        let c17 = sub.then_with_output(&slot, item);
        sub.output_with((
            c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17,
        ));
    });
}

fn main() {
    let _ = Mark(1);
}
