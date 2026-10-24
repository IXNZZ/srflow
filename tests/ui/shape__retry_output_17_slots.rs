//! T03 迁移自 `we_t04_e02_retry_output_17_slots.rs`（WE-T04 E02 负例，编译期拒绝）：Retry 输出
//! 超过 16 槽 flat tuple。
//!
//! `RefShape`／`ShapeOps`（以及 Node 输出 tuple 形状）只为 `()`、单 `Ref` 与 2～16 元 flat
//! `Ref` tuple 实现（`tuple_shapes!` 宏上界为 16）。17 元 flat tuple 不实现这些形状协议，
//! 因此 `flow.retry` 的输出类型在类型检查阶段被拒绝（任务书 §1：Retry 支持 2～16 flat tuple）。
//!
//! 期望主诊断：E0277（17 元 tuple 未实现形状协议）。
use srflow::{BodyError, Data, Flow, Query, Ref};

#[derive(Data)]
struct Item(u32);

async fn produce(query: Query<&Item>) -> Result<Item, BodyError> {
    Ok(Item(query.get().0 + 1))
}

type Slot = Ref<Item>;
type Tuple17 = (
    Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot, Slot,
    Slot, Slot,
);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe<'n>(flow: &mut Flow<'n>, item: Ref<Item>) {
    let _: Tuple17 = flow.retry_output(3, |attempt| {
        let s0 = attempt.then_with_output(&produce, item);
        let s1 = attempt.then_with_output(&produce, item);
        let s2 = attempt.then_with_output(&produce, item);
        let s3 = attempt.then_with_output(&produce, item);
        let s4 = attempt.then_with_output(&produce, item);
        let s5 = attempt.then_with_output(&produce, item);
        let s6 = attempt.then_with_output(&produce, item);
        let s7 = attempt.then_with_output(&produce, item);
        let s8 = attempt.then_with_output(&produce, item);
        let s9 = attempt.then_with_output(&produce, item);
        let s10 = attempt.then_with_output(&produce, item);
        let s11 = attempt.then_with_output(&produce, item);
        let s12 = attempt.then_with_output(&produce, item);
        let s13 = attempt.then_with_output(&produce, item);
        let s14 = attempt.then_with_output(&produce, item);
        let s15 = attempt.then_with_output(&produce, item);
        let s16 = attempt.then_with_output(&produce, item);
        let out_refs = (
            s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16,
        );
        attempt.output_with(out_refs);
    });
}

fn main() {
    let _ = Item(1);
}
