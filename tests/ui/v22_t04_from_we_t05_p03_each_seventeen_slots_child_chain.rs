//! T03 迁移自 `we_t05_p03_each_seventeen_slots_child_chain.rs`（WE-T05 P03 负例，编译期拒绝）：
//! item body 内 **17 槽** 的 Chain 子边界输出。显式连接更名为 `then_with_output`；原拒绝目的与
//! 范围不变。
//!
//! SPEC §7：“所有 child Flow 具有 then / chain / each / choose / retry / iter 能力。支持 `()`、
//! 单 Ref 和 flat tuple Output。”——能力开放不等于形状无限：SPEC §3.2 的 16 位置上限同样约束
//! item body 内的 child Flow 输出；17 槽 flat tuple 不被承诺，在 chain 定义处即编译期拒绝。
//!
//! 本夹具与 `v22_t03_from_we_t05_p02_each_body_output_17_slots.rs`（body 直接返回 17 槽）区分：
//! 这里 17 槽出现在**child Chain 的输出声明**上，是同一上限的第二个可观测位置。
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
    let _ = flow.each(items, |item, each| {
        each.chain_output(|sub| {
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
            let out_refs = (
                c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16,
                c17,
            );
            sub.output_with(out_refs);
        })
    });
}

fn main() {
    let _ = Mark(1);
}
