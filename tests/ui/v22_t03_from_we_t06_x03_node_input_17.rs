//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_x03_node_input_17.rs`）：Node Input 17 槽超上限。
//!
//! 原目的（X03／X24／C13／C15）：SPEC §3.2 Node Input 支持 `()`、单 Ref 与 2～16 个 Ref 的
//! flat tuple。把 17 个 Ref 组成的 flat tuple 传给 Node 登记入口不被接受；本夹具**只证明该
//! 形状不被接受**，主诊断落在该调用的真实调用点。
//!
//! 前 16 个位置来自合法的 16 槽 Root 输入，第 17 个位置由一次真实 Node 步骤产生。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)` 已删除，逐字改为取输出的显式连接
//! `flow.then_with_output(node, refs)`；其余（16 槽 Root 输入、identity Node、17 槽 Ref tuple
//! 实参）保持不变。旧闭包尾表达式改为 `output_with` 声明，不再以闭包返回形状参与判定。
//!
//! 期望主诊断：E0277（17 元组未实现 Node 输入形状）。

use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

async fn identity(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>,
         input: (
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
            Ref<Amount>,
        )| {
            let extra = flow.then_with_output(identity, input.0);
            // 17 个 Ref 的 flat tuple 作为 Node 输入：该形状不被接受。
            let _ = flow.then_with_output(
                identity,
                (
                    input.0,
                    input.1,
                    input.2,
                    input.3,
                    input.4,
                    input.5,
                    input.6,
                    input.7,
                    input.8,
                    input.9,
                    input.10,
                    input.11,
                    input.12,
                    input.13,
                    input.14,
                    input.15,
                    extra,
                ),
            );
            flow.output_with(());
        },
        (
            Amount(0),
            Amount(1),
            Amount(2),
            Amount(3),
            Amount(4),
            Amount(5),
            Amount(6),
            Amount(7),
            Amount(8),
            Amount(9),
            Amount(10),
            Amount(11),
            Amount(12),
            Amount(13),
            Amount(14),
            Amount(15),
        ),
    );
}
