//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_x03_root_output_17.rs`）：Root 成功 Output 17 槽超上限。
//!
//! 原目的（X03／X24／C13／C15）：SPEC §3.2「超过 16 个位置和 nested Ref tuple 不属于本版
//! 承诺」。编排 Output 的 flat tuple 形状只在 2～16 个位置有实现；本夹具**只证明该形状不被
//! 接受**，不是 Definition 构建期拒绝（构建期根本不会到达）。
//!
//! 迁移手法：新 Root 闭包固定返回 `()`，业务输出只能由 `Flow::output_with` 声明。原「闭包返回
//! 17 元 Ref tuple」的目的改由 `output_with(17 元 Ref tuple)` 保持：显式声明 17 槽 RefShape
//! 仍在**声明的真实调用点**被 rustc 按 Shape 拒绝，而不是靠「闭包尾表达式非 unit」这种无关
//! 错误冒充 arity 拒绝。期望 owned `Out` 显式写为 `()`，不引入额外推断诊断。
//!
//! 期望主诊断：E0277（17 元组未实现编排形状）。

use srflow::{Data, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, (), _>(
        |flow: &mut srflow::Flow<'_>, amount: Ref<Amount>| {
            let _ = flow;
            flow.output_with((
                amount, amount, amount, amount, amount, amount, amount, amount, amount, amount,
                amount, amount, amount, amount, amount, amount, amount,
            ));
        },
        Amount(0),
    );
}
