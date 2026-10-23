//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_x03_nested_ref_output.rs`）：nested Ref tuple 作为 Root 成功 Output。
//!
//! 原目的（X03／X24／C13／C15）：SPEC §3.2 只承诺 flat Ref tuple（`(Ref<A>, Ref<B>, …)`）；
//! `((Ref, Ref), Ref)` 这类嵌套形状不被接受。本夹具**只证明该形状不被接受**。
//!
//! 迁移手法：新 Root 闭包固定返回 `()`，业务输出只能由 `Flow::output_with` 声明。原「闭包返回
//! nested Ref tuple」的目的改由 `output_with(((a, b), c))` 保持：嵌套 Shape 仍在**声明的真实
//! 调用点**被 rustc 拒绝，而不是靠「闭包尾表达式非 unit」这种无关错误冒充 nested 目的。
//!
//! 期望主诊断：E0277（嵌套元组未实现编排形状）。

use srflow::{Data, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, (), _>(
        |flow: &mut srflow::Flow<'_>, amount: Ref<Amount>| {
            let _ = flow;
            let pair = (amount, amount);
            flow.output_with((pair, amount));
        },
        Amount(0),
    );
}
