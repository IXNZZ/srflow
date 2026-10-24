//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_wrong_root_shape.rs`）：错误 Root Shape（裸 owned 冒充 Root 输出）。
//!
//! 原目的（X24／C13／C15）：`Runtime::execute` 的 Root 成功输出必须是编排形状（`()`／单 `Ref`／
//! flat `Ref` tuple），返回裸业务值（owned `Amount`）不被接受，必须由 rustc 拒绝（SPEC §3.2／§4）。
//!
//! 阶段变化（T03 公开签名切换）：Root 闭包不再返回 `RefShape`，而是**固定返回 `()`**，业务输出
//! 只由 `Flow::output`／`Flow::output_with` 声明。因此「裸 owned 尾表达式冒充 Root 输出」不再由
//! 旧 `Out: RefShape` bound 拒绝，而由新的 **unit 返回约束**在同一处（闭包尾表达式）拒绝。
//! 拒绝阶段仍是**编译期**，不转为运行期拒绝；合法对照即把同一产物交给 `flow.output_with(refs)`。
//!
//! 期望主诊断：E0308（闭包尾表达式应为 `()`，实得 owned `Amount`；旧行为为 E0277）。
use srflow::{Data, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, Amount, _>(
        |flow: &mut srflow::Flow<'_>, amount: Ref<Amount>| {
            let _ = flow;
            let _ = amount;
            Amount(7)
        },
        Amount(0),
    );
}
