//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_x03_root_input_17.rs`）：Root Input 17 槽超上限。
//!
//! 原目的（X03／X24／C13／C15）：SPEC §3.2 Root Input 支持 `()`、单 Data 值与 2～16 个
//! Data 值的 **flat** tuple。17 个位置的 Root 输入形状不被接受；本夹具**只证明该形状不被
//! 接受**，主诊断落在 `execute` 的真实调用点（输入实参），不是 Definition 构建期拒绝。
//!
//! 迁移手法：Root 闭包改为固定返回 `()`（`F: FnOnce(&mut Flow<'n>, In::Refs)`），
//! 并补上显式 owned `Out = ()` 泛型，避免额外的类型推断诊断；17 槽输入实参原样保留。
//!
//! 期望主诊断：E0277（17 元组未实现 Root 输入形状）。

use srflow::Data;
use srflow::Runtime;

#[derive(Data)]
struct Amount(u32);

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, (), _>(
        |flow, input| {
            let _ = (flow, input);
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
            Amount(16),
        ),
    );
}
