//! WE-T01 负例 7：非 `'static` 借用手写 Data 声明被拒绝。
//!
//! 意图：Data 是 `Any`／`'static` 的空 marker。手写带借用参数的 `Data` 实现
//! 企图让带借用寿命的类型进入数据流，必须在编译期被拒绝；不得靠运行期检查。
//!
//! 预期主诊断：E0478（`Held<'a>` 的 `'static` 寿命界不满足），附加 E0803，
//! 两者都落在本文件的 `Data` 实现行。
//!
//! ```sh
//! rustc --edition=2024 --error-format=json --emit=metadata \
//!   tests/ui/v22_t03_from_we_t01_borrowed_data.rs -o /tmp/v22_t03_from_we_t01_borrowed_data.rmeta
//! ```

#[path = "../../src/core/mod.rs"]
mod core;

use crate::core::engine::data::Data;

/// 携带借用寿命的类型：不能作为 Data 叶子。
struct Held<'a>(&'a u32);

// 期望主诊断：E0478——`'a` 必须满足 `'static` 才能实现 `Data`。
impl<'a> Data for Held<'a> {}

fn main() {
    let local = 7u32;
    let held = Held(&local);
    let _ = held.0;
}
