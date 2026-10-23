//! V21-12 外部禁名负例：关联类型位的 pack 值类型不可达、不可命名。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v22_t03_from_legacy_v21_12_banned_pack_values.rs -o /tmp/v21_12_packs.rmeta
//! ```
//! 预期失败：E0432／E0603——`FlowInputs::Pack` 等关联位的值类型只作为不可命名的
//! 内部实现存在；业务侧不需要也不允许命名它们。
//! V22-T03 迁移：源 `tests/ui/v21_12_banned_pack_values.rs`。T03 未在 crate 根新增该旧名、`mod core` 仍私有，
//! 因此「根不可达」判据逐字承接：主诊断 E0432。

use srflow::NoShared;
use srflow::Targets1;
use srflow::Targets2;
use srflow::core as _;

fn main() {}
