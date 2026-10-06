//! V21-12 外部负例：业务侧不能从业务值或序号直接构造 `DataRef`。
//!
//! ```sh
//! cargo build --offline
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   tests/ui/v21_12_hardcoded_ref_is_not_constructible.rs -o /tmp/v21_12_ref.rmeta
//! ```
//! 预期失败：E0624（私有 `from_position`）／E0599（无 `new`）／E0308——没有公开构造入口。

use srflow::DataRef;

fn main() {
    let _ = DataRef::<u32>::from_position(0);
    let _ = DataRef::<u32>::new(1);
}
