//! R01 反例（证据用，不在活跃 manifest）：只有无关 `E0425`，不含目标 `E0277`。
fn main() {
    let _ = unknown_value;
}
