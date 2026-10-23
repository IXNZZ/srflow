//! T03 外部编译正例：原 T02「`AutoCallable` 根未导出（E0432）」判据转为公开支持项的导入正例。
//!
//! ```sh
//! cargo build --offline --lib
//! rustc --edition=2024 --emit=metadata --extern srflow=target/debug/libsrflow.rlib \
//!   -L dependency=target/debug/deps tests/ui/v22_t03_from_t02_neg_extern_auto_callable_root.rs \
//!   -o /tmp/v22_t03_from_t02_neg_extern_auto_callable_root.rmeta
//! ```
//!
//! 预期：编译成功（原 E0432 拒绝退场）。
//!
//! T03 迁移：自动输入入口的桥 trait `AutoCallable` 现由 crate 根以 `#[doc(hidden)]` 导出
//! （账本规定的 public/doc(hidden) 支持项），因此业务可达路径的根导入必须成功。自然自动调用与
//! `Ref<T>` 注解的运行承接由 C02 相关夹具负责；此处仅固定“根可达”这一编译事实。
//! 私有父模块路径 `srflow::core::…` 仍被拒绝，见 `v22_t03_from_t02_neg_extern_auto_bridge.rs`。

#![allow(unused_imports)]

use srflow::AutoCallable;

fn main() {}
