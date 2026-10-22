//! 业务 Data 声明与标准容器支持（SPEC §3.1）。
//!
//! Data 是 Any／'static 的空 marker：不 blanket 实现给任意类型，不增加
//! Clone／Copy／Send／Sync 或序列化要求。手写 `impl Data for X {}` 与
//! `#[derive(Data)]` 是两种等价的合法声明方式。

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;

/// 可作为非 unit 业务叶子进入数据流的类型声明。
pub trait Data: Any {}

macro_rules! builtin_data {
    ($($t:ty),+ $(,)?) => {$(
        impl Data for $t {}
    )+};
}

builtin_data!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, bool, char, String, f32, f64
);

impl<T: 'static + ?Sized> Data for Box<T> {}
impl<T: 'static> Data for Vec<T> {}
impl<T: 'static> Data for Option<T> {}
impl<T: 'static> Data for Rc<T> {}
impl<T: 'static> Data for Arc<T> {}
