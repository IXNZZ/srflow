//! Root 输入／输出的形状映射（`RootInputs<I>`／`RootOutputs<K>`）。
//!
//! 这两个 trait 是**独立于** [`OutKind`] 的 Root 形状映射：`OutKind::Output` 表示 Node
//! 值域（`Data<O>` → `O`，`Unit`／`Out2` → `()`），Orchestrator body 的 Future 返回
//! `NodeFut<'a, ()>`，都不能表达"Root 成功时交给 Application 的 owned 结果"。因此这里
//! 单独定义：
//!
//! - [`RootInputs<I>`]：把 `Runtime::execute` 收到的 owned 输入按 Signature 顺序交给登记
//!   边界（擦除值 + 真实类型名）；单输入在公开入口直接传值，双输入传 pair，内部统一
//!   适配成按 Signature 排列的 tuple，tuple 包装本身不额外分配 DataId；
//! - [`RootOutputs<K>`]：按 `K` 给出 owned 结果类型与提取次数，并把已按声明顺序 take 的
//!   擦除值组装成 Application 结果。
//!
//! 两者都是 sealed：只有本模块的三种形状可以有实现，新增形状必须同时在这里与
//! `Runtime::execute` 的支持范围内明确定义，不能由调用方自定义映射绕过提取预检。

use std::any::Any;

use super::signature::{BuildError, Data, DeclaredPort, Out2, OutKind, OutputKind, Unit};

mod sealed {
    /// 只有本模块的三种 Root 形状可以实现映射 trait（公开 trait 的 sealed 标记）。
    pub trait Sealed {}
}

/// Root 输入形状：`Self = I`，把 owned 输入按 Signature 顺序交给登记边界。
pub trait RootInputs<I: 'static>: sealed::Sealed {
    /// Application 侧传给 `Runtime::execute` 的输入形状：单输入直接传值，双输入传 tuple。
    type ApplicationInput;

    /// 将 Application 侧输入适配为内部按 Signature 顺序登记的 tuple。
    fn into_internal(input: Self::ApplicationInput) -> I;

    /// 按声明顺序交出 `(真实类型名, 擦除值)`；元组包装本身不是业务 Data。
    fn into_values(self) -> Vec<(&'static str, Box<dyn Any>)>;
}

impl<X: 'static> sealed::Sealed for (X,) {}

impl<X: 'static> RootInputs<(X,)> for (X,) {
    type ApplicationInput = X;

    fn into_internal(input: X) -> (X,) {
        (input,)
    }

    fn into_values(self) -> Vec<(&'static str, Box<dyn Any>)> {
        vec![(std::any::type_name::<X>(), Box::new(self.0))]
    }
}

impl<X: 'static, Y: 'static> sealed::Sealed for (X, Y) {}

impl<X: 'static, Y: 'static> RootInputs<(X, Y)> for (X, Y) {
    type ApplicationInput = (X, Y);

    fn into_internal(input: (X, Y)) -> (X, Y) {
        input
    }

    fn into_values(self) -> Vec<(&'static str, Box<dyn Any>)> {
        vec![
            (std::any::type_name::<X>(), Box::new(self.0)),
            (std::any::type_name::<Y>(), Box::new(self.1)),
        ]
    }
}

/// Root owned 输出映射：`Self = K`，给出 owned 结果类型与提取次数。
///
/// 由 crate 为 [`Unit`]／[`Data`]／[`Out2`] 实现（sealed）；消费者只在
/// [`Runtime::execute`](crate::Runtime::execute) 的结果类型中使用 [`Self::Owned`]。
pub trait RootOutputs<K: OutputKind>: sealed::Sealed {
    /// Application 取得的 owned 结果。
    type Owned: 'static;

    /// 本次 Root 的物理提取次数（Unit 为 0）。
    const TAKE: usize;

    /// 由已按声明顺序提取的擦除值组装结果。
    ///
    /// 调用前提：`prepare_root_extraction` 已逐项验证实际类型与 `K` 的声明一致，因此
    /// 这里的 downcast 不引入可恢复失败分支。
    fn assemble(values: Vec<Box<dyn Any>>) -> Self::Owned;
}

impl sealed::Sealed for Unit {}

impl RootOutputs<Unit> for Unit {
    type Owned = ();
    const TAKE: usize = 0;

    fn assemble(_values: Vec<Box<dyn Any>>) {}
}

impl<O: 'static> sealed::Sealed for Data<O> {}

impl<O: 'static> RootOutputs<Data<O>> for Data<O> {
    type Owned = O;
    const TAKE: usize = 1;

    fn assemble(mut values: Vec<Box<dyn Any>>) -> O {
        *values
            .remove(0)
            .downcast::<O>()
            .expect("root output type was verified by the preflight")
    }
}

impl<O1: 'static, O2: 'static> sealed::Sealed for Out2<O1, O2> {}

impl<O1: 'static, O2: 'static> RootOutputs<Out2<O1, O2>> for Out2<O1, O2> {
    type Owned = (O1, O2);
    const TAKE: usize = 2;

    fn assemble(mut values: Vec<Box<dyn Any>>) -> (O1, O2) {
        let second = *values
            .remove(1)
            .downcast::<O2>()
            .expect("root output type was verified by the preflight");
        let first = *values
            .remove(0)
            .downcast::<O1>()
            .expect("root output type was verified by the preflight");
        (first, second)
    }
}

/// Root 的声明输出端口与 `K` 的数量／类型预检。
///
/// 真实调用传入 Definition 的 `output_ports()`（`&[DeclaredPort]`），因此在进入 Root frame
/// 与登记输入之前完成：数量不符或声明类型不符都在这里被拒绝，不会拖到提取阶段。
/// `K::port_types()` 的顺序就是 `declare_finish_outputs`／`declare_common_outputs` 的声明
/// 顺序，因此第 `index` 项一一对应。
pub(crate) fn check_root_output_signature<K: OutKind>(
    ports: &[DeclaredPort],
) -> Result<(), BuildError> {
    let expected = K::port_types();
    for index in 0..ports.len().max(expected.len()) {
        match (ports.get(index), expected.get(index)) {
            (Some(port), Some((expected_name, expected))) => {
                if port.expected() != *expected {
                    return Err(BuildError::RootOutputSignatureMismatch {
                        index,
                        expected: expected_name,
                        actual: port.expected_name(),
                    });
                }
            }
            (Some(port), None) => {
                return Err(BuildError::RootOutputSignatureMismatch {
                    index,
                    expected: "<no port>",
                    actual: port.expected_name(),
                });
            }
            (None, Some((expected_name, _))) => {
                return Err(BuildError::RootOutputSignatureMismatch {
                    index,
                    expected: expected_name,
                    actual: "<no port>",
                });
            }
            (None, None) => unreachable!("the loop bound is the larger of the two lengths"),
        }
    }
    Ok(())
}
