use crate::core::{Executable, ExecutionError};

/// 所有 Executable 的统一执行入口。
///
/// `Runtime` 只做一件事：发起一次 Executable 执行。它不判断目标是 Node 还是组合型
/// Executable，不包含重试、路由、循环等控制语义，也不承担业务逻辑。
///
/// 组合型 Executable 在调用 child 时收到父级传入的 `&Runtime`，因此整棵执行树共用同一个
/// 入口；`Runtime` 自身可以在未来承载执行观察能力（如计时、诊断），但它们只能围绕“一次
/// Executable 调用”建立，不得改变执行语义。
///
/// `Runtime` 目前是无状态执行入口，获取方式见 [`Runtime::new`]；同一个 `Runtime` 可以反复
/// 发起执行。这里刻意不实现 `Copy`／`Clone`：一旦公开复制语义，将来 `Runtime` 承载执行上
/// 下文时就必须继续伪装成可自由复制的值。共享语义明确之前不承诺复制。
#[derive(Debug, Default)]
pub struct Runtime {
    _private: (),
}

impl Runtime {
    /// 创建一个 `Runtime`。
    pub fn new() -> Self {
        Self::default()
    }

    /// 执行一次 Executable，返回其 Output 或执行错误。
    ///
    /// 这是 SRFlow 的唯一执行入口：无论是叶子 Node 还是组合型 Executable，都通过它发起执行。
    /// `executable` 以引用传入，因此同一个 Executable 定义可以被反复调用。
    ///
    /// # 输入类型由编译器检查
    ///
    /// 输入类型是 Executable 的关联类型，类型不匹配的连接无法编译：
    ///
    /// ```
    /// use srflow::{ExecutionError, Node, Runtime};
    ///
    /// struct Square;
    /// impl Node for Square {
    ///     type Input = u32;
    ///     type Output = u32;
    ///     async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
    ///         Ok(input * input)
    ///     }
    /// }
    ///
    /// let runtime = Runtime::new();
    /// let output = futures::executor::block_on(runtime.execute(&Square, 7)).unwrap();
    /// assert_eq!(output, 49);
    /// ```
    ///
    /// ```compile_fail
    /// use srflow::{ExecutionError, Node, Runtime};
    ///
    /// struct Square;
    /// impl Node for Square {
    ///     type Input = u32;
    ///     type Output = u32;
    ///     async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
    ///         Ok(input * input)
    ///     }
    /// }
    ///
    /// let runtime = Runtime::new();
    /// // `Square` 的 Input 是 u32，传入 &str 无法编译。
    /// let _ = futures::executor::block_on(runtime.execute(&Square, "seven"));
    /// ```
    ///
    /// # 错误
    ///
    /// Executable 的技术执行失败原样返回给调用方：Runtime 不重试、不跳过、不生成默认结果，
    /// 也不把错误转换成某种业务 Output。
    ///
    /// # `Send`
    ///
    /// 返回的就是 [`Executable::execute`] 产生的 Future，本方法不做额外包装，也不捕获
    /// `executable`。因此它是否 `Send` 完全由该 Executable 自身的契约决定：一个合法的
    /// Executable（其 Future 为 `Send`）不会因为经过 `Runtime` 而被额外要求 `Sync`。
    pub fn execute<E: Executable>(
        &self,
        executable: &E,
        input: E::Input,
    ) -> impl Future<Output = Result<E::Output, ExecutionError>> + Send {
        executable.execute(self, input)
    }
}
