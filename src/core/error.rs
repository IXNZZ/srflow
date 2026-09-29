use std::error::Error as StdError;
use std::fmt;

/// Executable 执行失败。
///
/// `ExecutionError` 表示一次执行没有正常产生 Output，语义上区分两类：
///
/// - [`ExecutionError::Failed`]：技术执行失败。叶子业务操作失败、外部依赖失败、child
///   Executable 失败，以及未命中分支等由执行语义定义的失败。
/// - [`ExecutionError::Invariant`]：框架不变量被破坏（例如内部值存储与强类型连接不一致）。
///   它表示 SRFlow 自身实现出了问题，而不是业务结果不理想。
///
/// 业务上的否定结论（“不接受”“未通过”“无候选”等）不属于这里：它们是正常 `Output`，
/// 必须用 `Ok(...)` 返回。
///
/// [`Output`]: crate::core::Executable::Output
///
/// # 错误来源
///
/// `Failed` 完整保留底层错误：[`source`](StdError::source) 返回底层错误本身，因此调用方可以
/// 沿来源链检查原始类型：
///
/// ```
/// use std::error::Error;
/// use srflow::{ExecutionError, Node, Runtime};
///
/// #[derive(Debug)]
/// struct NetworkDown;
/// impl std::fmt::Display for NetworkDown {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         f.write_str("network down")
///     }
/// }
/// impl Error for NetworkDown {}
///
/// struct Fetch;
/// impl Node for Fetch {
///     type Input = ();
///     type Output = u32;
///     async fn run(&self, _: ()) -> Result<u32, ExecutionError> {
///         Err(ExecutionError::new(NetworkDown))
///     }
/// }
///
/// let runtime = Runtime::new();
/// let error = futures::executor::block_on(runtime.execute(&Fetch, ())).unwrap_err();
/// assert!(error.source().unwrap().is::<NetworkDown>());
/// ```
///
/// `Display` 当前输出底层错误或不变量的文本，但**这不是稳定契约**：可追溯性由
/// [`source`](StdError::source) 与 [`ExecutionError::Invariant`] 的匹配保证，`Display` 文本
/// 将来可能带上执行位置等上下文。不要依赖 `to_string()` 做判断。
///
/// # 外部错误如何进入执行错误
///
/// 本类型不提供从任意错误的空白 `From` 转换，[`?`] 不能直接用在外部错误上，需要显式转换：
///
/// ```no_run
/// # use srflow::ExecutionError;
/// # async fn read(path: &str) -> Result<String, ExecutionError> {
/// std::fs::read_to_string(path).map_err(ExecutionError::new)
/// # }
/// ```
///
/// 这是经过比较后的取舍，而不是只因为 coherence 冲突就停止判断：
///
/// | 方案 | `?` 便利性 | 代价 |
/// | --- | --- | --- |
/// | 本类型实现 [`StdError`] ＋ 显式 `map_err`（当前选择） | 无 | 每个外部错误调用点都要写一次转换 |
/// | 本类型不实现 [`StdError`]，为任意错误提供空白 `From`（`anyhow::Error` 的做法） | 有 | 框架错误无法进入 `Box<dyn Error>`／`anyhow::Result` 等标准错误通道，来源链只能靠自有方法 |
/// | 另立一个 Node 专用错误类型，由自动适配器转到本类型 | 有 | 多一套公共类型，并把“叶子错误／边界错误”的划分提前固化 |
///
/// 选择当前方案的理由：框架错误必须保持 [`StdError`]，才能被后续 Flow、控制型
/// Executable 与调用方按标准方式包装和追溯；而“叶子错误／边界错误”的划分要等后续任务
/// 真正出现框架不变量错误之后才有依据。代价是有限的、而且与 SRFlow 不隐式转换错误的取向
/// 一致：转换点在 Node 自己的代码里显式可见。
///
/// [`?`]: https://doc.rust-lang.org/reference/expressions/operator-expr.html#the-question-mark-operator
///
/// 后续任务会出现新的错误类别（例如尚未实现的执行语义），因此本枚举标记为
/// `#[non_exhaustive]`：使用者匹配时必须保留 `_` 分支，新增变体不会破坏既有代码。
#[derive(Debug)]
#[non_exhaustive]
pub enum ExecutionError {
    /// 技术执行失败，保留可追溯的来源错误。
    Failed(Box<dyn StdError + Send + Sync + 'static>),
    /// 框架不变量被破坏。
    Invariant(InvariantError),
}

impl ExecutionError {
    /// 用底层错误构造执行失败。
    ///
    /// 参数可以是任意实现了 `Into<Box<dyn Error + Send + Sync>>` 的类型：业务自定义错误类型、
    /// `String` 或 `&str`（用于临时消息）。它也是在 Node 里把外部错误接入执行错误的推荐写法：
    /// `外部调用().map_err(ExecutionError::new)?`。
    pub fn new<E>(source: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync + 'static>>,
    {
        Self::Failed(source.into())
    }

    /// 构造框架不变量错误；只由框架内部使用。
    pub(crate) fn invariant(message: &'static str) -> Self {
        Self::Invariant(InvariantError::new(message))
    }
}

// 只转发底层文本，不加前缀：文本内容不是契约（将来可能加入执行位置等上下文），
// 可追溯性由 `source()` 与变体匹配承担。
impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(source) => fmt::Display::fmt(source, f),
            Self::Invariant(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl StdError for ExecutionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Failed(source) => Some(source.as_ref()),
            Self::Invariant(_) => None,
        }
    }
}

/// SRFlow 自身的不变量被破坏。
///
/// 它出现时表示框架实现有缺陷（例如内部值存储与强类型连接不一致），而不是业务结果不理想。
/// 构造器不对业务侧开放：这类错误由框架产生，业务侧只需要识别它。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantError {
    message: &'static str,
}

impl InvariantError {
    pub(crate) fn new(message: &'static str) -> Self {
        Self { message }
    }

    /// 人类可读的说明，用于诊断框架缺陷。
    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for InvariantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "srflow invariant violated: {}", self.message)
    }
}

impl StdError for InvariantError {}
