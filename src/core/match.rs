//! Match：依据已有路由值执行唯一分支。
//!
//! [`Match<K, I, O>`](Match) 读取一个**已经形成**的路由值 `K`，从登记的分支中选择且只选择一个
//! 执行：
//!
//! ```text
//! JudgeNode → K
//!              │
//!         Match<K, I, O>，Input = (K, I)
//!              ├─ 命中 case → Runtime.execute(对应 branch, I)
//!              ├─ 未命中、有 default → Runtime.execute(default, I)
//!              └─ 未命中、无 default → NoMatch 执行错误
//! ```
//!
//! - **判断与路由分离**：`K` 由上游 Node、Flow Input 或其他已明确的数据位置产生；Match 不从
//!   `I` 推导路由，也不接受 `Fn(&I) -> K` 之类的隐藏业务判断。
//! - **异构分支、统一契约**：不同 case 的具体 Executable 类型可以不同（Node、Flow、Retry……），
//!   但它们必须共享同一个 `I` 与 `O`。异构存储留在框架内部，业务侧只看到强类型。
//! - **唯一执行路径**：一次执行最多调用一个分支，不按顺序试错，未选分支与 default 都不执行。
//!   被选分支的实际执行重新经过父级传入的 [`Runtime`]，Match 自身不执行也不重试。
//!
//! ```
//! use srflow::{ExecutionError, Match, Node, Runtime};
//!
//! #[derive(Debug, PartialEq, Eq)]
//! enum Route {
//!     Short,
//!     Long,
//! }
//!
//! struct Shorten;
//! impl Node for Shorten {
//!     type Input = String;
//!     type Output = String;
//!     async fn run(&self, input: String) -> Result<String, ExecutionError> {
//!         Ok(input.chars().take(3).collect())
//!     }
//! }
//!
//! struct Keep;
//! impl Node for Keep {
//!     type Input = String;
//!     type Output = String;
//!     async fn run(&self, input: String) -> Result<String, ExecutionError> {
//!         Ok(input)
//!     }
//! }
//!
//! let mut builder = Match::<Route, String, String>::builder();
//! builder.case(Route::Short, Shorten).unwrap();
//! builder.case(Route::Long, Keep).unwrap();
//! let matcher = builder.build();
//!
//! let runtime = Runtime::new();
//! let output = futures::executor::block_on(
//!     runtime.execute(&matcher, (Route::Short, String::from("abcdef"))),
//! )
//! .unwrap();
//! assert_eq!(output, "abc");
//! ```
//!
//! # 构建：case 与 default
//!
//! [`Match::builder`] 得到 [`MatchBuilder`]，逐个登记 case（键 + 分支），再用
//! [`build`](MatchBuilder::build) 完成构建。公开类型始终是可命名的
//! [`Match<K, I, O>`](Match)：登记多少个 case、分支是什么具体类型，都不会累积到使用者必须书写的
//! 泛型参数里。
//!
//! 构建期拒绝两类接线错误，见 [`MatchBuildError`]：重复的 case 键与重复的 default。两者都不会
//! `panic!`，也不会留下部分登记——校验先于提交，失败后构建对象可继续使用。被拒绝的键与分支由
//! 该次调用消费并丢弃，不作为错误载荷返还。
//!
//! 空 case 集合是合法的：有 default 时只执行 default，没有 default 时稳定返回 [`NoMatch`]。
//!
//! # 未命中与 default
//!
//! 只有**没有任何 case 键等于 `K`** 时才轮到 default。default 不是“被选分支失败后的备用路径”：
//! 被选分支或 default 自身返回 [`ExecutionError`] 时，Match 原样传播该错误，不继续寻找其他 case，
//! 也不改走 default。
//!
//! 无 default 且未命中时返回以 [`NoMatch`] 为来源的执行错误，外部可以按类型识别：
//!
//! ```
//! use std::error::Error;
//! use srflow::{ExecutionError, Match, Node, NoMatch, Runtime};
//!
//! #[derive(Debug, PartialEq, Eq)]
//! enum Route {
//!     Known,
//!     Other,
//! }
//!
//! struct Any;
//! impl Node for Any {
//!     type Input = String;
//!     type Output = String;
//!     async fn run(&self, input: String) -> Result<String, ExecutionError> {
//!         Ok(input)
//!     }
//! }
//!
//! let mut builder = Match::<Route, String, String>::builder();
//! builder.case(Route::Known, Any).unwrap();
//! let matcher = builder.build();
//!
//! let runtime = Runtime::new();
//! // 没有 case 命中、也没有 default：错误来源是 NoMatch。
//! let error = futures::executor::block_on(
//!     runtime.execute(&matcher, (Route::Other, String::from("x"))),
//! )
//! .unwrap_err();
//! assert!(matches!(
//!     error.source().and_then(|source| source.downcast_ref::<NoMatch>()),
//!     Some(_)
//! ));
//! ```
//!
//! # 三类错误的发生阶段
//!
//! | 阶段 | 类型 | 含义 |
//! | --- | --- | --- |
//! | 构建／登记 | [`MatchBuildError`] | 这条登记本身不合法（重复键、重复 default），不会产出 Match |
//! | 执行、未命中 | [`NoMatch`]（作为 [`ExecutionError`] 的来源） | 连接成立，但这次执行没有任何 case 命中且没有 default |
//! | 执行、分支 | [`ExecutionError`] | 被选分支或 default 自己失败，原样传播 |
//!
//! 未命中是 Match 的正常执行语义失败，不是框架不变量错误，也不是业务上“不接受”的结论——后者应当
//! 由分支的正常 `Output` 表达。
//!
//! # 所有权与 bounds
//!
//! - `K`、`I`、`O` 都不要求 `Clone`：路由只做等值比较，`I` 只交给被选分支一次，`O` 按值返回。
//! - 路由使用 [`Eq`]，不引入谓词、范围或字符串解析，也不因内部容器再要求 `Hash`／`Ord`／
//!   `Debug`。`Eq` 是语义契约：注册时按正常等价关系相等的键，路由时必须能命中自己。
//! - case 键直接存在 Match 中，因此键需要 `Eq + Send + Sync`；`I`／`O` 不被 Match 存储，只需要
//!   [`Executable`] 已有的 `Send`，也不要求 `Sync`。作为父 Flow 的 child 时另受
//!   [`FlowBuilder::then`](crate::FlowBuilder::then) 的既有约束（`Send + Sync + 'static`，以及
//!   `I`／`O` 的 `'static`）——这是 Flow child 契约，不是 Match 对业务数据的新要求。
//! - 分支在 Match 内长期持有，因此登记时要求 `Send + Sync + 'static`。分支的实际执行统一经
//!   Runtime，Match 不直接调用其 `Executable::execute`。

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use crate::core::{Executable, ExecutionError, Runtime};

/// 没有任何 case 命中，而且没有 default。
///
/// 它是 [`ExecutionError::Failed`] 的来源，外部可以按类型识别而未命中：
///
/// ```
/// use std::error::Error;
/// use srflow::{ExecutionError, Match, Node, NoMatch, Runtime};
///
/// #[derive(Debug, PartialEq, Eq)]
/// enum Route {
///     Known,
///     Other,
/// }
///
/// struct Any;
/// impl Node for Any {
///     type Input = String;
///     type Output = String;
///     async fn run(&self, input: String) -> Result<String, ExecutionError> {
///         Ok(input)
///     }
/// }
///
/// let mut builder = Match::<Route, String, String>::builder();
/// builder.case(Route::Known, Any).unwrap();
/// let matcher = builder.build();
///
/// let runtime = Runtime::new();
/// let error = futures::executor::block_on(
///     runtime.execute(&matcher, (Route::Other, String::from("x"))),
/// )
/// .unwrap_err();
/// assert!(matches!(
///     error.source().and_then(|source| source.downcast_ref::<NoMatch>()),
///     Some(_)
/// ));
/// ```
///
/// 它不携带路由值，因此错误类型不会为诊断额外要求业务键实现 `Debug`／`Clone`／`Sync`。
/// `K` 已按值传入本次执行；若调用方未另行保留键值，不能从这个错误中恢复具体的 `K`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoMatch;

impl fmt::Display for NoMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("没有任何 case 命中该路由值，且没有登记 default")
    }
}

impl std::error::Error for NoMatch {}

/// Match 构建阶段的登记错误。
///
/// 这类错误发生在 [`MatchBuilder`] 上：不合法的**本次登记**不会生效，已有配置保持不变；
/// 构建器仍可继续登记合法分支，并从已有配置产出可执行的 [`Match`]。它与 [`ExecutionError`]
/// 的分工是：构建错误表示“这条登记根本不该成立”，执行错误表示“登记成立，但某次执行失败了”。
///
/// 变体不携带路由值或分支类型，因此不需要业务键实现 `Debug`／`Clone`／`Sync`。后续任务可能补充
/// 新的登记错误，因此本枚举标记为 `#[non_exhaustive]`：使用者匹配时必须保留 `_` 分支。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MatchBuildError {
    /// 该 case 键已经登记过。
    ///
    /// 重复键没有隐式优先级：不是“第一个胜出”，也不是“最后一个覆盖”。需要按顺序尝试多个分支
    /// 应改用显式编排，而不是把它们登记成同一个键。
    DuplicateCase,
    /// 已经登记过 default。
    ///
    /// default 只有一个：“未命中路径”。第二次登记不会被静默覆盖。
    DuplicateDefault,
}

impl fmt::Display for MatchBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateCase => {
                f.write_str("该 case 键已经登记过：重复键没有隐式优先级，请只登记一个分支")
            }
            Self::DuplicateDefault => {
                f.write_str("default 已经登记过：default 表示唯一的未命中路径，不会被覆盖")
            }
        }
    }
}

impl std::error::Error for MatchBuildError {}

/// 异构分支的类型擦除表示。
///
/// 这是框架内部的适配层：唯一实现把所有调用转发给 [`Runtime::execute`]，因此被选分支的实际执行
/// 必然重新经过 Runtime。业务侧不需要也不应该看到这个 trait。
trait ErasedBranch<I, O>: Send + Sync {
    fn run<'a>(
        &'a self,
        runtime: &'a Runtime,
        input: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, ExecutionError>> + Send + 'a>>;
}

/// 所有 `Executable<I, O>` 都通过 Runtime 擦除为分支。
///
/// 擦除只隐藏“分支是什么具体类型”，不改变执行方式：这里不直接调用 `Executable::execute`。
impl<I, O, E> ErasedBranch<I, O> for E
where
    E: Executable<Input = I, Output = O> + Send + Sync,
{
    fn run<'a>(
        &'a self,
        runtime: &'a Runtime,
        input: I,
    ) -> Pin<Box<dyn Future<Output = Result<O, ExecutionError>> + Send + 'a>> {
        Box::pin(runtime.execute(self, input))
    }
}

/// [`Match`] 的构建器：登记 case 与可选 default。
///
/// 通过 [`Match::builder`] 取得。每个 case 关联一个路由键与一个 `Executable<I, O>`；分支的具体
/// 类型可以不同，但对外的 `I`／`O` 必须与 Match 的一致。
///
/// 登记方法在失败时返回 [`MatchBuildError`]，并且**先完整校验、再一次性提交**：失败不会留下部分
/// 登记，构建器可以继续使用。被拒绝的键与分支由该次调用消费并丢弃，调用方可以另备键和分支重新
/// 登记。
pub struct MatchBuilder<K, I, O> {
    cases: Vec<(K, Box<dyn ErasedBranch<I, O>>)>,
    default: Option<Box<dyn ErasedBranch<I, O>>>,
}

impl<K, I, O> MatchBuilder<K, I, O> {
    fn new() -> Self {
        Self {
            cases: Vec::new(),
            default: None,
        }
    }

    /// 完成构建。
    ///
    /// 没有 case 也可以构建：这样的 Match 有 default 时只执行 default，没有 default 时返回
    /// [`NoMatch`]。
    pub fn build(self) -> Match<K, I, O> {
        Match {
            cases: self.cases,
            default: self.default,
        }
    }
}

impl<K, I, O> MatchBuilder<K, I, O>
where
    K: Eq,
{
    /// 登记一个 case：键 `key` 命中时执行 `branch`。
    ///
    /// 分支的具体类型可以与其他 case 不同，但必须实现 `Executable<Input = I, Output = O>`。
    /// 分支在 Match 内长期持有，因此需要 `Send + Sync + 'static`；`I`／`O` 不额外要求 `Clone`
    /// 或 `Sync`。
    ///
    /// # 错误
    ///
    /// `key` 相等（按 [`Eq`]）的 case 已经登记过时返回 [`MatchBuildError::DuplicateCase`]，
    /// 本次登记不生效，已有的 case 保持不变，构建器可继续使用。被拒绝时 `key` 与 `branch` 已被
    /// 本次调用消费并丢弃。
    ///
    /// ```
    /// use srflow::{ExecutionError, Match, MatchBuildError, Node, Runtime};
    ///
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Route {
    ///     A,
    /// }
    ///
    /// struct Any;
    /// impl Node for Any {
    ///     type Input = String;
    ///     type Output = String;
    ///     async fn run(&self, input: String) -> Result<String, ExecutionError> {
    ///         Ok(input)
    ///     }
    /// }
    ///
    /// let mut builder = Match::<Route, String, String>::builder();
    /// builder.case(Route::A, Any).unwrap();
    /// // 同一个键第二次登记被拒绝，且不会覆盖已有分支。
    /// assert_eq!(builder.case(Route::A, Any), Err(MatchBuildError::DuplicateCase));
    /// let matcher = builder.build();
    ///
    /// let runtime = Runtime::new();
    /// let output =
    ///     futures::executor::block_on(runtime.execute(&matcher, (Route::A, String::from("x"))))
    ///         .unwrap();
    /// assert_eq!(output, "x");
    /// ```
    ///
    /// # 类型错误的分支无法编译
    ///
    /// 分支的 `Input`／`Output` 必须与 Match 的一致：
    ///
    /// ```compile_fail
    /// use srflow::{ExecutionError, Match, Node};
    ///
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Route {
    ///     A,
    /// }
    ///
    /// struct WrongOutput;
    /// impl Node for WrongOutput {
    ///     type Input = String;
    ///     type Output = usize;
    ///     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
    ///         Ok(input.len())
    ///     }
    /// }
    ///
    /// let mut builder = Match::<Route, String, String>::builder();
    /// // Match 需要 `Executable<Input = String, Output = String>`，这里 Output 是 usize。
    /// let _ = builder.case(Route::A, WrongOutput);
    /// ```
    pub fn case<E>(&mut self, key: K, branch: E) -> Result<(), MatchBuildError>
    where
        E: Executable<Input = I, Output = O> + Send + Sync + 'static,
    {
        if self.cases.iter().any(|(registered, _)| registered == &key) {
            return Err(MatchBuildError::DuplicateCase);
        }
        self.cases.push((key, Box::new(branch)));
        Ok(())
    }

    /// 登记 default：没有任何 case 命中时执行 `branch`。
    ///
    /// default 只表示“未命中路径”，不是被选分支失败后的备用路径。只有在 Match 尚无 default 时
    /// 才能登记。
    ///
    /// # 错误
    ///
    /// 已经登记过 default 时返回 [`MatchBuildError::DuplicateDefault`]，已有的 default 保持不变。
    /// 被拒绝时 `branch` 已被本次调用消费并丢弃。
    pub fn default<E>(&mut self, branch: E) -> Result<(), MatchBuildError>
    where
        E: Executable<Input = I, Output = O> + Send + Sync + 'static,
    {
        if self.default.is_some() {
            return Err(MatchBuildError::DuplicateDefault);
        }
        self.default = Some(Box::new(branch));
        Ok(())
    }
}

/// 依据已有路由值执行唯一分支的控制型 [`Executable`]。
///
/// `Input = (K, I)` 中的 `K` 是上游已经形成的路由值，`I` 是交给被选分支的业务 Input；
/// `Output = O` 是所有分支共同的输出。构建方式见 [`Match::builder`]，路由与错误语义见本模块文档。
///
/// ```text
/// Match<K, I, O>：Input = (K, I)，Output = O
///
/// 命中 case      → Runtime.execute(该 case 的分支, I)
/// 未命中 + default → Runtime.execute(default, I)
/// 未命中 + 无 default → ExecutionError，来源是 NoMatch
/// ```
///
/// 同一个 Match 定义可以反复调用，也可以有交叠的并发调用：一次调用携带的 `(K, I)` 不会被写入
/// Match 自身，因此不会串到另一次调用。
///
/// # 类型错误的父 Flow 接线无法编译
///
/// `Input` 是 `(K, I)`：父 Flow 里的 Binding 必须组装出这个元组，接线错误在构建期就被拒绝。
///
/// ```compile_fail
/// use srflow::{ExecutionError, FlowBuilder, Match, Node};
///
/// #[derive(Debug, PartialEq, Eq)]
/// enum Route {
///     A,
/// }
///
/// struct Text;
/// impl Node for Text {
///     type Input = String;
///     type Output = String;
///     async fn run(&self, input: String) -> Result<String, ExecutionError> {
///         Ok(input)
///     }
/// }
///
/// let mut builder = Match::<Route, String, String>::builder();
/// builder.case(Route::A, Text).unwrap();
/// let matcher = builder.build();
///
/// let mut flow = FlowBuilder::<String>::new();
/// let input = flow.input(); // Ref<String>
/// // Match 需要 `(Route, String)`，裸 `Ref<String>` 不是该 Input。
/// let _ = flow.then(matcher, input);
/// ```
pub struct Match<K, I, O> {
    cases: Vec<(K, Box<dyn ErasedBranch<I, O>>)>,
    default: Option<Box<dyn ErasedBranch<I, O>>>,
}

impl<K, I, O> Match<K, I, O>
where
    K: Eq,
{
    /// 开始构建一个 Match。
    ///
    /// 三个类型参数分别是路由值 `K`、被选分支的业务 Input `I` 与共同 Output `O`。
    ///
    /// ```
    /// use srflow::{ExecutionError, Match, Node};
    ///
    /// #[derive(Debug, PartialEq, Eq)]
    /// enum Route {
    ///     A,
    /// }
    ///
    /// struct Any;
    /// impl Node for Any {
    ///     type Input = String;
    ///     type Output = String;
    ///     async fn run(&self, input: String) -> Result<String, ExecutionError> {
    ///         Ok(input)
    ///     }
    /// }
    ///
    /// let mut builder = Match::<Route, String, String>::builder();
    /// builder.case(Route::A, Any).unwrap();
    /// let matcher = builder.build();
    /// # let _ = matcher;
    /// ```
    pub fn builder() -> MatchBuilder<K, I, O> {
        MatchBuilder::new()
    }
}

impl<K, I, O> fmt::Debug for Match<K, I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Match")
            .field("key_type", &std::any::type_name::<K>())
            .field("input_type", &std::any::type_name::<I>())
            .field("output_type", &std::any::type_name::<O>())
            .field("cases", &self.cases.len())
            .field("has_default", &self.default.is_some())
            .finish()
    }
}

impl<K, I, O> Executable for Match<K, I, O>
where
    K: Eq + Send + Sync,
    I: Send,
    O: Send,
{
    type Input = (K, I);
    type Output = O;

    async fn execute(&self, runtime: &Runtime, input: (K, I)) -> Result<O, ExecutionError> {
        let (key, business_input) = input;
        // 只做等值比较，不移动键：路由不改写 Input，也不需要 K: Clone。
        let branch = match self.cases.iter().find(|(candidate, _)| candidate == &key) {
            Some((_, branch)) => branch,
            None => match &self.default {
                Some(branch) => branch,
                None => return Err(ExecutionError::new(NoMatch)),
            },
        };
        // 被选分支的实际执行重新经过父级传入的同一个 Runtime。
        branch.run(runtime, business_input).await
    }
}
