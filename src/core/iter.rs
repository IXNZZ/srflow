//! Iter：携带上一轮状态的顺序推进。
//!
//! [`Iter<Item, T, B>`](Iter) 按 Item 顺序反复执行**同一个 Body**：每轮 Body 的正常 Output 成为下一轮
//! Input 的状态部分，最终只返回最后的 `T`。
//!
//! ```text
//! Iter Input = (Vec<Item>, T0)
//! Body       = Executable<Input = (T, Item), Output = T>
//! Iter Output = T
//!
//! (T0, Item1) → Body → T1
//! (T1, Item2) → Body → T2
//! (T2, Item3) → Body → T3
//!                         → T3
//! ```
//!
//! ```
//! use srflow::{ExecutionError, Iter, Node, Runtime};
//!
//! /// 当前轮的 Item；不需要 `Clone`。
//! struct Key(String);
//!
//! /// 跨轮携带的状态；不需要 `Clone`，也不需要 `Default`。
//! struct Prose {
//!     plan: String,
//!     prose: String,
//! }
//!
//! struct Advance;
//! impl Node for Advance {
//!     type Input = (Prose, Key);
//!     type Output = Prose;
//!     async fn run(&self, input: (Prose, Key)) -> Result<Prose, ExecutionError> {
//!         let (mut prose, key) = input;
//!         // 按顺序累积：结果对 Item 顺序敏感。
//!         prose.prose.push_str(&key.0);
//!         Ok(prose)
//!     }
//! }
//!
//! let iter = Iter::new(Advance);
//! let runtime = Runtime::new();
//! let keys = vec![Key(String::from("a")), Key(String::from("b")), Key(String::from("c"))];
//! let initial = Prose {
//!     plan: String::from("plan"),
//!     prose: String::new(),
//! };
//!
//! let prose = futures::executor::block_on(runtime.execute(&iter, (keys, initial))).unwrap();
//! assert_eq!(prose.plan, "plan", "不变上下文一直可用");
//! assert_eq!(prose.prose, "abc", "每轮都能看到上一轮形成的状态");
//! ```
//!
//! # 状态推进与结果
//!
//! - 上一轮 Future **完成并产出 `T` 之后**才开始下一轮；不会预先执行所有 Item 再排序，也不并发。
//! - `n` 个 Item 全部正常完成时 Body 恰好执行 `n` 次，Output 是第 `n` 轮产生的 `Tn`。
//!   **核心 Output 只有最终状态**，不是 `Vec<T>` 或 `(T, Vec<T>)`；中间状态历史属于后续可能的
//!   独立变体，不在本实现中。
//! - `T` 是下一轮所需的完整累积状态，可以同时装入持续变化的内容和每轮仍需的不变上下文
//!   （例如 `Prose { plan, prose }`：`plan` 一直可用，`prose` 连续变化）。Iter 不把 `T` 拆成隐式的
//!   共享输入，也不额外引入共享上下文概念。
//!
//! # 正常业务状态不触发提前停止
//!
//! Iter 只看 Item 的数量，不看业务含义：某轮正常返回一个带“不通过／需修订”标记的状态，仍然会继续
//! 处理下一个 Item。业务上的否定结论应当由 `T` 或 Body 的正常 Output 表达。
//!
//! # 空集合
//!
//! `Items = []` 时 Body 执行 0 次，**按值返回原始 `T0`**：不制造默认状态，也不要求 `T: Default` 或
//! `T: Clone`。空集合不是技术错误；若业务认为没有 Item 是非法的，应在进入 Iter 之前明确检查。
//!
//! # 错误
//!
//! 某轮 Body 返回 [`ExecutionError`] 时立即原样传播：后续 Item 不再启动，此前成功产生的 `T` **既不**
//! 作为 Iter 的正常 Output **也不**作为新增的“部分成功”载荷返回（公开返回类型只有
//! `Result<T, ExecutionError>`）。Iter 不自动重试，也不回滚已经发生的外部副作用。
//!
//! 由于状态按所有权移动（见下），技术错误发生后 Iter **不保证**能把已经交给 Body 的那个 `T` 还回来；
//! 需要保留状态的调用方应让 Body 自己按业务规则处理。
//!
//! # 所有权与 bounds
//!
//! - `Vec<Item>` 与 `T0` 按值进入 Iter；每个 Item 只移动一次，`T0`／上一轮的 `T` 被移动给 Body，正常
//!   返回的 `T` 再成为下一轮输入。`Item`／`T`／Body 都**不要求 `Clone`**，也没有克隆 Body 来模拟迭代的
//!   路径。
//! - Body 在同一次执行中被反复借用，该借用跨越每轮的 `.await`，而 [`Executable::execute`] 的 Future
//!   必须是 `Send`，因此即使单独经 [`Runtime`] 执行也要求 **`Body: Sync`**。这是与 `Each` 相同的选定
//!   实现策略：不克隆 Body，也不用锁包装 Body。`Item`／`T` 只受 [`Executable`] 既有的 `Send` 约束，
//!   不被额外要求 `Sync`、`Clone`、`Default` 或 `'static`；内部类型标记不会把这些性质传给 Iter 本身。
//! - 作为父 Flow 的 child 时，另受 [`FlowBuilder::then`](crate::FlowBuilder::then) 的既有约束
//!   （`Iter<Item, T, B>: Send + Sync + 'static`、`(Vec<Item>, T)` 的 `'static`）。
//! - 每轮 Body 的实际执行都重新经过父级传入的同一个 [`Runtime`]；Iter 不直接调用
//!   [`Executable::execute`]，也不把循环放进 Runtime。
//! - 同一 Iter 定义可以重复调用，也可以被交叠轮询：当前 `T`、Item 位置与瞬时结果都是每次调用的局部
//!   状态，不保存在迭代器定义里。

use std::fmt;
use std::marker::PhantomData;

use crate::core::{Executable, ExecutionError, Runtime};

/// 携带上一轮状态顺序推进的第四个控制型 [`Executable`]。
///
/// 契约是 `(Vec<Item>, T) → T`，Body 是 `(T, Item) → T`；语义、顺序、错误与所有权见本模块文档。
///
/// # 类型错误的分支无法编译
///
/// Body 的 `Input` 必须是 `(T, Item)` 且 `Output` 必须是同一个 `T`：
///
/// ```compile_fail
/// use srflow::{ExecutionError, Iter, Node};
///
/// struct WrongOutput;
/// impl Node for WrongOutput {
///     type Input = (u32, String);
///     type Output = String;
///     async fn run(&self, (state, item): (u32, String)) -> Result<String, ExecutionError> {
///         Ok(format!("{state}{item}"))
///     }
/// }
///
/// // Body 的 Output 是 String，不是 Input 里的状态类型 u32，因此无法构造 Iter。
/// let _ = Iter::new(WrongOutput);
/// ```
///
/// 父 Flow 里的位置类型也必须匹配 `(Vec<Item>, T)`，顺序写反会被拒绝：
///
/// ```compile_fail
/// use srflow::{ExecutionError, FlowBuilder, Iter, Node};
///
/// struct Advance;
/// impl Node for Advance {
///     type Input = (u32, String);
///     type Output = u32;
///     async fn run(&self, (state, item): (u32, String)) -> Result<u32, ExecutionError> {
///         Ok(state + item.chars().count() as u32)
///     }
/// }
///
/// // Iter 的 Input 是 (Vec<Item>, T) = (Vec<String>, u32)，这里的位置是反过来写的。
/// let mut flow = FlowBuilder::<(u32, Vec<String>)>::new();
/// let input = flow.input();
/// let _ = flow.then_move(Iter::new(Advance), input);
/// ```
pub struct Iter<Item, T, B> {
    body: B,
    // `fn(Item) -> T` 无条件 `Send + Sync`：业务 Item／T 的 `Sync` 性质不会外溢到 Iter 本身。
    _marker: PhantomData<fn(Item) -> T>,
}

impl<Item, T, B> Iter<Item, T, B>
where
    B: Executable<Input = (T, Item), Output = T>,
{
    /// 用给定 Body 构造 Iter。
    ///
    /// `Item`／`T` 由 Body 的 `Executable` 契约约束，日常调用不需要 turbofish：
    ///
    /// ```
    /// use srflow::{ExecutionError, Iter, Node, Runtime};
    ///
    /// struct Advance;
    /// impl Node for Advance {
    ///     type Input = (u32, u32);
    ///     type Output = u32;
    ///     async fn run(&self, (state, item): (u32, u32)) -> Result<u32, ExecutionError> {
    ///         Ok(state + item)
    ///     }
    /// }
    ///
    /// let iter = Iter::new(Advance);
    /// let runtime = Runtime::new();
    /// let total = futures::executor::block_on(
    ///     runtime.execute(&iter, (vec![1, 2, 3], 0)),
    /// )
    /// .unwrap();
    /// assert_eq!(total, 6);
    /// ```
    ///
    /// 需要写类型注解或把 Iter 存进结构体时，写完整的三参数形式：
    /// `Iter<u32, u32, Advance>`。
    pub fn new(body: B) -> Self {
        Self {
            body,
            _marker: PhantomData,
        }
    }
}

impl<Item, T, B> fmt::Debug for Iter<Item, T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Iter")
            .field("body_type", &std::any::type_name::<B>())
            .finish_non_exhaustive()
    }
}

impl<Item, T, B> Executable for Iter<Item, T, B>
where
    B: Executable<Input = (T, Item), Output = T> + Sync,
    Item: Send,
    T: Send,
{
    type Input = (Vec<Item>, T);
    type Output = T;

    async fn execute(&self, runtime: &Runtime, input: (Vec<Item>, T)) -> Result<T, ExecutionError> {
        let (items, mut state) = input;
        // 逐轮串行：上一轮产出 T 之后才发起下一轮。
        for item in items {
            state = runtime.execute(&self.body, (state, item)).await?;
        }
        Ok(state)
    }
}
