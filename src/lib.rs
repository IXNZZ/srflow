//! SRFlow —— 以强类型 Flow 为中心的系统执行框架。
//!
//! 本 crate 当前处于 T07：提供统一异步执行基础（[`Runtime`]、[`Executable`]、[`Node`]）、最小 Flow
//! （[`FlowBuilder`]、[`Flow`]、[`Ref`]）、Binding（整值读取、字段投影、多值组合与命名结构装配），
//! 以及四种控制型 Executable：[`Retry`]（正常业务 Output 驱动的有限重做）、[`Match`]（依据已有
//! 路由值执行唯一分支）、[`Each`]（按顺序逐项执行并收集结果）与 [`Iter`]（携带上一轮状态的顺序推进）。
//!
//! # 四个角色
//!
//! - [`Runtime`]：统一的异步执行入口，所有实际执行都从 [`Runtime::execute`] 开始。
//! - [`Executable`]：统一的执行协议，一个具体执行边界有明确、强类型的 `Input` 与 `Output`。
//! - [`Node`]：叶子业务实现；业务开发者只实现 `Node`，框架自动把它接入 `Executable`。
//! - [`Flow`]：按声明顺序编排 Executable，并连接数据；它本身也是 `Executable`，因此可以
//!   直接作为另一个 Flow 的 child（SubFlow）。
//!
//! [`Binding`] 是 Flow 的数据连接机制，不是第五个角色：它在 child 启动前把已有数据装配成
//! child 的 Input，不进入 [`Runtime`]，也不产生执行记录。
//!
//! # 快速上手：只实现 Node
//!
//! ```
//! use srflow::{ExecutionError, Node, Runtime};
//!
//! /// 计算一个整数的平方。
//! struct Square;
//!
//! impl Node for Square {
//!     type Input = u32;
//!     type Output = u32;
//!
//!     async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
//!         Ok(input * input)
//!     }
//! }
//!
//! let runtime = Runtime::new();
//! let output = futures::executor::block_on(runtime.execute(&Square, 7)).unwrap();
//! assert_eq!(output, 49);
//! ```
//!
//! 文档与示例里的 `futures::executor::block_on` 只是用来驱动异步代码：`srflow` 本身不依赖
//! 任何 executor，使用者需要在自己的项目里选择并添加一个（`futures` 只是本仓库的开发依赖，
//! 不会随 `srflow` 提供给使用者）。
//!
//! # 编排：Flow
//!
//! `FlowBuilder` 收集执行顺序与数据连接，`output` 之后得到可执行的 [`Flow`]：
//!
//! ```
//! use srflow::{ExecutionError, FlowBuilder, Node, Runtime};
//!
//! struct Length;
//! impl Node for Length {
//!     type Input = String;
//!     type Output = usize;
//!     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
//!         Ok(input.chars().count())
//!     }
//! }
//!
//! struct Double;
//! impl Node for Double {
//!     type Input = usize;
//!     type Output = usize;
//!     async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
//!         Ok(input * 2)
//!     }
//! }
//!
//! let mut flow = FlowBuilder::<String>::new();
//! let input = flow.input();
//! let length = flow.then_move(Length, input).unwrap();
//! let doubled = flow.then_move(Double, length).unwrap();
//! let flow = flow.output(doubled).unwrap();
//!
//! let runtime = Runtime::new();
//! let output = futures::executor::block_on(runtime.execute(&flow, String::from("abcd"))).unwrap();
//! assert_eq!(output, 8);
//! ```
//!
//! 同一个位置要被多个步骤使用，用 [`FlowBuilder::then`]（复用读取，要求该 Input 实现
//! [`Clone`]）；把值交给某一步之后不再读取，用 [`consume`] 或 [`FlowBuilder::then_move`]
//! （消费读取，不要求 [`Clone`]）。`then` 的执行顺序就是执行顺序，与谁读取谁无关。
//!
//! # 装配 Input：Binding
//!
//! `then(executable, binding)` 的第二个参数描述 child 的 Input 如何从当前 Flow 已有数据中
//! 形成。除整值 [`Ref`] 外，还可以用 [`field!`] 投影字段、用 tuple 组合多个来源、用
//! [`bind!`] 命名装配业务结构（也可嵌套）：
//!
//! ```
//! use srflow::{bind, field, ExecutionError, FlowBuilder, Node, Runtime};
//!
//! // 根结构没有实现 Clone：字段投影仍然可以读取它。
//! struct Story {
//!     plan: String,
//!     background: String,
//! }
//!
//! struct ProgressionInput {
//!     plan: String,
//!     key: u32,
//!     background: String,
//! }
//!
//! struct MakeKey;
//! impl Node for MakeKey {
//!     type Input = String;
//!     type Output = u32;
//!     async fn run(&self, input: String) -> Result<u32, ExecutionError> {
//!         Ok(input.len() as u32)
//!     }
//! }
//!
//! struct Progression;
//! impl Node for Progression {
//!     type Input = ProgressionInput;
//!     type Output = String;
//!     async fn run(&self, input: ProgressionInput) -> Result<String, ExecutionError> {
//!         Ok(format!("{}/{}/{}", input.plan, input.key, input.background))
//!     }
//! }
//!
//! let mut flow = FlowBuilder::<Story>::new();
//! let story = flow.input();
//! let key = flow.then(MakeKey, field!(story.plan)).unwrap();
//! let input = bind!(ProgressionInput {
//!     plan: field!(story.plan),
//!     key: key,
//!     background: field!(story.background),
//! });
//! let result = flow.then(Progression, input).unwrap();
//! let flow = flow.output(result).unwrap();
//!
//! let runtime = Runtime::new();
//! let story = Story {
//!     plan: String::from("plan"),
//!     background: String::from("bg"),
//! };
//! let output = futures::executor::block_on(runtime.execute(&flow, story)).unwrap();
//! assert_eq!(output, "plan/4/bg");
//! ```
//!
//! Binding 只能读取、投影、组合与构造当前 Flow 已有的值，不承担业务计算；判断与计算属于
//! [`Node`]。形状、封闭性与 Flow 归属见 [`Binding`]、[`field!`]、[`bind!`]、[`consume`]。
//!
//! # Binding 的结构性边界
//!
//! [`Binding`] 是封闭 trait：它继承一个不可命名的私有 supertrait，业务侧无法为自定义类型实现
//! 它，因此不存在把任意计算实现成 Binding 的入口。字段投影与命名装配只通过 [`field!`]、
//! [`bind!`] 表达。
//!
//! 为让宏在业务 crate 中可用，框架保留了两个 `#[doc(hidden)]` 的最小构造入口
//! （[`__project_field`]、[`__assemble`]）。它们是**唯一的开放面**，而且都不封闭：
//!
//! - `__project_field` 的回调签名是 `fn(&Root) -> &F`。Rust 不能要求返回值**来自**根：忽略根、
//!   返回 `&'static F` 的回调同样满足该签名，因此直接调用这个入口可以**把当前 Flow 之外的
//!   数据注入下游**（已用外部探针验证：`String` 根的 Flow，回调返回静态 `u32`，下游收到该值）；
//! - `__assemble` 的回调是 `Fn(已解析字段) -> 结构`，直接调用它可以在回调里写任意计算。
//!
//! 两者都是本阶段**已知的剩余漏洞**：由本文档与代码评审承担，类型系统没有封死它们。`field!`／
//! `bind!` 宏本身只生成字段路径与字段装配，不会注入；但宏参数里写出满足 Binding 约束的表达式
//! 仍可能绕过约定，同样属于评审红线。因此不要直接调用这两个入口，请使用 [`field!`]、[`bind!`]。
//!
//! # 控制：Retry
//!
//! [`Retry`] 是第一个控制型 [`Executable`]：用**语义上相同的业务 Input** 有限次重做同一个 Body，
//! 每轮只根据 Body 的**正常 Output** 决定停止或重做。它和 Node、SubFlow 一样经
//! `flow.then(retry, binding)` 连接；每轮 Body 的实际执行都重新经过同一个 [`Runtime`]。
//!
//! ```
//! use std::num::NonZeroUsize;
//! use srflow::{ExecutionError, Node, Retry, RetryDecision, Runtime};
//!
//! /// Body 把“是否接受”放进正常 Output。
//! struct Attempt;
//! impl Node for Attempt {
//!     type Input = String;
//!     type Output = (String, bool);
//!     async fn run(&self, input: String) -> Result<(String, bool), ExecutionError> {
//!         Ok((input.clone(), input.len() >= 3))
//!     }
//! }
//!
//! let retry = Retry::with_limit(
//!     Attempt,
//!     // Condition 只读取已形成的判断字段。
//!     |output: &(String, bool)| {
//!         if output.1 { RetryDecision::Stop } else { RetryDecision::Retry }
//!     },
//!     NonZeroUsize::new(3).unwrap(),
//! );
//!
//! let runtime = Runtime::new();
//! let output = futures::executor::block_on(runtime.execute(&retry, String::from("abcd"))).unwrap();
//! assert_eq!(output, (String::from("abcd"), true));
//! ```
//!
//! 要点：
//!
//! - **do-while**：Body 至少执行一次；`limit` 是 Body 的**总执行次数上限**，默认 8。
//! - **`limit = 0` 不可表示**：`limit` 是 [`NonZeroUsize`](std::num::NonZeroUsize)。
//! - **三种结果**：某轮 Condition 返回 [`RetryDecision::Stop`] → 返回该轮 `O`；一直
//!   `RetryDecision::Retry` 直到用满上限 → 返回**最后一次正常 `O`**（不是技术 Error）；某轮 Body
//!   返回 [`ExecutionError`] → 立即原样传播，不调用 Condition、不执行后续轮次。耗尽只表示“用完了
//!   允许的尝试次数”，业务是否失败由 `O` 表达。
//! - **业务重做 ≠ 技术重试**：Retry 不做退避／延迟，也不把技术 `Error` 当作“需要重做”。
//! - **`I: Clone` 只局部施加**：重复执行需要若干个 owned Input，因此 Retry 要求 `I: Clone`（非
//!   `Clone` 的 Input 不能经 Retry）；`O` 不要求 `Clone`。复制次数为 `n − [n == limit]`，详细的成本
//!   说明见 [`Retry`]。
//! - **Condition 是只读局部判断**：不得成为复杂业务评分器；需要复杂判断时先由 Body 内的 Node 产出
//!   明确字段。`Fn`／`&O` 无法从类型系统禁止内部可变性，这条红线由文档与评审约束。
//!
//! 另见 `examples/retry`。
//!
//! # 控制：Match
//!
//! [`Match<K, I, O>`](Match) 依据一个**已经形成**的路由值 `K` 执行唯一分支：`Input = (K, I)`，
//! `Output = O`。路由值由上游产生（例如一个判断 Node），Match 不参与业务判断；不同分支可以是
//! 不同具体类型，但共享同一个 `I`／`O`。它和 Node、SubFlow 一样经 `flow.then(matcher, binding)`
//! 连接，被选分支的实际执行重新经过同一个 [`Runtime`]。
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
//! 要点：
//!
//! - **判断与路由分离**：`K` 由上游 Node、Flow Input 或其他已明确的位置提供；Match 不从 `I` 计算
//!   路由，也不接受隐藏的业务判断。
//! - **唯一执行路径**：一次执行最多调用一个分支，不按顺序试错；未选分支与 default 都不执行。
//! - **default 只表示未命中**：命中分支失败时错误原样传播，不改走 default。
//! - **未命中无 default**：返回以 [`NoMatch`] 为来源的 [`ExecutionError`]；外部可以
//!   `error.source()?.downcast_ref::<NoMatch>()` 按类型识别。空 case 集合同样成立。
//! - **三类错误分阶段**：重复 case 键、重复 default 在构建期以 [`MatchBuildError`] 拒绝（不静默
//!   覆盖、不留下部分登记），未命中是执行期 `NoMatch`，分支自身失败是分支的 [`ExecutionError`]。
//! - **bounds**：`K` 需要 `Eq + Send + Sync`（case 键直接存在 Match 中），`I`／`O` 只需既有
//!   `Send`；`K`、`I`、`O` 都不要求 `Clone`。作为 Flow child 时另受 `then` 的既有 `Send + Sync +
//!   'static` 约束。
//!
//! 另见 `examples/match`。
//!
//! # 控制：Each
//!
//! [`Each<B>`](Each) 按输入顺序对集合中的每个 Item 调用**同一个 Body**，把结果按对应顺序收集为
//! `Vec<O>`：契约是 `Vec<I> → Vec<O>`，`I`／`O` 由 Body 的 `Executable::Input`／`Output` 决定。
//! Body 可以是 Node、Flow 或其他合法 Executable；每个 Item 的实际调用都重新经过同一个 [`Runtime`]。
//!
//! ```
//! use srflow::{Each, ExecutionError, FlowBuilder, Node, Runtime};
//!
//! struct Length;
//! impl Node for Length {
//!     type Input = String;
//!     type Output = usize;
//!     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
//!         Ok(input.chars().count())
//!     }
//! }
//!
//! struct Total;
//! impl Node for Total {
//!     type Input = Vec<usize>;
//!     type Output = usize;
//!     async fn run(&self, input: Vec<usize>) -> Result<usize, ExecutionError> {
//!         Ok(input.into_iter().sum())
//!     }
//! }
//!
//! let runtime = Runtime::new();
//!
//! // 直接经 Runtime 执行。
//! let lengths = futures::executor::block_on(
//!     runtime.execute(&Each::new(Length), vec![String::from("ab"), String::from("cde")]),
//! )
//! .unwrap();
//! assert_eq!(lengths, vec![2, 3]);
//!
//! // 作为父 Flow 的普通 child：集合由 Flow Input 提供，`Vec<O>` 继续交给下游。
//! let mut flow = FlowBuilder::<Vec<String>>::new();
//! let input = flow.input();
//! // 消费读取：整个集合交给 Each，不对元素做额外克隆。
//! let lengths = flow.then_move(Each::new(Length), input).unwrap();
//! let total = flow.then_move(Total, lengths).unwrap();
//! let flow = flow.output(total).unwrap();
//!
//! let total = futures::executor::block_on(
//!     runtime.execute(&flow, vec![String::from("ab"), String::from("cde")]),
//! )
//! .unwrap();
//! assert_eq!(total, 5);
//! ```
//!
//! 要点：
//!
//! - **次数由集合限定**：全部正常完成时每个 Item 恰好调用 Body 一次；若某项出错，后续项不再执行。
//!   没有独立 `limit`。
//! - **严格顺序**：第 k 项完成之后才开始第 k+1 项；不会先启动多个子调用再排序结果，也不会并行。
//! - **空集合不是错误**：`[] → []`，Body 执行 0 次；若业务认为空集合非法，应由进入 Each 之前的检查表达。
//! - **第一处错误即传播**：出错项之后的 Item 不再启动，先前结果不作为正常 `Vec<O>` 返回，外部副作用也不
//!   自动回滚。
//! - **Each 本身不建立跨项数据关系**：`Body(I2)` 的 Input 不包含 `O1`；需要跨项推进请用 Iter，而不是用
//!   共享可变状态模拟。Body 仍可访问共享的外部资源，因此不宣称各项绝对独立。
//! - **bounds**：`Body: Executable + Sync`（同一次执行中反复借用 `&self.body` 并跨越 `.await`）；
//!   `I`／`O` 只受既有 `Send` 约束，且与 Body 一样都不要求 `Clone`。作为 Flow child 时另受 `then` 的
//!   既有 `Send + Sync + 'static` 约束。
//!
//! 另见 `examples/each`。
//!
//! # 控制：Iter
//!
//! [`Iter<Item, T, B>`](Iter) 把**上一轮的正常 Output 作为下一轮 Input 的状态部分**，按 Item 顺序
//! 推进，最终只返回最后的 `T`：契约是 `(Vec<Item>, T) → T`，Body 是 `(T, Item) → T`。它建立的是
//! 显式的 `PreviousOutput → NextInput` 关系（“后一项需要看到前一项已经形成的结果”），因此与
//! [`Each`] 的逐项独立处理不同。
//!
//! ```
//! use srflow::{ExecutionError, Iter, Node, Runtime};
//!
//! struct Key(String);
//!
//! struct Prose {
//!     plan: String,
//!     prose: String,
//! }
//!
//! struct Advance;
//! impl Node for Advance {
//!     type Input = (Prose, Key);
//!     type Output = Prose;
//!     async fn run(&self, (mut prose, key): (Prose, Key)) -> Result<Prose, ExecutionError> {
//!         prose.prose.push_str(&key.0);
//!         Ok(prose)
//!     }
//! }
//!
//! let runtime = Runtime::new();
//! // Item／状态都由用法推断，无需 turbofish。
//! let prose = futures::executor::block_on(runtime.execute(
//!     &Iter::new(Advance),
//!     (
//!         vec![Key(String::from("a")), Key(String::from("b"))],
//!         Prose {
//!             plan: String::from("plan"),
//!             prose: String::new(),
//!         },
//!     ),
//! ))
//! .unwrap();
//! assert_eq!(prose.plan, "plan");
//! assert_eq!(prose.prose, "ab");
//! ```
//!
//! 要点：
//!
//! - **状态推进**：`T0 → T1 → … → Tn`，每轮都能看到上一轮实际形成的状态；`T` 可以同时携带持续变化的
//!   内容和每轮仍需的不变上下文。
//! - **核心 Output 只有最终 `T`**：不是 `Vec<T>`，也不返回中间历史（历史变体按设计属于后续能力）。
//! - **正常业务状态不提前停止**：某轮正常返回带“不通过／需修订”标记的状态，仍会继续处理下一个 Item。
//! - **严格顺序**：上一轮完成并产出 `T` 之后才开始下一轮；每轮的实际执行都重新经过同一个 [`Runtime`]。
//! - **空集合**：Body 执行 0 次，按值返回原始 `T0`；不要求 `T: Default` 或 `T: Clone`，也不是错误。
//! - **错误**：立即原样传播，后续 Item 不启动，不返回部分成功的 `T`，也不自动回滚外部副作用。由于状态
//!   按所有权移动，出错时 Iter 不保证能把已交给 Body 的那个 `T` 返还给调用方。
//! - **bounds**：`Body: Executable + Sync`（同一次执行中反复借用 `&self.body` 并跨越 `.await`）；
//!   `Item`／`T` 只受既有 `Send` 约束，都不要求 `Clone`。作为 Flow child 时另受 `then` 的既有
//!   `Send + Sync + 'static` 约束。
//!
//! 另见 `examples/iter`。
//!
//! # 扩展：实现组合型 Executable
//!
//! 需要新增执行语义（而不是新增业务操作）时直接实现 [`Executable`]。组合型实现通过父级传入的
//! [`Runtime`] 执行 child，因此每一个 child 的实际调用仍然经过同一个执行入口：
//!
//! ```no_run
//! use srflow::{ExecutionError, Executable, Node, Runtime};
//!
//! struct WordCount;
//!
//! impl Node for WordCount {
//!     type Input = String;
//!     type Output = usize;
//!
//!     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
//!         Ok(input.split_whitespace().count())
//!     }
//! }
//!
//! /// 对父级只暴露 `String → usize`，内部通过 Runtime 调用 child。
//! struct WordCountDoubled;
//!
//! impl Executable for WordCountDoubled {
//!     type Input = String;
//!     type Output = usize;
//!
//!     async fn execute(&self, runtime: &Runtime, input: String) -> Result<usize, ExecutionError> {
//!         let words = runtime.execute(&WordCount, input).await?;
//!         Ok(words * 2)
//!     }
//! }
//! ```
//!
//! 可运行版本见 `examples/`。
//!
//! # 错误与业务结果
//!
//! 业务上的否定结论（`accepted: false`、“未通过”、“无候选”等）是正常 `Output`，用
//! `Ok(...)` 返回；技术执行失败是 [`ExecutionError::Failed`]，框架不变量被破坏是
//! [`ExecutionError::Invariant`]。错误按 fail-fast 规则向调用方传播并保留来源，Runtime 不自动
//! 重试、跳过、回滚或切换备用路径。
//!
//! 接线本身不合法（跨 Flow 的 `Ref`、重复取走同一个位置、同一 Binding 内既消费又复用同一位置）
//! 不会等到执行期才失败：它在构建期以 [`FlowBuildError`] 拒绝，也不会产出一个可执行的 Flow。
//!
//! # 执行不变量
//!
//! - 所有 Executable 的实际执行都经过 [`Runtime::execute`]，包括 Flow 内部的每一个 child。
//! - 组合型 Executable 调用 child 时必须重新经过同一个 Runtime：`Executable::execute` 接收
//!   父级传入的 `&Runtime`，child 只能用它来执行。
//! - [`Node`] 是叶子：它不接收 Runtime，也不能编排其他 Executable。
//! - [`Flow`] 只定义顺序与数据连接，不做业务判断与计算。
//! - `Input` 在一次调用中是只读业务事实；跨调用传递业务数据必须通过显式的 `Input`／`Output`。
//! - `Ref` 属于特定 Flow，只读；Flow 内部位置不会越过 Flow 边界暴露给父级。
//!
//! # 实现决策与约束
//!
//! - **异步**：[`Executable::execute`] 与 [`Node::run`] 返回 `impl Future<Output = ...> + Send`，
//!   实现者直接使用 `async fn` 即可。要求 `Send` 是为了让执行树能在多线程 executor 上运行；
//!   代价是 `Input`／`Output`（关联类型已声明 `Send`）以及实现者跨 `await` 持有的状态也需要
//!   `Send`。用 `async fn` 实现时 `&self` 会被捕获，因此还需要 `Self: Sync`。
//! - **不绑定 executor**：核心只表达 `Future`，普通依赖为空；文档、测试与示例用
//!   `futures::executor::block_on` 驱动，那只是开发依赖，使用者需要自己选择并添加 executor。
//! - **错误转换**：外部错误进入 [`ExecutionError`] 需要显式转换
//!   （`外部调用().map_err(ExecutionError::new)?`）；取舍记录见 [`ExecutionError`] 的文档。
//! - **`Input` 按值传入**：语义上仍然只读，避免把实现绑死在某个生命周期上；Flow 的数据复用由
//!   构建期选择的读取方式决定（见 [`FlowBuilder`]）。
//! - **Flow 的数据所有权**：每个位置的值在产生时以类型擦除的形式存入本次执行的值存储；
//!   复用读取按需要克隆，最后一次复用/消费读取直接移动原值；字段投影借用根、只复制目标字段，
//!   因此根不必实现 `Clone`（字段需要）。单消费者链路不复制业务值，复用链路只复制真正需要
//!   多份的那几次；非 `Clone` 值可以经 [`consume`]／[`FlowBuilder::then_move`]／`output` 直通。
//! - **Binding 的结构性边界**：[`Binding`] 是封闭 trait，只由框架定义的结构类型实现，公开 API
//!   不提供 `map(any_function)`；投影与装配经 [`field!`]、[`bind!`] 表达。但为让宏可用，底层
//!   保留了两个 `#[doc(hidden)]` 的公开构造入口，二者**都不封闭**：`__project_field` 可注入
//!   根外数据，`__assemble` 可在回调里写任意计算。这是本阶段已知、需由评审约束的剩余漏洞，
//!   详见“Binding 的结构性边界”一节。
//! - **Flow 能容纳的 Executable**：为了保存在同一个 Flow 里，child 需要 `Send + Sync +
//!   'static`，Input／Output 需要 `'static`。这比 T01 的 `Executable` 契约更严，但没有修改
//!   T01 的公共 trait：不属于这一范围的 Executable 仍可单独经 Runtime 执行。
//! - **trait 不是 object-safe**：`Executable` 使用 RPITIT（`impl Future`），因此不能构造
//!   `dyn Executable`。Flow 内部的类型擦除发生在框架自己的适配层，业务侧看不到 `dyn`、`Any`
//!   或弱类型值。
//! - **MSRV**：`rust-version = 1.85`，下限来自 edition 2024；执行协议本身只需要 Rust 1.75
//!   的 RPITIT 与 `+ Send`。理由与依赖选择记录在 `Cargo.toml` 注释中。
//!
//! 普通使用场景从 crate 根部取得上述公共契约；直接使用核心接口的扩展作者可以从 [`core`]
//! 模块进入。

pub mod core;

pub use crate::core::{
    Assemble, Binding, Consume, Each, Executable, ExecutionError, Field, Flow, FlowBuildError,
    FlowBuilder, InvariantError, Iter, Match, MatchBuildError, MatchBuilder, NoMatch, Node, Ref,
    Retry, RetryDecision, Runtime, consume,
};

// 供 `field!`／`bind!` 宏展开调用；不是公开契约，请勿直接使用。
#[doc(hidden)]
pub use crate::core::binding::{__assemble, __project_field};
