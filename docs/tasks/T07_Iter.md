# T07 — Iter：携带上一轮状态的顺序推进

> 状态：**COMPLETED；2026-09-30 独立复审通过；G2 未关闭**。
> 前置：T01～T06 已通过；G1 已单独验收通过；G2 尚未关闭。
> 建议实施基线：`98df7af`（T06 完成提交）；实施前核对 HEAD 与工作区差异，不覆盖已有用户改动。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T07；通过后仍须**单独验收 G2**，不自动开始 T08。

## 1. 任务目标与边界

实现第四个控制型 Executable：`Iter`。它接收一组 Item 和一个初始状态 `T0`，按 Item 顺序反复执行**同一个 Body 定义**；每轮 Body 的正常 Output 成为下一轮 Input 的状态部分，最终只返回最后的 `T`：

```text
Iter Input = (Vec<Item>, T0)
Body       = Executable<Input = (T, Item), Output = T>
Iter Output = T

(T0, Item1) → Body → T1
(T1, Item2) → Body → T2
(T2, Item3) → Body → T3
                         → T3
```

这是显式的 `PreviousOutput → NextInput` 关系，与 T06 Each 的逐项独立装配不同；也不是 T04 Retry 对同一个原始 Input 的重做。Body 可以是 Node、Flow 或其他满足上述完整契约的 Executable。Iter 本身经 Runtime 执行，也可通过普通 `flow.then(iter, binding)` 接入父 Flow；**每一轮 Body 实际调用必须重新经过父级传入的同一个 Runtime**。

`T` 表示后续轮次所需的完整累积状态，可以同时装入持续变化的内容和每轮仍需的不变上下文（如 `ProseState { plan, prose }`）。不强迫每个字段变化，也不额外发明 `SharedContext` 核心概念。Iter 不从前一轮结果之外的隐藏状态推导下一轮业务 Input。

本任务不实现中间状态历史／`collect()`、提前停止条件、并行／有界并发、limit、技术故障重试、回滚、流式输出、恢复、日志、Trace、持久化、配置系统、真实 LLM Node 或新 crate。§9.5.8 允许将来按需求增加历史变体；T07 **显式推迟**它，不是修改上位设计。不得为 Iter 改写 Runtime、Executable、Flow、Binding、Retry、Match 或 Each 的既有语义。

## 2. 执行前必须阅读与先行验证

1. 阅读根目录 `AGENTS.md`、[任务总览](README.md)及 G1 记录、[T01](T01_Async_Execution_Foundation.md)～[T06](T06_Each.md) 的验收记录。核对当前 `Executable`／`Runtime`、`FlowBuilder::then`、tuple Binding／`consume`、错误传播、Each 的逐项 Runtime 调用及公开文档方式。确认 T06 完成提交是实施起点。
2. 阅读[规范性设计](../SRFlow_Design_v2.0.md)的 §3.8～3.10、§4.2～4.8、§5.2～5.5、§7.4～7.7、§8.1～8.3、§9.1、§9.5～9.6、§10.1～10.6、§10.12～10.13、§11.1～11.5、§11.9～11.10、§12.3、§13.3 第五轮及 §13.6 的 R-01～R-04、R-16～R-20、R-24。
3. **先复核已选公共形状的小型编译证据，再写完整实现。**T07 固定采用可命名的 `Iter<Item, T, B>`：Body 满足 `B: Executable<Input = (T, Item), Output = T>`；日常构造用 `Iter::new(body)`，不要求普通调用方手写 turbofish；把 Iter 存入结构体时可写完整三参数类型。`new` 只要求上述 Body 结构契约，不提前要求 `Sync`；`B: Sync` 放在 `Executable` 实现上。该实现还需显式写出 `Item: Send, T: Send`：这并非新增业务约束，而是当前编译器不会从 Body 的关联类型等式反推出这两个参数满足 `Executable` 的既有 `Send` 契约。执行者仍须从外部使用者视角实编译直接调用、具名字段／类型注解、父 Flow child 与错误接线。设计中的 `Iter<Item, T>` 是概念关系，不是 Rust 参数表。

   取舍依据：评审方外部编译探针确认三参数方案可用，且普通调用无须 turbofish。`Iter<B>` 加投影 trait 的朴素版本不能仅凭 `B: Step` 推出 `B: Executable`（E0277）；改为带自引用 `Executable` 约束的 supertrait 后虽可编译，却需新增公开协议并使诊断转向该投影。少一个类型参数不值得这些成本，本任务不采用，也不引入 `Any` 或业务侧可见的类型擦除。交接报告须记录这项已审定取舍，并核对所选形式的真实编译结果，不重新开放核心模型选择。

## 3. 本任务交付

### 3.1 执行与结果语义

- 入口固定为 `(Vec<Item>, T0)`，每轮 Body 固定接收 `(当前 T, 当前 Item)` 并正常返回下一个 `T`。按输入集合顺序执行；上一轮 Future **完成且产出 `T` 后**，才可启动下一轮。不能预先执行所有 Item、事后重新排序，也不能把同一个 `T0` 反复交给每轮。
- 输入含 `n` 个 Item 且全部正常完成时，Body 恰好执行 `n` 次，最终 Output 为第 `n` 轮产生的 `Tn`。核心 Output **仅**是最终 `T`，不是 `Vec<T>` 或 `(T, Vec<T>)`。业务上的“不通过／需修订”等合法结论仍由 `T` 或 Body 的正常 Output 表达，不自动停止或转成技术 Error。
- **正常业务状态不触发提前停止**：即使第二轮正常返回的 `T2` 带有 `rejected`／`needs_revision` 标记，第三轮仍必须运行，并将该标记按 Body 的业务规则带到最终 `T3`。测试须断言第三轮确实收到 `T2`、总执行次数等于 Item 数，而不只看最终类型。
- `Items = []` 时 Body 执行 0 次，**按值返回原始 `T0`**，不制造默认状态、不要求 `T: Default` 或 `Clone`；空集合不是技术错误。若业务认为无 Item 非法，应在进入 Iter 之前明确检查。
- 某轮 Body 返回 `ExecutionError` 时立即原样传播，后续 Item 不启动；此前成功的 `T` **不作为 Iter 的正常 Output 或框架新增的部分成功载荷返回**。Iter 不自动重试或回滚既有外部副作用。Body 可以自行定义错误来源，但 Iter 不替它包装“最后成功状态”。
- 同一 Iter 定义可以重复调用和交叠轮询；每次调用的当前 `T`、Item 位置与瞬时结果独立。Body 可显式持有客户端／缓存等资源，但这不成为 Iter 的隐式跨轮业务数据通道。

### 3.2 所有权、类型与异步边界

- `Vec<Item>`、`T0` 均按值进入 Iter。每个 Item 只移动一次；`T0` 或前一轮 `T` 被移动给 Body，正常返回的 `T` 再成为下一轮输入。**不得为了推进要求 `Item: Clone` 或 `T: Clone`**，也不得克隆／重建 Body 来模拟迭代。非 `Clone` Item 与非 `Clone` 状态必须走通，包含空集合直接返回初始非 `Clone` 状态。此 owned 路径决定技术错误时 Iter 不保证还能取回已交给 Body 的 `T`。
- 公开接线必须让编译器保证 Body 的 `Input = (T, Item)` 且 `Output = T`，以及 Iter 的 `Input = (Vec<Item>, T)`、`Output = T`。错误的 Body Input／Output、父 Flow Binding tuple 顺序或下游状态类型应在编译期拒绝。负向探针须有只差错误接线的正向对照和实际诊断，不只依赖 `compile_fail`。
- 公开三参数结构必须引用 `Item/T`，否则会报 E0392。类型标记固定采用 `PhantomData<fn(Item) -> T>`，使标记本身无条件 `Send + Sync`；**不得**使用 `PhantomData<(Item, T)>` 等会把业务 Item／T 的 `Sync` 性质传递给 Iter 自身的标记。外部反证已显示后者使 `Send + !Sync` 的业务值被错误拒绝。除 `Executable` 对输入输出既有的 `Send` 外，不得给 Item／T 无条件增加 `Sync`、`Clone`、`Default` 或 `'static`；作为父 Flow child 时另有 Flow 的 `'static` 限制。用外部正向探针同时覆盖单独执行与作为 Flow child 的 `Send + !Sync` Item／T。
- T07 优先沿用 T06 的**同一 Body 直接借用**方案：每轮通过 `&self.body` 调用并等待，不要求 Body `Clone`。按当前 `Send` Future 契约，这一方案要求 `Body: Sync`，单独经 Runtime 执行也成立；作为 Flow child 时再受现有的 `Send + Sync + 'static` 约束。这是选定实现策略的局部限制，不宣称任何可能的实现都无法支持 `!Sync` Body。用非 `Clone` Body 和 `Send + !Sync` Body 的正反向外部探针核实，不把 Body 约束倒灌到业务 Item／T。
- 实现须区分“按顺序轮询”与“前一轮确实完成后才启动下一轮”。用受控 Pending 的 Body 验证第一轮未完成时第二轮未启动，放行后事件日志为 `start(1), end(1), start(2), end(2), …`；同时断言第二轮收到**第一轮实际返回**的 T，不是旧状态或由隐藏计数器计算的替代值。不要用并发 Future 集合或在 Runtime 中新增循环控制。

### 3.3 组合、文档与示例

- 每轮只通过传入的 `Runtime::execute(&body, (current, item))` 或语义等价的统一入口调用 Body，不得直接调用 `Body::execute`。Body 为 Flow 时应保留 `Runtime → Iter → Runtime → Flow → Runtime → Node`；Iter 在父 Flow 中只是普通 child。
- 父 Flow 用现有 Binding 把两个**不同数据位置**的 `Vec<Item>` 与 `T0` 结构性组合成 `(Vec<Item>, T)`；至少验证一条 `(consume(items_ref), consume(initial_ref))` 之类的非 `Clone` 接线，并验证错误 tuple 顺序被拒绝。同一根位置不能为了装出两个 owned 值而重复消费；不要为 Iter 新增 Binding 特权入口。若父 Flow 的 Input 已是完整 `(Vec<Item>, T)`，也可直接用 `then_move` 交给 Iter，但不能让它替代多 Ref 组合的验证。
- 新增公共 Rustdoc、crate 首页说明和 README 示例入口，准确解释 `T0 → T1 → … → Tn`、空集合返回 `T0`、正常业务状态与技术 Error、严格顺序、无历史结果、所有权与实际 bounds；**技术错误时，已移给 Body 的状态不能保证由 Iter 返还**，不能暗示调用方仍持有原来的 T。不要把业务正文状态的结构固定成框架类型；`T` 可携带不变上下文，但 Iter 不把它拆成隐式共享输入。为与 Flow／Match／Each 的公开面一致，提供不要求 `Item/T/B: Debug` 的轻量 `Debug` 表示（例如只打印 Body 类型名），不暴露业务状态。
- 新增一个离线可运行、渐进的 `iter` 示例：先展示直接执行，再展示作为父 Flow child；使用 Fake Node／Flow 模拟“关键节点逐项推进正文”，例如每轮以 `ProseState { plan, prose }` 和当前 KeyNode 形成新状态，验证 `plan` 保持可用而 `prose` 连续变化。累积方式应**对 Item 顺序敏感**（例如按顺序追加节点文本），避免求和等交换律运算掩盖乱序。至少一条路径使用非 `Clone` Item／状态；示例不访问 SES 仓库、真实模型或外部服务。另展示空集合的自然结果，错误路径可留给测试。
- 测试覆盖 Node 与 SubFlow Body、空／单项／多项、显式跨轮状态传递、严格异步顺序、非 `Clone` Item／T／Body、`Send + !Sync` Item／T、首轮与中途错误及后续项不启动、父 Flow 的多 Ref Binding、同一实例重复及交叠执行。旧任务测试、文档测试与九个离线示例保持通过。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | `Iter<Item, T, B>` 是强类型 Executable：`(Vec<Item>, T) → T`，Body 为 `(T, Item) → T`；公开类型可命名，`Iter::new(body)` 可自然推导 | 外部直接调用、具名字段／类型注解、父 Flow child 的编译与运行；交接报告记录三参数方案与被否决投影 trait 方案的已审定取舍及诊断，不新增公共 Step trait |
| A02 | 每轮收到上轮实际 Output 与当前 Item，最后只返回最终 T；T 可包含不变上下文，正常业务标记不触发提前停止 | 至少三轮**顺序敏感**状态变换；逐轮记录输入／输出摘要（不克隆 T），断言 `T0 → T1 → T2 → T3` 并检查不变字段；第二轮正常返回 `rejected`／`needs_revision` 状态，第三轮仍收到该状态并执行，总轮数等于 Item 数；无历史输出公共路径 |
| A03 | 空集合零调用并移动返回同一个初始 T，不要求 `Default`／`Clone` | 非 `Clone`、非 `Default` 状态的空集合测试；Body 调用计数为 0；结果保留初始状态 |
| A04 | 严格异步顺序：上一轮 Future 完成之后才开始下一轮 | 受控 Pending 测试在第一轮挂起时断言下一轮未启动，放行后事件严格交替；下一轮状态值同时由前轮 Output 证明 |
| A05 | 首轮／中途错误原样传播，后续 Item 不启动，也不返回部分正常 T | 错误来源链、轮次日志与后续项零调用；审查公开返回类型及无部分成功路径；不宣称回滚 |
| A06 | 每轮 Body 经同一个 Runtime；Body 可为 SubFlow，Iter 可为父 Flow child | `Runtime → Iter → Runtime → Flow → Runtime → Node` 路径审查及嵌套测试；父 Flow 的 Binding／Output 接线 |
| A07 | 非 `Clone` Item／T／Body 可用，业务 Item／T 不被额外要求 `Sync`；类型标记不污染 `Send/Sync`，Body 的实际 bounds 明确 | 非 `Clone` 正向探针（含多轮及空集合）、`Send + !Sync` Item／T 在单独执行和 Flow child 两路径的正向探针；核对使用 `PhantomData<fn(Item) -> T>`，记录 `PhantomData<(Item, T)>` 会误添 `Sync` 的反证；`Send + !Sync` Body 两路径的负向探针各配只差 Sync 性质的正向对照与真实诊断 |
| A08 | Body、Binding tuple 顺序及下游状态错接在编译期被拒绝，且原因确为类型不匹配 | 三类负向探针各配只差错误接线的正向对照；交接报告附 rustc 诊断码、摘要、位置。Body 的 `Input/Output` 必须是同一个 T |
| A09 | 同一 Iter 实例的重复／交叠调用隔离；文档与示例可供外部使用者直接学习 | 用各自 Input 标识区分两次交错执行，不依赖 Body 共享计数器区分调用；文档测试、示例运行及全量回归 |
| A10 | 没有实现历史收集、提前停止、并发、limit、技术重试、回滚、日志、Trace、持久化、新 crate 或修改既有核心语义 | 改动范围、依赖与公开项审查；将 §9.5.8 的历史变体记录为显式推迟 |

**关键复审风险：**照搬 `Each<B>` 而在 Iter 的泛型推导上制造难用 API；下一轮误用 `T0` 或 Body 内部状态而不是上一轮 `T`；为维持 T 的所有权悄悄要求 `Clone`；把正常业务状态当成提前停止条件；错误后返回最后成功状态；预启动后续 Future；绕过 Runtime；为类型标记把 `Sync` 外溢到 Item／T；只做 `compile_fail` 却未确认失败原因。

## 5. 验证与交接要求

至少运行并报告：

```text
cargo fmt --all -- --check
cargo check --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo test --doc
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo run --example node_only
cargo run --example composite_executable
cargo run --example basic_flow
cargo run --example subflow
cargo run --example binding_projection
cargo run --example binding_assembly
cargo run --example retry
cargo run --example match
cargo run --example each
cargo run --example <T07 Iter 示例>
```

交接报告须列出实施基线 `98df7af` 与实际起点、修改文件、A01～A10 的对应证据、公共泛型／构造形式比较和真实 bounds、owned 状态移动路径、受控 Pending 顺序与真实状态回流证据、错误／空集合处理、递归 Runtime 调用路径、正反向编译探针诊断、依赖／MSRV 影响及未覆盖事项。执行者不得自行把 T07 标为 `COMPLETED`、关闭 G2 或开始 T08。

## 6. 审查与开始规则

本任务书已由用户审定并完成实施。T07 经独立复审通过；G2 仍须对 T04～T07 的控制语义和组合边界另做独立验收。本任务通过不自动开始 T08。

## 7. 验收记录（2026-09-30）

**结论：T07 PASS；G2 未关闭。** 对照规范性设计 §9.5、§10.6、§13.6 的 R-01／R-02／R-17～R-20 复核：Iter 按 Item 顺序把上一轮正常 Output 作为下一轮状态 Input，最终只返回 `T`；错误立即传播，不启动后续轮次。未发现需要修改上位设计的偏差。

| 验收项 | 复审证据 |
| --- | --- |
| A01～A03 | `Iter<Item, T, B>` 的 `Input = (Vec<Item>, T)`、`Output = T` 保持强类型，Body 契约为 `(T, Item) → T`。Node 与 Flow Body、具名类型及父 Flow child 均可用；三轮顺序敏感累积证明真实状态回流，不变上下文得以保留。正常的 `needs_revision` 状态不提前停止；空集合零调用并按值返回非 `Clone`、非 `Default` 的初始状态。三参数公开形状不引入额外投影 trait。 |
| A04～A06 | 受控 Pending 测试确认前一轮完成前不启动下一轮，并核查下一轮收到前轮实际 Output。首轮与中途错误均保留来源、停止后续项且不返回部分正常结果。每轮通过同一个 `Runtime::execute` 调用 Body；SubFlow Body 和父 Flow 的双位置 `consume` Binding 路径均通过。 |
| A07～A08 | 非 `Clone` Item／状态／Body 与 `Send + !Sync` Item／状态均可用；Body 的 `Sync` 约束如实记录。`PhantomData<fn(Item) -> T>` 不把业务值的 `Sync` 约束传给 Iter。交接报告中的 Body、Binding 顺序和下游类型错接均有成对编译探针及实际诊断。 |
| A09～A10 | 同一 Iter 实例的重复及交叠调用各自保有状态和 Item 位置。Rustdoc、README、离线示例可用；未加入历史收集、提前停止、并发、limit、技术重试、回滚或新依赖。 |

独立复跑：`cargo fmt --all -- --check`、`cargo check --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets`（125 项通过）、`cargo test --doc`（33 项运行、17 项预期编译失败）、`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`，以及十个离线示例，全部通过。复审期间修正了 README 的待审表述、测试中以 Body 内部计数器形成业务状态的问题、异步测试轮询边界，以及误称 `Box<[u8]>` 不可 `Clone` 的证据标记；最终测试用 `AtomicUsize` 字段明确证明 Body 不实现 `Clone`。

**保留边界：**`Body: Sync` 是当前同一 Body 直接借用并返回 `Send` Future 的实现约束，不宣称所有可能实现都无法支持 `!Sync` Body。Rust 1.85 MSRV 仍待 T11 发布验收前独立实测。T07 通过**不自动关闭 G2**，也不授权 T08。
