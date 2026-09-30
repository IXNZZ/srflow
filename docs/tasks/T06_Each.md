# T06 — Each：顺序逐项执行并收集结果

> 状态：**COMPLETED；2026-09-30 独立复审通过；G2 未关闭**。
> 前置：T01～T05 已通过；G1 已单独验收通过；G2 尚未关闭。按任务总览，T06 的能力依赖 T03，当前仓库以 T05 完成代码为实际起点。
> 建议实施基线：`39f7789`（T05 完成提交）；实施前须核对该提交与工作区差异，不覆盖既有用户改动。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T06；通过后不自动开始 T07，也不关闭 G2。

## 1. 任务目标与边界

实现控制型 `Executable`：`Each`。它接收一个 `Vec<I>`，按输入顺序对每个 `I` 调用**同一个 Body 定义**，把全部正常结果按对应顺序收集为 `Vec<O>`：

```text
Each(Body): Vec<I> → Vec<O>

[I1, I2, I3]
  ├─ Runtime.execute(Body, I1) → O1
  ├─ Runtime.execute(Body, I2) → O2
  └─ Runtime.execute(Body, I3) → O3
                              → [O1, O2, O3]
```

Body 满足 `Executable<Input = I, Output = O>`，可以是 Node、Flow 或其他已存在的合法 Executable。Each 自己也实现 Executable，可单独交给 Runtime，也可通过 `flow.then(each, binding)` 接入父 Flow。每个 Item 的 Body 调用都必须重新经过父级传入的**同一个 Runtime**；Runtime 不理解逐项处理语义。

**Each 不建立上一项 Output → 下一项 Input 的数据关系。** Body 可以显式持有资源或访问外部系统，但不得由 Each 把 `O1` 隐式传给处理 `I2` 的调用。需要跨项推进时属于 T07 Iter，不在本任务中用共享可变状态、回调或特殊 Binding 模拟。

本任务不实现 Iter、并行／有界并发 Each、limit、过滤、跳过失败项、流式输出、部分成功结果、自动技术重试、回滚、日志、Trace、持久化、配置系统、新 crate 或为此修改 Runtime／Flow／Binding 的既有语义。也不为演示 Each 提前实现真实 SES 或 LLM Node。

## 2. 执行前必须阅读与先行验证

1. 阅读根目录 `AGENTS.md`、[任务总览](README.md)及 G1 记录、[T01](T01_Async_Execution_Foundation.md)～[T05](T05_Match.md) 的验收记录。核对当前 `Executable`／`Runtime` 签名、`FlowBuilder::then`、`consume`、`ExecutionError`、现有控制型 Executable 的公共导出与文档方式。确认工作区原有改动归属。
2. 阅读[规范性设计](../SRFlow_Design_v2.0.md)的 §3.8～3.10、§4.2～4.8、§5.2～5.5、§7.4～7.7、§8.1～8.3、§9.1、§9.4、§9.6.2、§10.1～10.6、§10.12～10.13、§11.1～11.5、§11.9～11.10、§12.3、§13.3 第五轮及 §13.6 的 R-01～R-04、R-16、R-18～R-24。T07 的 §9.5 只用于确认边界，不实施 Iter。
3. 先做小型可编译公共用法验证：单独执行、作为父 Flow child 连接 `Vec<I>`、Body 为 SubFlow、非 `Clone` 的 Item／Output、`Send + !Sync` 业务值。比较至少两种简单构造形式（例如 `Each::new(body)` 与等价的工厂函数）并记录取舍；不要为单 Body 引入无必要的 builder、类型擦除或配置层。

## 3. 本任务交付

### 3.1 精确的执行语义

- 输入集合长度决定 Body 的调用次数；没有独立 `limit`。正常完成时，每个 Item **恰好调用 Body 一次**，得到一个对应 Output。Body 的完成顺序与 Item 顺序相同：`I1` 的异步调用完成之后才可开始 `I2`；不得先创建／轮询全部子 Future 再声称“结果按顺序收集”。
- `Input = []` 时不执行 Body，正常返回 `[]`。这不是错误，也不产生默认 Item／Output。
- 正常输出的长度与输入长度相同，位置一一对应；不得对结果排序、过滤、去重、提前停止或混入上一项 Output。业务上的“拒绝／不通过”若是正常结果，仍是一个 `O`，不能自动当作技术错误或跳过。
- 某项 Body 返回 `ExecutionError` 时立即原样向外传播；该项之后的 Item 不再启动。先前已产生的 `O` 不作为正常 `Vec<O>` 返回，不新增“部分成功”载荷；已发生的外部副作用不会自动回滚。不得把技术错误解释为“重试该项”或“继续下一项”。
- 同一个 Each 定义可重复调用，也可被交叠轮询；每次调用的输入迭代位置、暂存结果与错误路径彼此隔离。Body 可以显式持有资源，但 Each 不以隐藏状态承载跨项业务数据。

### 3.2 类型、所有权与异步边界

- 公共接口应自然表达 `Vec<I> → Vec<O>`，由 Body 的 `Executable::Input/Output` 决定元素类型；不把 `Vec` 中的元素拆成弱类型值、`Any`、字符串 key 或业务侧可见的类型擦除。
- T06 使用现有 owned Input 契约：接收 `Vec<I>` 后逐项**移动** `I` 给 Body，收集其按值返回的 `O`。Each 自己不得要求 `I` 或 `O` 实现 `Clone`；也不得仅为执行循环而复制 Item、Output 或 Body。非 `Clone` 的 Item／Output 必须可用。若作为 Flow child，非 `Clone` 的 `Vec<I>` 通过 `consume(ref)` 或等价的既有消费连接进入 Each；不要假称裸 `Ref<Vec<I>>` 可复用非 `Clone` 元素。
- **本任务采用同一 Body 的直接借用方案**：在一次 Each 调用中反复借用 `&self.body`，不要求或执行 Body 的 `Clone`。现有 `Executable::execute` Future 为 `Send`，该借用跨越逐项调用的 `.await`，因此本方案即使单独经 Runtime 执行也要求 `Body: Executable + Sync`。用 `Send + !Sync` Body 的成对外部探针核对这个约束。克隆 Body 会改变同一实例复用语义；用锁包装 Body 则引入锁粒度、取消安全与异步 Guard 的新取舍，本任务不为规避 `Sync` 添加这两种机制。**这是本任务选定方案的明确约束，不宣称任何可能的 Rust 实现都无法支持 `!Sync` Body。**作为 Flow child 还需满足 `FlowBuilder::then` 的 `E: Send + Sync + 'static` 及 Input／Output 的 `'static` 约束。不得把这些局部要求倒灌到所有 Node／Executable，也不得无条件要求业务 `I/O: Sync`；`I/O` 只受既有 `Send` 约束（作为 Flow child 再有 `'static`）。
- 保持异步但**顺序**。实现和测试应区分“依序调用了 `run`”与“前一项 Future 已完成后才开始下一项”。受控 Pending 测试应让第一项暂停，先断言第二项尚未启动；解除暂停后再断言事件日志严格为 `start(1), end(1), start(2), end(2), …`。这使提前启动下一项的实现直接失败，无须另造一个并发版 Each 作为反证。不要因使用 async 而引入并发调度、线程池、`join_all` 或 `FuturesUnordered`。
- 公开形状固定为仅由 Body 类型参数化的 `Each<B>`；`I/O` 由 `B: Executable` 的关联类型推导，不另加元素泛型。外部编译预验证已确认这种形状可表达 `Input = Vec<B::Input>`、`Output = Vec<B::Output>`。每次调用的结果容器应是局部状态，而非持久保存在 Each 定义里。

### 3.3 组合与公共文档

- 每项 Body 调用必须通过传入的 `Runtime::execute(&body, item)` 或语义等价的统一入口，不得直接调用 `Body::execute`。Body 为 Flow 时，预期链路是 `Runtime → Each → Runtime → Flow → Runtime → Node`。Each 作为父 Flow child 时仍由父 Flow 的 Runtime 调用，不新增特殊接线入口。
- 新增 Each 的使用者可见 Rustdoc、crate 首页说明与 README 示例入口。明确 `Vec<I> → Vec<O>`、顺序完成保证、空集合**不是错误**（若业务认为非法，应由进入 Each 之前的 Node 或明确检查表达）、第一处技术错误的传播与剩余项不执行、部分结果不作为正常 Output、外部副作用不回滚、无跨项 Output → Input 数据传递，以及实际所有权和 bounds。不得把各项描述为绝对“彼此独立”：Body 仍可访问共享资源；准确边界是 **Each 本身**不建立跨项数据关系。给出可运行的最小用法，不要求读者先理解内部实现。
- 新增至少一个离线可运行、循序渐进的 `each` 示例：先展示直接经 Runtime 执行，再展示作为父 Flow child 由现有 Binding 提供集合；至少一条路径使用非 `Clone` 元素的消费连接。示例应能让使用者一眼看出输出与输入顺序对应，以及为何该场景用 Each 而非 Iter。复杂的跨项积累示例留给 T07。
- 测试覆盖 Node Body、Flow Body、空／单项／多项、严格异步顺序、输出次序、第一项和中间项出错、错误来源与后续项未运行、非 `Clone` Item／Output／Body、`Send + !Sync` Item／Output、父 Flow child、同一 Each 实例的重复及交叠调用。另用 `Input = Output` 的 Body 记录实际收到的各 Item，同时返回与输入不同的值，证明上一项 Output 不回流。旧任务的测试、Rustdoc 和示例保持可运行。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | Each 是强类型 Executable，契约为 `Vec<I> → Vec<O>`；普通使用者只提供 Body | 独立使用者视角的 API 示例；Node 与 Flow 两种 Body 的编译及运行证据 |
| A02 | 输入决定调用次数：空为 0，非空每项恰好一次；无独立 limit；同一个 Body 实例被复用 | 空／单项／多项调用计数；持有非 `Clone` 的 `AtomicUsize` 等字段的 Body 处理三项后，其同一计数器为 3；公共签名不含 `B: Clone` |
| A03 | 前一项 Future 完成后才启动下一项，结果顺序与输入一致；不隐式传递上一项 Output | 第一项处于受控 Pending 时断言第二项未启动；放行后事件日志严格交替 `start(k), end(k), start(k+1)`，提前启动下一项会使断言失败。另用 `String → String` Body 记录 `a,b,c` 三个实际 Input，返回 `O(a),O(b),O(c)`，断言收到的仍是 `a,b,c` 而非上一项 Output；核对最终结果顺序 |
| A04 | 首项或中途技术错误立即原样传播，后续项不执行，先前结果不作为正常 Output 暴露 | 错误来源链与调用计数；错误后的 Item 未启动；没有部分正常输出类型／路径 |
| A05 | 每项 Body 经同一个 Runtime；Body 可为 SubFlow，Each 可为父 Flow child | `Runtime → Each → Runtime → Flow → Runtime → Node` 路径审查及嵌套测试；父 Flow Binding／Output 连接测试 |
| A06 | owned Item 逐项移动，无额外 Clone；非 `Clone` Item／Output／Body 可用；业务 `I/O` 不被额外要求 `Sync`，而直接借用方案的 Body 必须 `Sync` | 非 `Clone` Item／Output 正向探针；持有非 `Clone` 字段的 Body 实例与公开签名审查；`Send + !Sync` Item／Output 正向探针；`Send + !Sync` Body 在单独执行和 Flow child 两条路径上的负向探针，各配只差 `Sync` 性质的正向对照和实际诊断 |
| A07 | Body 的元素契约与父 Flow 集合 Binding、下游结果连接保持强类型；错接在编译期拒绝 | 错误的 `Vec<Item>` Binding 或错误的 `Vec<Output>` 下游连接，各配只差错误接线的正向对照；交接报告附实际 rustc 诊断码、摘要与位置，不仅依赖 `compile_fail`。`Each` 的元素类型由 Body 推导，不强求人为制造一个独立的“错误 Body 类型”入口 |
| A08 | 同一 Each 定义重复／交叠调用不串输入位置或部分结果 | 同一实例、不同输入的交错轮询与再次调用测试；用输入值区分两次调用并分别断言结果，Body 共用状态只用于总计数；审查每次调用的结果容器为局部状态 |
| A09 | Rustdoc、README、离线示例及现有能力均可用 | 文档构建、文档测试、旧示例与新增示例运行、全量回归、公共 API 形式比较 |
| A10 | 不加入 Iter、并行／有界并发、limit、部分成功、技术重试、日志、Trace、持久化、新 crate 或额外核心语义 | 代码改动范围、依赖与公开项审查 |

**关键复审风险：**只保证结果排列却提前启动多个子 Future；在循环里直接调用 Body 绕过 Runtime；中间错误后继续执行或返回部分 `Vec<O>`；为方便复用无条件克隆业务 Item；为了 `Send` Future 而把 `Sync` 加到 `I/O`；把共享状态形成的业务推进误报为 Each 的数据语义；`compile_fail` 因无关错误误通过。

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
cargo run --example <T06 Each 示例>
```

交接报告须列出实施基线 `39f7789` 与实际起点、修改文件、A01～A10 的对应证据、两种简单构造形式的比较、公开类型与实际 bounds、Input／Output 所有权路径、异步顺序的可复核测试、错误与部分结果处理、递归 Runtime 调用路径、正反向编译探针的实际诊断、依赖／MSRV 影响及未覆盖事项。不得自行把 T06 标为 `COMPLETED`、关闭 G2 或开始 T07。

## 6. 审查与开始规则

本任务书已由用户审定并完成实施。T06 经独立复审通过；G2 仍须在 T07 通过后单独检查 T04～T07 的组合执行语义。本任务通过不自动授权 T07 的实现。

## 7. 验收记录（2026-09-30）

**结论：T06 PASS；G2 未关闭。** 对照规范性设计 §9.4、§10.1～10.6、§13.6 的 R-01／R-02／R-16 复核：Each 对 `Vec<I>` 严格顺序执行同一个 Body，全部正常完成时返回对应的 `Vec<O>`；第一处执行错误立即向外传播，不启动后续项。未发现需要修改上位设计的偏差。

| 验收项 | 复审证据 |
| --- | --- |
| A01～A02 | `Each<B>` 的 `Input = Vec<B::Input>`、`Output = Vec<B::Output>` 保持强类型；Node 与 Flow Body 均可用。空集合零调用，单项／多项调用次数与输入长度一致；持有非 `Clone` 计数器的同一个 Body 实例处理三项后产出 `[1, 2, 3]`。 |
| A03～A04 | 受控 Pending 测试在第一项未完成时确认第二项尚未启动，并断言完整事件日志严格交替；`String → String` 测试确认上一项 Output 不回流。首项与中途技术错误均保留来源、停止后续项，也不返回部分正常结果。 |
| A05～A07 | 每项 Body 在实现中调用同一个 `Runtime::execute`；SubFlow Body 与父 Flow child 路径均通过。非 `Clone` 及 `Send + !Sync` 的 Item／Output 可用，直接借用 Body 方案的 `Sync` 限制如实说明；交接报告提供 Body 与集合／下游错接的成对编译探针及实际诊断。 |
| A08～A10 | 同一 Each 实例的交叠与重复调用各自保有输入位置和结果容器。Rustdoc、README、离线示例可用；未加入 Iter、并行、limit、部分成功、技术重试或新依赖。 |

独立复跑：`cargo fmt --all -- --check`、`cargo check --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets`（113 项通过）、`cargo test --doc`（30 项运行、15 项预期编译失败）、`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`，以及九个离线示例，全部通过。复审发现的两处文档偏差已修正：README 现在列出 Each；Each Rustdoc 把“每项恰好一次／不提前停止”限定为正常完成路径，与错误即停保持一致。

**保留边界：**`Body: Sync` 是本任务采用同一 Body 直接借用并返回 `Send` Future 的明确约束，不宣称所有可能实现都无法支持 `!Sync` Body。`Vec<O>` 按输入元素数预分配属于当前实现策略，不是框架语义。Rust 1.85 MSRV 仍待 T11 发布验收前独立实测。T06 通过**不自动关闭 G2**，也不授权 T07。
