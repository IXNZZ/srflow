# T02 — 最小 Flow、Ref 与执行期值存储

> 状态：**COMPLETED；实现与复审通过**。
> 前置：T01 已于 2026-09-28 复审通过；G1 尚未关闭。
> 建议实施基线：`983fae1`（T01 完成提交）。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T02；**不关闭 G1**，不自动开始 T03。

## 1. 任务目标

在 T01 的异步 `Runtime`／`Executable`／`Node` 契约上，建立第一个真正可执行的 Flow：

```text
Flow Input → Ref<I>
                  │
                  ├→ then(Node A, Ref<I>) → Ref<A>
                  └→ then(Node B, Ref<A>) → Ref<B>
                                          ↓
                                    output(Ref<B>)
                                          ↓
                           Runtime.execute(Flow, I)
                                          ↓
                                           B
```

要验证的不是某一种容器或 trait-object 写法，而是：强类型外部连接、异构 child、严格顺序执行、Flow-local 的只读 Ref、显式 Output、递归 Runtime 调用和每次执行独立的数据空间能否在现有异步契约下同时成立。

**范围分界：**T02 只实现最小的“整值 `Ref<T>` → `T`”输入连接；它是 Binding 的第一种形态，但不在本任务设计完整 Binding API。字段投影、多 Ref 组合、命名结构装配及嵌套装配属于 T03。不得为完成 T02 先发明任意闭包映射、胶水 Node 或宏 DSL。

## 2. 执行前必须阅读

1. 仓库根目录的 `AGENTS.md`、[任务总览](README.md)、[T01 验收记录](T01_Async_Execution_Foundation.md#7-验收记录2026-09-28)，以及当前 `src/core/` 公共接口与 crate 首页 Rustdoc。
2. [SRFlow 规范性设计](../SRFlow_Design_v2.0.md)的 §0.3～0.4、§3.4、§3.8～3.10、§5.3～5.4、§7 全章、§8.1～8.3 与 §8.7～8.10、§10.1～10.2、§10.5、§10.9～10.11、§11.1～11.6、§11.8～11.10、§12.3，以及 §13.6 的 R-01～R-10、R-18、R-21～R-24。

设计语义高于本任务书和历史 Compile Probe。若 T01 的具体签名妨碍实现，先给出最小复现和替代方案；可以调整实现 API，但不能静默改变上位设计的执行、数据或错误语义。

## 3. 本任务交付

### 3.1 Flow 构建与强类型连接

- 在 `src/core/` 增加职责清晰的 Flow／Ref／执行期存储实现，并从 crate 根部提供普通使用者需要的公共入口。保留单 crate 结构，不创建第二个 crate、空壳扩展模块或独立 Runtime 入口。
- Flow 构建时能取得代表自身 Input 的 `Ref<I>`。`then(executable, source)` 将不同具体类型、不同 Input／Output 类型的 Executable 按调用顺序加入 Flow，并返回该 child Output 的 `Ref<O>`。T02 的 `source` 只需支持整值 Ref；类型关系必须由编译器检查，错误类型连接不得退化为运行时 downcast 失败。
- `output(ref)` 显式选定最终输出类型，形成可被 Runtime 执行的 `Flow<I, O>`。输出可选任一步骤产生的 Ref，也可直接选 Flow Input；没有合法 Output 的构建中 Flow 不得作为完整 Executable 执行。是否使用 typestate、builder 或其他形式由实现决定，但需记录 API 取舍。
- `Ref<T>` 是数据位置的句柄，不是业务值；它不暴露可变访问，可被多个后续步骤引用，并在同一次 Flow 执行中指向同一已声明的数据位置。执行顺序与数据依赖独立：后续步骤即使不读取前一步 Output，也不得被跳过、重排或自动并行。
- 对同一 Flow 内尚未声明或已失效的位置不得提供可构造的正常 Ref；构建失败不得产生包含错误连接的可执行 Flow，builder 在失败后如何恢复由实现者明确。不要把 slot、`Any`、框架内部存储类型暴露给业务侧。

以下只是验收意图示意，不冻结构造器名、`Result` 位置或泛型形式：

```rust,ignore
let mut flow = Flow::<String>::new();
let input = flow.input();
let length = flow.then(LengthNode, input)?;
let checked = flow.then(CheckNode, length)?;
let flow = flow.output(checked)?;
let result = runtime.execute(&flow, String::from("example")).await?;
```

### 3.2 Ref 归属与构建期拒绝

- 每个 Ref 至少在内部保持类型、所属 Flow 和数据位置三种语义。把 Flow A 的 Ref 用在 Flow B 的 `then` 或 `output` 时，必须**在构建阶段**拒绝；两边内部 slot 恰好相同、Rust 类型也相同，仍必须失败。
- 拒绝方式应是业务侧可诊断的公开构建错误或等价的可恢复结果；不要静默接受，也不要把这类用户接线错误伪装为 child 执行失败。构建错误与 T01 的 `ExecutionError` 如何分工由实现者提出并记录，避免为了 T02 预建完整错误分类体系。
- `Ref` 的字段和任何可绕过归属检查的内部构造器不得公开。若内部类型擦除导致本应由强类型 API 保证的 downcast 失败，应作为框架不变量破坏处理，而不是普通业务否定 Output。

### 3.3 执行链、异构步骤与 SubFlow

- 完成后的 Flow 实现 `Executable<Input = I, Output = O>`；顶层由 `Runtime::execute` 发起，每个内部 child 都重新经同一个 Runtime 执行。Flow 自己负责顺序、整值数据连接和最终 Output，不让 Runtime 理解 Flow 内部步骤。
- Flow 必须能在内部保存不同具体类型、不同 Input／Output 的 child；内部可以有限类型擦除，但业务侧的 `then`、Ref 与最终 Flow Input／Output 保持强类型。
- Flow 可以作为父 Flow 的普通 child，无额外 SubFlow primitive。父级只能连接 ChildFlow 的公开 Input／Output，不能引用 ChildFlow 内部 Ref。嵌套执行仍须逐层经 Runtime。
- 任一步骤失败时，Flow fail-fast：后续 child 不执行，不返回部分正常 Output；已发生的外部副作用不声称回滚。正常业务否定仍属于 Output。合法构建的整值 Ref 不应在运行期解析失败；若内部存储不变量被破坏，必须在 child 启动前终止，不得伪造默认输入或业务 Output。

### 3.4 值存储与所有权取舍

- 每次 Flow 执行都创建独立的瞬时值存储；同一个 Flow 可顺序重复调用，也可有交叠的异步调用，调用间不得串值。业务 Input／Output 不得被 Flow 暗中修改。
- 选择一套**本阶段可用、可说明成本**的整值读取／复用策略，并比较 `Clone`、`Arc`、借用或所有权转移等候选。重点说明：同一 Ref 两次供下游使用时发生了什么；大对象复制代价在哪里；为什么所选方案能与 T01 的 owned Input、`Send` Future 和 T03 的结构性装配继续兼容。
- 不把 `Clone`、`Sync`、`'static` 等约束无说明地加到所有公开 Input／Output 上。至少用一个非 `Clone` 值验证无需复用时的直通或最终输出路径；若某些复用路径确实要求额外 trait bound，应局部呈现、明确文档化，并说明 T03 是否需要调整。
- 检查异构存储对 T01 合法 Executable 的组合能力：尤其是 `Send`／`Sync`、对象生命周期和 async Future 的要求。若 Flow 只能容纳其中一部分实现形态，提供可复核的编译证据并解释限制；不得为了让 Flow 编译而悄悄收紧 T01 的公共契约。
- ValueStore 的具体结构、slot 编号、内部擦除适配器与性能优化不作为设计规范。避免默认使用全局可变业务存储、跨 Flow 共享可变值或需要业务方传入 `Any`／字符串 key 的 API。

### 3.5 文档、示例与测试

- 为新增公共类型、方法与错误写面向 docs.rs 的 Rustdoc：说明 Flow 的构建／执行边界、顺序、Ref 归属、整值复用的所有权成本、错误发生阶段，并给出可运行的最短用法。更新 crate 首页和 README，让使用者清楚知道 T02 已提供什么、T03 尚未提供什么。
- 至少提供一个可离线运行的基础 Flow 示例，展示 Ref 复用与显式 Output；再提供一个简短的 SubFlow 示例（可作为同一文件中独立函数，但不能只靠复杂大示例解释）。既有 T01 示例保持可运行。
- 从独立 crate 使用者视角覆盖正常、边界、失败和编译期拒绝路径。编译期错误连接与未完成 Flow 不可执行要有可复核证据；跨 Flow 拒绝、fail-fast、重复／交叠执行和 SubFlow 隔离要有自动测试。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | 单 crate 分层仍清楚；公共 Flow／Ref API 强类型且没有泄漏 `Any`／slot；未完成 Flow 不可正常执行 | 公开 API 审查、编译失败证据、目录与依赖审查 |
| A02 | `then` 可按声明顺序保存并执行异构 child；整值 Ref 明确连接 Input，不存在隐式“上一输出自动输入” | 独立使用者测试、不同类型 Node 链和与数据依赖无关的顺序测试 |
| A03 | Ref 只读可复用；非 `Clone` 值至少有一次直通／最终输出路径；复用成本及额外 bounds 被准确记录 | 公共 API、运行测试、编译证据与所有权策略记录 |
| A04 | 同 slot、同类型的跨 Flow Ref 在 `then` 和 `output` 都于构建阶段被拒绝；不能通过公开 API 伪造归属 | 失败测试、公开构造器与私有字段审查 |
| A05 | `output(ref)` 确定强类型 `Flow<I, O>`；可直接输出 Flow Input；也可选择非最后一步的已声明 Output | 编译与运行测试，包括非最后一步作为 Output 的情形 |
| A06 | 内部 child 与嵌套 SubFlow 的每次实际执行都经 Runtime；父 Flow 无法引用子 Flow 内部 Ref | 执行路径源码审查、嵌套测试、归属拒绝测试 |
| A07 | 步骤失败立即停止；内部输入解析若遭遇不变量错误，也必须先于 child 执行而终止；不返回部分正常 Output、不自动回滚 | 步骤失败与副作用计数测试；不变量路径可用内部定向测试或源码审查 |
| A08 | 同一 Flow 重复及交叠调用时值存储互不串扰；执行期没有共享可变业务数据通道 | 不同 Input 的顺序／交叠运行测试与存储生命周期审查 |
| A09 | Rustdoc、README、基础 Flow／SubFlow 离线示例准确且能运行；T01 示例和测试不回归 | 文档构建、文档测试、示例运行、全量回归 |
| A10 | 没有提前实现字段投影、多 Ref／命名结构装配、控制型 Executable、LLM、日志、Trace、持久化或并行语义 | 改动范围和依赖审查 |

**审查重点：**A03 与 A04 不能只靠 happy-path 测试。非 `Clone`、跨 Flow 同 slot 同类型、异构 Future 的 `Send` 约束，都是 T02 对正式 API 的关键压力点。A06 不能靠 child 直接调用 `Executable::execute` 冒充递归 Runtime。A08 的“交叠”是两次 Flow 调用的隔离性验证，不允许因此把单次 Flow 内步骤改为并行。

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
cargo run --example <T02 新增基础 Flow 示例>
cargo run --example <T02 新增 SubFlow 示例（若独立文件）>
```

所有自动测试与示例必须离线运行，不访问真实模型、外部服务或 SES 仓库。交接报告逐项列出 A01～A10 的代码／测试证据、基于的提交、修改文件、依赖变化、值存储与错误 API 的取舍、编译期拒绝证据、性能成本边界及未覆盖事项。执行者不得自行把 T02 标为 `COMPLETED`、关闭 G1 或开始 T03。

## 6. 审查与开始规则

本任务书已获用户审定，可交给 OMP、Claude 或其他执行者实施。审查者按 §4 独立复核：未通过则修订同一任务；通过后更新本任务和总览的状态与验收记录，再写 T03 详细任务书。T02 单独通过仍不关闭 G1，G1 需等 T03 也通过后再审。

## 7. 验收记录（2026-09-29）

**结论：T02 PASS。** 对照规范性设计 §3.4、§3.8～3.10、§5.3～5.4、§7、§8.1～8.3、§8.7～8.10、§10.1～10.2、§10.5、§10.9～10.11、§11.1～11.6、§11.8～11.10、§12.3 与 §13.6 复核，未发现需要修改上位设计的偏差。

| 验收项 | 复审证据 |
| --- | --- |
| A01～A02 | `FlowBuilder` 不可执行，`output` 才形成强类型 `Flow<I, O>`；异构 child 按 `then` 声明顺序执行，输入来源均为显式 Ref。公开 API 不泄漏 slot、`Any` 或值存储。 |
| A03～A05 | `Ref<T>` 只读且可复用；`then`／`then_move` 区分复用和消费读取，非 `Clone` 值可直通；同槽同类型的外来 Ref 在 `then`／`output` 构建阶段被拒绝；Flow Input 与非最后一步 Output 均可作为最终输出。复制成本已按包含最终 `output` 在内的总读取次数修正并测试。 |
| A06～A08 | 内部步骤和 SubFlow 的 child 均经 `Runtime::execute`；错误 fail-fast 且不产生部分正常 Output；重复与交叠调用使用各自的值存储。内部输入解析失败在 child 启动前终止。 |
| A09～A10 | Crate Rustdoc、README、基础 Flow／SubFlow 示例与 T01 示例均通过验证；未提前实现 T03 Binding 扩展、控制型 Executable、LLM、日志、Trace、持久化或并行语义。 |

独立复跑：`cargo fmt --all -- --check`、`cargo check --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets`（38 项通过）、`cargo test --doc`（12 项通过、5 项预期编译失败）、`RUSTDOCFLAGS=-Dwarnings cargo doc --no-deps`，以及四个离线示例，全部通过。外部编译探针额外确认：`ExecutionError` 与 `FlowBuildError` 的 `#[non_exhaustive]` 拒绝穷尽匹配；一个可单独经 T01 Runtime 执行的非 `Sync` Node，仅在作为 Flow child 时被 `Sync` bound 拒绝。

保留的实现取舍：Flow 当前只容纳 `Send + Sync + 'static` 的 child，Input／Output 还需 `'static`；`then` 的复用读取要求其 Input 为 `Clone`，`then_move` 与最终 `output` 提供非 `Clone` 直通路径。每次执行使用内部异构值存储及动态 Future，T03 必须结合字段投影、多值装配和大值复用重新评估这些 API／所有权成本；当前策略与测试结果不视为永久冻结。Rust 1.85 MSRV 仍未在该工具链实测，最迟 T11 发布验收前补测。T02 通过**不关闭 G1**，也不自动开始 T03 实现。
