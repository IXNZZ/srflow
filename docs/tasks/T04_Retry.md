# T04 — Retry：正常业务 Output 驱动的有限重做

> 状态：**COMPLETED；2026-09-30 独立复审通过；G2 未关闭**。
> 前置：T01～T03 已通过；G1 已于 2026-09-30 单独验收通过。
> 建议实施基线：`66481b6`（T03 完成提交）。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T04；通过后不自动开始 T05～T07，也不关闭 G2。

## 1. 任务目标与边界

实现第一个控制型 Executable：`Retry`。它用**语义上相同的业务 Input** 有限次调用同一个 Body，每轮只根据 Body 的**正常 Output** 中已有的控制信息决定停止或重做：

```text
Input ──→ Runtime.execute(Body) ──→ Output ──→ Condition(&Output)
  │                ↑                              │
  └── 同一业务输入 ┘                  stop → 返回本轮 Output
                                    retry → 下一轮（未达上限时）
```

`Retry` 本身实现 `Executable<Input = Body::Input, Output = Body::Output>`；对父 Flow 而言，它和 Node、SubFlow 一样走 `flow.then(retry, binding)`。Condition 是读取已形成控制结果的局部判断，不是 Executable，也不经 Runtime。每次 Body 的实际调用必须重新经过父级传入的同一个 Runtime。

本任务**不实现**技术故障重试、退避／延迟、`Retry.collect()`、Attempt 历史输出、Match、Each、Iter、日志、Trace、持久化、配置系统或通用控制 DSL。§9.2.10 允许独立决定是否提供收集全部 Attempt 的变体；T04 明确**推迟**该能力，不是修改上位设计。Body 可以是已有的 Node 或 Flow，但不为演示 Retry 而实现 T05～T07。

## 2. 执行前必须阅读

1. 根目录 `AGENTS.md`、[任务总览](README.md)及其 G1 独立验收记录、[T01 验收记录](T01_Async_Execution_Foundation.md)、[T02 验收记录](T02_Minimal_Flow_And_Value_Store.md)、[T03 验收记录](T03_Binding_And_Input_Assembly.md)。核对现有 `Executable`、`Runtime`、`FlowBuilder::then`、`Binding`、`ExecutionError` 与可运行示例。
2. [规范性设计](../SRFlow_Design_v2.0.md)的 §3.8～3.10、§4、§5、§7.4～7.7、§9.1～9.2、§10.1～10.5、§10.7、§10.12～10.13、§11.1～11.5、§12.3、§12.6、§13.6。重点区分 §9.2 中的业务重做与 §12.6 中明确排除的技术故障重试。

先用小型可编译 API 验证构造、默认／显式 limit、Condition、Body Input／Output 类型关系和作为 Flow child 的用法，再扩写实现。比较至少两种合理公共形式，说明最终选择；不把历史 Compile Probe 的具体签名直接当作正式接口。

## 3. 本任务交付

### 3.1 精确的执行语义

- Retry 是 do-while：只要开始执行，Body 至少运行一次。`limit` 表示 **Body 总执行次数上限**，不是额外重试次数；默认上限为 **8**。显式 `limit = 0` 必须在构造／配置阶段被拒绝，不得等到运行时才失败或用 `panic!` 代替正常校验。可以采用 `NonZeroUsize` 或返回明确错误的构造器，交接报告说明取舍。
- 每轮 Body 正常产生一个 `O` 后，Condition **恰好观察一次 `&O`**，包括达到上限的最后一轮。T04 的公共控制结果固定为具名值 `RetryDecision::{Retry, Stop}`，不采用含义不明的裸 `bool`：`Stop` 立即返回该轮 `O`；`Retry` 且尚未到上限时启动下一轮；`Retry` 但已经到上限时仍返回该轮正常 `O`，不合成技术 Error。核心 Output 只是最终采用的 `O`，不保留 Attempt 列表。
- Body 返回 `ExecutionError` 时立即原样向外传播：该轮不调用 Condition，不执行后续轮次，也不把 Error 转为业务否定或技术重试。Retry 不回滚已发生的外部副作用。
- 每轮 Body 收到语义上相同的原始业务 Input；上一轮 `O` 不能自动成为下一轮 Input，且不得通过 Retry 内部隐藏可变状态传递业务数据。Body 自身可显式持有客户端、配置等资源，但不能借此替代 Input／Output 业务数据流。

### 3.2 owned Input 的所有权边界

当前 `Executable::Input` 按值传入。T04 选择**仅为 Retry 的重复执行路径局部要求 `I: Clone`**，用克隆提供前面轮次所需的 owned Input，最终允许的轮次可直接移动原始 Input；不得把 `Clone` 加到 `Executable`、`Node`、所有 Flow Input 或所有 Binding 上。**非 `Clone` Input 不能经这个通用 Retry 执行**，即使运行时 `limit = 1`；这一限制要在公共文档中明说，不能误称 T03 的非 `Clone` 直通能力已扩展到 Retry。

在所选“保留原始 Input、最后允许轮次直接移动”的策略下，若实际执行了 `n` 轮、上限为 `limit`，则 `I::clone` 调用次数应为 **`n − [n == limit]`**：提前在第 `n < limit` 轮停止时，前 `n` 轮必须各获得一个克隆，因为当时仍可能继续；耗尽或恰在上限停止时，最后一轮可移动原始 Input，只克隆 `limit − 1` 次。例如 `limit = 3`：第一轮停止复制 1 次，第二轮停止复制 2 次，执行满三轮复制 2 次。这里计数的是 `Clone` 调用，不承诺每个业务类型的实际复制字节数或自定义 `Clone` 的语义。

实现须用计数测试验证上述口径，并避免对每轮正常 Output 施加 `Clone`：Condition 借用 `&O`，最终 `O` 按值返回。`limit = 1` 时 Input 不复制，非 `Clone` Output 仍可正常返回。`Clone` 应提供等价业务输入，其语义正确性由业务类型的实现者承担。若提出不同所有权策略，须先用可编译外部用例证明它不改变 Body 的既有 owned Input 契约且不额外引入隐式业务数据流，再交付评审，不得静默替换本节选择。

### 3.3 Condition 的边界

Condition 的公共形态固定为 `Fn(&O) -> RetryDecision`，其中 `RetryDecision::Retry` 表示要求再执行一轮，`RetryDecision::Stop` 表示采用本轮 Output；两种结果及最后一轮仍调用 Condition 的语义须写入 Rustdoc 和测试。Condition 不得接收 Runtime、启动 child、修改 `O` 或成为复杂业务评分器。使用者需要复杂业务判断时，先由 Body 内的 Node 产出明确的 judgment 字段，Condition 只读取它。不得暴露 `FnMut` 驱动跨轮隐式业务状态。`&O`／`Fn` 本身不能从类型系统禁止内部可变性或复杂计算，公共文档与复审必须明确这条语义红线，不能宣称类型系统已完全封住。

Condition 不是另一个技术错误通道；技术错误由 Body 返回 `ExecutionError`。Condition 若自行 `panic` 属 Rust 调用方代码的 panic，不在 T04 中定义恢复机制。Condition 的运行次数、最后一轮调用及 Body 出错时不调用，必须测试。

### 3.4 组合、类型与错误接口

- 提供让普通使用者能直接理解的默认构造与显式上限用法。公开 API 应使 Body 的 Input／Output 与 Retry 的 Input／Output 强类型一致；错误 Body／Condition 类型在编译期拒绝，并用只差错误连接的正反向探针确认诊断原因。零上限必须在进入合法 Retry 之前被排除：若采用 `NonZeroUsize`，`0` 在类型层面不可传入且 `NonZeroUsize::new(0)` 返回 `None`；若采用接收普通整数并返回 `Result` 的构造器，则应使用与 `ExecutionError` 分离、可按类型／变体识别的构造错误。两种形式择其一并记录理由，不依赖错误文本匹配。
- Retry 应能被 `Runtime::execute` 单独执行，也能作为 Flow 的普通 child 经 `flow.then(retry, binding)` 连接；Body 为 Flow 时，执行链仍为 `Runtime → Retry → Runtime → Flow → Runtime → Node`。`Runtime` 本身不加入 Retry 条件或计数逻辑。
- T04 固定**同一个 Body 与 Condition 实例跨轮复用**，不以克隆 Body／Condition 规避所有权要求；两者不应被要求实现 `Clone`。执行 Future 必须为 `Send`，借用 `&self.body`、`&self.condition` 跨 `await` 时，即使 Retry **单独**经 Runtime 执行，Body 与 Condition 也均须 `Sync`。这属于 T04 所选实现的局部约束，**不是** T01 的 `Executable` trait 对所有实现无条件要求 `Self: Sync`。作为 Flow child 时还须满足既有的 `E: Send + Sync + 'static` 与 Input／Output 的 `'static` 要求；不要把这些附加要求倒灌到所有 Node／Executable。业务 Input 只需 `Clone + Send`，Output 只需 `Send`，两者均不得无条件要求 `Sync`。用非 `Clone` Body／Condition 及 `Send + !Sync` 的 Input **和** Output 外部探针核实约束，并在交接报告说明实际 bound。
- 对外错误仍遵守 T01 错误模型：技术失败保留原始 `ExecutionError` 来源；正常业务的“不接受／仍需重做／耗尽”由 `O` 表达。构造时的零上限校验错误与执行错误不要混成一类。

### 3.5 文档、示例与回归

在新增公共项的 Rustdoc 与 crate 首页写清：业务 Retry 与技术重试的区别、默认／显式上限、`limit` 计数口径、do-while、Condition 的只读边界、正常停止／耗尽／技术错误三种结果、Input 复制成本和相应类型约束。新增至少一个**离线可运行且循序渐进**的 `retry` 示例：以 Fake Node／Flow 的正常 Output 模拟“生成→检查→重做”，展示早停与上限耗尽；另有独立用例验证 Retry 作为父 Flow child。README 增加示例入口。旧示例与测试保持可运行。

测试必须覆盖默认 8、显式 1／3、零上限、`RetryDecision::Retry` 与 `RetryDecision::Stop` 两个方向、第一轮停止、中途停止、恰好上限停止、一直要求 retry 后耗尽、某轮技术失败、Condition 调用次数、每轮相同 Input、非 `Clone` Output、复制计数、重复／交叠调用隔离、Body 为 Flow 及 Retry 作为 Flow child。测试可用计数／日志替身观察行为，但不向正式 Runtime 添加日志或观察钩子。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | Retry 是强类型 Executable；默认 8、显式上限与零上限在构造前／构造时排除；Body 至少运行一次 | 公共 API 审查、默认／1／3 边界测试，`NonZeroUsize::new(0) == None` 或独立构造错误测试 |
| A02 | 每轮接收语义上相同的原始 Input，不把上一轮 Output 当 Input；`Clone` 约束只局部施加 | 不同 Attempt 的输入记录、编译约束探针、非 Retry 的非 `Clone` 回归 |
| A03 | Condition 对每个正常 Output 恰调用一次且只借用 `&O`；`RetryDecision::Retry`／`Stop` 含义明确，复杂判断留给 Body Node | 两种决策方向与最后一轮的 Condition 调用计数、非 `Clone` Output 测试、Rustdoc／示例审查 |
| A04 | 第一轮／中途／上限正常停止均返回对应轮 `O`，不多执行一轮 | 固定结果序列、Body 调用计数与最终值断言 |
| A05 | 始终要求 retry 且上限耗尽时返回最后正常 `O`，不是 Error | `limit = 1` 与 `limit = 3` 的耗尽测试 |
| A06 | Body 技术错误立即原样传播，Condition 与后续轮次不执行 | 错误来源链、Body／Condition 调用计数及无技术重试测试 |
| A07 | 每轮 Body 均经 Runtime；Retry 可独立执行、在父 Flow 内执行、以 Flow 为 Body 嵌套执行 | 执行路径审查、嵌套 Flow 测试、父 Flow 的 Binding／Output 连接测试 |
| A08 | owned Input 的复制成本符合 `n − [n == limit]`：`limit = 1` 为 0 次；`limit = 3` 第一轮停为 1 次、第二轮停为 2 次、执行满三轮为 2 次；不要求 `O`、Body、Condition 为 `Clone`，也不要求业务 Input／Output 为 `Sync` | 四种路径的 Clone 计数、非 `Clone` Output／Body／Condition、`Send + !Sync` Input／Output 外部探针及公共 bound 审查 |
| A09 | Rustdoc、README、离线示例准确解释使用与限制，T01～T03 回归不破坏 | 文档测试、示例运行、全量回归、API 方案比较与交接报告 |
| A10 | 没有实现 `Retry.collect()`、技术故障重试、退避、其他控制型 Executable、日志、Trace、持久化或新 crate | 改动范围、依赖与公开项审查 |

**关键复审风险：**`limit = 0` 不能用“Body 执行零次”悄悄通过；耗尽不能变成技术 Error；技术 Error 不能触发业务 Retry；Condition 不能隐藏复杂业务判断；所有 Body 调用必须经过 Runtime。Input 复制代价与 Condition 最后一轮的调用次数都需由可计数测试证明，不能只写在注释里。

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
cargo run --example <T04 Retry 示例>
```

交接报告须列出基线提交、所有修改文件、A01～A10 的对应证据、两种公共 API 形式的比较及最终选择、默认／显式 limit 的表示与零值拒绝阶段、每轮 Input 复制成本、Body／Condition 的 trait bounds、正常停止／耗尽／错误的区别、递归 Runtime 调用路径、依赖与 MSRV 影响、未覆盖项。执行者不得自行将任务标为 `COMPLETED`、关闭 G2 或开始 T05～T07。

## 6. 审查与开始规则

本任务书已由用户审定并完成实施。T04 经独立复审通过；G2 仍须在 T04～T07 均通过后单独验收。本任务通过不自动授权 T05～T07 的实现。

## 7. 验收记录（2026-09-30）

**结论：T04 PASS；G2 未关闭。** 对照规范性设计 §9.1～9.2、§10.12～10.13、§12.6 与 R-13～R-15 复核，Retry 的业务重做、技术错误和递归 Runtime 边界一致；未发现需要修改上位设计的偏差。

| 验收项 | 复审证据 |
| --- | --- |
| A01～A02 | `Retry<B, C>` 实现强类型 `Executable`；默认上限 8，显式上限使用 `NonZeroUsize`，零上限不可构造。每轮使用语义上相同的原始 Input，上一轮 Output 不作下一轮 Input；`Clone` 仅局部要求于 Retry 的 Input。 |
| A03～A05 | `RetryDecision::{Retry, Stop}` 是封闭的两值契约，执行处穷尽匹配；Condition 对每个正常 Output 恰调用一次，含最后一轮。第一轮、中途、上限停止及一直要求重做的路径均有计数和结果测试；耗尽返回最后正常 Output，而非技术 Error。 |
| A06～A07 | Body 技术错误立即原样传播，出错轮不调用 Condition、后续轮次不执行；每轮 Body 经同一个 Runtime。Body 可为 Flow，Retry 也可作为父 Flow 的普通 child，以 Binding 连接输入和下游。 |
| A08～A10 | Input 克隆计数符合 `n − [n == limit]`；非 `Clone` Output／Body／Condition 和 `Send + !Sync` 业务值可用。同一 Retry 实例的两次交叠调用各自保持 Input 与轮次隔离。Rustdoc、README 和离线示例覆盖生成、检查、重做；未加入技术重试、退避、`collect()`、其他控制型 Executable 或新依赖。 |

独立复跑：`cargo fmt --all -- --check`、`cargo check --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets`（81 项通过）、`cargo test --doc`（21 项运行、11 项预期编译失败）、`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`，以及七个离线示例，全部通过。交接报告提供 Condition 类型、非 `Clone` Input 与 Flow child 错误接线的成对编译探针及实际诊断。

**保留边界：**本阶段选择 `NonZeroUsize` 而非独立的零上限构造错误；非 `Clone` Input 即使上限为 1 也不能经通用 Retry。`Retry.collect()` 按 §9.2.10 推迟，非设计变更。Rust 1.85 MSRV 尚未在该工具链独立实测，最迟 T11 发布验收前补测。T04 通过**不自动关闭 G2**。
