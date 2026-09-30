# T03 — Binding 与 Input 装配

> 状态：**COMPLETED；实现与复审通过**。
> 前置：T01、T02 已通过；G1 尚未关闭。
> 建议实施基线：`c685ebb`（T02 完成提交）。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T03；通过后**另行审查 G1**，不自动开始 T04～T07。

## 1. 任务目标与边界

把 T02 只能用整值 `Ref<T>` 连接一个 child Input 的 Flow，扩展为可从已有 Flow 数据中**结构性**形成 Input：

```text
已有 Ref ─┬─ 整值读取 ────────────┐
         ├─ 字段投影 ────────────┤
         └─ 多来源组合／嵌套装配 ──┼→ child 的强类型 Input
                                  ↓
                         Runtime.execute(child)
```

本任务必须完成整值引用、字段投影、多值元组、命名业务结构体及嵌套结构装配的公共使用路径。Binding 是**数据连接机制**，不是 Executable：它只读取、投影、组合、构造当前 Flow 已有的值，不进入 Runtime，也不产生业务判断或新业务意义。

T03 不实现 Retry、Match、Each、Iter、LLM、日志、Trace、持久化或并行调度；也不把“为装配 Input 而写胶水 Node”当作完成方案。不要把历史 Compile Probe 的宏、`Clone` 策略或 ValueStore 结构直接升格为正式 API。

## 2. 执行前必须阅读

1. 根目录 `AGENTS.md`、[任务总览](README.md)、[T02 验收记录](T02_Minimal_Flow_And_Value_Store.md#7-验收记录2026-09-29)，以及现有 `FlowBuilder`、`Ref`、`ValueStore`、错误契约和示例。
2. [规范性设计](../SRFlow_Design_v2.0.md)的 §0.3～0.4、§2.5～2.6、§3.8～3.10、§7.4～7.11、§8 全章、§10.2、§10.9～10.11、§11.1～11.5、§11.8～11.10、§13.3 的第六轮、§13.4～13.6 中的 R-01、R-03～R-10、R-18、R-21～R-24。§13.3 是**设计文档中对历史 Compile Probe 的总结**；本仓库没有该 Probe 原始产物，本任务不要求另行查找或照搬它。

**先用小型可编译 API 验证再扩写实现。** 至少比较两种能与当前 `flow.then(executable, source)` 方向相容的公共写法，实际检验下文的四来源业务 Input、字段投影与错误连接；在交接报告里说明为何选择最终形式。宏、trait、builder 或组合方案均可评估，但不得未经评审新增第二个 crate、proc-macro crate 或 workspace。若单 crate 约束与必要语义确实冲突，先报告证据，不自行改变工程结构。

## 3. 本任务交付

### 3.1 统一的 Binding 入口

- 普通使用者通过 `flow.then(executable, binding)` 声明 child 与 Input 来源。**本任务固定读取语义，不由执行者重新选择：**裸 `Ref<T>` 默认保持 T02 的可复用整值读取，因要形成 owned Input，该路径可局部要求 `T: Clone`；同一 Ref 可以在多个后续 Binding 中重复使用。非 `Clone` 整值直通用显式的消费型 Binding（下文暂记 `consume(ref)`）交给同一个 `then`，它移动整值，不要求 `T: Clone`；消费后不能再读取该位置。`consume` 是语义占位名，最终拼写可经编译验证确定，不能改变其显式消费含义。
- T02 的 `then_move(executable, ref)` **保留为兼容便捷方法**，内部委托上述消费型 Binding 与统一的 `then` 实现；它不得维持第二套独立的登记、解析或执行路径。本任务不删除 `then_move`，也不要求立即标记弃用；交接报告写明其公共定位及何时可能收敛。`output(ref)` 保持最终消费语义。先复用再消费同一位置允许；消费之后再复用必须在构建期拒绝。同一个 Binding 内如既复用又消费同一根，T03 统一在构建期拒绝，避免引入隐式的字段求值顺序。
- Binding 的输出类型必须在编译期与 `Executable::Input` 对齐；错误字段类型、错误 tuple 形状及错误命名结构字段类型均应被编译器拒绝。业务侧不得填写 slot、`Any`、动态类型标签或字符串 key 来连接普通数据。
- 支持直接整值读取；从 `Ref<Struct>` 投影字段；把至少四个来源组成下游 Input；以命名字段构造业务 struct；至少一个嵌套结构装配案例。tuple Binding 至少支持 2～4 元；更高元数本任务不要求，实际支持上限须写入文档，不把固定上限当作设计语义。允许把已存在的 Binding 再组合为更大 Binding。语法不预先冻结，但示例应能从业务字段来源一眼看出数据流。
- 所有输入值必须来自当前 Flow 已声明的数据位置或它们的结构性字段；Binding 不能成为注入新业务常量、执行任意函数、过滤、排序、评分或文本变换的隐蔽通道。需要这些操作时用 Node。

以下仅说明必须能表达的业务形状，**不是要求实现 `field!`／`bind!` 这两个宏，也不冻结其语法**：

```rust,ignore
let input = bind!(ProgressionInput {
    plan: field!(story.plan),
    key_node: key_node,
    background: field!(story.background),
    previous_prose: previous_prose,
});
let next = flow.then(ProgressionNode, input)?;
```

这里四个字段分别来自当前 Flow 已有的 Input／Step Output。`ProgressionNode` 可以有真实业务逻辑；Binding 本身只能完成上述结构装配。

如果采用 `macro_rules!`，必须用真实编译验证参数形式：`$root:expr . $field:ident` 违反 fragment follow-set 规则；`field!(source.plan)` 这类简单形式可用受限的 `ident`／token 形式表达，也可改为 `field!(source, plan)`。不得把示意语法直接当作已验证 API。

### 3.2 结构性边界与扩展面

- 优先尝试**封闭公开 Binding 实现面**：评估公开 `Binding` trait 配私有 sealed supertrait（或同等封闭方案），只让 `Ref<T>`、字段投影、显式消费、tuple 和受控结构装配等框架定义的结构类型实现它。公开 API 不提供 `Binding::map(any_function)`、任意 `Fn`／closure 变换或可由业务侧实现后随意执行计算的无约束扩展口。内部为实现字段访问、构造器或类型擦除使用函数对象是实现策略，但不能把任意业务计算能力直接暴露成常规 Binding 用法。
- 必须尝试负向编译探针：原始业务值、`bind!(Input { x: compute(a) })` 等任意计算结果不能直接作为 Binding 输入；另用正向探针证明合法结构装配可用。逐一审查公开构造入口、宏展开和转换实现，尤其不得经接受任意 `Fn`／closure、函数指针或原始业务值的入口把计算重新带入 Binding；若仍有漏洞，提供**实测可编译反例**，再把入口缩到最少并在 Rustdoc 写明边界。sealed 方案若配合受控构造入口，可以禁止任意业务值充当 Binding；用户在构建 Flow 时调用 helper 函数并产生副作用，则属于框架外的用户代码，不等于 Binding 在解析时执行计算，也不应被宣称为框架可禁止的行为。
- Binding 解析在 child 启动之前完成；它本身不通过 Runtime，也不增加可观察的 Executable 次数。无业务意义的 `ExtractFieldNode`／`BuildInputNode` 不得作为主要实现手段；业务计算仍应留在 Node。

### 3.3 Ref 归属、失败阶段与恢复

- 任意深度 Binding 中的每个根 Ref 都必须属于当前 Flow。外来 Ref 无论藏在字段投影、tuple、命名 struct 还是嵌套装配中，都在**构建阶段**被拒绝；同 slot、同 Rust 类型不能绕过检查。
- 多来源／嵌套 Binding 必须先形成**完整读取计划**：列出所有根 Ref 及其读取模式，保留同一根重复出现的次数（不是去重后的集合）；统一校验归属、slot 有效性、既有消费状态、计划内部的复用／消费冲突与计数溢出，然后一次性提交全部读取登记与 child Step。任一校验失败时，`reads`、`consumed`、Step 列表及可继续使用的 builder 均保持原样。不得逐个调用 T02 的 `claim` 并在第 k 个 Ref 失败后留下前 k−1 个读取记录。
- 构建失败不得登记一个可执行的错误步骤。T03 沿用并扩展 T02 对外来 Ref 的**构建期拒绝**（`FlowBuildError`）：它比设计 §10.10 所描述的 Binding 解析失败链更早发现接线错误，不改变“child 不得执行”的语义；§11.4.1 不要求只能在运行时校验归属。不要把接线错误留到 child 执行时才发现，或把它转换成默认业务值。
- 编译期类型错接由 Rust 拒绝；合法构建的 Binding 若在执行期遇到内部缺值、类型不一致等不变量破坏，必须在 child 启动前返回可区分的框架错误。不要将框架不变量错误误报为业务否定结果。

### 3.4 T02 值存储的压力测试与取舍

- 必须为字段投影增加一条**不消费根、借用 `&Root`、仅形成目标字段 owned 值**的读取路径；T02 `read_shared` 会克隆整个根，不能充当这条路径。字段投影和整值复用都必须计入该根位置的读取计划，使最终的整值消费或 `output(root)` 只会在此前借用完成后移动根。`reads_left`／`is_last` 可重构，但必须解释非最终投影、最终投影、先投影后整值消费和 `output(root)` 的实际转移；不得因“最后一次投影”就取走仍需输出的根。
- 至少证明一个**根结构没有实现 `Clone`**、但其中若干可读取字段能被投影并装配为 child Input 的真实可编译用法；不能为取一个字段强迫整个根结构实现 `Clone`。用带计数的较大兄弟字段验证投影小字段时不会复制不相关的大值。
- 由于当前 `Executable::Input` 是 owned 值，**不消费根的投影必须能复制目标字段本身**（通常要求该字段 `Clone`，`Copy` 也满足 `Clone`）；根本身无需 `Clone`。不消费根却从中凭借用移出非 `Clone` 字段，T03 不要求也不得假装可行。若消费整个根，移动其中一个非 `Clone` 字段在 Rust 中可以成立，但属于另一种显式消费语义；T03 不要求新增该公共路径，若实现则必须同样满足登记原子性与后续不可复用。
- 记录每种读取方式的所有权与复制代价：整值复用、同一根结构的多个字段投影、一个字段被多个下游复用、多个 Ref 组成一次 Input，以及最终 `output` 是否额外读取。不要只用“通常零复制”概括；给出可复核的测试或计数。
- 解释投影字段的复制、整值消费的移动，以及它与 `then_move` 兼容入口和非 `Clone` 直通如何共存。不得暗中复制整个根值或假装 Ref 不可复用，也不得引入隐藏共享可变业务状态。
- 新增约束须局部而明确：不能让所有 Flow Input、所有业务 struct 或所有 Binding 无条件要求 `Clone`。明确最终 `Binding` trait／adapter 在构建与异步执行时各自需要的 `Send`、`Sync`、`'static` bound，并证明这些 bound 不会**无条件**传染给被绑定的业务值。T01/T02 已要求 `Executable::Input: Send`，所以本任务不承诺支持 `!Send` Input；须用 `Send + !Sync` 的业务 Input／根结构检验未额外要求数据本身 `Sync`。异构 child `E: Send + Sync + 'static` 的既有要求仍保留，任何额外限制要有具体必要性和反例。

### 3.5 文档、示例与回归

- 新增公共能力同步写可独立阅读的 Rustdoc：用途、结构性红线、Flow 归属、错误发生阶段、所有权／复制成本及可运行的最短示例。更新 crate 首页和 README；保留 T01/T02 示例的可运行性，若 API 经合理调整，连同示例一起迁移。
- 至少增加两个循序渐进、离线可运行的示例：一例展示非 `Clone` 根结构的字段投影与 tuple；另一例展示四来源、命名业务 struct 与嵌套装配。不能只交付一个巨大 SES 式流程，T08 才负责端到端决策网络。
- 测试从独立使用者视角覆盖正常、边界、错误连接与编译失败路径；内部不变量可用定向单元测试。构建、执行、示例不依赖 SES 仓库、真实模型或外部网络。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | `flow.then(executable, binding)` 是统一入口：裸 Ref 默认可复用，显式消费 Binding 支持非 `Clone` 整值直通，`then_move` 仅为同一路径的兼容方法；`output` 保持最终消费 | 外部使用者正反向示例、T02 旧测试／示例回归、同一 Ref 先复用后消费及消费后拒绝的测试；公共接口审查 |
| A02 | 字段投影、2～4 元 tuple Binding、至少四来源组合、命名 struct 与嵌套装配均能形成真实 Node Input，无胶水 Node | 两个分层示例、四元 tuple 外部集成测试、清楚的数据来源及已文档化的 tuple 元数上限 |
| A03 | Input、字段、tuple 及命名结构形状错接在编译期被拒绝，且失败原因确实是对应类型不匹配 | 每个负向探针配一个只差错误接线的正向探针；交接报告附实际编译器诊断（错误码／消息摘要及报错位置）。`compile_fail` doctest 单独不能证明失败原因 |
| A04 | 任何深度的外来 Ref 经投影或组合仍在构建阶段被拒绝；多来源登记全有或全无，失败不会登记 child，也不损坏可继续使用的 builder | 同 slot 同类型的跨 Flow 测试、嵌套中后位失败、同根重复出现及读／消费冲突测试；失败后再接合法 Step 并执行的恢复测试 |
| A05 | Binding 只做结构装配、不产生 Execution；child 仍经 Runtime；优先封闭公共 Binding 实现面与任意计算入口 | 公共 API／源码与调用路径审查；测试 Node 记录 child 调用序列并断言仅已声明的 Executable 被调用、Binding 未引入额外 child；sealed 方案验证，合法结构装配正向及任意计算负向编译探针；若无法封死，附实测反例与剩余红线。不为此新增正式日志或 Runtime 观察接口 |
| A06 | 非 `Clone` 根结构可借用投影可复制字段；不相关大字段不被复制；投影与整值消费／最终 Output 的计数、移动和复制成本准确可测 | 非 `Clone` 根及计数大字段测试；同一根多投影后 `output(root)` 或显式消费的测试；所有权／读取计划说明 |
| A07 | 输入解析的不变量失败发生在 child 运行之前；不产生默认业务 Output、后续步骤不执行 | 内部定向测试或可复核源码路径、child 调用计数 |
| A08 | Flow 顺序、SubFlow、重复／交叠调用隔离、错误 fail-fast 均不回归；Binding 不引入隐式并行 | T01/T02 回归与新增组合测试 |
| A09 | Rustdoc、README 和两个离线示例能让独立使用者理解常见用法与限制；API 选择、Binding bound 及遗留漏洞如实记录；不额外要求业务值 `Sync` | 文档测试、示例运行、API 方案比较、`Send + !Sync` 业务 Input／根的外部编译和执行探针及交接报告 |
| A10 | 未实现 T04～T07 控制语义、LLM、日志、Trace、持久化、通用表达式 DSL 或第二个 crate | 改动范围、依赖与公开项审查 |

**关键复审风险：**A03 的 `compile_fail` 不能仅因缺少 import 等无关错误而通过；可用一次性编译探针取得诊断，无需创建第二 crate。A04 必须逐一核查深层及重复根 Ref，并证明失败登记为零。A05 不可用“不执行 Runtime”掩盖任意业务计算，也不能把用户构建期 helper 的副作用误认为 Binding 解析期计算；测试侧 child 记录须与调用路径审查配合，不能单独替代后者。A06 不可只测小 `Copy` 字段，而忽略大结构复制。T03 通过后也要单独审查 G1，不能自动把 G1 标为完成。

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
cargo run --example <T03 字段投影／tuple 示例>
cargo run --example <T03 命名／嵌套装配示例>
```

交接报告须列出基于的提交、所有修改文件、A01～A10 对应证据、两种 API 候选的比较与最终取舍、`then_move` 的兼容定位、公开结构性约束及实测剩余漏洞、构建期归属错误与执行期不变量错误的区分、读取计划的原子性、所有权／复制成本、`Binding` 的实际 trait bounds 及对业务值的影响、编译失败探针的真实诊断、增加的依赖与 MSRV 影响，以及未覆盖或未通过项。执行者不得自行将任务标为 `COMPLETED`、关闭 G1 或开始任何控制型 Executable。

## 6. 审查与开始规则

本任务书已由用户审定并完成实施。T03 经独立复审通过；G1 仍须单独审查，不能因本任务通过而自动关闭。只有 G1 单独通过，才能按下一份任务书开始控制型 Executable。

## 7. 验收记录（2026-09-30）

**结论：T03 PASS；G1 尚未关闭。** 复审覆盖统一 Binding 入口、强类型装配、Flow 归属、读取计划原子性、字段投影的所有权与复制成本、Runtime 调用链及公开结构性边界。未发现需要修改 SRFlow v2 上位设计的偏差。

| 验收项 | 复审证据 |
| --- | --- |
| A01～A02 | `then(executable, binding)` 统一接线；裸 `Ref` 复用、`consume(ref)` 显式消费，`then_move` 委托同一路径。字段投影、2～8 元 tuple、四来源命名结构与嵌套装配均经外部测试和两个离线示例验证。 |
| A03～A04 | 错误 Input／字段／tuple／命名结构接线在编译期拒绝；交接报告提供成对正反向探针及实际诊断。外来 Ref 藏于投影、tuple、命名装配中仍在构建期拒绝；内部测试直接断言失败后 `reads`、`consumed`、Step 列表均未改动。 |
| A05～A07 | Binding 不作为 Executable，也不增加 child 执行；解析错误先于 child。非 `Clone` 根可借用投影可复制字段，不复制无关大字段；新增混合读取顺序计数测试证实整值克隆成本，`output(root)` 与投影可共存。 |
| A08～A10 | T01/T02 回归、SubFlow、顺序与 fail-fast 均通过；`Send + !Sync` 业务值可经投影与消费流转。Rustdoc、README 和示例可用；未引入新依赖、第二 crate 或后续控制语义。 |

独立复跑：`cargo fmt --all -- --check`、`cargo check --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets`（61 项通过）、`cargo test --doc`（17 项运行、8 项预期编译失败）、`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`，以及六个离线示例，全部通过。

**保留的实现边界：**`Binding` trait 虽已封闭，供稳定 `macro_rules!` 在业务 crate 中展开的两个 `#[doc(hidden)]` 公开构造入口仍非封闭：直接调用 `__project_field` 可注入根外数据，直接调用 `__assemble` 可执行任意计算。外部探针已复现前者；公开 Rustdoc 已如实说明两者，并要求通过文档与评审守住结构性边界。字段投影只复制目标字段；非消费投影不能从根中移出非 `Clone` 字段。Rust 1.85 MSRV 仍未在该工具链独立实测，最迟 T11 发布验收前补测。T03 通过**不自动关闭 G1**。
