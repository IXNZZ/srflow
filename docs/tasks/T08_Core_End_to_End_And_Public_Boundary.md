# T08 — 核心端到端与公共边界收口

> 状态：**COMPLETED；2026-09-30 独立复审通过；G3 未关闭**。
> 前置：T01～T07 已通过；G1、G2 均已单独验收通过。
> 建议实施基线：`fef5c3b`（T07 完成提交）；实施前核对 HEAD 与工作区差异，保留已有用户改动。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T08；通过后仍须单独验收 G3，不自动实施 T09／T11。

## 1. 目标与边界

用**离线 Fake Node** 把已经分别通过的核心能力写成一条读得懂、跑得通的 SES 式正文生成决策流程，并从 crate 外部使用者视角审查其公开 API。T08 验证的是：现有 `Runtime / Executable / Node / Flow / Ref / Binding / SubFlow / Retry / Match / Each / Iter` 能否在同一真实形状的业务流程中自然组合，而不靠框架私有入口、隐式状态或新增核心概念救场。

业务案例仅作为压力测试；不在核心 crate 中定义正式的 SES `Plan`、`KeyNode`、`ProseState` 等业务模型，也不接真实模型、网络、文件、数据库或用户数据。Fake Node 的输出必须确定、可观察，足以让错误接线、错误路由、错误轮次或隐式状态回流被测试发现。

本任务允许：为经外部使用者探针证实的公共 API 缺陷做**最小、兼容上位设计的修正**，并补相应测试和文档。不得为了让案例通过而修改冻结的执行语义、扩大 Binding 为业务计算引擎、引入新控制器或第二 crate。若发现需要改变核心契约或上位设计，停止该冲突部分，给出最小复现与取舍，先交用户评审；不能在 T08 中静默重设计。

本任务不实现 `llm` feature、日志／Trace、持久化、恢复、技术故障重试、自动并行、事务回滚、动态 DSL、发布元数据收口或 crates.io 发布。`Binding` 现有两个 `#[doc(hidden)]` 公开构造入口的结构性漏洞是 G1 已记录的保留边界：T08 必须从外部视角复核其影响并如实记录，不得把文档约束夸称为类型系统完全封闭；若发现比既有记录更严重的越界，再按上段处理。

## 2. 执行前阅读与基线检查

1. 阅读根目录 `AGENTS.md`、[任务总览](README.md)及 G1／G2 独立验收记录、[T01](T01_Async_Execution_Foundation.md)～[T07](T07_Iter.md) 的验收记录。核对 `fef5c3b` 和未提交改动；不得覆盖已有用户文件（当前已知 `.gitignore` 有独立改动）。
2. 阅读[规范性设计](../SRFlow_Design_v2.0.md) §3.8～3.10、§4～§8（特别是 §7.8、§8.8～8.10）、§9.1～9.6、§10.1～10.13、§11.1～11.10、§12.3～12.6、§13.2、§13.5～13.6。以规范语义约束案例，而不是倒过来用案例改写设计。
3. 从**外部 crate 可见 API** 出发，先写一页以内的构图草案：列出父 Flow 的 Input／Output、每个 SubFlow 与控制器的 Input／Output、关键 `Ref`／Binding 接线、所有权复用／消费位置及预期失败路径。随后再写 Fake Node 与集成测试。构图草案可置于 T08 交接报告，不必新增生产接口。

## 3. 本任务交付

### 3.1 一条真实形状的端到端流程

在本仓库的集成测试和至少一个离线示例中表达下列**语义角色与数据依赖**，具体 Fake 类型和函数名可由实施者选择。它是 DAG，不是每步 Output 直接接下一步 Input 的直线：

```text
Brief → Retry(GeneratePlan → CheckPlan 的 SubFlow) → PlanAttempt
                                                        ├─ 复用 Plan → JudgeRouteNode → Route ─┐
                                                        ├─ 复用 Plan → Match 的业务 Input ─────┤
                                                        └─ 后继消费 → MakeInitialNode → T0      │
                                     (Route, Match 的业务 Input) ──┘
                                                        ↓
                         Match 的唯一分支 → Vec<KeySeed> → Each → Vec<KeyNode>
                                                              (Vec<KeyNode>, T0)
                                                                       ↓
                         Iter(携带 Plan 与上一轮 Prose；Body 可为 SubFlow)
                                                                       ↓
                                                                  FinalResult
```

其中 `Match` 各分支的共同 Output 必须是可交给 `Each` 的集合（例如 `Vec<KeySeed>`）；`Each` 的 `Vec<O>` 元素才是 `Iter` 的 Item，`Iter` 的 `T0` 则来自**另一数据位置**。同一 `PlanAttempt` 位置可以先经投影／复用供 Judge 与 Match 使用，再在更晚步骤消费给 `MakeInitialNode`。整值复用会克隆整值，字段投影只克隆所选字段；这些实际复制须计入 A07，不能只按最终消费次数估算。也可以由独立的已有值构造 `T0`，但不能把它伪装成 `Each` 的 Output。图中的节点顺序只示意依赖，实际执行仍按父 Flow 的 `then` 声明顺序。

- `Retry` 的 Body 每轮使用同一个原始业务 Input；生成与检查由可见的 Node／SubFlow 完成，Condition 只读正常 Output 中的接受标记。Fake 可用显式的测试替身计数器模拟不同候选，但**不得**让该计数器替代 Iter 的业务状态传递。
- `JudgeRouteNode` 产生明确的 Route；`Match` 仅据此选择一个分支。至少两个具体类型不同而 Input／Output 契约一致的分支可编译、可执行，且未选分支有零调用证据。
- `Each` 用于各关键节点可逐项处理、但**不需要上一项 Output**的阶段；不要用它偷偷累计正文。`Iter` 用于后一个节点必须看到前一轮正文的阶段；其 `T` 同时保留每轮需要的 Plan 和变化的 Prose。三轮以上的最终结果须对节点顺序敏感。
- 父 Flow 至少演示一次复用已有 `Ref`、一次字段投影、一次三来源及以上的结构性命名 Input 装配，以及一次由两个不同位置组成的消费型 tuple Binding；所有跨 SubFlow 边界的数据仅通过 Input／Output 传递。若现有 API 使某种装配不自然，记录准确的代码与原因，再判断是任务内小修还是需另行评审。
- 所有业务判断、生成和修改正文的操作在 Fake Node 中；Flow 只排顺序，Binding 只读／投影／装配，四种控制器只承担各自控制语义。**纯投影、纯 tuple／struct 装配**不得借 `ExtractFieldNode`／`BuildTupleNode` 绕开 Binding；由业务数据构造控制器所需初始状态或业务结构则属于 Node 的正常职责，例如现有 `examples/iter.rs` 中的 `MakeInitial`／`MakeKeys`。也不得把 `judge`／`rewrite` 写进 Binding 回调。

### 3.2 组合路径、失败与边界

- 正常路径须能从一个 `Runtime::execute(&parent_flow, brief)` 运行到最终结果，并证明 `Runtime → Flow → 控制器 → Runtime → SubFlow → Runtime → Node` 的递归调用路径。不能只把四段互不相连的小例子放在同一文件里冒充端到端。
- 运行时至少覆盖：Retry 先拒绝后接受、Retry 上限耗尽仍返回最后正常 Output、Match 两个不同分支与未命中、Each 空集合与中途错误、Iter 空集合与中途错误、被选分支错误不改走 default、正常 `needs_revision` 状态不被当作技术 Error／提前停止。失败路径可用同一流程的可控 Fake 输入或独立的缩小版组合，但必须检查后续 child 零调用和错误来源；不要求所有失败同时出现在一条执行中。
- 对“已发生的外部副作用不自动回滚”只需用测试替身的可观察事件记录与文档说明，不添加事务能力。失败组合不返回部分正常 Output；Retry 的技术错误不自动重试。
- 使用受控日志／计数核查顺序、唯一分支、真实状态回流、每项／每轮调用次数；不能只核查最终字符串，因为错误路径可能碰巧产生相同结果。`Runtime` 当前没有观察钩子，不得为了本任务加入日志系统；递归 Runtime 入口以现有测试加代码路径审查共同证明。

### 3.3 公共边界与使用体验审查

从 `tests/`／`examples/` 的外部 crate 视角审查并记录：

1. **强类型接线：**错误 Body 契约、父 Flow 的错误 Binding 输入、Match 分支 Output 或 Iter 状态类型应在编译期被拒绝。选至少三组、且分属**不同组件**的错接做成对正反向探针（例如 Match 分支 I/O、Each 集合类型、Iter Body 契约或 Binding tuple 顺序，各取其一），交接时附实际诊断码／位置；单纯 `compile_fail` 通过不足以证明失败原因。
2. **组合封装：**父级只接触 SubFlow／控制器的 Input／Output，内部 `Ref` 不能跨 Flow 使用；外来 Ref 即便藏在字段投影或命名装配里也应被拒绝。区分构建期归属错误与执行期框架不变量错误。
3. **所有权与 bounds：**记录哪几处复用导致 `Clone`、哪几处消费支持非 `Clone`，并检查是否出现不必要的 `Sync`／`'static` 外溢。至少一条关键数据路径应使用非 `Clone` Item／状态；不得为了让案例可编译而全局派生 `Clone` 或把大型正文无说明地反复复制。
4. **公共表面：**核对 crate 根 re-export、构造与错误 API、Rustdoc 导航、示例可发现性；业务侧不应需要访问 `core` 私有模块或 `Any`／擦除适配器。对每个不自然之处给出最小代码、严重度、任务内修复或保留理由。已知 Binding 构造入口漏洞须继续如实呈现。

### 3.4 文档与示例收口

- 新增一个可离线运行的 `examples/story_workflow.rs`（名称可按仓库约定微调），先用简短流程图说明四种控制器为何各在其位置，再展示完整构建与执行；示例不依赖 SES 仓库或真实服务。正文生成只是**示意业务压力场景**，不要给 Fake 结果披上真实生成质量的表述。
- README 增加按学习顺序排列的示例入口：最小 Node → Flow／Ref → Binding → SubFlow → Retry／Match → Each／Iter → 嵌套流程，并提供一段足够短的首次使用路径。crate 首页 Rustdoc 与当前能力同步（包括清除“目前处于 T07”等阶段性过时表述），公共错误、所有权与异步驱动说明保持准确；不在 T08 重写与本任务无关的设计文档。T08 的文档收口限于学习路径、当前能力和阶段性表述；发布元数据、feature 文档、打包与 docs.rs 发布前预检留给 T11。
- 保证所有原有示例和新增示例可离线运行；文档测试与公共 Rustdoc 入口不依赖读者阅读私有源码。Fake 类型／构图若需在 `examples/story_workflow.rs` 和 `tests/story_workflow.rs` 共用，默认置于非公开的 `examples/support/story_workflow.rs`，两处分别用相对路径的 `#[path = "..."] mod support;` 包含同一源文件；不得为省复制而把 SES 专属业务模型放入 `src/` 的正式核心公共 API，也不新建 crate。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | 一条父 Flow 真实组合 Retry、Match、Each、Iter、Binding 与 SubFlow，最终产出明确业务 Output | 外部集成测试＋离线示例；构图与强类型 Input／Output 表；不是四段孤立演示 |
| A02 | 四种控制语义没有互相偷换 | Retry 同一 Input、Match 单分支、Iter 上轮状态回流的事件与值断言；Each Body 记录**实际收到的原始 Item 序列**（或在 `I = O` 时让 Output 明显不同于 Input），证明上一项 Output 未回流；至少三轮顺序敏感结果；Each／Iter 空集合行为 |
| A03 | 多来源装配与跨边界传值自然且安全 | Ref 复用、字段投影、三来源命名装配、双位置消费 tuple 的真实接线；外来／内部 Ref 被拒绝的证据 |
| A04 | 嵌套子执行全部经 Runtime，Runtime 本身不理解控制规则 | 关键代码路径审查与现有递归调用测试；SubFlow Body／branch 与父 Flow child 的行为测试 |
| A05 | 失败不会被误当业务重做、默认分支、部分成功或回滚 | Retry 耗尽／技术 Error、Match 未命中／分支 Error、Each／Iter 中途 Error、正常修订标记的分离测试；后续 child 零调用与来源断言 |
| A06 | 公共 API 保持强类型、Flow-local、无内部擦除泄漏 | 至少三组且分属不同组件的错接成对正反向外部编译探针，报告实际诊断码与位置；归属错误测试；crate 根使用代码不接触内部 `Any` |
| A07 | 所有权与实际 bounds 对业务使用者可解释 | 非 `Clone` 关键路径、复用复制成本记录；没有无理由增加 `Sync`／`'static`／`Clone`；保留约束写入 Rustdoc |
| A08 | 使用文档与示例能够按学习路径独立使用 | README 示例索引与短入门、crate Rustdoc、全部离线示例和文档测试通过；无过时阶段状态 |
| A09 | 发现的公共 API 不自然之处得到分类处置 | 外部使用者审查表：复现、影响、最小修正／保留理由；重大设计冲突停止并提交评审，不静默改上位设计 |
| A10 | 未越过 T08 范围，G3 不被自动关闭 | 改动与依赖审查：无真实 LLM、并发／日志／持久化／新控制器／新 crate／发布行为；任务通过后单独验收 G3 |

**关键复审风险：**为凑齐四种控制器而写出不自然业务流程；用 Fake Node 共享计数器偷偷代替 Iter 的 `T`；`Match` 内部做判断；Binding 回调做生成／检查／改写；把 `needs_revision` 当 Error；结果正确但错误 child 也已启动；只做 `compile_fail` 而未查诊断；为了跨层接线暴露内部 Ref；对复制代价和已知 Binding 漏洞给出过度承诺。

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
cargo run --example iter
cargo run --example story_workflow
```

交接报告须列出实施基线与实际起点、修改文件、端到端构图、A01～A10 逐项证据、每个控制器在业务图中的理由、成功／失败事件序列、Runtime 递归路径、外部编译探针诊断、公共 API 使用体验及已知漏洞、所有权／复制代价、依赖与 MSRV 影响及未解决问题。不得自行把 T08 标为 `COMPLETED`、关闭 G3、开始 T09／T11，或提交／推送未经当前授权的额外内容。

## 6. 审查与开始规则

本任务书已获用户授权并完成实施。T08 经独立复审通过；G3“核心可用”仍须单独验收，不自动开始 T09／T11。G2 PASS 只证明四种控制语义和递归执行入口，不替代本任务的端到端及公共边界验收。

## 7. 验收记录（2026-09-30）

**结论：T08 PASS；G3 未关闭。** 对照规范性设计 §3.8～3.10、§7～§10、§13.2 与 R-01～R-20，离线 Fake 流程从一次 `Runtime::execute(&parent_flow, brief)` 运行到 `FinalResult`，在同一个强类型 DAG 中组合 Retry、Match、Each、Iter、Binding 与 SubFlow；没有为了案例修改核心执行语义或公开签名。

| 验收项 | 独立复审核对 |
| --- | --- |
| A01～A03 | Retry 的计划生成／检查 SubFlow、由 JudgeNode 得出的 Match 路由、Each 独立加工、Iter 跨轮正文状态及 Finalize 在同一父 Flow 中相连。`RevisionPolicy` 是第三个独立 Ref 位置，其值通过五字段 `bind!` 进入最终 Output；双位置消费 tuple、字段投影、先投影后消费与跨 Flow Ref 构建期拒绝均有外部测试。正逆序测试现在使用同一组三个节点，仅顺序不同。 |
| A04～A05 | 组合型 child 的实际调用重新经过 Runtime；事件序列与现有递归调用测试支持路径审查。Retry 早停／耗尽／技术错误、Match 唯一分支／未命中／分支错误、Each／Iter 空集合与中途错误、正常修订状态均分开验证；错误后无部分正常 Output，已发生的测试替身事件不被宣称回滚。 |
| A06～A07 | 四组分属 Retry／Match／Each／Iter 的外部正反向编译探针已存于 `tests/compile_probes/`；独立复核正向均编译通过、负向各仅有目标类型错误且诊断位置与交接报告一致。关键 Item／状态不要求 `Clone`；投影复制口径已更正为本流程 7 次 String 字段克隆，根 `Brief`／最终位置的 `StoryState` 不因投影被移动。复审进一步把“整值读取 N−1 次克隆”的简式限定为没有投影／消费混合的情形。 |
| A08～A10 | README 增加首次使用路径与 11 个示例的学习顺序，crate 首页清除 T07 阶段性表述；Fake 模型仅在示例／测试共享文件中，不进入正式核心公共 API。外部 API 审查未发现必须调整的签名，T03 已知 `#[doc(hidden)]` Binding 构造入口漏洞继续如实保留；无新依赖、feature、crate 或发布行为。 |

独立复跑：`cargo fmt --all -- --check`、`cargo check --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets`（141 项通过）、`cargo test --doc`（33 项运行、17 项预期编译失败）、`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` 与全部 11 个离线示例，均通过。另以仓库 README 的方法逐一编译四组探针：每组正向通过，负向分别得到预期的 E0271／E0308，且均只有目标错误。

**保留边界：**当前测试与示例不证明 Binding 的隐藏公开构造入口已被类型系统封死，不证明真实性能、真实 LLM 或发布包质量。`tests/compile_probes/` 是可复核的独立源文件，不是 Cargo 自动测试目标。Rust 1.85 的 MSRV、包元数据与 docs.rs 发布前检查留给 T11；T08 通过**不自动关闭 G3**。
