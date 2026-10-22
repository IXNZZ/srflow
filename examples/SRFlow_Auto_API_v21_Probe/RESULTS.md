# SRFlow v2.1 Public API 重构 Probe 结果

日期：2026-10-21。来源提交：`bb8d19497c70c828f35140a43edd2ba692b2371b`，准备时正式工程工作区干净。工具链：Rust/Cargo 1.97.1 stable，edition 2024，safe Rust。

**结论：在已验证范围内可行。新 API 能落到现有 Definition、Node Adapter 与控制结构，不需要重新设计 Execution Core。** 需要调整的是输入类型推导桥、显式输出的构建元数据、Choose/Each 的类型化登记时机，以及可复用 Definition 的 Root 执行入口。以下结论是本次可行性自检，尚未接受独立复审，也不表示正式 API 已冻结。

## 1. 可行性矩阵

“可直接表达”指目标用法已在隔离副本编译或执行，不表示当前正式 crate 已提供该 API。“需内部调整”保留用户确定的使用语义。

| 编号 | 结论 | 实际证据与必要处理 |
| --- | --- | --- |
| P01 统一 Body | 可直接表达 | `FnOnce(&mut Flow<'n>) -> ()`；`fn module(&mut Flow)` 用于 Chain，Root 用 `output`。函数返回 Ref 的旧 Body 写法编译拒绝。见 `p01_modular_uniform_body_and_chaining`、最小示例及 `body_return_ref.rs`。 |
| P02 自动 Query 注入 | 需 Builder 调整，可行 | 从既有 PositionRecord 中逆序筛选可见且有公开业务来源的位置，直接存入现有 NodeStep.inputs。前后同类型来源、深层祖先、未导出 child、sibling、缺少来源均有见证；缺少来源在 Factory/Node 执行前拒绝。`p02_*`、`automatic_connections_store_concrete_immutable_positions`。 |
| P03 ref_of | 可直接表达 | 与自动注入共用 resolver；支持 Item、Current 与同类型遮蔽前保存 Ref。结构检查确认独立查询不增加 Step、Import、位置或 Export；只在后续实际使用时沿原机制记录 Import。`p03_*`、`ref_lookup_registers_neither_step_nor_import`。 |
| P04 Node / Provide | 需内部类型桥，可行 | 四种 then、两种 provide、普通方法链式返回、函数/函数引用/结构体/借用结构体/Arc/async closure 均编译并执行。原 Callable 的投影不能反推自动输入；通过 doc-hidden AutoCallable 直接匹配具体 Query 借用形状，随后仍调用原 Callable/Node Adapter。`p04_*`、`projection_inference.rs`。 |
| P05 Factory 重复执行 | 同步 Fn 方案可行 | Factory 作为零输入 Node；Each 每项、Retry 每次 Attempt 创建独立不可 Clone Token，真实 Drop 验证失败轮与成功轮均清理。构建计数为 1，构建期 Factory 调用数为 0；同一 Workflow 再执行仍按步骤创建值。`p05_*`、`p10_empty_each_*`。 |
| P06 Output / RefShape | 需声明元数据，可行 | 保留缺省/显式 unit 标记；声明一次、自动选择时固定来源、多输出与 output_with、错误 Shape、重复声明均检查。tuple 使用原生 RefShape，`Ref<(u32,u64)>` 和 `Ref<()>` 编译拒绝。`p06_*`、`output_is_frozen_at_declaration_not_end_of_body`。 |
| P07 五种控制结构 | 需调整静态登记，可行 | Chain/Choose/Each/Retry 接收 Body 内 output；Iter 只接收 next。保留原 register_* 和 boundary 执行。五种结构嵌套执行、Choose 仅选中分支、无匹配、Retry 预算、IterBreak 在 Next 已创建时仍返回 Current、多状态 Promote、次数耗尽均通过。`p07_*`。 |
| P08 Root Workflow | 可行，需复用入口 | `Workflow<'n,I,O>` 保存现有不可变 Definition；Root 声明与 O 在构建时核对，unit 可省略；Runtime 接收 `&Workflow` 和 owned I，每次新建 Execution，最终仍原样整组提取 owned O。错误输入类型编译拒绝。`p08_*`、最小示例、`wrong_runtime_input.rs`。 |
| P09 Rust 约束 | 已编译验证，范围见 §6 | 单一 Flow lifetime、链式可变借用、借用跨 await、非 Send 值/闭包、0/1/2/16 位置与完整 16 输入自动推导均通过。13 个拒绝样本＋1 个消费者正例。嵌套两次可变借用须使用中间变量，见 §2。 |
| P10 原执行语义 | 限定回归通过 | 原工程 536 库＋62 集成＋1 doctest；副本保留原 536 库测试全部通过，另有 3 项结构检查、32 项新消费者测试及 UI。Each 非 owned 来源拒绝、Root DataId 重复提取拒绝、Failure 不重试、控制预算、取消 Future 先于借用来源销毁、嵌套交接均有新见证。 |

测试名可在 [消费者测试](tests/auto_api.rs)、[结构测试](src/core/engine/probe_api.rs)、[编译验证入口](tests/ui_probe.rs) 中直接检索。完整最小用法见 [auto_minimal.rs](examples/auto_minimal.rs)。

## 2. Rust API 的具体约束与最小写法

### 2.1 自动 then 的输入推导

现有 `Callable<A,K>` 通过 `<A::Spec as InputSpec>::Borrowed<'a>` 描述函数参数。移除显式 input 后，编译器无法从该非可逆投影唯一推导 A；最小反例 [projection_inference.rs](tests/ui/projection_inference.rs) 得到 **E0283**。

Probe 加入一个内部适配桥，直接匹配 `Query<()>`、`Query<&A>`、`Query<(&A,&B,...)>`，由此确定既有 RefShape，再转交原 Callable。Struct Node 的 Input 通过现有 RootInput/RefShape 映射取得。业务不需要 turbofish、额外 Node 包装或实现新 Trait；没有修改 Node、Query 或运行期 Adapter 协议。新 trait/marker 是证明用的内部推导工具，正式工程应保持为使用者无需接触的支持面，不冻结本次命名或布局。

### 2.2 Factory 的已验证边界

已验证签名为 `F: Fn() -> T + 'n, T: Data`。普通构造函数、借用捕获以及持有 Rc/Cell/RefCell 等非 Send 依赖的闭包可用；每次真正执行才调用 Factory。没有 Clone、Copy、Send、Sync 或额外 `'static` 的 Factory 约束。

编译拒绝的情况：

| 写法 | 证据 | 最小处理 |
| --- | --- | --- |
| `provide(value)` | `factory_owned_value.rs`，E0277 | 改为构造函数/Factory；应用程序已有的不可复制 owned 值走 Root Input。 |
| `provide(move || owned_string)` | `factory_once.rs`，E0507 | 此闭包需要移出捕获值，只能调用一次；改为每次构造新值，或用 Root Input。 |
| 捕获 `count` 并直接 `count += 1` | `factory_mut.rs`，E0594 | 当前最小协议选择 Fn；需要显式共享计数时，可用调用者持有的 Cell/RefCell，Probe 不替用户隐式包装。 |
| `provide(async || ...)` | `factory_async.rs`，E0277 | 当前 Factory 返回同步 Data。异步生产走原零输入 Node。 |

这里没有宣称 FnMut 在架构上不可实现，也没有为 Provide 增加第二套异步协议。

### 2.3 RefShape 返回类型与构建错误

Node 的输出由 Node 类型确定，`then_output` 可直接推导。统一的非泛型 Flow Body 不通过 Rust 返回值携带控制结构输出类型，因此 `_output` 的接收方需要给出期望 RefShape，例如：

```rust
let report: Ref<Report> = flow.chain_output(|sub| {
    sub.then(&generate);
    sub.output::<Report>();
});
```

Probe 在构建时比较该期望与 Body 声明的逐槽类型；错误 Shape 不进入 Runtime。`Workflow<I,O>` 的 O 同样在构建时核对，**不是**声称 Rust 能静态追踪闭包内部曾调用的 output 方法。

自动方法逐槽选最近来源，不猜测同类型数据的业务角色。`Query<(&T,&T)>` 会连接同一最近来源两次；角色不同时应用 `then_with(node,(a,b))`。`output::<(T,T)>()` 同样选择同一位置两次，Root 的既有重复输出校验会拒绝；两个不同 owned 输出用 `output_with((a,b))`。这保持既有 Root 单次提取规则。

以下自然的嵌套调用会因两次可变借用得到 **E0499**：

```rust
flow.output_with(flow.provide_output(Config::new));
```

最小写法只是引入局部变量：

```rust
let config = flow.provide_output(Config::new);
flow.output_with(config);
```

链式 `.provide(...).then(...).chain(...)` 已验证不受此限制。

## 3. 与当前正式代码的差异清单

以下链接指向上两级的正式 crate；基线行号以准备时来源提交为准。

| 当前代码 | 当前行为 | 本次目标与最小差异 |
| --- | --- | --- |
| [flow.rs](../../src/core/engine/flow.rs) §then（原第 37 行） | `then(node,input) -> OutputRefs` | 原能力成为 then_with_output；新增自动输入入口以及普通链式形式。 |
| 同文件 §chain/choose/retry | Body 返回 RefShape，返回类型决定静态输出 | Body 返回 unit，显式声明输出；收集完声明后调用原 register_chain_outputs/register_retry。Choose 延后共享输出位置分配，核对所有分支 Shape 后登记。 |
| 同文件 §each（原第 231 行） | 闭包接收 Item Ref 并返回 O；O 决定 typed Collector | Body 只接 Flow；formal Item 已由原 Builder 分配，可被 resolver 查到。output::<O> 登记时捕获原 EachCollect 的 begin/finish 函数及聚合类型，复用原 register_each。 |
| 同文件 §iter（原第 175 行） | 闭包 `(current,flow) -> next` | formal Current 的 owner 仍为 round Body；通过 ref_of 获取；next 声明记录槽位，并继续使用 initial Shape 的 register_states/state_import_slots 和原 register_iter。 |
| [definition.rs](../../src/core/engine/definition.rs) 原第 873/1664/1721 行 | Builder/finish/build_definition 接收已有 Ref 与闭包返回输出 | 增加仅定义期 resolver、Root 输入登记/收口入口；Body 输出声明元数据在前端收集，统一来源校验仍走原 finish。内部 Exit/Bridge/EachLocal 不可被自动查找。 |
| [node.rs](../../src/core/engine/node.rs) 原第 61/73 行 | Callable 用投影输入映射；显式 Ref 为推导锚点 | 保留其执行桥，新增具体 Query 形状匹配的内部推导入口，见 E0283 证据。 |
| [runtime.rs](../../src/core/engine/runtime.rs) 原第 85 行 | execute 收定义闭包与 owned 输入，每次先构建 Definition | 增加执行已验证 Definition 的内部入口；旧 execute 仍可调用该入口，新 Runtime 使用 Workflow 内的同一 Definition。原执行函数体仅因 built 已为引用而去掉两处多余借用。 |
| [shape.rs](../../src/core/engine/shape.rs)、[data.rs](../../src/core/engine/data.rs) | tuple 是 Shape，Data 无 native tuple 实现 | 无需修改，没有增加 Tuple Data、Bundle 或字段级 Ref。 |

隔离副本只改了 4 个既有文件：lib.rs、engine/mod.rs、definition.rs、engine/runtime.rs；新增 proof API 与测试/示例。全部既有文件差异见 [BASELINE_DELTA.patch](BASELINE_DELTA.patch)。不是在正式 crate 外写一个只能访问旧 Public API 的包装器：这次证明包含隔离副本的 crate 内 Builder 扩展，正式落实同样需要内部改动。

## 4. 最小实施建议：按职责拆分

1. **Public API**：新增普通/_output、自动/_with 入口，统一 Body、output/next、ref_of、Factory、Workflow。保持 Node/Query/Ref/Data 原协议。显式标明错误发生在定义构建还是 Rust 编译。
2. **Definition Builder**：保留已有 Position/Body/Step；加当前位置的合法来源解析与缺少来源诊断，加每 Body 的声明状态及逐槽类型/位置。原静态 inputs 固化解析结果。Choose 延后输出登记；Each 从输出声明捕获现有 typed Collector 回调。元数据只能是定义期信息，不能保存执行值或变成第二执行计划。
3. **Adapter**：新增内部自动输入推导桥；Provide 通过零输入 Node 进入既有 Adapter。原借用、await、owned 输出登记路径可以原样保留。
4. **Engine 入口**：从原 Runtime::execute 拆出执行已验证 Definition 的入口，允许 Workflow 复用；每次调用继续新建 Execution/Context/Container。
5. **无需改变的算法**：run_body、Chain/Choose/Each/Retry/Iter 的实际控制、Export/Consume/Promote、取消退出、Scope owner、Container、Root 预检/提取算法。

正式实施应在用户审定的新契约/任务边界下进行；本 Probe 没有更新现有任务状态或修订正式规范，也没有为迁移、发布、缓存、并发或持久化扩展范围。

## 5. 证据与复跑结果

| 检查 | 结果 | 日志 |
| --- | --- | --- |
| 原工程 `cargo test --offline` | 536 lib＋62 集成＋1 doctest，通过 | [baseline_tests.log](evidence/baseline_tests.log) |
| 隔离副本 `cargo test --offline` | 539 lib（原536＋新3）＋32 消费者＋1 UI 入口＋1 doctest，通过 | [probe_tests.log](evidence/probe_tests.log) |
| UI 入口展开 | 13 编译拒绝＋1 编译并运行正例，通过 | [ui_tests.log](evidence/ui_tests.log)、[诊断](evidence/ui) |
| 最小示例 | 同一 Definition 执行两次，输出 42、20 | [example.log](evidence/example.log) |
| clippy workspace/all-targets，`-D warnings` | 通过 | [clippy.log](evidence/clippy.log) |
| fmt 检查 | 通过 | [format.log](evidence/format.log) |
| 原工程指纹 | 全部保持准备时内容；只有新增 Probe 目录 | [source_audit.json](evidence/source_audit.json) |

独立副本的 adapter.rs、boundary.rs、context.rs、scope.rs、data_container.rs、Core runtime.rs 等执行与数据算法文件与基线逐字相同。源码指纹证明没有修改这些文件；测试证明列出场景的可观察行为。两种证据各有范围，不能据此推定所有尚未覆盖的组合都已通过。

## 6. 真实限制与尚待确定项

没有发现要求推翻原 Core 才能实现本方案的阻碍。以下事项仍须保留边界：

- **Factory 最终支持面**：已证明简单同步 Fn 可用，FnOnce 不能承载重复 Body。是否进一步支持 FnMut，需要后续明确共享可变捕获与重入约束；当前方案不依赖它。
- **构建失败如何暴露**：目标 `Flow::build -> Workflow` 示例未规定错误返回形式。Probe 让 Workflow 保存 `Result<Definition,ProbeError>`，支持 validate，Runtime 拒绝无效定义。正式采用 `Result<Workflow,BuildError>` 或另一种显式形式需要定案；本次字符串诊断与校验方法不冻结。
- **声明元数据归属**：副本用随 Workflow 保留的 BodyContract 区分“未声明”与“显式 unit”，实际输出继续使用原 Body.output。正式工程应把该状态放回恰当的 Definition 构建/契约结构；不应沿用旧的空 Vec 代表两种状态。
- **验证广度**：新自动适配覆盖 0/1/2/16 输入和 16 位置 Root 输出；3～15 的各 arity 宏已随库编译，但未逐一运行消费者矩阵。多槽 Each/Iter 运行见证以 2 槽为代表，原库测试补既有机制回归；未宣称覆盖全部嵌套、故障注入、类型组合或迁移兼容性。
- **工程交付边界**：没有重新跑历史外部审查脚本、完整 UI/变异证据、性能或独立发布验证；没有测试并发执行同一 Workflow、线程转移、Definition 缓存或序列化。原阶段报告的未覆盖范围不因本 Probe 自动关闭。
- **错误路径占位**：无法解析或错误 RefShape 时，前端记录构建错误，并用不可登记的 handle 暂时完成 Rust 返回类型；该路径在 finish/Runtime 前整体拒绝，不分配假业务 Data，也不能作为合法来源。正式错误恢复布局可另选，不能省略拒绝保证。

这些是 API/交付细节与覆盖限制，不在本次 Probe 中被写成已确认的新增系统设计。
