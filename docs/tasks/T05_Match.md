# T05 — Match：依据已有路由值执行唯一分支

> 状态：**DRAFT；待用户审定，尚未授权实现**。
> 前置：T01～T04 已通过；G1 已单独验收通过；G2 尚未关闭。
> 建议实施基线：T04 完成后的代码与提交；当前任务书起草时 T04 尚未提交，不以旧的 `66481b6` 作为实施基线。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T05；通过后不自动开始 T06～T07，也不关闭 G2。

## 1. 任务目标与边界

实现第二个控制型 Executable：`Match`。它读取**已经形成**的路由值 `K`，从多个分支中选择且只选择一个执行；被选分支接收同一次调用携带的业务 Input `I`，返回共同的 Output `O`：

```text
JudgeNode → K
             │
        Match<(K, I), O>
             ├─ 命中 case → Runtime.execute(对应 branch, I)
             ├─ 未命中、有 default → Runtime.execute(default, I)
             └─ 未命中、无 default → 可识别的 NoMatch 执行错误
```

不同分支可以是不同具体 Rust 类型（例如 Node、Flow、Retry），但都必须满足 `Executable<Input = I, Output = O>`。`Match` 自身实现 `Executable<Input = (K, I), Output = O>`；它与其他 child 一样通过 `flow.then(match, binding)` 接入父 Flow。Match 只做路由，不从业务数据中推导 `K`，不运行多个分支，不按顺序试错。已选分支返回 `ExecutionError` 后立即原样传播，不能转去 default。

本任务不实现谓词路由、范围匹配、排序优先级、动态表达式 DSL、并发分支、技术故障恢复、Match／Each／Iter 之外的控制语义、日志、Trace、持久化或第二个 crate。T04 的 Retry 可作为现有分支类型使用，但不得为了 Match 改变 Retry 语义。

## 2. 执行前必须阅读

1. 根目录 `AGENTS.md`、[任务总览](README.md)及 G1 验收记录、[T01](T01_Async_Execution_Foundation.md)～[T04](T04_Retry.md) 的验收记录。核对现有 `Executable`、`Runtime`、`FlowBuilder::then`、Binding、`ExecutionError`／`FlowBuildError` 和公开示例，确认 T04 代码是实施起点。
2. [规范性设计](../SRFlow_Design_v2.0.md)的 §3.8～3.10、§4.2～4.8、§5.2～5.5、§7.4～7.7、§8.1～8.3、§9.1、§9.3、§10.3～10.5、§10.8～10.13、§11.1～11.3、§11.9～11.10、§12.3、§13.3 第四轮及 §13.6 的 R-01、R-02、R-04、R-11、R-12、R-18～R-24。

先做小型可编译 API 验证：两个不同具体类型、同一 `I/O` 的分支如何注册；`K` 与 `I` 如何从父 Flow 的 Binding 组合；default／NoMatch 的公共用法；无需把异构分支类型暴露给普通使用者。比较至少两种合理公开构造形式并记录取舍，不把 Compile Probe 的 `Vec`、trait object 名称或宏语法当作必须照抄的方案。

## 3. 本任务交付

### 3.1 路由与分支契约

- Match 的一次 Input 由已有匹配值 `K` 和被选分支的业务 Input `I` 组成。`K` 应由上游 JudgeNode、Flow Input 或其他已明确的数据位置提供；Match 不接收 `Fn(&I) -> K` 一类隐藏业务判断，也不从 `I` 中自行计算路由。
- 每个 case 关联一个 `K` 与一个 `Executable<I, O>`。分支具体类型可以不同，但 `I/O` 必须完全一致；错误 Input、Output 或父 Flow Binding 接线在编译期被拒绝。`I` 仅交给被选分支一次，`O` 按值返回；不得为试探分支而克隆 `I/O`，也不得要求它们普遍实现 `Clone`。
- 路由使用 `K` 的等值语义，不引入谓词、范围或字符串解析作为普通 case 入口。默认以 `K: Eq` 为最小公共比较契约；不因内部查找容器而额外要求 `Hash`、`Ord`、`Clone` 或 `Debug`。如实现发现必须增加 bound，先用外部编译证据说明原因并提交复审，不得静默收紧。
- 一次执行最多调用一个分支。case 无论注册顺序如何，只根据 `K` 选择；未选分支及 default 不执行。路由选择本身不经 Runtime；**被选分支的实际执行**必须重新经过父级传入的同一个 Runtime。Match 不能把选择逻辑塞进 Runtime。

### 3.2 未命中、default 与错误

- 只有没有 case 命中时才可执行 default。无 default 时返回可由外部代码按**类型**识别的 `NoMatch` 执行失败（例如作为 `ExecutionError::Failed` 的来源）；不得依赖错误文本、返回默认 `O`、静默跳过或自动取第一个 case。`NoMatch` 不应为了描述 `K` 而强迫业务键实现 `Debug`／`Clone`／`Sync`。
- 已命中的 branch 若返回 `ExecutionError`，包括它自身的技术错误，Match 必须原样传播；不得继续寻找其他 case 或改走 default。default branch 自身出错也原样传播。未命中错误是 Match 的正常执行语义失败，不是框架内部 `Invariant`，也不是业务上“不接受”的正常 Output。
- 本任务选择**在构建／注册阶段拒绝重复 case 键**（按 `Eq` 判断），不采用“第一个胜出”或“最后一个覆盖”的隐式优先级。重复注册不得 `panic!`，要有与运行期 `NoMatch`／`ExecutionError` 分离、可识别的构建错误；失败后不得留下部分注册，也不能损坏可继续使用的构建对象。若 API 允许设置 default，第二次设置不得静默覆盖，须明确拒绝或通过类型状态阻止。空 case 集合可以存在：有 default 时只执行 default，无 default 时稳定返回 `NoMatch`，不在运行时产生不变量错误。
- `Eq` 实现本身的等价关系正确性由键类型提供者负责；本任务不定义 case 优先级、模糊匹配或匹配值的字符串化诊断。公开文档须区分**注册错误**、**未命中执行错误**和**被选分支错误**三个阶段。

### 3.3 Rust 类型、所有权和异步边界

- Match 内部可有限类型擦除以保存异构分支；公开构造、Flow 接线和 Output 必须保持强类型，不要求业务方使用 `Any`、原始 trait object、slot 或动态类型标签。擦除适配层仅转发到 `Runtime::execute`，不直接调用 child 的 `Executable::execute`。
- 分支作为 Match 内部长期持有的 Executable，可能需要 `Send + Sync + 'static`；所选方案必须解释每个 bound 的来源，并区分**单独经 Runtime 执行**与**作为 Flow child**的约束。不得把这些约束无条件加到所有 Node／Executable 或业务 `I/O` 上。至少证明非 `Clone` 的 `K`、`I`、`O` 可完成一次路由；`I/O` 只需既有 `Send`，不应被额外要求 `Sync`。`K` 在 `Executable::Input` 中也需 `Send`；若实现方案额外要求 `K: Sync`，先给出必要性证据与替代方案比较，不能仅因把键存进容器就默认扩大公共约束。
- 同一 Match 定义可反复调用，也可有交叠的异步调用；一次调用的 `(K, I)`、选中结果和瞬时状态不得串到另一调用。允许 branch 自身显式持有资源，但 Match 不得把上一次 Output 当作下一次路由值或 Input。
- 若异构分支需要包装异步 Future，应维持 T01 的 `Send` Future 契约。包装、分配与查找复杂度属于实现策略，应在交接中写清成本；不为追求零分配提前修改 `Executable`、`Runtime` 或 Flow 核心接口。

### 3.4 文档、示例与回归

新增公共项及 crate 首页 Rustdoc：解释 `(K, I) → O`、路由值由上游产生、异构分支的统一 `I/O`、唯一执行路径、default 的严格触发条件、`NoMatch` 的类型化识别、注册错误与执行错误的区别、Input／Output 所有权与实际 bounds。README 增加 `match` 示例入口。

新增至少一个离线可运行的循序渐进示例：`JudgeNode` 先输出清晰的路由值，再用 Binding 把该值和已有业务 Input 组装给 Match；至少展示两个不同具体类型的分支（例如 Node 与 Flow），以及未命中时有 default 的路径。另用外部测试覆盖无 default 的 `NoMatch`，不以一个庞大的 SES 场景代替最小用法。可用已有 Retry 作为第三种分支做组合验证，但不要求示例为了展示它而变复杂。

测试至少覆盖：首／中／末 case 命中且仅被选分支执行；case 顺序不构成优先级；未命中有 default／无 default；空 case 集合两种情况；重复键与重复 default 的构建期处理及失败后恢复；被选 case 与 default 分别出错时原样传播且不改路由；非 `Clone` 键／输入／输出；不同具体类型的 branch；SubFlow branch；Match 作为父 Flow child；同一实例的重复与交叠执行。旧任务的测试、Rustdoc 与示例保持可运行。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | `Match` 是强类型 `Executable<(K, I), O>`，异构具体分支拥有共同 `I/O`；`K` 是已有判断而非 Match 内部计算 | 公共 API 审查、JudgeNode → Binding → Match 的外部示例、两种以上分支类型的正向探针 |
| A02 | 命中时只执行对应 case，顺序不形成试错链；Input 仅交给一个分支 | 首／中／末 case 的独立调用计数与结果断言，未选分支和 default 计数均为零 |
| A03 | default 只处理未命中；无 default 返回可按类型识别的 `NoMatch`，不返回空 Output | 有／无 default、空 case 集合测试；`Error::source` 的下转型或同等类型化识别 |
| A04 | 被选分支与 default 的错误原样传播，不转走另一分支、不回滚 | 含 default 的命中 branch 失败测试、default 自身失败测试、错误来源与调用计数 |
| A05 | 重复 case 键及重复 default 不发生静默覆盖，构建失败无部分改动 | 明确的构建错误或类型状态证据；失败后继续注册合法分支并执行的外部测试 |
| A06 | 错误分支 `I/O` 或父 Flow Binding 类型在编译期拒绝，且失败原因正确 | 每项负向探针配只差错误接线的正向对照；交接报告附实际编译诊断码、摘要与报错位置，不只依赖 `compile_fail` |
| A07 | 选中 branch 重新经过同一个 Runtime；异构存储不泄漏至普通使用者 | 执行路径审查、SubFlow branch 嵌套测试、Match 作为 Flow child 的测试，必要时用组合型 branch 计数 |
| A08 | 不为路由克隆 `K/I/O`，不额外要求业务 `I/O: Sync`；实际 `Send`／`Sync`／`'static` bound 已说明 | 非 `Clone` 键／输入／输出与 `Send + !Sync` 业务值的外部探针；存储与 Future 包装策略说明 |
| A09 | 同一 Match 定义重复／交叠调用隔离；文档、示例、旧功能不回归 | 同一实例两次交叠调用使用不同 `(K, I)` 的测试、文档测试、七个旧示例与新增示例运行 |
| A10 | 不实现谓词 DSL、并行分支、技术错误改路由、Each／Iter、日志、Trace、持久化或新 crate | 改动范围、依赖与公开项审查 |

**关键复审风险：**把路由生成塞进 Match；把重复键变成隐含优先级；把 branch 错误误认为“未命中”；只测默认分支却不测无 default；内部异构包装绕过 Runtime；为了方便存储而给业务数据加上不必要的 `Clone`／`Sync`；`compile_fail` 因无关错误误通过。

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
cargo run --example <T05 Match 示例>
```

交接报告须列出实施基线、修改文件、A01～A10 对应证据、两个公共 API 候选及取舍、重复键／default 策略、`NoMatch` 的公开识别方式、三类错误的发生阶段、异构分支存储与 Future 的成本和 bounds、外部正反向编译探针的实际诊断、依赖／MSRV 变化及未覆盖事项。不得自行将 T05 标为 `COMPLETED`、关闭 G2 或开始 T06～T07。

## 6. 审查与开始规则

本任务书目前只是草稿，须经用户审定后才能交给执行者实施。实现完成后独立复审；不通过则修订同一任务，通过后由审查者更新 T05 与任务总览的状态和证据。T05 单项通过不自动关闭 G2，也不自动授权下一个控制型 Executable。
