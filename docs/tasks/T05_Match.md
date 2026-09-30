# T05 — Match：依据已有路由值执行唯一分支

> 状态：**COMPLETED；2026-09-30 独立复审通过；G2 未关闭**。
> 前置：T01～T04 已通过；G1 已单独验收通过；G2 尚未关闭。
> 建议实施基线：`f56cba5`（已提交的 T04 完成代码与本任务书草稿）；实施前须核对该提交及工作区差异。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T05；通过后不自动开始 T06～T07，也不关闭 G2。

## 1. 任务目标与边界

实现第二个控制型 Executable：`Match`。它读取**已经形成**的路由值 `K`，从多个分支中选择且只选择一个执行；被选分支接收同一次调用携带的业务 Input `I`，返回共同的 Output `O`：

```text
JudgeNode → K
             │
        Match<K, I, O>，Input = (K, I)
             ├─ 命中 case → Runtime.execute(对应 branch, I)
             ├─ 未命中、有 default → Runtime.execute(default, I)
             └─ 未命中、无 default → 可识别的 NoMatch 执行错误
```

不同分支可以是不同具体 Rust 类型（例如 Node、Flow、Retry），但都必须满足 `Executable<Input = I, Output = O>`。公开 Match 类型固定为可命名的 `Match<K, I, O>`，实现 `Executable<Input = (K, I), Output = O>`；它与其他 child 一样通过 `flow.then(match, binding)` 接入父 Flow。Match 只做路由，不从业务数据中推导 `K`，不运行多个分支，不按顺序试错。已选分支返回 `ExecutionError` 后立即原样传播，不能转去 default。

本任务不实现谓词路由、范围匹配、排序优先级、动态表达式 DSL、并发分支、技术故障恢复、Match／Each／Iter 之外的控制语义、日志、Trace、持久化或第二个 crate。T04 的 Retry 可作为现有分支类型使用，但不得为了 Match 改变 Retry 语义。

## 2. 执行前必须阅读

1. 根目录 `AGENTS.md`、[任务总览](README.md)及 G1 验收记录、[T01](T01_Async_Execution_Foundation.md)～[T04](T04_Retry.md) 的验收记录。核对现有 `Executable`、`Runtime`、`FlowBuilder::then`、Binding、`ExecutionError`／`FlowBuildError` 和公开示例，确认 T04 代码是实施起点。
2. [规范性设计](../SRFlow_Design_v2.0.md)的 §3.8～3.10、§4.2～4.8、§5.2～5.5、§7.4～7.7、§8.1～8.3、§9.1、§9.3、§10.3～10.5、§10.8～10.13、§11.1～11.3、§11.9～11.10、§12.3、§13.3 第四轮及 §13.6 的 R-01、R-02、R-04、R-11、R-12、R-18～R-24。

先做小型可编译 API 验证：两个不同具体类型、同一 `I/O` 的分支如何注册；`K` 与 `I` 如何从父 Flow 的 Binding 组合；default／NoMatch 的公共用法；无需把异构分支类型暴露给普通使用者。比较至少两种合理公开构造形式并记录取舍，例如可从 `Match::<K, I, O>::builder()` 开始，但具体构造器拼写由编译验证决定。构建期间无论已加入多少个 case，公开类型仍须可命名，不能把 case 数量或每个具体 branch 类型累积进使用者必须书写的泛型类型。不把 Compile Probe 的 `Vec`、trait object 名称或宏语法当作必须照抄的方案。

## 3. 本任务交付

### 3.1 路由与分支契约

- Match 的一次 Input 由已有匹配值 `K` 和被选分支的业务 Input `I` 组成。`K` 应由上游 JudgeNode、Flow Input 或其他已明确的数据位置提供；Match 不接收 `Fn(&I) -> K` 一类隐藏业务判断，也不从 `I` 中自行计算路由。
- 每个 case 关联一个 `K` 与一个 `Executable<I, O>`。分支具体类型可以不同，但 `I/O` 必须完全一致；错误 Input、Output 或父 Flow Binding 接线在编译期被拒绝。`I` 仅交给被选分支一次，`O` 按值返回；不得为试探分支而克隆 `I/O`，也不得要求它们普遍实现 `Clone`。
- 路由使用 `K` 的等值语义，不引入谓词、范围或字符串解析作为普通 case 入口。**有意选择 `K: Eq` 作为语义契约**：`PartialEq` 虽足够完成一次线性比较，却不保证自反性；Match 应能使一个按正常等价关系注册的键命中自身。这个要求不是查找容器的副产物。不因容器选择再要求 `Hash`、`Ord`、`Clone` 或 `Debug`。`Eq` 实现是否真正满足等价关系仍由键类型提供者负责。
- 一次执行最多调用一个分支。case 无论注册顺序如何，只根据 `K` 选择；未选分支及 default 不执行。路由选择本身不经 Runtime；**被选分支的实际执行**必须重新经过父级传入的同一个 Runtime。Match 不能把选择逻辑塞进 Runtime。

### 3.2 未命中、default 与错误

- 只有没有 case 命中时才可执行 default。无 default 时返回公开、非泛型、**不携带业务键 `K`** 的 `NoMatch` 错误类型；它实现 `Display + std::error::Error + Send + Sync + 'static`，作为 `ExecutionError::Failed` 的来源，使外部能通过 `error.source()?.downcast_ref::<NoMatch>()` 识别。不得依赖错误文本、返回默认 `O`、静默跳过或自动取第一个 case。`NoMatch` 不因诊断需求对 `K` 另加 `Debug`／`Clone`／`Sync` 约束。
- 已命中的 branch 若返回 `ExecutionError`，包括它自身的技术错误，Match 必须原样传播；不得继续寻找其他 case 或改走 default。default branch 自身出错也原样传播。未命中错误是 Match 的正常执行语义失败，不是框架内部 `Invariant`，也不是业务上“不接受”的正常 Output。
- 本任务选择**在构建／注册阶段拒绝重复 case 键**（按 `Eq` 判断），不采用“第一个胜出”或“最后一个覆盖”的隐式优先级。公开构建错误定名 `MatchBuildError`，至少区分 `DuplicateCase` 与 `DuplicateDefault`，并标记 `#[non_exhaustive]` 以允许今后新增构建错误；不得把这些变体塞进 Flow 专用的 `FlowBuildError`，也不得与运行期 `NoMatch`／`ExecutionError` 混用。重复注册不得 `panic!`。采用 T03 `FlowBuilder::commit_plan` 的**先完整校验、再一次性提交**先例：失败后原有 case/default 保持不变，构建对象可继续使用。`case(k, executable)` 若因重复而被拒绝，传入的 `k` 与 `executable` 由该调用消费并丢弃，不作为错误载荷返还；调用方可另备键和分支重新注册。第二次设置 default 返回 `DuplicateDefault`，不静默覆盖；被拒绝的 default 分支同样由调用消费并丢弃。
- **空 case 集合可以存在**：有 default 时只执行 default，无 default 时稳定返回 `NoMatch`，不在运行时产生不变量错误。这是设计 §9.3.6／§10.8 对“没有 case 命中”的直接推论，不是新增的控制语义；因此 API 不得使用“至少一个 case”的 typestate 限制。重复键的构建期拒绝是本任务为避免隐含优先级作出的**已审定任务级 API 选择**，与 §9.3.4 的唯一执行路径一致；上位设计未规定具体处理阶段，本次不改写已评审的规范性设计。
- 本任务不定义 case 优先级、模糊匹配或匹配值的字符串化诊断。公开 Rustdoc 与 crate 根导出须区分**`MatchBuildError` 注册错误**、**`NoMatch` 未命中执行错误**和**被选分支返回的 `ExecutionError`**三个阶段。

### 3.3 Rust 类型、所有权和异步边界

- Match 内部可有限类型擦除以保存异构分支；公开构造、Flow 接线和 Output 必须保持强类型，不要求业务方使用 `Any`、原始 trait object、slot 或动态类型标签。擦除适配层仅转发到 `Runtime::execute`，不直接调用 child 的 `Executable::execute`。
- T05 **选择直接、不可变地存储 case 键**，不为规避 trait bound 而用 `Mutex<K>` 包住本来只读的键。在这个选择下，Match 作为 Flow child 时须满足 `FlowBuilder::then` 的 `E: Send + Sync + 'static`；因为 Match 持有 `K`，键须 `K: Eq + Send + Sync + 'static`。这里的 `Sync` 是**本任务实现策略与 Flow child 契约共同造成的约束**，不是 Rust 类型系统对所有可能的 Match 存储方式的绝对定理：本地编译已确认 `Mutex<Cell<u32>>: Sync`，以锁包装 `Send + !Sync` 键在类型层面可行，但会给纯路由增加同步与锁错误处理，T05 不采用。若单独执行的实现无需 `K: 'static`，不应把 Flow child 的 `'static` 无条件提前加到所有构造入口；交接报告写明各阶段的真实 bounds。
- `I/O` 不存于 Match 定义中，执行值只需既有 `Send`；作为 Flow child 时再受现有 `I/O: 'static` 约束，不得额外要求业务 `I/O: Sync` 或 `Clone`。`K`、`I`、`O` 均无需 `Clone`。用正向外部探针证明非 `Clone` 的 `K/I/O` 和 `Send + !Sync` 的 `I/O` 可执行；另用只差键的 `Sync` 性质的正反向探针，证明本任务的直接存储方案拒绝 `Send + !Sync` 的 `K` 作为 Flow child，诊断应确实落在相关 trait bound（不强求必须在 `then` 那一行）。不得把这一负向结果误报成“所有 Rust 实现都不可能支持 `!Sync K`”。
- 分支作为 Match 内部长期持有的 Executable，可能需要 `Send + Sync + 'static`；所选方案必须解释每个 bound 的来源，并区分**单独经 Runtime 执行**与**作为 Flow child**的约束。不得把这些约束无条件加到所有 Node／Executable 或业务 `I/O` 上。
- 同一 Match 定义可反复调用，也可有交叠的异步调用；一次调用的 `(K, I)`、选中结果和瞬时状态不得串到另一调用。允许 branch 自身显式持有资源，但 Match 不得把上一次 Output 当作下一次路由值或 Input。
- 若异构分支需要包装异步 Future，应维持 T01 的 `Send` Future 契约。包装、分配与查找复杂度属于实现策略，应在交接中写清成本；不为追求零分配提前修改 `Executable`、`Runtime` 或 Flow 核心接口。

### 3.4 文档、示例与回归

新增公共项及 crate 首页 Rustdoc：解释可命名的 `Match<K, I, O>` 与 `(K, I) → O`、路由值由上游产生、异构分支的统一 `I/O`、唯一执行路径、default 的严格触发条件、`NoMatch` 的类型化识别、`MatchBuildError` 与执行错误的区别、Input／Output 所有权与实际 bounds。README 增加 `match` 示例入口。

新增至少一个离线可运行的循序渐进示例：`JudgeNode` 先输出清晰的路由值，再由父 Flow 用 tuple Binding 把该 Ref 与已有业务 Input 组成 `(K, I)` 交给 Match；至少展示两个不同具体类型的分支（例如 Node 与 Flow）、未命中时有 default 的路径，以及**一条非 `Clone` 数据路径**。当某个 Ref 指向非 `Clone` 的 `K` 或 `I` 时，须在 tuple 内用 `consume(ref)` 显式交出它，不能用裸 Ref 假装可复制；不要求示例同时让 `K` 和 `I` 都非 `Clone`，但两者均非 `Clone` 的能力须由外部测试覆盖。另用外部测试覆盖无 default 的 `NoMatch`，不以一个庞大的 SES 场景代替最小用法。可用已有 Retry 作为第三种分支做组合验证，但不要求示例为了展示它而变复杂。

测试至少覆盖：首／中／末 case 命中且仅被选分支执行；case 顺序不构成优先级；未命中有 default／无 default；空 case 集合两种情况；重复键与重复 default 的构建期处理及失败后恢复；被选 case 与 default 分别出错时原样传播且不改路由；非 `Clone` 键／输入／输出；不同具体类型的 branch；SubFlow branch；Match 作为父 Flow child；同一实例的重复与交叠执行。旧任务的测试、Rustdoc 与示例保持可运行。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | 可命名的 `Match<K, I, O>` 是强类型 `Executable<Input = (K, I), Output = O>`，异构具体分支拥有共同 `I/O`；`K` 是已有判断而非 Match 内部计算 | 公共 API 审查、JudgeNode → Binding → Match 的外部示例、两种以上分支类型的正向探针 |
| A02 | 命中时只执行对应 case，顺序不形成试错链；Input 仅交给一个分支 | 首／中／末 case 的独立调用计数与结果断言，未选分支和 default 计数均为零 |
| A03 | default 只处理未命中；无 default 返回不携带 `K`、可按类型识别的 `NoMatch`，不返回空 Output | 有／无 default、空 case 集合测试；外部代码对 `Error::source` 下转型为 `NoMatch`，并确认错误类型自身满足 `Send + Sync + 'static` |
| A04 | 被选分支与 default 的错误原样传播，不转走另一分支、不回滚 | 含 default 的命中 branch 失败测试、default 自身失败测试、错误来源与调用计数 |
| A05 | 重复 case 键及重复 default 不发生静默覆盖，`MatchBuildError` 与运行错误分离，构建失败无部分改动 | `DuplicateCase`／`DuplicateDefault` 错误变体；失败后继续注册合法分支并执行的外部测试；被拒绝的键与分支按文档消费并丢弃 |
| A06 | 错误分支 `I/O` 或父 Flow Binding 类型在编译期拒绝，且失败原因正确 | 每项负向探针配只差错误接线的正向对照；交接报告附实际编译诊断码、摘要与报错位置，不只依赖 `compile_fail` |
| A07 | 选中 branch 重新经过同一个 Runtime；异构存储不泄漏至普通使用者 | 执行路径审查、SubFlow branch 嵌套测试、Match 作为 Flow child 的测试，必要时用组合型 branch 计数 |
| A08 | 不为路由克隆 `K/I/O`，不额外要求业务 `I/O: Sync`；直接存储键的 `K: Sync` 与 Flow child 的 `'static` 限制如实说明 | 非 `Clone` 键／输入／输出及 `Send + !Sync` 的 `I/O` 正向探针；`Send + !Sync K` 作为 Flow child 的成对负向探针；存储与 Future 包装策略说明 |
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

交接报告须列出实施基线 `f56cba5` 及实际起点、修改文件、A01～A10 对应证据、两个公共 API 候选及取舍、重复键／default 策略、`NoMatch` 的公开识别方式、三类错误的发生阶段、异构分支存储与 Future 的成本和 bounds、外部正反向编译探针的实际诊断、依赖／MSRV 变化及未覆盖事项。不得自行将 T05 标为 `COMPLETED`、关闭 G2 或开始 T06～T07。

## 6. 审查与开始规则

本任务书已由用户审定并完成实施。T05 经独立复审通过；G2 仍须在 T04～T07 均通过后单独验收。本任务通过不自动授权 T06～T07 的实现。

## 7. 验收记录（2026-09-30）

**结论：T05 PASS；G2 未关闭。** 对照规范性设计 §9.1、§9.3、§10.8、§13.6 的 R-01／R-11／R-12 复核，Match 只消费已形成的路由值，选择唯一分支；未命中与分支错误遵守设计的不同路径。未发现需要修改上位设计的偏差。

| 验收项 | 复审证据 |
| --- | --- |
| A01～A02 | 公开 `Match<K, I, O>` 实现 `Executable<Input = (K, I), Output = O>`；Node、Flow、Retry 等不同具体分支共用强类型 `I/O`。首／中／末 case 测试逐一断言仅选中分支执行，登记顺序不构成优先级；JudgeNode → tuple Binding → Match 示例展示判断与路由分离。 |
| A03～A05 | default 只在未命中时执行；无 default 返回来源可下转型为 `NoMatch` 的执行错误，空 case 集合两种路径均有测试。被选分支与 default 的错误原样传播，不改走其他路径。重复键／default 在登记期由独立的 `MatchBuildError` 拒绝，失败不覆盖既有配置，构建器可继续使用。 |
| A06～A08 | 分支及父 Flow 的错误类型接线在编译期拒绝；交接报告提供成对正反向探针和实际诊断。私有分支擦除层只调用 `Runtime::execute`，SubFlow、嵌套 Match 与父 Flow child 路径均经验证。非 `Clone` 的 `K/I/O` 与 `Send + !Sync` 的 `I/O` 可执行；直接存键的 `K: Sync` 限制有对应负向编译证据。 |
| A09～A10 | 同一 Match 实例的交叠／重复调用保持输入与路由隔离；Rustdoc、README、离线示例均可用。未加入谓词 DSL、并行分支、技术错误改路由、Each／Iter、日志、Trace、持久化、新依赖或第二个 crate。 |

独立复跑：`cargo fmt --all -- --check`、`cargo check --all-targets`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-targets`（98 项通过）、`cargo test --doc`（27 项运行、13 项预期编译失败）、`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`，以及八个离线示例，全部通过。复审发现的两处 Rustdoc 不准确表述已修正：`NoMatch` 不携带且不能恢复已移入执行的键；登记错误只拒绝本次登记，不妨碍已有配置继续构建。

**保留边界：**重复 case 键在构建期拒绝是已审定的 T05 任务级 API 选择；§9.3 上位设计只规定唯一执行路径，本次未改写规范性设计。`NoMatch` 为不携带 `K` 的类型化错误，不能提供具体键值诊断。当前异构分支每次执行分配一个 boxed Future，查重与查找采用线性扫描；这些是可替换实现策略，不是设计语义。Rust 1.85 MSRV 尚未在该工具链独立实测，最迟 T11 发布验收前补测。T05 通过**不自动关闭 G2**。
