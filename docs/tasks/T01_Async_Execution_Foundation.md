# T01 — 单 crate 工程与异步统一执行基础

> 状态：**READY；已获用户审定并授权执行，待实现与复审**。
> 前置：任务总览 G0 已通过。
> 实施仓库：SRFlow 独立 Git 仓库根目录。
> 本任务关闭范围：T01；**不关闭 G1**，也不自动开始 T02。

## 1. 任务目标

建立可继续演进的单个 `srflow` library crate，并在正式工程中打通最小异步执行闭环：

```text
Runtime.execute(Executable, Input)
                ↓
           Output / Error
```

普通使用者只需实现叶子型 `Node`，即可把它交给同一个 Runtime 入口；实现新执行语义的开发者可以直接实现 `Executable`，并在组合型 Executable 内通过 Runtime 调用 child。T01 的关键不是增加功能数量，而是证明两条路径共用同一执行协议，且错误与业务正常结果不混淆。

## 2. 执行前必须阅读

1. 仓库根目录的 `AGENTS.md` 与[任务总览](README.md)。
2. [SRFlow 规范性设计](../SRFlow_Design_v2.0.md)的 §3.1～3.3、§3.7～3.10、§4.1～4.5、§4.7、§5.1～5.5、§6.1～6.4、§10.2、§10.4、§10.11、§11.1～11.2、§11.7、§12.3，以及 §13.6 中与 Runtime、Node、错误和实现边界相关的规则。

上述设计语义高于本任务书。若 Rust 编译结果表明某个预想签名不合适，可以调整实现策略；不得因此让 Node 获得隐式编排职责，或让 child 绕过 Runtime。

## 3. 本任务交付

### 3.1 工程骨架

- **先建立并检查工程骨架，再实现执行协议。** 创建一个名为 `srflow` 的 library crate，不创建 workspace、第二个 crate 或示例用业务工程。T01 完成时的目录形状应为：

```text
srflow/                         # 本 Git 仓库根目录
├── Cargo.toml                   # 唯一的 crate manifest
├── src/
│   ├── lib.rs                   # crate 文档与常用公共入口
│   └── core/
│       ├── mod.rs               # 核心层公开边界
│       ├── executable.rs        # 统一执行契约
│       ├── runtime.rs           # 统一异步执行入口
│       ├── node.rs              # 叶子 Node 与自动适配
│       └── error.rs             # 本任务必需的错误支持
├── examples/                    # 本任务的两个实际可运行示例
├── tests/                       # 从 crate 外部视角使用公共 API
└── docs/                        # 已存在的设计文档与任务文档
```

- `src/lib.rs` 负责让普通使用者从 crate 根部取得 Runtime、Node、Executable 等常用公共契约；`src/core/` 保留给需要直接使用核心接口的扩展作者。公共类型可以重导出，但不得因此生成两套不同的类型或执行入口。
- 上述 `core` 文件各承担对应职责；测试专用组合型 Executable 放在测试或示例中，不进入生产核心。后续任务在 `src/core/` 下加入 Flow、Binding、控制语义模块，在 `src/llm/` 下加入可选 LLM 扩展；T01 不创建这些空壳目录或模块。具体测试和示例文件名可由执行者选择，但不得提交空文件占位。
- 明确 Cargo edition、最低支持 Rust 版本（MSRV）与当前直接依赖，并记录选择理由。crate 版本在 T01 中可以是暂定值，不代表 crates.io 发布版本已确定。
- 核心执行接口采用异步 Rust，但普通依赖中不强制绑定某个具体 executor。测试可以使用开发依赖运行异步代码。不要为了 T01 引入 HTTP／LLM SDK。

### 3.2 统一执行契约

- `Executable` 是公开的强类型执行协议；每个具体执行边界有明确 Input 与 Output。优先采用设计已验证的 associated type 方向；若采用其他形式，必须记录编译证据和理由。
- `Runtime` 提供对 Executable 的统一异步执行入口。Runtime 不判断目标是 Node 还是组合型 Executable，也不承担 Retry、Match、Flow 等尚未实现的语义。
- 普通 Node 使用者只实现 Node 本身，就能直接经 Runtime 执行，不需要手写第二份 Executable 实现，也不需要在调用处显式包装成另一类执行对象。
- Node 的业务执行接口不接收用于编排 SRFlow child 的 Runtime。Node 可以显式持有配置、客户端或共享资源，但本任务不得借此建立隐式业务数据流。
- Executable 的实现应能够在自己的执行期间通过同一个 Runtime 入口调用 child。T01 只用一个**测试用**组合型 Executable 证明此能力，不把它发展成生产 Flow、Retry 或新的核心概念。

### 3.3 结果与错误边界

- 正常业务结论（包括“不接受”“未通过”等否定结果）属于 Output；执行失败属于 Error。不要用 Error 代表正常业务判断，也不要把执行失败转换成默认业务 Output。
- child 的执行错误按默认 fail-fast 规则向调用方传播，并保留可追溯的错误来源；Runtime 不自动重试、跳过、回滚或切换备用路径。
- 具体 Rust Error 类型尚未由设计文档冻结。T01 只需形成足够表达上述语义、供后续 Flow 和控制型 Executable 继续传播的最小错误契约；记录其扩展性与取舍，避免预建日志、Trace 或完整错误分类系统。
- 同一 Executable 定义可多次调用；每次调用的瞬时执行数据不能无声明地成为下一次调用的 Input。Node 可以显式持有状态，但不能用该状态代替应由 Input／Output 表达的业务数据关系。

### 3.4 文档、示例与测试

- crate 首页 Rustdoc 提供最短上手路径，并说明 Runtime、Node、Executable 的分工；所有本任务新增的公共项都写明用途、关键边界和错误行为。
- 至少提供两个可离线运行的示例：一个展示普通使用者实现 Node 并调用 Runtime；另一个展示高级扩展者实现测试规模的组合型 Executable，并通过 Runtime 执行 child。示例不能依赖 SES 仓库、真实服务或密钥。
- 测试从公开 API 使用者角度覆盖正常 Output、业务否定 Output、执行错误、child 错误传播、重复调用与错误 Input 类型的编译期拒绝。后者可用编译失败测试或同等可复核证据，不规定具体测试工具。
- 对异步 trait、Future 的 `Send`／生命周期要求、普通依赖和开发依赖作简短实现决策记录，特别说明这些选择是否可能约束 T02 的异构 Flow；这不是要求提前实现 T02。

## 4. 验收矩阵

| 编号 | 必须证明 | 可接受证据 |
| --- | --- | --- |
| A01 | 仓库是单个可编译的 `srflow` library crate；目录与 §3.1 的层次和职责一致，根入口可取得常用公共契约，`core` 可供扩展作者使用，且不泄漏测试替身或未来功能空壳 | 目录树、Cargo 元数据、构建结果、公开项审查 |
| A02 | 用户只实现 Node，即可通过统一的异步 Runtime 入口获得强类型 Output | 仓库外部使用者视角的测试与入门示例 |
| A03 | 高级使用者可实现具有明确 Input／Output 的 Executable；输入类型错误被编译器拒绝 | 正向示例与编译失败证据 |
| A04 | 测试用组合型 Executable 调用 child 时重新经过 Runtime，而不是直接调用 child 的执行方法 | 测试用组合型实现、调用链测试与代码复审 |
| A05 | 正常业务否定是 Output，技术执行失败是 Error，child 错误保留来源并沿执行边界传播且不触发自动重试 | 正常、否定和错误路径测试 |
| A06 | 同一 Executable 可用不同 Input 重复调用；无隐式跨次业务数据传递 | 重复调用测试与接口审查 |
| A07 | 公开 Rustdoc 和两个离线示例可供独立使用者理解并运行；核心不强制绑定具体 executor | 文档构建、示例运行、依赖审查 |
| A08 | 没有实现 Flow、Ref、Binding、Retry、Match、Each、Iter、LLM、日志、Trace 或持久化 | 改动范围审查 |

验收 A04 不能靠在正式 Runtime 中加入生产用调用计数器、Trace 或诊断系统来完成；测试专用证据与代码路径审查即可。T01 通过不意味着异步 trait 的最终语法或全部公共 API 已永久冻结，T02／T03 可以在不改变设计语义的前提下作必要调整。

## 5. 验证与交接要求

执行者至少报告以下检查的命令和结果；若最终工程组织使某条命令不适用，应说明等价验证方式：

```text
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo test --doc
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

自动测试与示例的执行过程不得访问真实外部服务。首次解析或下载构建依赖与测试运行期间的业务网络调用应明确区分。

交接报告应列明：基于哪个提交实施、修改了哪些文件、使用了哪些直接／开发依赖及原因、异步 trait 与错误契约的主要取舍、各验收编号的证据、未解决的风险和所有未通过的检查。执行者不得自行修改本任务状态、关闭 G1 或开始 T02。

## 6. 审查与完成规则

本任务书已获用户审定，可以交给 OMP、Claude 或其他执行者实现。实现完成后由审查者对照 §4 独立复核；若不通过，继续修订 T01。全部验收通过后，审查者更新本任务与任务总览的状态和证据，再编写 T02 详细任务书。
