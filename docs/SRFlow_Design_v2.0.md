# System Runtime Workflow (SRFlow) Design v2.0

> **状态：SUPERSEDED（2026-10-03）**。当前总规范为 [SRFlow Design v2.1](SRFlow_Design_v2.1.md)。本文保留 v2.0 的历史条款及旧实现的验收依据；以下正文中的“当前”“最高设计标准”等措辞均属于规范切换前的历史语境，不再约束新的 v2.1 任务。

> **新实现依据：本文不再作为新实现、任务拆分或新验收的规范依据。** 新工作使用 v2.1、Core Design 和 Runtime Implementation Design；引用本文时须明确是历史对照、旧实现验收追溯或迁移差异说明。

> **文档名称**：System Runtime Workflow (SRFlow) Design v2.0
> **文件名**：`SRFlow_Design_v2.0.md`
> **框架简称**：SRFlow
> **文档性质**：已被替代的历史规范性设计文档
> **适用范围**：v2.0 旧实现、历史验收与迁移参考
> **设计状态**：核心模型与控制语义已完成业务反向验证和 Rust Compile Probe 验证

---

# 目录

## 0. 文档定位与规范性

- 0.1 文档目的
- 0.2 规范性地位
- 0.3 设计语义与实现策略的区分
- 0.4 已冻结、实现待定与暂不设计
- 0.5 文档演进规则

## 1. SRFlow 的目标与边界

- 1.1 SRFlow 要解决的问题
- 1.2 SRFlow 的核心目标
- 1.3 SRFlow 不是什么
- 1.4 SRFlow 的能力边界
- 1.5 面向 SES 的设计取向

## 2. 设计理念与基本理论

- 2.1 从真实执行需求中生长
- 2.2 Flow 是 SRFlow 的中心
- 2.3 调用、编排、执行语义与业务实现必须分离
- 2.4 Executable 是统一扩展协议
- 2.5 执行顺序与数据依赖相互独立
- 2.6 Flow 连接数据，Node 处理业务意义
- 2.7 业务判断与流程控制分离
- 2.8 不使用万能循环抽象
- 2.9 组合边界优先于内部结构
- 2.10 框架语义优先于 Rust 实现便利
- 2.11 不提前抽象未来能力
- 2.12 设计验证方法
- 2.13 SRFlow 的演进原则

## 3. 核心模型总览

- 3.1 四个核心角色
- 3.2 Runtime
- 3.3 Executable
- 3.4 Flow
- 3.5 Node
- 3.6 控制型 Executable
- 3.7 核心关系
- 3.8 执行关系
- 3.9 数据关系
- 3.10 核心模型不变量

## 4. Executable

- 4.1 定义
- 4.2 Input / Output 契约
- 4.3 一次执行的语义
- 4.4 重复调用
- 4.5 叶子型与组合型 Executable
- 4.6 Executable 边界
- 4.7 Executable 与错误
- 4.8 扩展规则

## 5. Runtime

- 5.1 定义
- 5.2 Runtime.execute
- 5.3 统一执行入口
- 5.4 递归 Runtime 调用
- 5.5 Runtime 不负责的事情
- 5.6 后续基础设施扩展边界

## 6. Node

- 6.1 定义
- 6.2 Node 的叶子语义
- 6.3 Node 的 Input / Output
- 6.4 Node 与 Runtime 的关系
- 6.5 通用 Node 与业务 Node
- 6.6 Node 不承担流程编排

## 7. Flow

- 7.1 定义
- 7.2 顺序语义
- 7.3 Flow Input
- 7.4 then
- 7.5 Ref
- 7.6 数据复用
- 7.7 Flow Output
- 7.8 SubFlow
- 7.9 Flow 边界
- 7.10 Flow 与数据依赖
- 7.11 Flow 不承担业务计算

## 8. Binding 与 Input 装配

- 8.1 Binding 的定义
- 8.2 Binding 不是 Executable
- 8.3 整值引用
- 8.4 字段投影
- 8.5 多值组合
- 8.6 命名结构装配
- 8.7 Flow 归属
- 8.8 Binding 的纯结构性约束
- 8.9 Binding 与 Node 的边界
- 8.10 正式 Rust API 的开放问题

## 9. 控制型 Executable

- 9.1 总体原则
- 9.2 Retry
- 9.3 Match
- 9.4 Each
- 9.5 Iter
- 9.6 四种控制语义的边界
- 9.7 控制型 Executable 的统一规则

## 10. 执行、数据与错误语义

- 10.1 顺序执行
- 10.2 Input 只读语义
- 10.3 Output 语义
- 10.4 错误传播
- 10.5 部分执行与副作用
- 10.6 空集合
- 10.7 Retry 耗尽
- 10.8 Match 未命中
- 10.9 Flow 内部失败
- 10.10 Binding 错误
- 10.11 框架错误与业务错误
- 10.12 Error 不隐含重试
- 10.13 Error 不隐含回滚
- 10.14 执行语义总结

## 11. Rust 实现边界

- 11.1 设计语义与实现策略
- 11.2 强类型外部接口
- 11.3 内部类型擦除
- 11.4 Ref 与值存储
- 11.5 Clone / Arc / Borrow
- 11.6 typestate
- 11.7 associated type
- 11.8 Binding API
- 11.9 Probe 中不应固化的实现细节
- 11.10 性能优化原则

## 12. 非目标与延后能力

- 12.1 为什么明确写非目标
- 12.2 Parallel
- 12.3 Async
- 12.4 Timeout
- 12.5 Race
- 12.6 技术 Retry
- 12.7 持久化
- 12.8 Trace / 日志
- 12.9 Pause / Resume
- 12.10 崩溃恢复
- 12.11 动态 Graph / DSL
- 12.12 插件式 Runtime Primitive
- 12.13 新能力进入核心的条件

## 13. 设计验证与实现约束

- 13.1 验证方法
- 13.2 业务反向验证
- 13.3 Compile Probe 验证阶段
- 13.4 已验证结论
- 13.5 Compile Probe 不证明什么
- 13.6 实现必须遵守的设计规则
- 13.7 后续设计变更流程
- 13.8 SRFlow v2 设计完成条件
- 13.9 最终设计原则

---

# 0. 文档定位与规范性

## 0.1 文档目的

本文定义 **System Runtime Workflow v2（SRFlow v2）** 的核心设计。

本文不是某个实验 Runner 的实现说明，也不是一组 Rust API 示例的汇总。本文定义的是 SRFlow 在 v2.x 生命周期内必须保持稳定的：

- 概念边界；
- 核心职责；
- 执行语义；
- 数据关系；
- 控制语义；
- 设计不变量；
- 演进原则。

后续 SRFlow 实现、重构、性能优化和能力扩展，都必须以本文定义的语义为基准。

SRFlow 的具体 Rust 实现可以变化，但只要仍称为 SRFlow v2.x，就不得在未修改本文设计的情况下改变本文规定的核心语义。

---

## 0.2 规范性地位

规范切换前，本文是 SRFlow v2.0 的最高设计标准；自 2026-10-03 起，该地位由 [SRFlow Design v2.1](SRFlow_Design_v2.1.md) 接替。以下流程保留为历史规范的原始约束。

当实现代码、测试代码、辅助库、实验性原型与本文发生冲突时，默认判断顺序为：

```text
设计文档
    ↓
判断当前实现是否违背设计
    ↓
若实现错误，则修改实现
    ↓
若确认设计本身需要改变，
必须先重新进行设计评审并更新本文
    ↓
再修改实现
```

不得因为某种 Rust 写法更容易实现，就直接改变既有框架语义。

不得因为某个局部需求可以通过增加新的 Runtime Primitive 快速解决，就跳过设计层评审。

本文不是静态不可修改的文件，但任何修改都必须被视为 **SRFlow 设计本身发生变化**，而不是普通代码重构。

---

## 0.3 设计语义与实现策略的区分

本文严格区分：

```text
设计语义
```

与：

```text
实现策略
```

例如：

> Flow 中已经产生的数据是只读并可重复引用的。

这是设计语义。

至于 Rust 中最终使用：

```text
Clone
Arc<T>
borrow
内部引用
其他值存储方式
```

属于实现策略。

再例如：

> Binding 只能进行已有数据的结构性读取、投影和装配，不承担业务计算。

这是设计语义。

至于最终使用：

```rust
field!(...)
bind!(...)
derive(...)
builder API
```

属于实现策略。

实现策略可以随着性能、Rust 类型系统或工程需求变化；设计语义不能因此被隐式改变。

---

## 0.4 已冻结、实现待定与暂不设计

本文中的内容分为三种状态。

### 已冻结的设计语义

以下内容已经经过设计推导、真实 SES 业务流程反向验证以及 Rust Compile Probe 验证：

```text
Runtime
Executable
Flow
Node

Flow Input / Output
then
Ref
SubFlow
Binding

Retry
Match
Each
Iter

递归 Runtime 调用
强类型 Input / Output
Ref 的 Flow 归属
数据只读与可复用
Binding 的结构性语义
基本错误传播规则
```

这些内容构成 SRFlow v2 的设计基线。

### 实现待定

以下内容尚未冻结为唯一实现：

```text
值存储具体结构
Clone / Arc / Borrow 策略
内部类型擦除方式
Any / downcast 的最终使用方式
Binding 的最终 Rust API
field! / bind! 的最终语法
Match 的内部查找容器
trait object 的最终包装方式
typestate 的最终外部暴露形式
```

这些问题可以在不改变核心语义的前提下继续优化。

### 暂不设计

以下能力当前不属于 SRFlow v2 核心设计：

```text
自动并行调度
异步调度与并发控制语义
Timeout
Race
技术故障 Retry
持久化运行状态
Trace 系统
日志系统
Pause / Resume
崩溃恢复
动态 Graph DSL
```

“暂不设计”不代表永远禁止，而表示当前没有足够真实需求证明它们应进入 SRFlow 核心。

---

## 0.5 文档演进规则

SRFlow 的后续升级必须遵守：

> **新需求首先被视为业务需求，而不是框架缺失。**

只有当某个需求被多个真实执行流程反复证明，并且无法通过现有 Executable、Flow、Node 与 Runtime 自然表达时，才应考虑修改核心设计。

新增能力时优先检查：

```text
是否只是一个具体业务操作？
→ 是：作为 Node 实现

否则，能否通过现有 Node / Flow / Executable / Binding 组合表达？
→ 能：使用现有模型组合

否则，是否形成了新的、稳定的执行语义？
→ 是：优先新增 Executable

只有以上都无法成立时，
才考虑修改 Runtime / Flow / Executable 等核心模型。
```

SRFlow 不追求提前覆盖所有可能的执行模式。

---

# 1. SRFlow 的目标与边界

## 1.1 SRFlow 要解决的问题

SES 的研究与实现过程中，会不断出现这样的执行需求：

```text
准备输入
↓
执行某个业务处理
↓
检查结果
↓
根据结果选择路径
↓
必要时重新执行
↓
批量处理多个对象
↓
让前一轮结果进入下一轮
↓
组合成更大的执行流程
↓
得到最终业务结果
```

如果这些流程全部由每个测试工程自行编写，业务设计会不断被以下工程问题干扰：

```text
谁先执行
谁后执行
前一个结果如何交给后一个
如何组合子流程
如何重复执行
如何根据结果路由
如何表达批量处理
如何表达连续推进
```

SRFlow 的目标，就是把这些稳定的执行组织问题从具体 SES 业务逻辑中抽离出来。

SRFlow 不负责决定小说应该怎么写，也不负责判断某段正文是否合格。

SRFlow 负责让这些业务判断和业务处理可以被清晰组织和执行。

---

## 1.2 SRFlow 的核心目标

SRFlow 的核心目标不是提供最多的 Workflow 功能，而是：

> **让 SES 的执行流程可以以简单、强类型、可组合、可验证的方式表达。**

SRFlow 应使业务代码能够接近业务流程本身。

理想情况下，一个 Flow 应让读者直接看到：

```text
先生成
然后检查
不通过则重做
通过后分解
然后逐节点推进
每轮使用上一轮结果
最后生成正文
```

而不是看到大量：

```text
状态机管理
Execution ID 处理
Graph 节点注册
调度器配置
动态 lookup
内部生命周期控制
```

SRFlow 的价值首先来自降低 SES 业务实验的执行组织成本，而不是追求成为覆盖所有领域的大型基础设施平台。

---

## 1.3 SRFlow 不是什么

SRFlow 不是通用 Workflow Engine。

SRFlow 不是 Agent Framework。

SRFlow 不是分布式任务调度系统。

SRFlow 不是持久化状态平台。

SRFlow 不是 Event Sourcing 系统。

SRFlow 不是动态 Graph DSL。

SRFlow 也不试图预先覆盖未来所有可能出现的执行模式。

SRFlow 可以在后续演进中获得新的能力，但这些能力必须来自真实使用需求，而不是来自对“一个成熟工作流框架应该有什么”的想象。

---

## 1.4 SRFlow 的能力边界

SRFlow 当前只关心：

```text
什么被执行
↓
以什么 Input 执行
↓
按什么顺序执行
↓
前面产生的数据如何连接到后面
↓
需要怎样的控制语义
↓
最终产生什么 Output
```

因此 SRFlow v2 的核心边界集中在：

```text
Runtime
Executable
Flow
Node
Binding
控制型 Executable
```

至于：

```text
模型供应商
Prompt 内容
小说质量标准
业务状态含义
文件保存方式
数据库
日志格式
监控系统
持久恢复
```

均不是核心执行模型的一部分。

---

## 1.5 面向 SES 的设计取向

SRFlow 的第一服务对象是 SES。

因此 SRFlow 的设计优先级是：

```text
能否自然表达 SES 的真实流程
>
概念是否足够少
>
职责是否稳定
>
Rust 类型是否清晰
>
是否容易扩展
>
是否具备通用 Workflow Engine 的完整能力
```

如果“框架通用性”与“SES 流程表达自然性”发生冲突，应优先保证后者。

SRFlow 可以被其他系统使用，但这不是其设计正确性的主要判断依据。

---

# 2. 设计理念与基本理论

## 2.1 从真实执行需求中生长

SRFlow 不采用“先设计完整框架，再寻找业务映射”的方式。

SRFlow 的能力必须从实际执行流程中抽取。

设计顺序应当是：

```text
真实业务流程
↓
识别重复出现的执行问题
↓
形成最小语义抽象
↓
用真实业务反向验证
↓
用 Rust Compile Probe 验证可实现性
↓
冻结设计
```

没有真实需求支持的抽象不进入核心。

“未来可能需要”本身不足以成为核心设计依据。

---

## 2.2 Flow 是 SRFlow 的中心

SRFlow 最重要的对象是 Flow。

Node 很重要，但单个 Node 的执行并不是 SRFlow 最困难的问题。

SES 的真正复杂性来自：

```text
多个业务步骤如何组合
步骤之间如何传递数据
什么时候重复
什么时候选择分支
什么时候逐项执行
什么时候携带上一轮结果继续推进
```

这些问题本质上属于 Flow。

因此：

> **SRFlow 以 Flow 为核心编排模型。**

Runtime、Executable、Node 和控制型 Executable 都服务于 Flow 的表达和执行。

---

## 2.3 调用、编排、执行语义与业务实现必须分离

SRFlow 将四种责任明确分离：

```text
Runtime
负责统一发起执行

Flow
负责编排执行

Executable
定义统一执行契约以及一种具体执行语义

Node
实现具体业务操作
```

这四种责任不得重新融合。

例如：

Runtime 不应该理解：

```text
Retry 为什么重试
Match 为什么选择某个分支
Flow 中某个 Node 的业务意义
```

Flow 也不应该直接实现：

```text
文本判断
方案选择
LLM 调用
业务状态计算
```

这种分离是 SRFlow 后续长期可演进的基础。

---

## 2.4 Executable 是统一扩展协议

SRFlow 不试图把所有未来控制能力写进 Flow。

相反，SRFlow 定义统一的 Executable 契约：

```text
Input
  ↓
Executable
  ↓
Output
```

Node 是 Executable。

Flow 是 Executable。

Retry、Match、Each、Iter 也是 Executable。

未来如果出现新的真实执行语义，例如：

```text
Parallel
Timeout
Race
```

优先的设计方向应当是：

```text
NewType implements Executable
```

而不是：

```text
修改 Flow 核心
修改 Runtime 核心
增加大量特殊分支
```

因此：

> **Executable 是 SRFlow 的统一扩展协议。**

Flow 保持稳定，新的执行语义通过新的 Executable 实现进入系统。

---

## 2.5 执行顺序与数据依赖相互独立

Flow 中存在两个不同维度：

```text
执行顺序
```

与：

```text
数据依赖
```

`then` 定义执行顺序。

Ref 与 Binding 定义数据依赖。

例如：

```text
ex1 → ex2 → ex3
```

表示执行顺序。

但 ex3 完全可以读取 ex1 的 Output：

```text
ex1 → A ─────────→ ex3
ex2
```

因此：

> **前后相邻不代表自动传值。**

同样：

> **没有数据依赖也不代表 Runtime 可以自行并行。**

Flow 的顺序本身就是明确语义。

如果未来需要并行，应通过明确的新执行语义表达，而不是由 Runtime 猜测。

---

## 2.6 Flow 连接数据，Node 处理业务意义

Flow 必须能够组织已有数据，否则真实业务流程会产生大量没有业务意义的胶水 Node。

因此 Flow 可以通过 Binding：

```text
读取已有值
投影字段
组合多个已有值
构造下游 Input 的结构
```

但 Flow 不执行新的业务计算。

例如下面属于结构性数据装配：

```text
Plan + InitialInput
→ CheckInput

StoryInput.key_nodes
→ Vec<KeyNode>

plan + prose
→ ProseState
```

下面则属于业务行为：

```text
判断方案是否合格
计算评分
选择更好的候选
修改正文
调用 LLM
```

这些必须由 Node 实现。

因此：

> **Flow 负责连接数据；Node 负责产生新的业务意义。**

这是防止 Flow 演化成隐藏业务 DSL 的关键边界。

---

## 2.7 业务判断与流程控制分离

控制结构不应承担业务判断本身。

例如 Match 不负责判断：

```text
这个方案应该进入哪条路径？
```

正确方式是：

```text
业务数据
↓
JudgeNode
↓
Route
↓
Match
```

JudgeNode 形成明确判断。

Match 只根据已有 Route 做路由。

同样，Retry 的 Condition 应尽量读取已经形成的业务判断结果，而不是把复杂业务逻辑塞进 Retry 本身。

因此：

> **业务判断由 Node 完成，控制型 Executable 消费已经形成的控制信息。**

---

## 2.8 不使用万能循环抽象

“重复执行”并不是一种单一语义。

SRFlow 将其拆分为不同原因驱动的执行模式。

### Retry

```text
同一个 Input
→ 执行
→ 判断结果
→ 必要时重新执行
```

其本质是：

> 重做。

### Each

```text
一组外部 Item
→ 每项执行一次
→ 收集所有结果
```

其本质是：

> 逐项处理。

### Iter

```text
上一轮状态 + 当前 Item
→ 新状态
→ 进入下一轮
```

其本质是：

> 逐项推进。

因此：

```text
Retry ≠ Each ≠ Iter
```

SRFlow 不使用一个万能 `Loop` 同时表达三种不同业务语义。

一个抽象越“通用”，并不意味着它越正确。

---

## 2.9 组合边界优先于内部结构

一个组合型 Executable 内部可以非常复杂。

例如：

```text
Retry
└─ Flow
   ├─ Node
   ├─ Match
   └─ SubFlow
```

但对父 Flow 来说，它仍然只是：

```text
Input
  ↓
Executable
  ↓
Output
```

父级不应直接依赖内部临时结果。

内部 Ref 不跨 Executable 边界泄漏。

因此：

> **组合边界由 Input / Output 定义，而不是由内部结构定义。**

这是 SubFlow、Retry、Match、Each、Iter 能够自由组合的基础。

---

## 2.10 框架语义优先于 Rust 实现便利

SRFlow 是 Rust 实现的，但 SRFlow 的设计不能被某一种 Rust 技巧反向决定。

如果：

```text
异构 Executable
强类型 Ref
只读复用数据
```

在 Rust 中实现较困难，应解决 Rust 实现问题。

不能因为：

```text
Vec<Box<dyn Trait>>
```

难写，就删除强类型关系。

不能因为 borrow 较复杂，就把业务数据语义改成可变共享状态。

不能因为某个 API 更容易实现，就让 Flow 承担业务计算。

因此：

> **先确定正确的执行语义，再寻找能够承载它的 Rust 实现。**

Compile Probe 的作用正是验证这个边界。

---

## 2.11 不提前抽象未来能力

SRFlow 不为未出现的需求预留复杂机制。

如果当前不存在明确需求，就不因为：

```text
成熟框架通常都有
```

而增加：

```text
Parallel
Timeout
Race
Pause / Resume
Scheduler
Dynamic Graph
```

未来新需求出现时，应重新从真实场景推导。

如果最终证明：

```text
Parallel implements Executable
```

足够，就不需要修改 Flow。

如果证明不够，再重新评审。

因此：

> **抽象应当晚于需求，而不是早于需求。**

---

## 2.12 设计验证方法

SRFlow 的设计采用三层验证。

第一层是：

> **设计推导。**

检查抽象本身的职责、边界与执行语义是否自洽。

第二层是：

> **真实业务反向验证。**

即拿真实 SES 流程检查抽象是否自然。

例如正文生成决策网络验证了：

```text
Retry
Match
Iter
SubFlow
Binding
多 Ref Input
上一轮正文进入下一关键节点
```

该场景同时验证了 Each 与 Iter 的适用边界：关键节点之间需要上一轮正文，因此这里应使用 Iter，而不是 Each。

第三层是：

> **Rust Compile Probe。**

Compile Probe 不实现完整框架，只验证：

```text
设计语义是否能在稳定 Rust 中以足够小的接口成立
```

验证顺序为：

```text
Flow / Ref / then
↓
递归 Runtime 调用
↓
Retry
↓
Match
↓
Each / Iter
↓
Binding
```

只有设计推导、业务语义和 Rust 可实现性都成立的设计，才进入冻结状态。

---

## 2.13 SRFlow 的演进原则

SRFlow 后续演进必须遵守以下方向：

```text
业务需求
↓
优先用现有 Node / Flow / Executable 组合解决
↓
若确有新的稳定执行语义
优先新增 Executable
↓
只有现有核心模型无法表达时
才修改 Runtime / Flow / Executable 基础契约
```

SRFlow 的目标不是不断增加能力，而是长期保持：

```text
概念少
边界清楚
组合稳定
业务表达自然
```

任何升级如果让核心模型越来越难解释，应首先怀疑设计方向是否出现偏移。

---

# 3. 核心模型总览

## 3.1 四个核心角色

SRFlow 的核心由四个角色构成：

| 对象 | 核心职责 |
|---|---|
| `Runtime` | 统一发起 Executable 的实际执行 |
| `Executable` | 定义统一执行契约与具体执行语义 |
| `Flow` | 按顺序编排 Executable，并连接数据 |
| `Node` | 实现具体叶子业务操作 |

其中：

```text
Flow 是核心编排模型
Executable 是统一扩展协议
Runtime 是统一执行入口
Node 是业务实现叶子
```

---

## 3.2 Runtime

Runtime 只提供统一的实际执行入口。

概念上：

```text
Runtime.execute(executable, input)
→ output / error
```

Runtime 不理解具体业务。

Runtime 不判断：

```text
这是 Node
这是 Retry
这是 Match
```

它只要求目标满足 Executable 契约。

组合型 Executable 在执行 child 时，也必须重新经过同一个 Runtime。

---

## 3.3 Executable

Executable 表示：

> **一个可以被 Runtime 独立调用的执行单元。**

其统一抽象为：

```text
Input
  ↓
Executable
  ↓
Output
```

Executable 可以：

```text
自己完成业务操作
```

也可以：

```text
通过 Runtime 调用其他 Executable 完成自身语义
```

Node 属于前者。

Flow、Retry、Match、Each、Iter 属于后者。

---

## 3.4 Flow

Flow 是 SRFlow 的核心编排模型。

Flow 本身也是 Executable。

Flow 的默认语义是：

> **按照 `then` 定义的顺序逐个执行内部 Executable。**

Flow 主要负责两件事情：

```text
定义执行顺序
定义数据连接
```

Flow 不负责理解内部 Executable 的具体类型，也不负责业务计算。

---

## 3.5 Node

Node 是叶子型 Executable。

Node 实现一个具体业务动作：

```text
Input
↓
业务处理
↓
Output
```

例如：

```text
调用 LLM
判断方案
解析结果
生成正文
计算业务数据
修改文本
```

Node 不编排其他 Executable。

Node 不负责控制整个执行流程。

---

## 3.6 控制型 Executable

SRFlow 当前定义四种已经验证的控制型 Executable：

```text
Retry
Match
Each
Iter
```

它们不是 Flow 的特殊语法。

它们全部实现 Executable。

因此父 Flow 对待：

```text
Node
SubFlow
Retry
Match
Each
Iter
```

的方式完全一致：

```text
then(executable, binding)
```

---

## 3.7 核心关系

整体关系为：

```text
                       Executable
                     /     |      \
                    /      |       \
                 Node     Flow     Control
                           |        ├─ Retry
                           |        ├─ Match
                           |        ├─ Each
                           |        └─ Iter
                           |
                    ordered Executables

所有实际执行
      ↓
   Runtime
```

Executable 是接口层抽象。

Node、Flow 和各种控制结构是其实现。

Runtime 不属于 Executable 体系。

Runtime 是执行入口。

---

## 3.8 执行关系

顶层执行：

```text
Runtime.execute(MainFlow, Input)
```

MainFlow 内部：

```text
Runtime.execute(child1, input1)
↓
Runtime.execute(child2, input2)
↓
Runtime.execute(child3, input3)
```

如果 child2 本身是 Retry：

```text
Runtime.execute(Retry)
        ↓
Retry
        ↓
Runtime.execute(Body)
        ↓
必要时再次 Runtime.execute(Body)
```

如果 Body 又是 SubFlow：

```text
Runtime
→ Retry
  → Runtime
    → SubFlow
      → Runtime
        → Node
```

因此：

> **所有 Executable 的实际调用始终重新经过 Runtime。**

---

## 3.9 数据关系

Flow 中的数据通过强类型只读引用连接。

概念上：

```text
Flow Input
    ↓
   Ref<I>
    │
    ├─────────────┐
    ↓             ↓
   ex1           ex2
    ↓ A           ↓ B
    └──────┬──────┘
           ↓
         Binding
           ↓
          ex3
           ↓ C
           ↓
      Flow Output
```

执行顺序与数据依赖不要求相同。

Ref 可以被多个后续 Executable 重复使用。

Binding 可以从已有 Ref 中：

```text
读取整值
投影字段
组合多个值
构造结构化 Input
```

但不得产生新的业务意义。

---

## 3.10 核心模型不变量

以下规则构成 SRFlow v2 的核心不变量：

1. 所有 Executable 的实际执行必须经过 Runtime。
2. Flow 的 `then` 顺序就是执行顺序。
3. Runtime 不得根据数据依赖自行重排或并行 Flow。
4. Executable 对外只通过 Input / Output 建立组合边界。
5. Node 是叶子业务执行，不编排其他 Executable。
6. Flow 编排 Executable，但不承担业务计算。
7. Binding 只负责结构性数据连接，不产生 Execution。
8. Ref 是只读、可复用、属于特定 Flow 的数据引用。
9. SubFlow 内部 Ref 不向父 Flow 泄漏。
10. Match 只根据已有匹配值路由，不承担复杂业务判断。
11. Retry 使用同一个 Input 多次执行 Body。
12. Retry 的技术错误不自动转化为业务 Retry。
13. Each 对输入集合逐项执行，不把上一轮 Output 传给下一轮。
14. Iter 将上一轮状态作为下一轮 Input 的组成部分。
15. 新执行语义优先通过新的 Executable 实现扩展。
16. 不得为了 Rust 实现便利改变已经冻结的框架语义。

这些规则不是当前实现习惯，而是 SRFlow v2 的设计约束。

---

# 4. Executable

## 4.1 定义

`Executable` 是 SRFlow 的统一执行协议。

任何能够被 Runtime 独立发起执行的对象，都必须以 Executable 的形式进入 SRFlow 执行体系。

Executable 的最基本关系是：

```text
Input
  ↓
Executable
  ↓
Output
```

概念上，一个 Executable 定义：

```text
Executable<I, O>
```

其中：

- `I` 表示该 Executable 一次调用所需要的完整输入；
- `O` 表示该 Executable 正常完成后产生的完整输出。

Rust 实现可以通过 associated type、泛型参数或其他方式表达该关系，但必须保持：

> **一个具体执行边界具有明确、强类型的 Input 与 Output。**

---

## 4.2 Input / Output 契约

Executable 的 Input / Output 不只是 Rust 类型约束，也是 Executable 的组合边界。

父级只需要知道：

```text
它需要什么 Input
它产生什么 Output
```

父级不需要知道：

```text
它内部包含多少 Node
是否存在 Retry
是否存在 Match
是否包含 SubFlow
是否进行多轮迭代
```

例如：

```text
PlanGenerationFlow

Input:
    PlanRequest

Output:
    AcceptedPlan
```

即使内部实际是：

```text
Retry
└─ Flow
   ├─ GeneratePlanNode
   └─ CheckPlanNode
```

对父级而言仍然只是：

```text
PlanRequest
    ↓
Executable
    ↓
AcceptedPlan
```

因此：

> **Input / Output 定义组合边界，内部执行结构不得泄漏成为父级依赖。**

---

## 4.3 一次执行的语义

一次 Executable 调用表示：

```text
给定一个确定的 Input
↓
执行该 Executable 定义的执行语义
↓
正常产生一个 Output
或
返回一个执行错误
```

每次调用中的 Input 在语义上是固定、只读的。

“只读”表示：

> Executable 不通过修改调用方已有 Input 来传递结果。

执行产生的新业务结果必须通过 Output 返回。

这并不要求 Rust 实现一定使用 `&I`。

例如：

```rust
fn execute(input: I) -> Result<O>
```

仍然可以满足只读语义，只要执行模型不存在“调用后由外部观察被修改 Input”的设计。

因此必须区分：

```text
语义上的只读
```

与：

```text
Rust 所有权形式
```

---

## 4.4 重复调用

Executable 是执行定义，不是一次执行实例。

同一个 Executable 可以被 Runtime 调用多次。

例如：

```text
Runtime.execute(executable, I1)
Runtime.execute(executable, I2)
Runtime.execute(executable, I3)
```

每次调用拥有独立的瞬时执行状态。

前一次调用的内部临时状态不得自动成为下一次调用的 Input。

Executable 可以显式持有配置、缓存、外部资源句柄或内部共享状态；SRFlow 不要求 Executable 是无状态对象。但这些状态不构成 Flow 的隐式数据依赖，也不得替代应由 Input / Output 显式表达的业务数据流。

如果业务需要：

```text
O1 → 下一次执行
```

必须显式通过 Flow、Iter 或其他已定义的数据关系表达。

Executable 可以访问外部系统，因此 SRFlow 不要求所有 Executable：

```text
纯函数
确定性
无副作用
```

但这些外部效果不得成为隐式 Flow 数据通道。

---

## 4.5 叶子型与组合型 Executable

Executable 可以分为两种执行形态。

### 叶子型 Executable

叶子型 Executable 自己完成具体业务操作。

SRFlow 当前的主要叶子型 Executable 是：

```text
Node
```

Node 不通过 Runtime 组织 child Executable。

---

### 组合型 Executable

组合型 Executable 通过其他 Executable 完成自己的执行语义。

例如：

```text
Flow
Retry
Match
Each
Iter
```

组合型 Executable 可以拥有 child Executable。

但所有 child 的实际调用仍必须重新经过 Runtime。

例如：

```text
Retry.execute
    ↓
runtime.execute(body, input)
```

而不是：

```text
body.execute(input)
```

---

## 4.6 Executable 边界

Executable 的边界必须保持封闭。

如果一个 Executable 内部产生：

```text
temporary_a
temporary_b
temporary_c
```

父级不能直接引用这些内部值。

只有被声明为该 Executable Output 的结果才能离开边界。

因此：

```text
Parent Flow
    ↓
SubFlow<Input, Output>
```

父级只能看到：

```text
Input
Output
```

不能看到 SubFlow 内部：

```text
Ref<A>
Ref<B>
Ref<C>
```

这一规则同时适用于所有组合型 Executable。

---

## 4.7 Executable 与错误

Executable 的执行结果在概念上为：

```text
Output
or
Execution Error
```

技术执行错误不是普通业务 Output。

例如：

```text
模型请求失败
文件读取失败
内部类型不变量被破坏
child Executable 执行失败
```

都应沿执行链传播为错误，除非某个明确的 Executable 语义规定如何处理该错误。

SRFlow 不自动：

```text
重试
忽略
转换为默认业务结果
切换备用分支
```

例如 Match 已经选择某个分支以后，该分支发生执行错误：

```text
Match
→ Branch A
→ Error
```

结果是：

```text
Error
```

而不是：

```text
尝试 default
```

---

## 4.8 扩展规则

当出现新的执行需求时，不应首先修改 Flow 或 Runtime。

应优先判断它是否可以表达为：

```text
struct NewExecutionSemantic { ... }

impl Executable for NewExecutionSemantic
```

只有新的需求无法以：

```text
Input
↓
Executable
↓
Output
```

以及现有 Runtime / Flow 组合机制自然表达时，才允许重新评审核心协议。

因此：

> **新增 Executable 是 SRFlow 的主要能力扩展方式。**

---

# 5. Runtime

## 5.1 定义

Runtime 是所有 Executable 的统一实际执行入口。

概念接口：

```text
Runtime.execute(executable, input)
→ output / error
```

Runtime 的核心职责只有一个：

> **发起一次 Executable 执行。**

Runtime 不负责定义该 Executable 的业务语义。

---

## 5.2 Runtime.execute

概念上：

```rust
runtime.execute(&executable, input)
```

表示：

```text
Runtime
↓
调用 Executable
↓
Executable 执行
↓
Output / Error
```

任何 Executable 都必须能够通过同一入口调用。

例如：

```text
runtime.execute(Node)
runtime.execute(Flow)
runtime.execute(Retry)
runtime.execute(Match)
runtime.execute(Each)
runtime.execute(Iter)
```

Runtime 不需要为不同 Executable 类型建立不同业务入口。

---

## 5.3 统一执行入口

Runtime 的统一入口是 SRFlow 的重要不变量。

不得出现：

```text
Node 经过 Runtime
Flow 直接调用
Retry 走另一套入口
SubFlow 再使用其他执行方式
```

所有真正的 Executable 调用必须汇聚到：

```text
Runtime.execute(...)
```

这样 Runtime 才能在未来不改变 Executable 语义的情况下统一承载：

```text
执行观察
计时
Trace
诊断
技术上下文
```

是否在某个版本实际提供这些能力，不影响统一入口本身的必要性。

---

## 5.4 递归 Runtime 调用

组合型 Executable 在调用 child 时，必须重新经过 Runtime。

例如：

```text
Runtime.execute(MainFlow)
    ↓
MainFlow
    ↓
Runtime.execute(NodeA)
    ↓
Runtime.execute(Retry)
                 ↓
               Retry
                 ↓
          Runtime.execute(SubFlow)
                       ↓
                     SubFlow
                       ↓
                Runtime.execute(NodeB)
```

必须保证：

> **执行树上的每一个 Executable 都经过 Runtime。**

因此禁止组合型 Executable 绕过 Runtime：

```text
child.execute(...)
```

即使这种直接调用在 Rust 中更简单，也不得采用。

---

## 5.5 Runtime 不负责的事情

Runtime 不负责：

### 不负责编排顺序

```text
ex1 → ex2 → ex3
```

属于 Flow。

### 不负责 Retry 条件

```text
是否继续尝试
```

属于 Retry。

### 不负责 Match 选择

```text
命中哪个 case
```

属于 Match。

### 不负责 Each / Iter 循环

属于相应 Executable。

### 不负责数据连接

```text
哪个 Output 成为哪个 Input
```

属于 Flow / Binding。

### 不负责业务逻辑

例如：

```text
检查正文是否合格
选择最佳方案
解析模型输出
```

属于 Node。

---

## 5.6 后续基础设施扩展边界

未来如果 Runtime 增加：

```text
logging
trace
timing
metrics
execution context
```

这些能力只能围绕：

```text
一次 Executable 调用
```

建立。

它们不得改变 Executable 原有语义。

例如 Runtime 可以记录：

```text
Executable 开始
Executable 结束
耗时
错误
```

但不得因为观察到：

```text
两个 Step 没有数据依赖
```

就自行并行执行。

也不得因为：

```text
Node 执行失败
```

就自动重试。

因此：

> **Runtime 可以包围执行，但不得重新定义执行。**

---

# 6. Node

## 6.1 定义

Node 是 SRFlow 的叶子业务执行单元。

Node 的抽象为：

```text
Input
  ↓
Node
  ↓
Output
```

Node 负责执行具体业务操作，并通过 Output 显式返回需要向后续执行暴露的业务结果。不需要传递业务值时，Output 可以是空值（如 `()`）。

典型 Node 包括：

```text
LLM 调用
文本生成
结果解析
业务判断
业务计算
候选选择
文本修订
数据转换
```

---

## 6.2 Node 的叶子语义

“叶子”表示：

> Node 不负责组织其他 Executable 的执行。

Node 内部不能：

```text
启动 SubFlow
控制 Retry
选择 Match branch
执行 Each
调用另一个 SRFlow Node
```

这些属于 Flow 或控制型 Executable。

Node 可以调用普通业务库、外部服务或 Adapter，但不能以此建立隐式 SRFlow 编排。

---

## 6.3 Node 的 Input / Output

Node 应有明确的业务 Input 和 Output。

例如：

```text
CheckPlanNode

Input:
    PlanCheckInput {
        plan,
        original_request,
    }

Output:
    PlanCheckResult
```

Node 不应通过隐式 Flow 状态获取：

```text
当前方案
上一个节点输出
某个最新结果
```

这些值必须通过 Node Input 显式提供。

同样，Node 产生的业务结果必须通过 Output 显式返回。

---

## 6.4 Node 与 Runtime 的关系

Node 作为 Executable，实际执行仍然经过 Runtime。

但 Node 不使用 Runtime 进行编排。

概念关系为：

```text
Runtime
  ↓
Node
  ↓
business operation
```

而不是：

```text
Node
  ↓
Runtime
  ↓
child executable
```

具体 Rust 接口是否为了统一 Executable trait 而向 Node 的执行适配器传入 Runtime，属于实现问题。

无论具体接口如何：

> **Node 在设计语义上不拥有 SRFlow child execution 权限。**

---

## 6.5 通用 Node 与业务 Node

不是所有 Node 都必须是某个具体写作业务独有。

SRFlow 允许存在通用 Node，例如：

```text
ParseNode
SelectNode
TransformNode
ValidateNode
```

但一个通用操作是否值得抽成 Node，应由真实复用需求决定。

不能为了减少几行 Binding，而创建：

```text
GetFieldNode
BuildTupleNode
CopyValueNode
```

这类本质只是数据连接的伪业务 Node。

结构性数据连接属于 Binding。

---

## 6.6 Node 不承担流程编排

以下逻辑不得放在普通 Node 内部：

```text
如果失败，再执行三次
如果是 A，执行 Flow1
如果是 B，执行 Flow2
遍历所有关键节点
把上一轮结果传入下一轮
```

这些分别属于：

```text
Retry
Match
Each
Iter
```

否则流程结构将被隐藏在 Node 内部，使 Flow 无法真实表达业务执行关系。

因此：

> **如果一段逻辑的主要意义是“如何执行其他步骤”，它通常不属于 Node。**

---

# 7. Flow

## 7.1 定义

Flow 是 SRFlow 的核心编排模型。

一个 Flow 可以被视为：

```text
Flow<I, O>
```

其外部关系为：

```text
I
↓
Flow
↓
O
```

Flow 内部由一组有序 Executable 构成，并通过 Ref 与 Binding 建立数据连接。

Flow 本身也是 Executable，因此可以被其他 Flow 当作普通 child 使用。

---

## 7.2 顺序语义

Flow 默认就是顺序执行。

例如：

```text
Flow
├─ ex1
├─ ex2
└─ ex3
```

其语义明确为：

```text
ex1 完成
↓
ex2 开始
↓
ex2 完成
↓
ex3 开始
```

这不是当前 Runtime 的调度习惯，而是 Flow 的设计语义。

即使：

```text
ex2 不读取 ex1 的 Output
```

也不能推断：

```text
ex1 与 ex2 可以并行
```

如果未来存在并行语义，必须由显式 Executable 表达。

---

## 7.3 Flow Input

Flow 有一个明确 Input。

例如：

```text
Flow<StoryInput, StoryOutput>
```

Flow Input 可以是结构体：

```rust
StoryInput {
    plan,
    characters,
    background,
    prose,
}
```

因此“一个 Flow 只有一个 Input”不意味着只能接收一个业务字段。

一个 Input 可以包含完整的业务输入结构。

Flow Input 在 Flow 内部可以被表示为一个强类型数据引用：

```text
Ref<StoryInput>
```

并可被多个后续 Step 重复使用。

---

## 7.4 then

`then` 是 Flow 最核心的编排操作。

概念形式：

```text
flow.then(executable, binding)
→ Ref<Output>
```

`then` 同时完成两件事：

```text
把 executable 加入 Flow 的执行顺序
```

以及：

```text
声明该 executable 的 Input 从哪里获得
```

例如：

```rust
let plan = flow.then(generate_plan, input);

let check = flow.then(
    check_plan,
    (plan, input),
);
```

这里：

```text
执行顺序：
GeneratePlan
→ CheckPlan
```

数据关系：

```text
GeneratePlan.Output ─┐
                     ├→ CheckPlan.Input
Flow.Input ──────────┘
```

二者是独立概念，但通过 `then` 汇合。

---

## 7.5 Ref

`Ref<T>` 表示：

> **当前 Flow 中某个已经声明的数据位置的强类型只读引用。**

Ref 不是业务值本身。

在 Flow 构建阶段：

```rust
let plan = flow.then(...);
```

此时 `plan` 并不是实际 Plan。

它表示：

```text
运行到这里以后，
这个位置会产生一个 Plan。
```

因此 Ref 是 Flow 构建阶段的数据连接句柄。

---

### Ref 的基本属性

Ref 必须满足：

```text
强类型
只读
可重复使用
属于特定 Flow
```

例如：

```rust
let plan = ...;

flow.then(check_a, plan);
flow.then(check_b, plan);
```

在语义上合法。

Ref 自己不能用于修改所引用的数据。

---

## 7.6 数据复用

Flow 数据允许被多个后续 Executable 使用。

例如：

```text
              ┌→ CheckNode
Plan Ref ─────┤
              └→ WriteNode
```

这表示：

```text
同一个业务结果被两个后续步骤读取。
```

这不意味着底层实现必须复制两份完整业务对象。

具体使用：

```text
Clone
Arc
borrow
共享存储
```

属于实现策略。

设计层只规定：

> **同一个 Flow 数据可以被多个后续 Binding 只读引用。**

---

## 7.7 Flow Output

Flow 必须显式声明最终 Output。

概念形式：

```text
flow.output(ref)
```

被选中的 Ref 决定：

```text
Flow<I, O>
```

中的 `O`。

因此一个未定义 Output 的构建中 Flow，不应被视为一个完整可执行 Flow。

Flow Output 可以来源于：

```text
某个 Node Output
某个 SubFlow Output
Retry Output
Match Output
Each Output
Iter Output
Flow Input
```

只要该数据合法属于当前 Flow。

---

## 7.8 SubFlow

因为 Flow 实现 Executable，所以 Flow 可以直接成为另一个 Flow 的 child。

例如：

```text
ParentFlow
├─ NodeA
├─ ChildFlow
└─ NodeB
```

ParentFlow 只看到：

```text
ChildFlow.Input
ChildFlow.Output
```

ChildFlow 内部：

```text
Ref<X>
Ref<Y>
Ref<Z>
```

不能被 ParentFlow 引用。

因此 SubFlow 不需要成为额外的 Runtime Primitive。

它只是：

> **Flow 作为 Executable 被另一个 Flow 组合使用。**

---

## 7.9 Flow 边界

Flow 边界定义局部数据作用域。

一个 Flow 内产生的 Ref：

```text
只属于该 Flow。
```

不得：

```text
将 FlowA 的 Ref<T>
直接传入 FlowB 的 then
```

即使：

```text
slot 相同
T 相同
```

也必须被视为错误连接。

跨 Flow 传递业务数据必须通过：

```text
Flow Input / Output
```

发生。

因此：

> **Ref 是 Flow-local 的；Input / Output 是 Executable-boundary 的。**

---

## 7.10 Flow 与数据依赖

Flow 的顺序和数据依赖必须显式区分。

例如：

```text
Step1 → A
Step2 → B
Step3 → C
```

并不自动表示：

```text
A → Step2
B → Step3
```

真实关系可能是：

```text
Step1 → A ──────────────┐
                        ↓
Step2 → B ───────────→ Step3
```

因此 Flow 不使用：

```text
“上一个 Output 自动成为下一个 Input”
```

这种隐式管道规则。

所有数据依赖必须由 Binding 明确表达。

---

## 7.11 Flow 不承担业务计算

Flow 的职责是：

```text
顺序
组合
数据连接
控制型 Executable 的组织
```

Flow 不应该出现：

```text
if score > 0.8
choose_best(...)
merge_prose(...)
calculate(...)
```

如果这些操作产生新的业务意义，应成为 Node。

Flow 可以表达：

```text
RouteRef → Match
```

但不能自己判断 Route。

Flow 可以表达：

```text
字段投影 + Input 装配
```

但不能借装配之名执行任意函数。

因此：

> **Flow 的表达能力必须足够组织业务，但不能演化成业务脚本语言。**

---

# 8. Binding 与 Input 装配

## 8.1 Binding 的定义

Binding 描述：

> **一个 Executable 的 Input 如何从当前 Flow 已有数据中结构性形成。**

例如：

```text
Ref<Plan>
+
Ref<StoryInput>
↓
PlanCheckInput
```

Binding 解决的是数据连接问题，而不是执行问题。

---

## 8.2 Binding 不是 Executable

Binding 本身：

```text
不产生 Execution
不进入 Runtime
不形成独立业务步骤
```

例如：

```text
Flow
↓
Binding
↓
Node
```

Runtime 观察到的执行仍然只是：

```text
Flow
Node
```

而不是：

```text
Flow
Binding
Node
```

Binding 在 child Executable 开始之前解析出其 Input。

因此：

> **Binding 是 Flow 数据连接机制，不是执行单元。**

---

## 8.3 整值引用

最简单的 Binding 是直接读取一个 Ref：

```text
Ref<T>
→ T
```

例如：

```rust
let plan = flow.then(generate_plan, input);
flow.then(check_plan, plan);
```

表示：

```text
GeneratePlan.Output
直接成为
CheckPlan.Input
```

这种读取在设计上是只读的。

---

## 8.4 字段投影

Binding 可以从结构化 Flow 数据中投影字段。

例如：

```rust
StoryInput {
    plan,
    initial_prose,
    key_nodes,
}
```

可以表达：

```text
Ref<StoryInput>
    ↓
.plan
    ↓
Plan
```

以及：

```text
Ref<StoryInput>
    ↓
.key_nodes
    ↓
Vec<KeyNode>
```

字段投影的目的，是避免创建没有业务意义的：

```text
GetPlanNode
GetKeyNodesNode
```

字段投影只允许读取已有结构。

它不产生新的业务判断或业务计算。

---

## 8.5 多值组合

Binding 可以组合多个已有 Binding。

例如：

```text
Ref<Plan>
+
Ref<CheckResult>
→
(Plan, CheckResult)
```

或者：

```text
Plan
KeyNode
Background
PreviousProse
↓
GenerationInput
```

因此 `then()` 不应被限制为：

```text
一个 Ref 恰好就是完整 Input
```

真实 SES 流程经常需要多个已有数据共同构成下一个 Executable Input。

---

## 8.6 命名结构装配

当下游 Input 是业务结构体时，Binding 应能够直接结构性装配。

例如：

```rust
ProgressionInput {
    plan,
    key_node,
    background,
    previous_prose,
}
```

其字段可以分别来自不同 Ref 或字段投影。

概念上：

```text
Binding<Plan>
Binding<KeyNode>
Binding<Background>
Binding<Prose>
↓
ProgressionInput
```

这种装配不属于业务计算。

它只是将已有业务值放入下游明确声明的 Input 结构。

---

## 8.7 Flow 归属

任何 Binding 最终依赖的 Ref 都必须属于当前 Flow。

如果：

```text
FlowA.Ref<T>
```

被用于：

```text
FlowB.then(...)
```

Binding 解析必须失败。

字段投影不能绕过该限制。

例如：

```text
field!(foreign_ref.plan)
```

仍然属于外来 Ref。

因此：

> **Binding 可以改变数据形状，不能改变数据归属。**

---

## 8.8 Binding 的纯结构性约束

Binding 可以执行：

```text
读取
字段投影
元组组合
结构体字段装配
嵌套结构装配
```

Binding 不可以承担：

```text
任意业务函数调用
业务判断
计算
排序
过滤
选择
文本变换
LLM 调用
副作用
```

判断一个操作是否属于 Binding，可以使用以下问题：

> **这个操作是在重新组织已经存在的数据，还是在产生新的业务意义？**

如果只是重新组织：

```text
属于 Binding。
```

如果产生新的业务意义：

```text
属于 Node。
```

例如：

```text
StoryInput.plan
```

是投影。

而：

```text
score(plan)
```

是计算，应为 Node。

再例如：

```text
Plan + Prose → RewriteInput { plan, prose }
```

是装配。

而：

```text
Plan + Prose → better_prose
```

是业务处理，应为 Node。

---

## 8.9 Binding 与 Node 的边界

Binding 的存在不是为了减少 Node 数量。

它的目的，是防止本来没有业务意义的数据操作被伪装成 Node。

因此应避免：

```text
ExtractFieldNode
TupleNode
BuildInputNode
CopyNode
```

但另一方面，也不能为了“不想增加 Node”而不断增强 Binding。

例如：

```text
FilterCandidatesBinding
BestCandidateBinding
MergeProseBinding
```

都属于错误方向。

一旦某个操作需要解释：

```text
为什么这个结果应该这样产生
```

它通常已经进入业务逻辑，应成为 Node。

---

## 8.10 正式 Rust API 的开放问题

结构性 Binding 的能力已经经过 Compile Probe 验证，但最终 Rust 公共 API 尚未冻结。

已经确认必须能够表达：

```text
Ref<T> → T

Ref<Struct>.field → Field

(Binding<A>, Binding<B>, ...)
→ tuple

多个 Binding
→ named struct
```

但以下具体形式仍属于实现问题：

```rust
field!(source.plan)

bind!(SomeInput {
    plan: ...,
    prose: ...,
})
```

是否使用宏、derive、builder 或其他方式，可以继续评估。

最终 API 必须满足：

1. 强类型；
2. Flow 归属不能被绕过；
3. 业务侧写法简洁；
4. 不要求为了取字段创建胶水 Node；
5. 不鼓励任意计算进入 Binding；
6. 不因内部 ValueStore 实现泄漏大量框架细节；
7. 不改变 `flow.then(executable, binding)` 的核心数据连接模型。

当前 Compile Probe 中的构造器、宏能力范围和 Clone 策略，不构成正式 API 标准。

---

# 9. 控制型 Executable

## 9.1 总体原则

控制型 Executable 用来表达：

> **“其他 Executable 应当以什么控制语义被执行。”**

SRFlow v2 当前定义四种已经验证的控制型 Executable：

```text
Retry
Match
Each
Iter
```

它们与 Node、Flow 一样，都实现 Executable。

因此对于父 Flow：

```text
Node
SubFlow
Retry
Match
Each
Iter
```

不存在不同的编排入口。

统一表达为：

```text
flow.then(executable, binding)
```

控制型 Executable 可以拥有一个或多个 child Executable，但 child 的实际执行必须重新经过 Runtime。

例如：

```text
Runtime.execute(Retry)
    ↓
Retry
    ↓
Runtime.execute(Body)
```

控制型 Executable 自己负责其控制语义。

Runtime 不理解 Retry、Match、Each 或 Iter 的内部规则。

---

### 9.1.1 控制型 Executable 不产生新的业务判断

控制型 Executable 可以读取已经形成的控制信息，但不应成为复杂业务判断发生的地方。

推荐结构：

```text
业务数据
↓
JudgeNode
↓
明确的业务判断结果
↓
控制型 Executable
```

而不是：

```text
控制型 Executable
↓
内部执行复杂业务判断
↓
决定控制路径
```

SRFlow 应尽量让：

```text
业务判断
```

与：

```text
流程控制
```

保持可见且可分离。

---

### 9.1.2 控制语义必须明确

不同控制型 Executable 不能因为“都包含循环或分支”而被合并成一个万能控制结构。

SRFlow 当前认为：

```text
Retry = 重做
Match = 路由
Each  = 逐项处理
Iter  = 逐项推进
```

它们解决的是不同问题。

不同业务含义应使用不同 Executable 明确表达。

---

## 9.2 Retry

### 9.2.1 定义

Retry 表达：

> **使用同一个业务 Input 重复执行同一个 Body，直到正常 Output 表示不再需要重试，或达到有限执行上限。**

其基本关系为：

```text
Retry<I, O>

Input:
    I

Body:
    Executable<I, O>

Condition:
    O → retry / stop

Output:
    O
```

执行形态：

```text
             ┌───────────────┐
             │               │
I → Body → O1 → retry? ─ yes ┘
             │
             no
             ↓
             O1
```

如果继续：

```text
I → Body → O1
I → Body → O2
I → Body → O3
```

每轮 Body 接收的是语义上相同的 Input。

上一轮 Output 不自动成为下一轮 Body Input。

---

### 9.2.2 Retry 的本质是重做

Retry 适用于：

```text
生成候选
↓
检查候选
↓
不接受
↓
重新生成
```

例如：

```text
PlanInput
↓
PlanAttemptFlow
↓
PlanAttemptResult
↓
未接受
↓
再次用同一个 PlanInput 执行 PlanAttemptFlow
```

因此 Retry 的核心不是“循环”，而是：

> **在同一个问题条件下重新尝试产生新的结果。**

---

### 9.2.3 Retry 至少执行一次

Retry 是 do-while 语义。

只要 Retry 被执行，其 Body 至少执行一次。

因此：

```text
limit = 0
```

没有有效业务语义，不允许作为合法 Retry 配置。

---

### 9.2.4 Retry 必须有限

Retry 必须存在明确执行上限。

SRFlow v2 的默认上限为：

```text
8 次 Body 执行
```

这里的 limit 表示：

> **Body 总共允许执行的最大次数。**

不是：

> 第一次执行以后还允许额外 Retry 多少次。

例如：

```text
limit = 3
```

最多产生：

```text
Attempt 1
Attempt 2
Attempt 3
```

---

### 9.2.5 Retry Condition

Retry Condition 只观察一次 Body 正常产生的 Output，并决定：

```text
继续
```

或：

```text
停止
```

Condition 本身不是 Executable。

简单形式可以是：

```text
Output → bool
```

例如：

```text
attempt.accepted == false
```

复杂业务判断不应塞进 Condition。

如果是否重试需要复杂判断，应先由 Body 内的 Node 形成明确结果：

```text
AttemptResult {
    candidate,
    judgment,
}
```

然后 Condition 只读取：

```text
judgment
```

因此：

> **Retry Condition 是控制判断，不应成为隐藏业务判断层。**

---

### 9.2.6 正常停止

如果某轮 Body 正常返回 Output，并且 Condition 表示：

```text
stop
```

Retry 立即结束并返回该 Output。

例如：

```text
Attempt1 → RETRY
Attempt2 → RETRY
Attempt3 → ACCEPT
```

最终：

```text
Retry.Output = Attempt3.Output
```

后续 Attempt 不再执行。

---

### 9.2.7 上限耗尽

如果每次正常 Output 都要求 Retry，直到达到 limit：

```text
Attempt1 → RETRY
Attempt2 → RETRY
...
Attempt8 → RETRY
```

Retry 不把“达到上限”自动转换为技术执行错误。

它返回：

```text
最后一次正常 Output
```

业务是否认为这个结果代表：

```text
失败
未接受
需要人工处理
```

应由 Output 自己的业务状态表达。

因此：

> **Retry limit 限制执行次数，不替业务定义成功与失败。**

---

### 9.2.8 技术错误

如果某轮 Body 返回执行错误：

```text
Attempt1 → normal
Attempt2 → Error
```

Retry 立即返回该 Error。

不得：

```text
把 Error 当作 retry = true
继续下一轮
```

因此：

> **业务 Retry 与技术故障重试是两个不同问题。**

SRFlow v2 的 Retry 只处理正常业务 Output 驱动的重新执行。

---

### 9.2.9 Retry 与 Runtime

每次 Body 调用必须重新经过 Runtime：

```text
Runtime.execute(Retry)
    ↓
Retry
    ↓
Runtime.execute(Body)
    ↓
Runtime.execute(Body)
    ↓
Runtime.execute(Body)
```

即使 Body 本身是 Flow，也保持同样规则：

```text
Runtime
→ Retry
  → Runtime
    → SubFlow
      → Runtime
        → Node
```

---

### 9.2.10 Retry 的 Output 历史

Retry 的核心 Output 是：

```text
最后被采用或最后允许执行的 Output
```

SRFlow 已验证可以提供一种保留所有正常 Attempt Output 的变体，例如概念上的：

```text
Retry.collect()
→ Vec<O>
```

这对实验和调试有价值。

但：

> **保留全部 Attempt 结果不是 Retry 核心控制语义的必要条件。**

是否把该能力作为首版正式公共 API，可以在实现阶段独立决定。

---

## 9.3 Match

### 9.3.1 定义

Match 表达：

> **根据一个已经存在的匹配值，从多个 Executable 中选择且只选择一个执行。**

其基本关系为：

```text
Match<K, I, O>

Input:
    (K, I)

Cases:
    K → Executable<I, O>

Default:
    optional Executable<I, O>

Output:
    O
```

其中：

- `K`：匹配值；
- `I`：真正交给被选中 branch 的业务 Input；
- `O`：所有 branch 共同的 Output。

---

### 9.3.2 判断与路由分离

Match 不负责生成 K。

例如：

```text
Candidate
↓
JudgeNode
↓
Route
↓
Match
```

而不是：

```text
Candidate
↓
Match 内部分析业务
↓
决定 Route
```

因此：

> **Match 消费判断，不生产判断。**

---

### 9.3.3 Case

每个 case 表达：

```text
某个 K
→
某个 Executable<I, O>
```

多个 case 的具体 Executable 类型可以不同：

```text
Short → NodeA
Long  → FlowB
Other → RetryC
```

但它们必须具有相同外部契约：

```text
Input = I
Output = O
```

否则 Match 无法作为统一 Executable 对外暴露：

```text
Executable<(K, I), O>
```

---

### 9.3.4 唯一执行路径

一次 Match 执行最多选择一个 branch。

例如：

```text
K = Long
```

则：

```text
Short   不执行
Long    执行
Default 不执行
```

Match 不是：

```text
满足多个条件就执行多个 branch
```

也不是：

```text
按顺序尝试 branch
```

它表达的是单一路由。

---

### 9.3.5 Default

Match 可以定义 default branch。

只有：

```text
没有任何 case 匹配 K
```

时，default 才执行。

因此 default 的含义是：

> **未匹配路径。**

不是：

> **被选路径执行失败后的备用路径。**

---

### 9.3.6 无匹配

如果：

```text
没有 case 匹配
```

并且：

```text
没有 default
```

Match 执行失败。

该失败表示：

> 当前 Match 定义无法处理这个 K。

不得静默跳过。

---

### 9.3.7 Branch Error

如果 case 已经成功匹配：

```text
K → BranchA
```

但 BranchA 执行返回 Error：

```text
BranchA → Error
```

Match 直接传播 Error。

不得：

```text
继续寻找其他 case
```

也不得：

```text
执行 default
```

因为路由已经完成。

---

### 9.3.8 Match 与 Runtime

选中 branch 的实际执行必须通过 Runtime：

```text
Runtime.execute(Match)
    ↓
Match
    ↓
选择 Branch
    ↓
Runtime.execute(Branch)
```

如果 Branch 是 SubFlow：

```text
Runtime
→ Match
  → Runtime
    → SubFlow
      → Runtime
        → Node
```

---

## 9.4 Each

### 9.4.1 定义

Each 表达：

> **对输入集合中的每一个 Item，按集合顺序执行同一个 Body，并收集所有正常 Output。**

其基本关系为：

```text
Each<I, O>

Input:
    Vec<I>

Body:
    Executable<I, O>

Output:
    Vec<O>
```

执行形态：

```text
[I1, I2, I3]
      ↓
Body(I1) → O1
Body(I2) → O2
Body(I3) → O3
      ↓
[O1, O2, O3]
```

---

### 9.4.2 执行次数由集合决定

Each 不需要 limit。

输入有多少 Item，就最多执行多少次 Body。

例如：

```text
3 Items
→ 3 次 Body
```

空集合：

```text
0 Items
→ 0 次 Body
```

因此额外的：

```text
Each.limit
```

不是当前 Each 语义的一部分。

如果业务只想处理前 N 个 Item，应显式形成只含 N 个 Item 的输入集合。

---

### 9.4.3 顺序语义

Each v2 按输入集合顺序执行 Body。

例如：

```text
[I1, I2, I3]
```

保证：

```text
I1 完成
↓
I2 开始
↓
I2 完成
↓
I3 开始
```

Output 顺序与 Input Item 顺序对应：

```text
[O1, O2, O3]
```

Runtime 不得自动并行 Each。

---

### 9.4.4 Each 不传递前一轮 Output

Each 的关键性质是：

```text
Body(I1) → O1

Body(I2) 的 Input
不包含 O1

Body(I3) 的 Input
不包含 O2
```

SRFlow 因此把 Each 定义为：

> **不携带上一轮 Output 的逐项处理。**

这不意味着 Body 在业务世界中绝对没有共享状态。

Body 仍可能访问：

```text
数据库
外部服务
内部共享资源
```

因此不应把 Each 描述为绝对“彼此独立”。

准确说法是：

> **Each 本身不建立跨轮 Output → Input 数据关系。**

---

### 9.4.5 空集合

如果：

```text
Input = []
```

则：

```text
Body 执行 0 次
Output = []
```

空集合不是错误。

---

### 9.4.6 Body Error

如果：

```text
I1 → O1
I2 → Error
I3 尚未执行
```

Each 立即停止并返回 Error。

不得继续执行 I3。

不得向调用方返回：

```text
[O1]
```

作为正常 Each Output。

但 O1 对外部世界已经产生的副作用不会因此自动回滚。

---

### 9.4.7 Each 与 Runtime

每个 Item 的 Body 调用必须重新经过 Runtime：

```text
Runtime.execute(Each)
    ↓
Each
    ├→ Runtime.execute(Body, I1)
    ├→ Runtime.execute(Body, I2)
    └→ Runtime.execute(Body, I3)
```

Body 可以是 Node，也可以是 SubFlow 或其他合法 Executable。

---

## 9.5 Iter

### 9.5.1 定义

Iter 表达：

> **按顺序处理一组 Item，并让上一轮产生的累积状态成为下一轮 Body Input 的组成部分。**

基本关系：

```text
Iter<Item, T>

Input:
    (Vec<Item>, T)

Body:
    Executable<(T, Item), T>

Output:
    T
```

其中：

- `Item`：当前轮处理对象；
- `T`：跨轮携带的累积状态。

---

### 9.5.2 状态推进

Iter 的核心执行关系为：

```text
T0 + Item1
↓
Body
↓
T1

T1 + Item2
↓
Body
↓
T2

T2 + Item3
↓
Body
↓
T3
```

最终：

```text
Iter.Output = T3
```

这也是 Iter 与 Each 的根本区别。

---

### 9.5.3 Iter 的本质是逐项推进

例如正文关键节点：

```text
初始正文 P0
+
KeyNode1
↓
P1

P1
+
KeyNode2
↓
P2

P2
+
KeyNode3
↓
P3
```

后一个关键节点需要看到前一个关键节点已经形成的正文。

因此 Each 不适合：

```text
Each:
K1 → P1
K2 → P2
K3 → P3
```

因为 Each 不建立：

```text
P1 → K2
P2 → K3
```

Iter 正是表达这种连续推进关系。

---

### 9.5.4 T 不要求只有变化字段

Iter 中的 `T` 表示：

> **每轮继续执行所需要携带的累积状态。**

它可以同时包含：

```text
变化的数据
+
后续每轮仍需要的不变数据
```

例如：

```rust
ProseState {
    plan,
    prose,
}
```

其中：

```text
plan
```

可以在每轮保持不变，

而：

```text
prose
```

持续变化。

SRFlow 不要求 T 中每个字段都必须发生变化。

只有当真实业务证明大量只读上下文随 T 传递造成明显问题时，才应重新评估共享上下文设计。

---

### 9.5.5 空集合

如果：

```text
Items = []
Initial = T0
```

则：

```text
Body 执行 0 次
Iter.Output = T0
```

这是 Iter 的自然单位元语义。

---

### 9.5.6 Body Error

如果：

```text
T0 + I1 → T1
T1 + I2 → Error
```

Iter 立即停止并返回 Error。

不得继续 I3。

也不得把：

```text
T1
```

作为正常 Iter Output 返回。

同样，I1 执行过程中已经发生的外部副作用不因此自动回滚。

---

### 9.5.7 Iter 与 Runtime

每轮 Body 调用必须经过 Runtime：

```text
Runtime.execute(Iter)
    ↓
Iter
    ↓
Runtime.execute(Body, (T0, I1))
    ↓
T1
    ↓
Runtime.execute(Body, (T1, I2))
    ↓
T2
```

---

### 9.5.8 Iter Output 历史

Iter 的核心 Output 是：

```text
最终 T
```

SRFlow 当前不要求 Iter 必须公开所有中间状态：

```text
[T1, T2, T3]
```

如果未来调试或研究明确需要，可以增加类似 collect 的变体。

但该能力不是 Iter 的核心控制语义。

---

## 9.6 四种控制语义的边界

四种控制型 Executable 可以用下表区分：

| Executable | 重复/选择由什么驱动 | 下一轮是否使用上一轮 Output | 核心结果 |
|---|---|---|---|
| `Retry` | 当前 Output 是否要求重做 | 否；每轮仍使用同一个原始 Input | 最后一轮正常 Output |
| `Match` | 已有匹配值 K | 不适用 | 被选 branch 的 Output |
| `Each` | 外部 Item 集合 | 否 | `Vec<O>` |
| `Iter` | 外部 Item 集合 | 是；上一轮 `T` 进入下一轮 | 最终 `T` |

可以进一步概括为：

```text
Retry
= 同一个问题再做一次

Match
= 根据已有判断选一条路

Each
= 每个对象都做一次

Iter
= 每个对象做一次，并把结果继续带到下一轮
```

---

### 9.6.1 Retry 与 Iter

Retry：

```text
I → O1
I → O2
I → O3
```

Iter：

```text
T0 → T1 → T2 → T3
```

两者虽然都有重复执行，但业务意义完全不同。

因此不得把：

```text
上一轮 Output 成为下一轮 Input
```

塞进 Retry。

那是 Iter。

---

### 9.6.2 Each 与 Iter

Each：

```text
I1 → O1
I2 → O2
I3 → O3
```

Iter：

```text
T0 + I1 → T1
T1 + I2 → T2
T2 + I3 → T3
```

判断标准非常简单：

> **下一项是否需要上一项执行后形成的业务状态？**

如果不需要：

```text
Each
```

如果需要：

```text
Iter
```

---

### 9.6.3 Match 与业务判断

Match 不是：

```text
Judge
```

Match 是：

```text
Route
```

任何需要分析业务内容才能决定路径的过程，都应先形成明确判断 Output。

---

## 9.7 控制型 Executable 的统一规则

所有控制型 Executable 必须遵守：

1. 自身实现 Executable。
2. 对父级暴露明确 Input / Output。
3. 内部 child 的实际调用必须经过 Runtime。
4. 不得要求 Runtime 理解其控制语义。
5. 不得泄漏内部临时 Ref。
6. child Error 默认向外传播，除非该控制型 Executable 的正式语义明确规定其他处理。
7. 不得通过隐式全局状态向 child 传递 Flow 业务数据。
8. 新控制语义应有明确业务含义，不能只是为了统一代码而抽象。
9. 不得把复杂业务判断隐藏进控制机制。
10. 新控制型 Executable 的加入原则上不应要求修改现有 Flow / Runtime 核心协议。

---

# 10. 执行、数据与错误语义

## 10.1 顺序执行

SRFlow v2 的 Flow、Each 和 Iter 当前均具有明确顺序语义。

Flow：

```text
then(A)
then(B)
then(C)
```

表示：

```text
A 完成
↓
B 开始
↓
B 完成
↓
C 开始
```

Each：

```text
[I1, I2, I3]
```

表示：

```text
I1
↓
I2
↓
I3
```

Iter 同样按照 Item 输入顺序推进。

顺序不是性能实现细节。

它是业务执行语义。

Runtime 不得自行推断所谓“独立任务”并改变该顺序。

---

## 10.2 Input 只读语义

一次 Executable 执行接收确定 Input。

SRFlow 的设计语义要求：

> **该 Input 在本次调用中作为只读业务事实使用。**

Executable 不通过修改调用方已有 Input 向外传递结果。

如果执行需要产生变化后的业务数据，应返回新的 Output。

例如：

```text
Input:
    ProseState P0

Node:
    revise

Output:
    ProseState P1
```

而不是要求调用方在执行后读取被 Node 修改过的 P0。

---

### 10.2.1 只读不等于 Clone

只读属于设计语义。

Clone 属于实现策略。

SRFlow 不要求：

```text
读取一次 Ref
=
必须复制完整业务值一次
```

未来实现可以使用其他方式，只要保持：

```text
调用方看不到隐式修改
同一数据可以被多个后续 Binding 安全引用
```

---

## 10.3 Output 语义

正常 Output 表示：

> **该 Executable 已经完成自身定义的执行语义，并产生了一个有效业务结果。**

Output 可以包含业务上的：

```text
PASS
FAIL
RETRY
REJECTED
NOT_ACCEPTED
```

这些仍然可以是正常 Output。

例如：

```text
CheckResult {
    accepted: false
}
```

不等于技术执行错误。

因此 SRFlow 必须区分：

```text
业务结果不理想
```

与：

```text
执行失败
```

前者属于 Output。

后者属于 Error。

---

## 10.4 错误传播

SRFlow 的默认错误规则是：

> **Fail Fast，向当前 Executable 的调用方传播。**

如果：

```text
Parent
↓
Child
↓
Error
```

而 Parent 没有明确设计处理该错误的语义，则：

```text
Parent → Error
```

SRFlow 不默认：

```text
自动重试
自动跳过
自动生成默认值
自动选择其他路径
自动回滚
```

---

### 10.4.1 业务失败不是 Execution Error

例如：

```text
PlanCheckResult {
    accepted: false
}
```

表示 Node 正常执行成功，只是业务判断为不接受。

因此：

```text
accepted = false
```

可以进入 Retry Condition。

而：

```text
LLM request failed
```

属于执行错误。

不能自动当成：

```text
accepted = false
```

否则会把：

```text
业务重做
```

和：

```text
技术故障恢复
```

混为一谈。

---

## 10.5 部分执行与副作用

组合型 Executable 返回 Error，只表示：

> **该组合执行没有正常产生最终 Output。**

它不表示：

> **此前已经发生的所有动作已经回滚。**

例如 Each：

```text
Item1
→ Node 写入外部系统成功

Item2
→ Error
```

Each 返回 Error。

但 Item1 已产生的外部效果仍然可能存在。

同样：

```text
Flow
├─ NodeA → 外部效果成功
├─ NodeB → Error
└─ NodeC → 未执行
```

Flow Error 不表示 NodeA 被回滚。

因此：

> **SRFlow v2 不提供通用事务语义。**

如果业务要求：

```text
原子提交
补偿操作
事务
```

必须由对应业务系统或未来明确设计的机制承担。

---

## 10.6 空集合

空集合在 Each 与 Iter 中都有明确正常语义。

### Each

```text
Each([], Body)
→ []
```

Body 执行 0 次。

### Iter

```text
Iter([], T0, Body)
→ T0
```

Body 执行 0 次。

空集合不是技术错误。

业务如果认为：

```text
没有 Item 是非法状态
```

应在进入 Each / Iter 之前由 Node 或明确的业务检查表达。

---

## 10.7 Retry 耗尽

Retry 达到 limit 时：

```text
不自动返回 Error
```

而是：

```text
返回最后一次正常 Body Output
```

例如：

```text
Attempt1 → NOT_ACCEPTED
Attempt2 → NOT_ACCEPTED
Attempt3 → NOT_ACCEPTED
limit = 3
```

最终返回：

```text
Attempt3 Output
```

是否将该业务结果解释为：

```text
本阶段失败
请求人工处理
终止更大 Flow
切换其他路径
```

属于上层业务逻辑。

---

## 10.8 Match 未命中

Match 存在两种合法未命中处理。

### 有 default

```text
没有 case 命中
↓
执行 default
```

### 无 default

```text
没有 case 命中
↓
Match Error
```

不得：

```text
静默返回空结果
```

也不得：

```text
自动选择第一个 case
```

---

## 10.9 Flow 内部失败

如果 Flow 内任意 Step 返回 Error：

```text
Step1 → success
Step2 → success
Step3 → Error
Step4 → 不执行
```

Flow 立即返回 Error。

Flow 不产生正常 Output。

内部已经形成但尚未暴露的 Ref：

```text
Ref<A>
Ref<B>
```

不会因为 Flow 失败而成为父级可用数据。

父级只通过该 Flow 的正式：

```text
Output
```

获得数据。

因此：

> **一个 Executable 要么正常跨边界返回 Output，要么跨边界返回 Error；内部部分结果不突破组合边界。**

这不否认前序 Step 已经发生的外部副作用。

---

## 10.10 Binding 错误

Binding 在 child Executable 开始前解析 Input。

如果 Binding 发现：

```text
外来 Flow Ref
类型不匹配
不存在的数据位置
框架内部不变量破坏
```

则：

```text
Binding 解析失败
↓
child 不得执行
↓
当前 Flow 返回 Error
```

Binding 不应：

```text
产生默认业务值
跳过该 child
尝试猜测其他 Ref
```

---

## 10.11 框架错误与业务错误

SRFlow 允许未来建立更细致的 Error 类型体系，但语义上至少必须区分：

### 业务正常结果

例如：

```text
Rejected
NotAccepted
NeedsRevision
NoCandidate
```

如果这些是业务允许出现的结果，它们属于 Output。

### 执行错误

例如：

```text
child execution failed
external dependency failed
binding resolution failed
```

属于 Error。

### 框架不变量错误

例如：

```text
错误的 Ref 归属
内部 slot 类型不一致
Ready Flow 没有 Output
```

表示 SRFlow 自身构建或实现的不变量遭到破坏。

具体 Rust Error enum 如何划分，属于实现设计。

但不得模糊：

```text
正常业务结论
```

与：

```text
执行失败
```

---

## 10.12 Error 不隐含重试

SRFlow 的一个重要全局规则是：

```text
Error
≠
Retry request
```

任何技术 Error 都不会因为处于：

```text
Retry
Each
Iter
Flow
Match
```

内部而自动重新执行。

如果未来确有：

```text
网络瞬时错误自动再请求
```

这属于单独的技术 Retry 问题。

不能借用当前业务 Retry 的语义。

---

## 10.13 Error 不隐含回滚

同样：

```text
Error
≠
Rollback
```

SRFlow v2 没有全局事务。

错误发生后：

```text
此前已经完成的外部作用
```

可能仍然存在。

设计和实现不得使用：

```text
Flow failed
```

推导：

```text
Nothing happened
```

这对后续模型调用、文件写入、数据库操作和外部服务调用都成立。

---

## 10.14 执行语义总结

SRFlow v2 的执行模型可以概括为：

```text
明确 Input
↓
Runtime 统一执行
↓
Executable 按自身语义运行
↓
所有 child 再次经过 Runtime
↓
正常产生一个明确 Output
或
立即传播 Error
```

控制结构只改变：

```text
child 应该如何被组织执行
```

不改变：

```text
Executable Input / Output 边界
Runtime 统一调用
业务结果与技术错误分离
```

这些规则共同构成 SRFlow v2 的稳定执行基础。

---

# 11. Rust 实现边界

## 11.1 设计语义与实现策略

SRFlow v2 的核心设计已经冻结，但具体 Rust 实现仍保留必要自由度。

必须始终区分：

```text
设计必须成立
```

与：

```text
当前 Probe 恰好这样实现
```

例如：

```text
Ref 只读、可复用
```

属于设计。

而：

```text
ValueStore 使用 Box<dyn Any>
```

不属于设计。

同样：

```text
所有 Executable 必须经 Runtime 执行
```

属于设计。

而：

```text
Executable trait 的具体函数签名
```

仍可以在不改变语义的前提下调整。

因此：

> **正式实现应服从设计语义，而不是复制 Compile Probe。**

---

## 11.2 强类型外部接口

SRFlow 面向业务侧的核心连接必须保持强类型。

至少包括：

```text
Executable Input
Executable Output
Ref<T>
Flow Input / Output
Binding 结果
控制型 Executable 的类型关系
```

例如：

```text
Executable<Input = A, Output = B>
```

如果某个 Flow 将错误类型连接到该 Executable，应尽可能在构建期被拒绝。

SRFlow 不应退化成：

```text
String key
+
JSON value
+
运行时猜类型
```

这种弱类型工作流系统。

内部可以进行有限类型擦除，但外部业务连接应保持明确类型。

---

## 11.3 内部类型擦除

一个 Flow 可以包含不同具体类型的 Executable：

```text
NodeA
FlowB
RetryC
MatchD
```

因此正式实现可能需要：

```text
trait object
type erasure
Any
内部 adapter
```

这属于合理实现策略。

但内部类型擦除必须满足：

> **外部强类型，内部有限擦除。**

不得因为内部需要异构存储，就要求业务侧使用：

```text
Box<dyn Any>
Value
JSON
RuntimeType
```

来连接普通 Flow 数据。

如果内部 downcast 失败，而该连接本应由构建期 API 保证正确，应将其视为：

```text
框架不变量被破坏
```

而不是普通业务错误。

---

## 11.4 Ref 与值存储

Ref 是：

> 当前 Flow 中某个数据位置的强类型只读引用。

正式实现可以通过：

```text
slot
index
handle
arena key
其他内部地址
```

表示数据位置。

但 Ref 必须至少保留以下语义：

```text
类型 T
所属 Flow
目标数据位置
```

Ref 本身不拥有业务数据。

Ref 不向业务侧暴露可变访问。

---

### 11.4.1 Flow 归属

正式实现必须能够拒绝：

```text
FlowA 的 Ref<T>
被用于 FlowB
```

即使：

```text
T 相同
内部 slot 恰好相同
```

也必须失败。

这可以通过：

```text
flow identity
runtime validation
brand
lifetime
其他可靠机制
```

实现。

SRFlow v2 不要求必须在编译期完成该证明。

当前已验证的运行时归属检查足以满足设计语义。

---

## 11.5 Clone / Arc / Borrow

Compile Probe 为了验证最小语义，在部分路径使用了 Clone。

这不构成正式实现要求。

后续实现可以评估：

```text
Clone
Arc<T>
引用
内部共享存储
所有权转移
Copy-on-write
```

选择标准应包括：

```text
业务侧 API 是否自然
数据是否保持只读语义
Ref 是否可安全复用
大型文本是否产生不合理复制
Rust 生命周期复杂度是否可控
```

不得为了消除 Clone 而引入：

```text
隐式全局可变状态
跨 Flow 可变引用
业务不可见的数据修改
```

性能优化不能破坏数据模型。

---

## 11.6 typestate

Compile Probe 使用了类似：

```text
Flow<I, Building>
Flow<I, Ready<O>>
```

的 typestate 方式表达：

```text
只有定义 Output 后
Flow 才完整可执行
```

该思路与设计语义一致。

但正式实现不要求必须保留完全相同的类型形式。

唯一必须保证的是：

> **一个未完成定义、没有合法 Output 的 Flow 不应被当作正常 Executable 执行。**

如果其他 Rust API 能更自然地保证这一点，也可以采用。

---

## 11.7 associated type

当前验证使用：

```rust
trait Executable {
    type Input;
    type Output;
}
```

该方式已证明能够自然表达 SRFlow v2。

它带来的约束是：

> 一个具体 Executable 类型通常只有一组 Input / Output。

当前没有真实需求证明：

```text
同一个具体类型
必须同时拥有多组不同执行签名
```

因此无需为了理论灵活性提前改成更复杂的泛型协议。

如果未来出现真实需求，再重新评估。

---

## 11.8 Binding API

正式 Binding API 必须支持至少：

```text
整值 Ref
字段投影
多 Binding 组合
命名结构 Input 装配
嵌套结构装配
```

业务侧应能够自然表达：

```text
已有多个 Ref
↓
组成下一个 Node Input
```

而不需要：

```text
GetFieldNode
BuildInputNode
TupleNode
```

---

### 11.8.1 Binding API 必须保持结构性

正式 API 应尽可能限制 Binding 只能执行：

```text
读取
投影
组合
构造
```

而不是任意：

```text
Fn
closure
业务计算
```

如果 Rust 可见性、宏展开或 trait 设计无法完全通过类型系统封死该边界，也必须：

```text
在公共 API 上减少任意计算入口
在文档中明确禁止
在代码评审中作为设计红线检查
```

不得因为实现方便就公开一个：

```text
Binding::map(any_function)
```

然后事实上把 Binding 变成隐藏 Node 系统。

---

## 11.9 Probe 中不应固化的实现细节

以下内容仅用于 Compile Probe，不得直接作为正式设计要求：

```text
Box<dyn Any>
当前 ValueStore 结构
slot 的具体编号方式
当前 Error enum
当前 Runtime call counter
Rc / RefCell 测试替身
field! 宏的具体语法
bind! 宏的具体语法
tuple 只实现到固定数量
Match 使用 Vec 线性搜索
当前 trait object wrapper 名称
当前 PhantomData 写法
```

实现团队可以替换这些细节。

但替换后必须继续满足本文定义的外部语义。

---

## 11.10 性能优化原则

性能优化必须服从设计边界。

允许：

```text
减少 clone
减少 allocation
优化 Match lookup
缓存内部 adapter
更紧凑的 ValueStore
更低成本的 Ref
```

不允许：

```text
为了性能改变 then 顺序
隐式并行
跳过 Runtime
让 Node 直接访问其他 Flow 数据
让 Binding 做业务计算
用共享可变状态替代显式 Output
```

原则是：

> **优化实现，不优化掉语义。**

---

# 12. 非目标与延后能力

## 12.1 为什么明确写非目标

一个框架最容易失控的方式之一，是不断添加：

```text
“以后大概会需要”
```

的能力。

因此 SRFlow 必须明确记录：

```text
当前没有设计
```

不等于：

```text
遗漏
```

而是：

> 当前证据不足以证明它应进入核心。

---

## 12.2 Parallel

SRFlow v2 不定义 Parallel。

当前：

```text
Flow
Each
Iter
```

都具有明确顺序语义。

Runtime 不得根据：

```text
无数据依赖
```

自行推断并行。

未来如果真实 SES 场景需要并行，应优先考虑：

```text
Parallel implements Executable
```

并单独定义：

```text
Input
Output
错误规则
完成规则
顺序保证
资源限制
```

不得直接改变 Flow 的默认语义。

---

## 12.3 Async

SRFlow v2 的设计语义不依赖同步或异步 Rust 函数。

正式实现未来可以采用：

```text
async fn
Future
tokio
其他 executor
```

但：

> async 是执行技术，不是新的业务控制语义。

异步实现不能自动意味着：

```text
并行
乱序
取消
竞速
```

这些都需要独立设计。

---

## 12.4 Timeout

SRFlow v2 不定义 Timeout。

如果未来需要：

```text
某 Executable 最多执行 N 秒
```

应作为明确执行语义设计。

不能偷偷嵌入 Runtime 全局策略，导致所有业务 Executable 获得隐式超时行为。

---

## 12.5 Race

SRFlow v2 不定义：

```text
多个 Executable 同时执行
谁先完成采用谁
```

这一类 Race 语义。

如果未来需要，应作为新的控制型 Executable 单独验证。

---

## 12.6 技术 Retry

SRFlow v2 已定义的 Retry 是：

> 正常业务 Output 驱动的重新生产。

它不处理：

```text
HTTP timeout
网络抖动
连接重置
服务暂不可用
```

等技术故障。

未来如果模型 Adapter 或外部服务需要技术重试，应：

```text
由 Adapter 自身负责
```

或设计独立技术恢复机制。

不得改变业务 Retry 的语义。

---

## 12.7 持久化

SRFlow v2 核心不定义：

```text
Flow 状态持久化
Execution checkpoint
Ref 持久化
中间 Output 持久化
自动恢复
```

业务可以自行保存 Output。

框架未来是否需要持久执行状态，必须由真实恢复需求重新验证。

---

## 12.8 Trace / 日志

统一 Runtime 入口为未来：

```text
Trace
日志
指标
耗时
执行树
```

提供了天然接缝。

但 SRFlow v2 不把具体 Trace / Logging 模型纳入核心设计。

未来新增这些能力时，应围绕：

```text
Executable invocation
```

观察执行，而不得改变业务数据流。

---

## 12.9 Pause / Resume

SRFlow v2 不定义：

```text
暂停一个运行中的 Flow
稍后继续
持久化恢复点
跨进程恢复
```

人工复核等需求如果当前存在，可以表现为：

```text
当前执行正常结束
产生一个等待人工处理的 Output
```

后续人工结果形成新的 Input，并启动新的执行。

无需提前建立复杂 Resume Runtime。

---

## 12.10 崩溃恢复

SRFlow v2 不保证：

```text
进程崩溃后恢复到上一个 Step
```

也不保证：

```text
Exactly Once
At Least Once
事务性恢复
```

如果后续真实生产运行需要，再独立设计。

---

## 12.11 动态 Graph / DSL

SRFlow v2 采用 code-first Flow。

当前不定义：

```text
JSON workflow
YAML workflow
动态节点注册
字符串节点名称
运行时 Graph 构造
通用工作流 DSL
```

SRFlow 优先利用 Rust 类型系统保证 Flow 连接正确。

动态化本身不是设计目标。

---

## 12.12 插件式 Runtime Primitive

不得因为未来某个新能力出现，就直接给 Runtime 增加：

```text
runtime.retry(...)
runtime.branch(...)
runtime.loop(...)
runtime.parallel(...)
```

SRFlow 的优先扩展路径仍然是：

```text
新的 Executable
```

Runtime 应长期保持薄。

---

## 12.13 新能力进入核心的条件

一个新能力要进入 SRFlow 核心，至少应满足：

1. 来自真实 SES 执行需求；
2. 不只是单个业务 Node 的局部需求；
3. 在多个流程中具有稳定重复语义；
4. 无法通过现有 Executable 组合自然表达；
5. 边界可以清楚定义；
6. 不迫使核心职责重新混合；
7. 可以通过真实业务反向验证；
8. 可以通过独立 Compile Probe 验证 Rust 可实现性。

只有满足这些条件，才应进入正式设计。

---

# 13. 设计验证与实现约束

## 13.1 验证方法

SRFlow v2 采用三层验证原则：

```text
设计推导
↓
真实业务反向验证
↓
Rust Compile Probe
```

三层解决不同问题。

### 设计推导

回答：

```text
这个抽象在语义上是否合理？
```

### 业务反向验证

回答：

```text
它能否自然表达真实 SES 流程？
```

### Compile Probe

回答：

```text
稳定 Rust 能否以足够小、可读的接口承载该设计？
```

这三者缺一不可。

---

## 13.2 业务反向验证

SRFlow v2 使用真实正文生成决策网络进行过反向验证。

该类流程至少包含：

```text
生成方案
↓
检查方案
↓
失败则重新生成
↓
生成关键节点
↓
检查关键节点
↓
逐关键节点处理
↓
每个节点生成推进
↓
检查推进
↓
生成呈现
↓
检查呈现
↓
写正文
↓
检查正文
↓
必要时修订
↓
下一关键节点继续使用上一轮正文
```

该流程实际要求：

```text
Flow
SubFlow
多 Ref Input
Retry
Match
Iter
Binding
```

并对：

```text
上一轮 Output 进入下一轮
多个已有值组成下游 Input
业务判断与路由分离
```

提出了真实压力。

---

## 13.3 Compile Probe 验证阶段

SRFlow v2 核心设计经过六轮最小 Compile Probe。

本章记录了历史 Compile Probe 的验证结论与边界。原可编译原型仅是设计证据，不是正式实现规范；本工程的设计与代码不依赖该原型所在的旧仓库。

### 第一轮：Flow / Ref / then

验证：

```text
Executable Input / Output
Node → Executable
Flow
then
Ref<T>
多 Ref Input
异构 Executable
Flow Output
```

证明：

> 强类型 Flow 数据模型在稳定 Rust 中可成立。

---

### 第二轮：递归 Runtime / Ref 归属

验证：

```text
所有 child 调用重新经过 Runtime
SubFlow 同样经过 Runtime
Ref 带 Flow 归属
跨 Flow Ref 被拒绝
```

证明：

> Runtime 可以真正成为统一执行入口，而不是只有顶层调用经过 Runtime。

---

### 第三轮：Retry

验证：

```text
Retry implements Executable
默认 limit = 8
limit >= 1
同一 Input 重复执行
正常 Output 决定继续
Error 立即传播
耗尽返回最后 Output
Body 可以是 SubFlow
Retry 可以嵌入 Flow
```

证明：

> Retry 不需要修改核心 Executable / Runtime / Flow 模型。

---

### 第四轮：Match

验证：

```text
Match implements Executable
业务判断与路由分离
异构 branch
统一 I/O
唯一命中 branch
default
NoMatch
branch Error
SubFlow branch
```

证明：

> 路由控制同样可以作为独立 Executable 存在。

---

### 第五轮：Each / Iter

验证：

```text
Each 顺序逐项执行
Each 收集 Vec<O>
Iter 传递上一轮 T
空集合
Error 中止
非 Clone Item / State
SubFlow Body
父 Flow 组合
```

证明：

```text
Retry
Each
Iter
```

确实是不同、可独立实现的控制语义。

---

### 第六轮：Binding

验证：

```text
字段投影
命名结构装配
多个 Binding 组合
Ref 复用
跨 Flow 检查
非 Clone 根结构
无额外 Runtime Execution
真实正文 Iter Input 装配
```

证明：

> 结构性 Binding 足以消除没有业务意义的提取 Node，同时保持 `flow.then(executable, binding)` 的核心模型。

---

## 13.4 已验证结论

截至 SRFlow v2 设计冻结，以下语义已获得 Compile Probe 支持：

```text
Executable        PASS
Runtime           PASS
Node              PASS
Flow              PASS
then              PASS
Ref               PASS
Binding 装配能力  PASS
Retry             PASS
Match             PASS
Each              PASS
Iter              PASS
SubFlow           PASS
递归 Runtime      PASS
跨 Flow Ref 防护  PASS
```

这里的 Binding 验证只覆盖字段投影、多值组合和命名 Input 装配的可实现性；它没有证明正式公共 API 已能阻止任意业务计算进入 Binding。该 API 边界仍按 §8.10 和 §11.8 处理。

更重要的是：

> Retry、Match、Each、Iter 和 Binding 的逐步加入，都没有要求推翻 Executable / Runtime / Flow / Ref / then 的核心关系。

这说明核心模型具有足够稳定性。

---

## 13.5 Compile Probe 不证明什么

Compile Probe 只证明：

> 当前设计语义可以在稳定 Rust 中实现。

它不证明：

```text
当前代码适合生产
当前 ValueStore 是最优实现
当前 Clone 策略性能足够
当前宏 API 应成为正式 API
当前错误类型已经完整
当前 trait object 设计最终最佳
当前实现线程安全
当前实现支持 async
当前实现支持持久化
```

不得把 Probe 代码直接升级为生产规范。

---

## 13.6 实现必须遵守的设计规则

以下规则是 SRFlow v2 实现的强制设计约束。

---

### R-01 所有 Executable 必须通过 Runtime 执行

允许：

```text
runtime.execute(child, input)
```

禁止：

```text
child.execute(input)
```

组合型 Executable 不得绕过 Runtime。

---

### R-02 Runtime 不得理解业务控制语义

Runtime 不得包含：

```text
if Retry
if Match
if Each
if Iter
```

这些控制逻辑属于各自 Executable。

---

### R-03 Flow 的 then 顺序不得被重排

```text
then(A)
then(B)
then(C)
```

必须表示：

```text
A → B → C
```

不得根据数据依赖自动并行或重排。

---

### R-04 数据依赖必须显式

不得实现：

```text
上一个 Output 自动成为下一个 Input
```

Input 来源必须由 Ref / Binding 显式表达。

---

### R-05 Ref 必须是 Flow-local

任何 Ref 只能用于所属 Flow。

跨 Flow 数据必须通过：

```text
Executable Input / Output
```

传递。

---

### R-06 内部 Ref 不得突破组合边界

SubFlow、Retry Body、Match branch、Each Body、Iter Body 内部产生的临时 Ref，不得被父级直接引用。

---

### R-07 Node 必须保持叶子职责

Node 不得通过 SRFlow Runtime 编排 child Executable。

如果逻辑本质是：

```text
怎么执行其他步骤
```

应使用 Flow 或控制型 Executable。

---

### R-08 Flow 不得执行新业务计算

Flow 可以：

```text
排列
连接
装配
组合
```

但不能：

```text
判断
计算
选择
生成
修改业务内容
```

这些属于 Node。

---

### R-09 Binding 必须保持结构性

Binding 可以：

```text
读取
投影
组合
构造 Input
```

不得成为：

```text
map
filter
judge
score
merge
rewrite
```

等业务计算系统。

---

### R-10 Binding 不产生 Execution

Binding 解析不进入 Runtime，也不产生独立执行记录。

如果某操作需要被视为一次业务执行，它应该是 Executable。

---

### R-11 Match 不负责业务判断

必须优先：

```text
JudgeNode
↓
Route
↓
Match
```

不得把复杂业务判断隐藏在 Match 内部。

---

### R-12 Match 只执行一个分支

命中 case 后，不执行其他 case。

branch Error 不回退 default。

default 只表示：

```text
没有 case 命中
```

---

### R-13 Retry 每轮使用同一个业务 Input

如果下一轮需要上一轮 Output：

```text
不是 Retry
```

应考虑 Iter 或其他显式数据推进。

---

### R-14 Retry 必须有限

Retry 不允许无限执行。

默认 limit 为 8。

具体业务可以显式设置其他正整数 limit。

---

### R-15 Retry 的技术错误不得自动重试

```text
Error
```

不得自动等价为：

```text
should_retry = true
```

业务重做和技术故障恢复必须分离。

---

### R-16 Each 不传递上一轮 Output

Each 只建立：

```text
Item → Output
```

不建立：

```text
PreviousOutput → NextInput
```

---

### R-17 Iter 必须显式携带状态 T

Iter 的核心关系必须保持：

```text
(T, Item) → T
```

不得通过隐式全局状态模拟迭代推进。

---

### R-18 Error 不等于业务失败

正常业务：

```text
Rejected
NotAccepted
NeedsRevision
```

只要属于合法业务结果，就应通过 Output 表达。

Execution Error 只表示执行本身失败。

---

### R-19 Error 不等于 Retry

除非明确的新机制规定，否则任何 Error 都不自动触发重新执行。

---

### R-20 Error 不等于 Rollback

组合型 Executable Error 不代表此前外部副作用已撤销。

SRFlow v2 不提供通用事务保证。

---

### R-21 不得为了 Rust 实现方便改变语义

如果某个语义难以实现，应：

```text
改实现
```

而不是：

```text
偷偷改设计
```

任何真正的设计修改都必须先修改本文。

---

### R-22 新控制语义优先实现为 Executable

新增：

```text
Parallel
Timeout
Race
...
```

时，应首先验证能否：

```text
implements Executable
```

而不是修改 Runtime / Flow 核心。

---

### R-23 Runtime 必须保持薄

Runtime 可以承载：

```text
统一执行入口
未来技术观察
```

但不得演化成：

```text
工作流解释器
调度 DSL
业务状态机
控制逻辑集合
```

---

### R-24 不得预建未验证抽象

任何新核心概念必须有真实业务需求和独立验证。

不得因为：

```text
以后可能需要
```

而直接加入核心。

---

## 13.7 后续设计变更流程

如果实现或真实业务发现 SRFlow v2 无法自然表达某个需求，不应立即修改代码结构。

必须经过以下过程：

```text
1. 描述真实业务场景
↓
2. 明确现有模型为什么无法表达
↓
3. 检查是否能通过现有 Node / Flow / Executable 组合解决
↓
4. 若确需新执行语义，定义最小新抽象
↓
5. 用真实业务反向验证
↓
6. 编写独立 Compile Probe
↓
7. 评审是否破坏现有不变量
↓
8. 修改本设计文档
↓
9. 再修改正式实现
```

不得采用：

```text
先改代码
↓
能跑
↓
再把文档补成和代码一致
```

的方式演进 SRFlow。

---

## 13.8 SRFlow v2 设计完成条件

当本文正式冻结后，SRFlow v2 的设计阶段视为完成。

后续工作进入：

```text
正式 Rust API 设计
↓
核心实现
↓
单元测试
↓
控制语义测试
↓
真实 SES Flow 适配
↓
性能与所有权优化
```

实现过程中允许调整：

```text
内部结构
类型擦除方式
宏
ValueStore
所有权策略
Error enum
模块划分
```

但不得违反本文规定的核心语义与不变量。

---

## 13.9 最终设计原则

SRFlow v2 可以最终归结为以下几句话：

> **Runtime 负责调用。**

> **Flow 负责编排。**

> **Executable 定义统一执行契约和具体执行语义。**

> **Node 负责业务实现。**

> **Binding 负责结构性数据连接。**

> **Retry 表达重做。**

> **Match 表达路由。**

> **Each 表达逐项处理。**

> **Iter 表达逐项推进。**

> **执行顺序必须显式，数据依赖必须显式，业务意义必须由业务组件产生。**

> **新能力从真实需求中生长，并优先以新的 Executable 扩展，而不是让 Runtime 与 Flow 不断膨胀。**

> **SRFlow 的目标不是成为功能最多的工作流框架，而是长期保持一个简单、稳定、清晰，并能自然承载 SES 执行设计的工程底座。**

---
