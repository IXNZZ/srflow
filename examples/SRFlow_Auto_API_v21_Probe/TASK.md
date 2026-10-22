# 本次 Probe 的授权与设计基线

用户于 2026-10-21 要求：按“继续设计 SRFlow v2.1”讨论后的 Public API 重构设计做一个 Probe，并在本 chat 完整提供方案总结。该直接提供的正文是本次工作依据；引用会话工具的助手正文只有占位符，不作为设计证据。

## 已确定的语义

1. 已实现的 Execution Core、Container、Scope 与控制执行算法保留。本轮仅验证可行性，不实施正式重构，不改写冻结 SPEC。
2. 自动依赖在 Definition 阶段按当前位置、顺序与词法可见性选最近合法 Ref，后续来源不得反向影响已建立依赖；运行期不按类型搜索。
3. `ref_of::<T>()` 复用上述解析规则，只查询定义期身份，不读 Data、不登记 Step、不声明 Export；Each Item、Iter Current 可通过它获得。
4. Node 支持 `then`、`then_output`、`then_with`、`then_with_output`。普通操作返回 `&mut Flow`，显式返回 Ref 的形式保持相同执行行为。
5. Provide 只提供运行期同步 Factory 的验证方向，作为零输入 Node 复用生产与登记机制；不接收编排期 owned value，不隐式复制值。
6. 统一 Body 为 `FnOnce(&mut Flow)`，构建一次，重复执行时复用已有 Body。`output`／`output_with` 每 Body 最多一次、一次声明完整输出；没有声明与显式 `()` 保持可区分。Root unit 输出可以省略声明。
7. Chain／Choose／Each／Retry 使用 output，分别保留 Export／分支选择／Consume 聚合／成功 Attempt Export。Choose 各分支输出 Shape 一致，显式 selector 保留。
8. Iter 只用 `next`／`next_with`，与 initial 同 Shape；Normal 接受 Next，IterBreak 交付进入本轮时 Current，不接受未提交 Next，耗尽继续报原错误；自动导出最终 Current，不添加额外 output。
9. Root 使用 `Workflow<I,O>` 和 `Flow::build`；Root Input 仍是 owned 数据入口，输出声明在构建时与 O 核对；Runtime 保留 owned 整组提取。
10. 原生 tuple 是 Shape，不默认实现 Data。`output::<(A,B)>()` 是两个 Ref 位置，不引入 `Ref<(A,B)>` 的新抽象。

## 验证与交付

逐项验证 P01 统一 Body、P02 自动注入、P03 ref_of、P04 Node/Provide、P05 Factory 重复执行、P06 输出契约、P07 控制结构、P08 Workflow、P09 Rust 类型约束、P10 原语义回归。

交付可行性矩阵、最小可编译示例、与当前代码的冲突清单、按层划分的最小实施建议及真实未解决项。采用现有唯一 Definition、Adapter、RefShape、Scope 机制；禁止第二执行计划、额外数据容器、运行期类型注入、隐式 Clone 或改写控制语义。

本次由单个执行者完成实现与自检，没有独立审查者。结果不授予正式任务验收、Gate 关闭或生产 API 冻结结论。
