# SRFlow Core Compile Probe v0.1

> 状态：**第一轮 Probe 执行基线，P01～P07 已完成**。设计输入为 [SRFlow Core 概念与执行模型 v0.1](SRFlow_Core_Design_v0.1.md)，实测结论见 [第一轮结果](SRFlow_Core_Compile_Probe_Results_v0.1.md)。本文保留原计划的局部验证范围，不定义正式 Rust API；当前内部实现选择及后续整合验收以 [Runtime 实现基线](SRFlow_Core_Runtime_Implementation_Design_v0.1.md) 为准。

## 1. 目的与工作规则

本轮只问：候选 Core 语义能否在 **stable Rust、safe Rust** 下，以可维护的内部结构承载？Probe 成功证明局部可行，不等于完成正式 Runtime 或冻结公开接口。先做最小反例，再增加足以区分“确实成立”与“只是样本太简单”的正反样本。

Probe 期间不随手改动 Core Semantic Baseline。发现失败时记录最小代码、编译诊断或运行轨迹，先判断是当前实现技巧的限制、类型接口表达不足，还是语义模型矛盾；只有确认属于模型问题，才回到 Core 文档单独评审和修订。任何临时绕法须写入结果，不能在代码里暗改所有权或 Scope 规则。

原计划先执行 P01～P07，再形成 Runtime／DataContainer 实现设计与总规范迁移依据。目前七项局部 Probe 已完成，三层设计已审定并按 [v2.1 §25](SRFlow_Design_v2.1.md#25-审定与规范切换记录) 完成规范切换；正式实现仍需整合验证。本文件的 P01～P07 是**本轮 Core Probe 编号**，与此前借用式 Node Probe 的 P01～P04 分开；既有 Probe 只作为 Node 适配可行性的证据。

每项结果记为 `PASS`、`FAIL` 或 `INCONCLUSIVE`：正向样本运行通过、负向样本在预期边界被拒绝，且没有 `unsafe` 或未声明的隐式 `Clone`，才能记为 `PASS`。仅能编译、不验证生命周期或错误路径，记为 `INCONCLUSIVE`。失败报告须附最小反例和失败归因，不能只写“Rust 不支持”。

## 2. 验证顺序总览

| Probe | 首要问题 | 最小通过证据 |
| --- | --- | --- |
| P01 | Node 与 Orchestrator 能否共用 CallSite／Flow Step | 函数 Node、结构体 Node、child Flow 在同一异构 Step 列表中；`then` 输入输出强类型 |
| P02 | 单一 ExecutionContext 下 Scope 能否导入、导出并转移责任 | parent 数据保留，child 输出转移，child 临时数据清理，parent 继续解析输出 |
| P03 | CollectionItem 能否安全借用并受 item 生命周期限制 | 非 `Clone` item 可借用和向 descendant 传递；逃逸被拒绝 |
| P04 | Each collector 能否只消费可转移的 owned 输出 | imported 完整 Data／CollectionItem 不被消费；新产出 Data 可被收集 |
| P05 | Iter promotion 是否保留 imported 初始状态 | parent-owned 初始状态不被 drop；Loop-owned 中间状态可替换清理 |
| P06 | same-DataId state replacement 是否安全 | 新旧 target 同一 DataId 时不重复销毁或改变 owner |
| P07 | Root Output 重复 DataId 能否在 take 前拒绝 | 所有输出先解析与验证；重复时 `take` 次数为零 |

依次推进：P01 先验证执行结构；P02 建立最小 Data Core；P03／P04 在其上验证 Each；P05／P06 验证 Loop；P07 验证 Root 移交边界。每项只补当前必需的支撑代码，不预建完整框架。

## 3. P01 — 统一 CallSite 与异构 Flow Step

**攻击假设：** Node 与 Orchestrator 在运行语义和 trait 上分离，同时能由一个 Builder `then` 接入 CallSite，并存入异构 `Vec<Step>`；调用方仍得到由输入输出 Signature 决定的 `DataRef<T>`。

**最小场景：** 一个 `fn(&A) -> Result<B>`、一个持有配置且借用 `&B` 的结构体 Node，以及一个接收相应输入的 child Flow／Orchestrator，连续接入同一父 Flow。P01 可使用最小符号引用和执行桩，**不实现完整 DataContainer／Scope**。既有借用式 Node Probe 已覆盖函数和结构体 Node 本身，本项只验证它们与 Orchestrator 的共存。

**通过条件：** 同一个 `flow.then(...)` 入口可接收三类对象；调用方不手写内部 Marker 或类型擦除包装；参数类型／数量错接在构建时编译失败；各 Step 可异构保存；CallSite 的内部调度机制能够分别进入 Node 调用路径或 Orchestrator 调用路径，二者不必共享同一个运行时 trait。内部可使用适配层或类型擦除，但业务输入输出关系不因此退化为运行时 `Any` 检查。

**失败判据：** 只有拆成 `then_node`／`then_orchestrator` 才能接线；必须让业务 Node 假装实现 Orchestrator；或正确类型的调用无法推导、错误类型只能执行时才发现。若只因某个包装技巧不成立，先尝试等价的最小适配，不直接判模型失败。

## 4. P02 — SubFlow Scope 与 ownership transfer

**攻击假设：** 一个 Root Execution 只用一个 ExecutionContext 和 DataContainer；child Scope 的 `refs` 绑定与 `owned` 责任可以独立维护，并在 Export 时原子转移。

**最小场景：** parent 拥有 `D1 = A`，`R1 → D1`；SubFlow 将其导入为 `R9 → D1`，自身产生 `D2 = B` 和一份未导出的临时 Data；将 `D2` 导出并绑定 parent 的 `R2`。child finalization 后，parent 仍能读取 `D1` 与 `R2 → D2`，而临时 Data 已销毁。

**通过条件：** 导入不让 child 取得 `D1` 的销毁责任；`D2` 的 parent 绑定与责任转移同时生效；child 退出后不删除 `D1`／`D2`，只清理未导出的 owned Data。用非 `Clone` 数据及 Drop 计数或等价观测证明所有权路径。负向样本：child 拥有 `D2`，但在 Export commit 前令目标有效性或生命周期校验失败（可用仅供 Probe 使用的验证条件）；失败后 parent 不得出现输出绑定，child 仍承担 `D2` 的生命周期责任，不留下“引用已绑定、owner 未转移”的半成品。

**失败判据：** child 退出误删 parent Data、导出后误删 `D2`、临时 Data 泄漏、或 parent 只能靠复制值而非目标／责任转移继续执行。若无法在 safe Rust 下保持这些不变量，记录最小卡点。

## 5. P03 — CollectionItem 借用与生命周期

**攻击假设：** `Vec<T>` 整体由 DataContainer 持有，item 可作为受限 `CollectionItem` 目标借用为 `&T`，不为每个 item 建立独立 DataId。

**最小场景：** `D1 = Vec<T>`，其中 `T` 不实现 `Clone`；ItemScope 绑定 `R_item → CollectionItem(D1, index)`。一个 Node 借用 `&T` 产出新 Data；descendant Scope 合法导入同一 item 目标。再尝试将 item 目标导出到超出来源有效期的 parent。

**通过条件：** 正向路径不 `Clone`、不 move 原 item、不使用 `unsafe`；descendant 在 ItemScope 存活期间可借用；item 逃逸在编译期或明确的运行时生命周期检查处被拒绝。若采用运行时检查，越界后的 `CollectionItem` target 必须无法再次成功 resolve，且不得形成超出来源生命周期的 Rust borrow。

**失败判据：** 只能复制／移出 `T` 才能让 child 读取，或 item 目标能越过来源生命周期进入 parent 并继续被解析。若不能用类型系统静态拒绝，但能安全地运行时拒绝，应记录验证位置和代价，不直接当作失败。

## 6. P04 — Each collector 的可消费输出

**攻击假设：** collector 只消费当前 item 调用链中新产生、且生命周期责任可合法转给 EachScope 的 owned Data；仅重新暴露 imported Data 的 body Output 不可被移入 `Vec<O>`。

**最小场景：** parent 拥有 `D5 = Rules`，Each 把它作为 shared input 导入。负向 body 直接返回 `Data(D5)`；另一负向 body 返回 `CollectionItem`。正向 body 由 Node 执行 `&Rules → new Rules`，产生 item-owned `D20`，再收集为 `Vec<Rules>`。

**通过条件：** 两个负向输出均在任何 `take(D5)`／移走原 item 之前被拒绝，parent 的 `D5` 与后续 item 仍有效；正向 `D20` 可经责任交接被 collector 消费，旧 DataId 失效，最终 `Vec<Rules>` 成为普通 Data。全过程不隐式 `Clone`。

**失败判据：** collector 只检查 target 是不是 `CollectionItem`，却允许 `Data(D5)` 被消费；或合法的新产出也只能靠复制才能收集。错误路径若已经移走 parent Data 才报告失败，也算失败。

## 7. P05 — Iter promotion 与 imported 初始状态

**攻击假设：** 当前 state 位置的变化不等于 ownership responsibility 的转移；Loop 不能处置 parent-owned imported state。

**最小场景：** parent 拥有 `D1 = State0`，Loop 导入它作为初始 state。Round 1 产生 Loop-owned `D2 = State1` 并提升为当前 state；Round 2 产生 `D3 = State2` 再提升。使用可观测的 Drop 计数区分三个实例。

**通过条件：** 替换后 `D1` 仍归 parent，Loop 完成后 parent 可继续借用；`D2` 不再需要时由 Loop 的责任边界清理；`D3` 在成为最终输出时合法导出。提升只更新 state target，不使 imported Data 自动成为 Loop-owned。

**失败判据：** Loop 把 `D1` 当旧 state 销毁，或把 `D2` 留为无 owner 的数据，或导出 `D3` 后仍由 Loop 退出清理。

## 8. P06 — same-DataId state replacement

**攻击假设：** Iter 一轮返回的 next-state target 可以与 old-state target 指向同一 DataId；此时“替换”不得执行销毁旧值再使用新值。

**最小场景：** 分别以 imported state 和 Loop-owned state 为当前目标，让一轮 body 原样返回该目标；随后继续下一轮或完成并导出。

**通过条件：** 两种来源均保持 DataId 有效，生命周期责任不重复增加、不被错误转移；后续轮次或最终输出仍能借用目标。没有重复 Drop、悬空 RefTarget 或第二个 owner。

**失败判据：** state 位置更新导致同一 DataId 被销毁、重复拥有，或只在 imported／Loop-owned 其中一种情形下成立。

## 9. P07 — Root Output 提取前的别名校验

**攻击假设：** Root 只有在全部输出目标解析并验证后才开始 owned value 提取；两个输出指向同一 DataId 必须整体拒绝。

**最小场景：** 两个输出 RefId 均解析到 `D10`，以及一个相同 RefId 被重复指定的变体；对照样本为两个不同 DataId 的正常 tuple 输出。

**通过条件：** 重复情形在任何 `take` 前拒绝，内部观测到提取次数为零，也没有部分正常输出返回；两个不同 DataId 的对照样本均成功移交给 Application，不隐式 `Clone`。

**失败判据：** 先提取第一个值，第二个才发现重复；或为了让 `(a, a)` 成立而复制数据；或仅检查 RefId 是否相同，漏掉不同 RefId 指向同一 DataId 的情况。

## 10. 本轮排除项与结果记录

本轮不验证 parallel Each、Bundle、通用 Collection、`Arc<dyn Node>`、完整 async Future `Send`、derive macro、漂亮的公开 API、性能、复杂错误类型、tracing 或发布包装。P01 可以使用内部适配和最小执行桩；P02 之后只扩展当前 Probe 必需的 Data Core。所有样本保持离线，不依赖正式业务数据或外部服务。

每项结果至少写明：所用 stable Rust 版本、最小正向／负向样本、实际编译或运行结果、是否使用 `unsafe`／`Clone`、与通过条件的差距、失败归因及是否需要修改 Core Semantic Baseline。若七项都在所限定范围内成立，再进入 Runtime／DataContainer 实现设计；若某项不成立，先分析最小反例，只修订受影响的 Core 规则，不顺手扩展其余设计。
