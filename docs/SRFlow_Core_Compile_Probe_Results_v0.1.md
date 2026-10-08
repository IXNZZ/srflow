# SRFlow Core Compile Probe v0.1：第一轮结论

> 状态：**P01～P07 局部执行结论，保留原实测范围**。本文件汇总 [Probe 执行基线](SRFlow_Core_Compile_Probe_v0.1.md) 的实测结果；[Core Semantic Baseline](SRFlow_Core_Design_v0.1.md) 已审定。此处的 `PASS` 只针对各项 Probe 的最小场景，不等于正式 Runtime 或公开 API 已完成。

## 1. 总体结论

P01～P07 **全部在各自限定范围内 PASS**。这些样本表明，当前七处高风险 Core 语义能够在 stable、safe Rust 下用最小模型表达和执行；本轮未发现必须修改 Core Semantic Baseline 的语义矛盾。

这一结论有明确边界：P01 的强类型 Builder／CallSite 与 P02～P07 的 Data Core 是两套尚未整合的 Probe 模型。后六项主要由测试手动驱动 Scope 与生命周期操作。七项通过，证明了各局部不变量的可行性，**尚未证明它们在同一条真实 Flow／Runtime 调用路径中共同成立**。

## 2. 逐项结果

| 项目 | 结论与关键证据 | 详细记录 |
| --- | --- | --- |
| P01：Node／Orchestrator 与异构 Step | 同一个 `then` 接入普通函数 Node、`Arc<具体结构体 Node>` 和子 Flow；异构 Step 分别进入 Node／Orchestrator 调用路径。四个类型或参数数量错接在 `then` 处以 E0308 编译失败。函数适配需要区分一参数和二参数 Marker 的 tuple 形状，这是实现层的 trait coherence 取舍。 | [P01 结果](../../v3/SRF_Core_Compile_Probe/P01_RESULTS.md) |
| P02：SubFlow Scope import／export | 单一 ExecutionContext 中，child 借用 parent 的 D1 而不取得销毁责任；D2 导出时引用绑定和责任一起转移；child 临时 Data 被清理。提交前验证失败时，parent 未绑定输出，child 仍拥有 D2。 | [P02 结果](../../v3/SRF_Core_Compile_Probe/P02_RESULTS.md) |
| P03：CollectionItem 生命周期 | 非 `Clone` 的 `Vec<T>` item 可在 ItemScope 及 descendant 中临时借用，不产生逐 item DataId；目标不能导出到 cap 之外，Scope 关闭后保留的目标也无法再次 resolve。真实 Rust 借用越过 finalization 的负例以 E0502 编译失败。 | [P03 结果](../../v3/SRF_Core_Compile_Probe/P03_RESULTS.md) |
| P04：Each collector 所有权 | 直接重新输出 imported `Data(D5)`、输出 `CollectionItem`，以及仅由 EachScope 拥有却缺少 item 来源的 Data，均不能被 collector 消费；拒绝发生在取值前。item Node 显式产生的新 owned D20 可转移给 EachScope 并收入 `Vec<Rules>`，无需隐式复制。 | [P04 结果](../../v3/SRF_Core_Compile_Probe/P04_RESULTS.md) |
| P05：Iter state promotion | parent-owned 的 imported D1 被新状态替换后仍归 parent；Loop-owned 的中间 D2 在被 D3 替换且无有效引用后清理；最终 D3 导出给 parent 后不被 Loop finalization 删除。 | [P05 结果](../../v3/SRF_Core_Compile_Probe/P05_RESULTS.md) |
| P06：same-DataId 替换 | imported state 和 Loop-owned state 原样返回为下一轮状态时，promotion 都不会销毁 Data、制造第二个 owner 或使后续轮次借用失效；最终导出仍有效。 | [P06 结果](../../v3/SRF_Core_Compile_Probe/P06_RESULTS.md) |
| P07：Root 重复提取 | 同一 RefId 重复出现，以及不同 RefId 指向同一 DataId，均在任何 `take` 前整体拒绝，提取计数为零；两个不同 DataId 的对照样本可一起移交给 Application。 | [P07 结果](../../v3/SRF_Core_Compile_Probe/P07_RESULTS.md) |

## 3. 本轮确认的 Core 边界

1. **逻辑引用与物理数据身份分离。** `DataRef`／RefId 可保持编排强类型；运行时的所有权冲突必须按 DataId 判断，不能只比较 RefId。P01 与 P07 分别覆盖了这两端。
2. **当前引用位置不决定生命周期责任。** import 不转移 owner；Export 在验证成功后才同时完成绑定与责任交接。Iter 的旧 state 若属于 parent，替换当前状态也不会改变其 owner。
3. **借用目标与 owned output 分离。** `CollectionItem` 可以在有效期内被解析为临时 `&T`，但不能作为 owned item 收集；完整 `Data(DataId)` 若只是 imported alias，同样不能被 collector 移走。可收集的是 item 调用链中新产生且责任可转移的 owned Data。
4. **销毁与移交前先检查身份及有效期。** same-DataId promotion 避免误删；Root 多输出先验证再提取；负向路径没有先移动业务 Data 再报告错误。

这些是 Probe 对现有设计规则的局部验证，**不是新增 Core 规则**。

## 4. 执行证据与范围

Probe 工程位于 [SRF_Core_Compile_Probe](../../v3/SRF_Core_Compile_Probe/)。验证环境为 `rustc 1.97.1`、Rust 2024 edition、离线构建、无外部依赖。全轮 14 个运行样本通过；`cargo test --offline`、`cargo build --offline`、`cargo clippy --offline --all-targets -- -D warnings` 与 `cargo fmt --all -- --check` 均通过。五个独立的编译期负向样本按预期失败：P01 四个 E0308，P03 一个 E0502。

Probe 代码没有使用 `unsafe`，业务 Data 不依赖隐式 `Clone`。用于 Drop 观测的 `Arc` 句柄复制不复制业务 Data。各项的最小场景、观测值及单项限制以表中的详细记录为准。

## 5. 未验证事项与下一步

当前最重要的未验证事项是**端到端整合**：把 P01 的 typed CallSite／Flow Step 接到 P02～P07 的 ExecutionContext、DataContainer、Scope、collector、Loop promotion 和 Root extraction，并在实际内部调用路径上重跑相同的不变量。现有结论不能推断整合后自动通过。

本轮也没有验证完整异步 Future `Send`、取消与错误退出清理、parallel Each、通用 Collection、任意数量的 Root 输出、生产级错误与 tracing、性能或正式公开 API。P07 只覆盖两个 Root 输出的最小提取边界；P02 的原子性仅覆盖顺序执行时的提交前验证失败。

本轮结论支持形成 Runtime／DataContainer 实现设计。后续已审定的 [Runtime 实现基线](SRFlow_Core_Runtime_Implementation_Design_v0.1.md) 选择 ItemScope 直接 Consume、Loop controller state 无 Ref-binding Promote；原 P04 的 Export／collectible 路径和 P05／P06 的 current_ref 重绑定不能直接充当这两项实现选择的通过证据，须在实际调用链中复验。

三层设计已按 [v2.1 §25](SRFlow_Design_v2.1.md#25-审定与规范切换记录) 完成规范切换，当前实现阶段见 [任务入口](tasks/README.md)。若整合时出现反例，应保留最小失败样本，先区分实现技巧与语义问题，再决定是否单独修订 Core 设计。
