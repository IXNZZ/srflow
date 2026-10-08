# SRFlow v2.1 Public API Probe 全过程任务

用户于 2026-10-08 授权本 chat 独立完成整个 Probe：新建工程验证 Public API，成立后形成 SPEC，不成立则根据反例调整或重新设计。范围到 Public API 定案为止，后续正式 Definition / IR 与原工程改造另行考虑。

依据为 `engineering/srflow/docs/SRFlow_v2.1_Public_API_Probe_Baseline_v0.1.md` 的 P01～P16、S01～S10、AP01～AP08，以及同目录三层 Core 规范的数据和生命周期约束。既有 SRFlow 源码保持不变。本项目是一次性验证工程，不是正式实现。

执行范围：

1. AP01～AP05：Root、Query、Node、Ref、Shape、Chain、Fragment 的实际 stable / safe Rust 正反例。
2. AP06～AP08：Each、Choose、Retry、Iter、嵌套信号与真实所有权边界。
3. 独立消费者场景、构建拒绝、编译失败、错误链、Drop 与取消证据。
4. 记录所有设计调整和未覆盖项；不凭类型 PASS 宣称 Core 接入成立。
5. 成立后输出独立 Public API SPEC 与结果报告；不改原工程，不提交、不发布。

Probe 可使用最小 Definition 执行表示和业务替身，不能靠 unsafe、隐式 Clone、按值消费输入、全局 Ref 解析、第二 Container 或错误出口搬出业务状态。

由本 chat 完成设计、执行与复核。全部结论逐项关联可复核证据；项目级自检不伪装成另一个独立审查者的结论。
