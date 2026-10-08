# SRFlow v2.1 Public API Probe 结果

日期：2026-10-08。结论：**PASS WITH DOCUMENTED API REVISIONS**。

已在独立工程证明调整后的 Public 使用模型可在 stable、safe Rust 中编译并执行。原基线不是逐字 PASS：Struct Node 原 Query 签名被实际编译反例拒绝；Data 叶子声明与 native tuple 范围也需要明确。最终契约为 [Public API SPEC v0.1](../../srflow/docs/SRFlow_v2.1_Public_API_SPEC_v0.1.md)。

用户授权本 chat 完成整个 Probe、根据反例调整设计并形成 SPEC。结果是本 chat 的执行与复核结论，不伪装成用户指定的另一个独立审查者已经审定。

2026-10-08 用户确认其余 SPEC 内容，并增补 `#[derive(Data)]` 与零输入 Root。本报告包含增补后的复验结果；最初 63 项测试、零输入 Root 被排除的结论属于增补前状态。

## 1. 工作基线与隔离

- 工程：engineering/v3/SRFlow_Public_API_v21_Probe，独立 Cargo workspace，publish=false。
- 原 srflow 提交：f9b6e2054b89e6295bc21766313aa8197d2e2ba2。
- 实际工具链：rustc 1.97.1 (8bab26f4f 2026-07-14)，cargo 1.97.1 (c980f4866 2026-06-30)。
- Rust edition 2024，最低声明 rust-version=1.97；更低版本未验证。
- 执行核心与派生 crate 均 #[forbid(unsafe_code)]；执行核心不依赖第三方库，Public 包依赖本地 data-derive proc-macro crate（syn 2.0.119、quote 1.0.47、proc-macro2 1.0.107）；测试 / 示例使用已缓存 futures 0.3.34。
- Core 为独立快照；原 srflow tracked 源码和 .gitignore 未改动，结束时 git status / diff 为空。
- 原设计基线转为历史输入并指向本报告 / SPEC；没有改写原 Core 规范或历史任务 / Gate。

## 2. 验证总览

增补后 Cargo 测试共 **71 项通过**，其中 70 项为实际行为 / 构建拒绝测试，1 项为验证 18 个编译拒绝样本的 harness。原 zero_root 拒绝样本由正向执行测试替代，新增 derive_borrow_escape 拒绝样本，故 UI 总数仍为 18。样本中包含两个原协议设计反例，不把它们当成新接口成功例。

| 检查 | 结果 | 范围 |
| --- | --- | --- |
| cargo test --offline | PASS | 全部独立消费者测试，含 UI harness |
| cargo clippy --offline --workspace --tests --examples -- -D warnings | PASS | Probe、Data 派生 crate、消费者与最小用法；16 输入证明保留明确的局部 type_complexity 例外 |
| cargo fmt --all -- --check | PASS | 新 Probe / 测试 / 示例；Core 快照有意不由 formatter 重写 |
| cargo run --offline --example spec_minimal | PASS | SPEC 最小例得到 Amount(43) |
| 原 srflow git status / diff | CLEAN | 原工程 tracked 文件未改动 |

Box 业务叶子用例保留一处明确的 borrowed_box lint 例外，因为测试必须验证 &Box<tuple> 作为单 Data，而不是改成另一种输入 Shape。例外不掩盖 unsafe、借用错误或类型错接。

增补复验同时将 examples/minimal.rs 中未使用的 Root 输入绑定改为 `_rules`，以通过全部示例的 warnings 检查；执行行为不变。

## 3. AP01～AP08

| 组 | 结论 | 主要证据 |
| --- | --- | --- |
| AP01 | PASS | query_signature 的自然 Root / Output；zero_root 的零输入 Root；shapes / large_shapes 的 1～16 位置；composition 的无关 Definition 拒绝、节点前构建拒绝 |
| AP02 | PASS AFTER REVISION | 函数、函数引用、struct、Arc、零输入 / unit；lifecycle 的真实挂起借用与取消；UI 原 GAT / 新协议拒绝样本 |
| AP03 | PASS IN DECLARED-LEAF PROFILE | Data 派生 struct / enum / 泛型、字段不要求 Data / Clone / Copy / Send / Sync；Copy Ref 对非 Clone / Copy 的 Tracked；同类型独立位置；flat Shape；重复 owned alias 拒绝；Box tuple 单叶区分 |
| AP04 | PASS | 多层 ancestor capture；Chain 中间值先清理；child / sibling / foreign Ref 拒绝；item cap 内 descendant 传递；非法 item 收集拒绝 |
| AP05 | PASS | 普通 closure factory 与 context 函数均可复用，Node 依赖可借用，Root 之间可重用定义逻辑 |
| AP06 | PASS WITH CORE EXTENSION | Each 单 / 多 / unit / empty、顺序、错误中止、非法消费、成组预检；Choose 单路径 / otherwise / 未命中 / 重复注册 / 分支失败 |
| AP07 | PASS WITH CORE EXTENSION | Retry N+1 / N=0 / 最后动态原因与 source；Iter current Break / limit / N=0 拒绝；单 / tuple state、同目标、交换与 16 状态位置 |
| AP08 | PASS IN MINIMAL CORE BRIDGE | 最近对应捕获、异类穿越、同类嵌套、耗尽终止、Each 深层 Break；S08 / S09 / S10、真实 Drop 与 Future 取消 |

以上结论针对 SPEC 的实际支持范围，不证明任意 Any、dyn Node、自动并发、nested Shape 或正式原 Invocation 适配已完成。

## 4. 必要的设计收敛

### D01 — Struct Node 的 Query 类型

原候选把 trait 的 GAT Borrowed 生命周期与 impl 的具体 tuple-of-references 签名直接对应。省略 lifetime 和简单加一个共同 lifetime 的两种候选均被 E0195 拒绝，见 tests/ui/original_gat_elision.rs 与 original_gat_explicit.rs。

这些反例证明的是候选不能直接匹配，不是“Rust 永远无法表达这种业务协议”。最终采用：

```rust
impl Node for MyNode {
    type Input = (A, B);
    type Output = C;
    async fn run(&self, query: Query<&Self::Input>) -> Result<C, BodyError> {
        let (a, b) = query.get();
        // a: &A，b: &B。
        todo!()
    }
}
```

具体 Query<&(A, B)> 和单输入 Query<&A> 同样通过。函数 Node 保留 Query<(&A, &B)>。get 返回相同业务借用；不生成 owned 输入 tuple，不要求 invocation lifetime 注解。

### D02 — Data 与 unit / tuple

为使普通 then 按输出自然得到 Ref<T> 或 `()`，本版明确用 Data marker 区分非 unit 叶子。业务 struct / enum 只声明一次，不增加字段，不实现控制 trait，不随 arity 分类。

用户增补后可使用 `#[derive(Data)]`，手写空 impl 仍合法。派生仅生成符合完整类型 'static 约束的 marker 实现，不附加字段 Data 或 Clone / Copy / Send / Sync 约束。已验证普通 struct、enum、泛型 where、const 泛型和静态借用；短生命周期借用仍被 E0597 拒绝。最小示例已改用派生。

这是取舍而非零成本：未注册外部类型不能一律直接接入；default native tuple 作为连接 Shape，单组合叶子需要业务 struct 或已注册 Box。tuple_leaf_output 被编译拒绝，Box tuple 的完整产生、借用与 Root 输出已运行。若今后要求任意 Any / native tuple 直接接入，应重新评审映射方案，不把本版声明为已经做到。

### D03 — Choose 错误边界

值比较使用 PartialEq。重复 case、重复 otherwise、空配置是 Definition 拒绝；未命中且无 otherwise 为独立 RunError::NoMatchingCase。selected branch Failure 不换 fallback。共同 OutputShape 的错接在编译期拒绝。

### D04 — 当前单线程与 flat 位置范围

不附加 Send / Sync，支持非 Send Data / Future、&Node 和 Arc<具体 Node>。flat 输入、编排输出、StateShape 为 1～16 位置，不出现 arity 能力类。零输入 Node / 无输出已验证；用户增补后零输入 Root 同样进入冻结范围。unit state、nested Shape 不进入冻结范围。

零输入 Root 的 InputSpec / RootInput / RefShape 均以 `()` 表示零个位置，没有 DataId 或 Ref<()> 占位。五项新增执行测试覆盖用户原示例、struct source + Chain + 多输出、空流程与 unit 输出及未用值销毁、Retry / 未捕获控制、Failure 后清理与停止后续步骤。无需改动 Core 快照或 execute 执行算法。

### D05 — 原开放项

- Flow 顺序遵守已有 Core 规范；S10 的 Stop / Choose 无直接相互依赖，仍严格按定义先后执行，Break 后 Choose 不运行。
- Control adapter 与特定边界的依赖保留。diagnose / choose_revision 是业务 Node，stop_if_acceptable 明确使用 IterBreak；不宣称完全消除耦合。
- Fragment factory 首选成立；context 函数备选也成立。必要的定义期 Node lifetime 属普通 Rust 依赖寿命。
- 并发、缓存 / 预编译未加入；不需它们完成本轮 API 验证。

## 5. Core 接入证据与限制

实际 Data 只在快照的 DataContainer 中存放，ScopeCoordinator 执行 Import / Export、item cap、owned 责任、Root 整组预检、Collector 和 State 操作。没有备用 Any 仓库、隐式业务 Clone 或父子间 take / 重新传值。

原 Core 单 Consume / Promote 会关闭来源 Scope，不能对同一 Item / Round 重复调用来得到多输出 / 多状态。独立 scope.rs 副本只增加两项标注扩展：

| 扩展 | 预检与提交 | 覆盖 |
| --- | --- | --- |
| consume_item_group_probe | 验证全部 owned 目标和 collector、拒绝重复 DataId；随后 Container 内部移动全部输出，关闭 Item 一次 | 非法 ancestor 输出、重复 alias、两与十六输出、Drop 一次性、部分结果清理 |
| promote_group_probe | 验证全部 state / target；更新状态与责任，不重绑父 Ref；关闭 Round 一次，再按原机制回收 pending | 二与十六状态、交换 imported 目标、共享一个 next target、Root alias 拒绝 |

它们证明目标外部语义与底座所有权机制兼容，不是原工程已具备这两条正式调用路径的证明。

Probe 的 Run 是最小执行上下文，使用 ScopeCoordinator；没有把原完整 ExecutionContext / Invocation / CallSite 重构成新门面。旧 BodyError / 终止状态不能直接当作新 Control 通道。正式适配必须保持调用权限，并区分“已捕获控制退出”与“全局终止失败”；这属于后续内部设计与实现整合。

本轮取消证据是实际挂起后 drop Root Future，所有 Container 数据销毁一次。没有证明并发 child join、业务 panic、任意故障注入后的双诊断、长期身份耗尽或生产性能。原 Core cfg(test) 样本没有被计入本轮 PASS。

## 6. 场景映射

| 场景 | 消费者证据 |
| --- | --- |
| S01 | query_signature.rs、shapes.rs、spec_minimal.rs |
| S02 | query_signature.rs、shapes.rs、UI、lifecycle 挂起借用 |
| S03 | composition 的 factory / context Fragment、多层 Chain 与 Drop |
| S04 | composition 的 Each；lifecycle 的成组消费与失败清理；large_shapes |
| S05 | composition / control 的 Choose；UI Shape 错配 |
| S06 | control 的 Retry 各分支；lifecycle 的完整 attempt 清理 |
| S07 | control 的 Iter；shapes 的 tuple；large_shapes；lifecycle 的旧状态回收 |
| S08 | ses 的 s08_complete_nested_six_operations，真实六操作完整组合 |
| S09 | control 的传播矩阵；lifecycle 的第二轮 Retry、已产生 next 后 Break、取消 |
| S10 | ses 的 A / B 共同 Node、basis 一次、Stop 优先、所有策略与最后一轮无额外 Judge |

SES 替身只产生可观察的简单结果，未访问真实模型；API / 生命周期通过不授予正文质量实验结论。

## 7. 证据复核与结束边界

编译拒绝样本（完整可复跑源码位于 tests/ui，stderr 由 harness 保存至 target/ui）：

| 样本 | 实际拒绝类别 |
| --- | --- |
| original_gat_elision / original_gat_explicit | E0195：原候选 trait / impl lifetime 不匹配 |
| elided_struct_multi | E0053：旧 tuple-of-references 不是修订后的 Struct 输入标记 |
| wrong_input / wrong_arity | E0271：函数输入与接线 Shape 不一致 |
| by_value_node | E0277：不是借用 Query 的 Callable 协议 |
| choose_shape_mismatch / iteration_state_mismatch | E0308：分支 / next state 的类型不一致 |
| borrow_escape | E0277：借用返回不符合 owned Data Output |
| mutable_input | E0277：未提供可变 Query 输入 |
| private_core | E0603：Core 模块不可达 |
| ref_cannot_read | E0599：Ref 无数据读取方法 |
| derive_borrow_escape | E0597：Data 派生不允许短生命周期业务借用逃逸 |
| dyn_node | E0038：Node 不承诺 dyn compatibility |
| synchronous_node | E0277：同步 Result 函数不是异步 Node |
| definition_owned_value | E0277：Definition 返回的是 RefShape，不是任意 owned Data |
| tuple_leaf_output | E0277：native tuple 不属于默认 Data 叶子 |
| short_lived_node | E0597：定义 closure 内局部 Node 的借用不能留待后续执行 |

执行命令、消费者样本、拒绝类别、数据销毁和最小示例均可复跑。源码禁止 unsafe；原 srflow git status / diff 为空，Core 来源与副本差异见 CORE_PROVENANCE.md。

当前交付为独立 Probe、上述结果和正式 Public API SPEC。API 的可行性在明确修订及支持范围内成立；原设计反例仍保留。未开始正式 Definition / IR、源码迁移、旧规范批量迁移、发布或后续任务。
