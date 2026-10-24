# TEST-CLEANUP-01 独立复审 R4

> 日期：2026-10-24。结论：**R02 仍未通过；问题限于清单的验证意图与载体不对应。代码整理验收结论保持，不登记 COMPLETED。**
> 本轮核表并比对指纹，没有运行 Cargo/UI，没有修改正式代码、测试、作者清单或暂存区。证据在 `target/test-cleanup-review-R5/`。

## 1. 已接受的修订

- 本轮仅 CASE_MAP、UI_DELETION_GROUPS 和交接摘要发生变化。Rust 源码、测试、UI 驱动/夹具及消费者继续与已独立验证的版本一致，沿用原检查结果，R01/R03 保持关闭。
- 分组表现为 202 组，321 个成员完整且互不重复，226/93/2 分类保持；载体文件均存在。
- 上轮指出的消费者路径、自动/显式 Query 规则和 p35 函数名已修正，K01～K13 及 B11/B12 等关键回归去向保留。
- docs 的 10,432 项非构建材料继续保持不变。

这些部分无需继续返工。存在性校验通过，但不能据此确认删除项与载体有同一验证意图。

## 2. R02 · P2 · “无错误码”条目仍被统称为 Definition 拒绝

位置：`CASE_MAP.tsv:4`～`:8`、`:14`～`:20`；UI_DELETION_GROUPS 的对应 `contract=-/-` 分组。

目前 67 个历史项、12 个原活跃项等被混入整文件级载体 `tests_v22_t01.rs`，理由为“运行期 Definition 拒绝”。但原成员既有编译负例，也有正常运行正例和旧阶段可见性禁令，不能共享这条理由。原始错误码/符号缺失时，需要读取原用例意图，不能把空值 `-/-` 当作同一契约。

以下是已对照 HEAD 原文件确认的反例与可用修正方向：

| 当前对应 | 独立核对 | 最小修正方向 |
| --- | --- | --- |
| `v22_t04_from_we_t02_nonstatic_node_borrow.rs` → T01 整文件、“Definition 拒绝” | 原文件是**编译运行成功**的短借用 Node 正例，断言结果 `7-suffix`；头部明确要求 exit 0 | 指向 `tests/internal/engine/tests_runtime.rs::b03_fragment_forms_execute_with_non_static_nodes` 等真正的非 static Node 正例，保留“允许使用”的判据 |
| `e07_function_wrong_input_type.rs` 等旧类型/arity 负例 → T01 整文件、“Definition 拒绝” | 原文件检查调用点的编译期类型约束；旧 `core::builder` 接口已退出。不能由任意 Definition 拒绝取代编译约束 | 区分旧协议本身退出与仍须保留的 Node 输入类型规则；后者已有 `tests/ui/node__wrong_input_type.rs`、`node__arity_mismatch.rs` |
| a14/b19/c23 的借用负例 → `context_tests.rs::d15_cancel_while_input_borrow_is_pending`、“Definition 拒绝” | 编译器拒绝借用冲突、Future 进入 Pending 后取消、Definition 构建失败是三种不同观察点 | 说明由现有编译借用夹具代表哪一共同 Rust 借用规则；运行期取消用例归取消规则，不给它附加编译期拒绝证明 |
| T02 外部 provide/then 可见性负例 → T02 整文件、“运行期 Definition 拒绝” | T02 的“内部接入、尚不公开”是阶段约束，后续 T03 已公开对应门面 | 对已失效的阶段禁令明确登记阶段结束；现行 API 可用性由当前公开入口正例承接，不能声称新 API 应在 Definition 拒绝 |
| 两个 witness fault 控制文件 → T01 整文件、“Definition 拒绝” | 两个文件用于检验 UI 判定器是否误接收装配错误/意外成功，未涉及 Builder 规则 | 指向当前 UI 判定器自检/见证及其相应判定，不指向 Definition 测试 |

同类问题还可见于正例 `p12_empty_collection_finishes_empty_vecs_without_items`、`b01_spec_minimal_example_returns_43` 等被标为“运行期 Definition 拒绝”；这些函数确实存在，但其成功断言与该理由相反。`we-t05-positive-ok`、`we-t06-five-signatures` 是成功 marker，也不应被拼成拒绝原因。

## 3. 收口要求

继续修订同一 R02，只调整两个清单及摘要：

1. 将每组明确标为**编译拒绝、Definition 拒绝、执行成功、执行期失败/清理、判定器自检、阶段契约退出**中的实际行为，保留其关键判据；不需要增加表列。
2. 拆开上述混合组，指向确实检查该行为的函数或编译夹具。旧 API 已退出的成员可以按具体旧协议归组，不要求恢复历史夹具。
3. 文件/函数存在性校验继续保留，同时人工核对载体内的断言方向和失败阶段。若原测试的断言已被选定代表取代，说明共同约束即可，不必为每个重复断言另写记录。

现有代码与测试不需要重做；也不因清单修订重跑全套 Cargo/UI。清单中正确的具体映射可以保留，重点处理缺元数据的混合组及复制的统一理由。分组数不是验收目标，允许用更少且准确的契约组替换当前自动分组。

本轮原始反例分别保存为 `BASELINE_e07_function_wrong_input_type.rs`、`BASELINE_v22_t04_from_we_t02_nonstatic_node_borrow.rs`，与其当前分组的对应见 `SEMANTIC_COUNTEREXAMPLES.json`；结构/成员/docs 结果见 `AUDIT.json`。R02 保持一项 P2，未新增运行代码问题；未提交、推送或发布。
