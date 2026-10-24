# TEST-CLEANUP-01 独立复审 R5

> 日期：2026-10-24。结论：**R02 清单语义仍需修订，代码整理验收结论保持，不登记 COMPLETED。**
> 本轮未运行 Cargo/UI；没有改动正式代码、测试、作者清单或暂存区。证据目录：`target/test-cleanup-review-R6/`。

## 1. 已确认的修订

- 本轮仅三个清单/交接文件改变。代码、测试、夹具、驱动和消费者继续与已验证版本相同，沿用既有验证结果；R01/R03 保持关闭。
- 非 static Node 正例、T02 外部可见性阶段禁令、witness fault 自检的分类已按上轮意见修正。
- 当前 86 组共 **321 个互异成员**，**226/93/2** 分类完整；K01～K13、B11/B12 及前轮已修正引用保留。
- docs **10,432** 项非构建材料保持不变。

## 2. R02 · P2 · 其余组仍存在错误阶段、反向断言与不相干载体

以下均为当前文件中的实际关系，已打开原夹具或载体断言核对：

| CASE_MAP 位置 | 当前关系 | 实际判据及需修订之处 |
| --- | --- | --- |
| :15 | `v22_t03_from_we_t04_e04_iter_zero_rounds_rejected.rs` → `node__arity_mismatch.rs`，标“编译拒绝” | HEAD 原夹具明确编译成功，运行断言 **RunError::Definition 且 Node 调用为 0**。Node 输入 arity 编译拒绝不能承接零轮预算。应回到零预算的 Definition 载体；该问题在 R2 已指出 |
| :16 | `v22_t02_neg_provide_async.rs` 等 → T02 整文件，标“Definition 拒绝” | 异步 Factory 是 Future 不满足 Data 的**编译拒绝**；现存 `tests/ui/factory__async_rejected.rs` 已检查同一约束。FnOnce/FnMut-only/unit/value 等成员也有各自现存 Factory 夹具，不能统一记为运行期构建错误 |
| :53 | `v22_t05_each_output_missing_aggregate.rs` → `p09_single_output_aggregates_in_index_order`，标“Definition 拒绝” | p09 明确 `outcome.expect("…succeeds")` 并断言 `[2,4,6]`，是聚合成功正例；它不能证明缺失聚合/声明被拒绝 |
| :63 | `h15_item_borrow_blocks_mutation.rs` → `a14_shortening_the_borrow_allows_mutation`，标“Definition 拒绝” | a14 直接操作 DataContainer，结束借用后 `destroy(...).unwrap()`，未构建任何 Definition。这是借用结束后的合法变更正对照，不能替代借用仍存活时的编译拒绝 |
| :4 | 64 个历史成员 → T01 整文件，统一标“Definition 拒绝” | 组内仍含 ScopeId/DataId 类型混用、Ref 私有/无 accessor、同步 Node、运行正例等不同契约。改一个组名仍不能证明这些行为由 T01 覆盖 |

原零预算夹具的头部说明和完整断言已保存为 `BASELINE_zero_rounds.rs`；当前表的具体行见 `COUNTEREXAMPLES.json`。其余证据直接位于相应保留测试文件，无需修改测试重现。

## 3. 最小收口方式

继续只修同一 R02。需要同时匹配 **原用例验证的规则、断言方向、失败阶段**；载体存在或同属“编译拒绝”仍不足以证明等价。

- 将剩余整文件兜底组按真实规则拆开；原接口失效的项注明对应旧接口退出的原因。
- 打开被引用函数的断言，成功场景不能标成拒绝；Core 的 DataContainer/ScopeError 观察不能直接标成 Builder 的 RunError::Definition。
- UI 驱动的 `ok`/exit 0 表示“用例验证通过”；程序内部仍可能在断言 Definition 拒绝。分类以这些断言为准。
- 允许用少量现有编译夹具代表共同类型/借用规则；用例归组后说明共同约束即可，不要求恢复全部旧用例。

代码与测试保持已验收状态；本轮没有新增运行代码问题。后续仅改清单时，继续核表与指纹即可，无需重复全套 Cargo/UI。R02 仍为同一项 P2；未提交、推送或发布。
