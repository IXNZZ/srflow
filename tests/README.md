# tests/ 日常测试说明（TEST-CLEANUP-01 整理后）

## 1. 布局

| 位置 | 内容 | 编译方式 |
| --- | --- | --- |
| `tests/internal/` | Core／Engine 内部用例及其专用辅助实现（48 个文件） | 由原所属模块以 `#[cfg(test)] #[path = "..."] mod X;` 加载，**仍作为库内单元测试**编译，保持原有私有访问边界与观察通道 |
| `tests/internal/*_observe.rs` | 从生产文件迁出的测试观测／故障注入实现（实现主体） | 同上，由对应生产文件的 `#[cfg(test)] pub(crate) mod observe;` 加载 |
| `tests/*.rs` | 顶层集成测试（8 个） | Cargo 自动发现；只经公开 API 验证外部使用者可见契约 |
| `tests/ui/**` | 编译器负例夹具（63 个） | 非 Cargo target；由 `tests/support/ui_check.py` 驱动 |
| `tests/support/` | UI 驱动（`ui_check.py`）、两份清单、独立 Cargo 消费者 | 见下 |
| `tests/maintenance/test_cleanup/` | 保留／删除理由（`CASE_MAP.tsv`）、删除组完整成员（`UI_DELETION_GROUPS.tsv`）、交接（`HANDOVER.md`）、任务书评审 | 文档 |

`src/` 中不再有独立测试文件、内嵌测试主体或成块测试辅助实现；只保留必要的 `#[cfg(test)]` 加载声明与最小观测接点（逐项清单见 `target/test-cleanup-evidence/SRC_HOOKS.tsv`）。

## 2. 日常命令（工程根目录，离线）

```sh
cargo test --offline                     # 库内 524 + 集成 72 + doctest 1 passed / 4 ignored
cargo clippy --offline --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTDOCFLAGS="-D warnings" cargo doc --offline --no-deps

python3 tests/support/ui_check.py              # 62 项：编译负例 + 运行正例 + 独立消费者
python3 tests/support/ui_check.py --self-test  # 13 项判定器自检
python3 tests/support/ui_check.py --witnesses  # 2 项见证（宽松误通过 + 严格拒绝）
```

- 内部用例的加载点：`src/core/engine/mod.rs`（25 个 engine 模块 + `observe`）、`src/core/mod.rs`（`test_support`／`v21_08_tests_cap`／`v21_10_tests_defence`）、以及 `context.rs`／`scope.rs`／`data_container.rs`／`identity.rs`／`ref_id.rs`／`runtime.rs`／`engine/{definition,shape,boundary}.rs` 中的 `#[cfg(test)] #[path]` 声明。
- 普通库构建（`cargo build --offline --lib`）不包含任何内部测试及其辅助实现。
- UI 诊断与消费者 target 输出到 `target/test-cleanup-evidence/`；本目录与脚本**不向 `docs/` 写入任何文件**。
- 生产不变性核对：`python3 target/test-cleanup-evidence/tools/prod_blocks.py compare HEAD <改动的 src 文件…>`；已登记差异与理由见同目录 `PROD_EXCEPTIONS.tsv`（按统一 diff 的 sha256 指纹校验）。

## 3. 关键场景（K01～K13）载体

见 `tests/maintenance/test_cleanup/CASE_MAP.tsv`（逐条含最终路径与承接函数）与 `target/test-cleanup-evidence/K_MAP_FINAL.tsv`（生成自最终文件）。
