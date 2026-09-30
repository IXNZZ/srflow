# T08 编译探针（外部使用者视角）

这些文件**不是 cargo target**：它们位于 `tests/` 的子目录且没有 `main.rs`，`cargo build/test/clippy/fmt`
都不会编译或格式化它们。它们的作用是让 A06 的"成对正反向探针 + 实际诊断"可以独立复核。

每个组件一组：

| 文件 | 期望结果 |
| --- | --- |
| `retry_binding_ok.rs` / `retry_binding_wrong.rs` | 前者编译通过；后者 `E0271: <Ref<String> as Binding>::Output == u32` |
| `match_branch_ok.rs` / `match_branch_wrong.rs` | 前者通过；后者 `E0271: <Branch as Executable>::Output == Vec<String>` |
| `each_collection_ok.rs` / `each_collection_wrong.rs` | 前者通过；后者 `E0308: expected Ref<Vec<String>>, found Ref<Vec<u32>>` |
| `iter_tuple_order_ok.rs` / `iter_tuple_order_wrong.rs` | 前者通过；后者 `E0271: <(Consume<String>, Consume<Vec<String>>) as Binding>::Output == (Vec<String>, String)` |

负向文件里标注了 `[错误]` 的那一行就是诊断应当落到的探针行号（文件内注释也写了期望的 trait bound）。
所有文件都补全了 `use`，因此**不会**因为无关错误（未导入的类型、未使用的 crate）而"误通过"。

复核命令（先在仓库根目录 `cargo build` 一次，再对每个文件执行）：

```sh
R=$PWD
S=$(ls "$R"/target/debug/deps/libsrflow-*.rlib | head -1)
F=$(ls "$R"/target/debug/deps/libfutures-*.rlib | head -1)
rustc --edition 2024 --crate-type lib --emit=metadata \
  --extern srflow="$S" --extern futures="$F" -L "$R/target/debug/deps" \
  tests/compile_probes/<file>.rs -o /tmp/probe.rmeta
```
