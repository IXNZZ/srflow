# Core 快照来源与差异

来源：engineering/srflow，提交 `f9b6e2054b89e6295bc21766313aa8197d2e2ba2`。2026-10-08 从当时 clean tracked 工作区复制。src/core/mod.rs 是 Probe 入口清单，原 cfg(test) 验收文件未列为本轮消费者证据。

16 个普通模块通过逐文件 cmp 验证与原文件完全相同。scope.rs 的唯一差异为两项标注 PROBE EXTENSION 的成组收口方法，完整差异见 [CORE_SCOPE_DELTA.patch](CORE_SCOPE_DELTA.patch)。其余原 Scope 代码未改变。

| 普通快照文件 | SHA-256（原文件 / 副本相同） |
| --- | --- |
| builder.rs | 7384f8cc399b2f32b1e620d4101f904807fedb5ca795262478ab4970ef4e38ed |
| context.rs | 70d6f700c195498a64d0e766d4f3ca95ee4a5830d5217b29e6f236f9e6a98ae5 |
| data_container.rs | 9d15aa491d4df6b6cde788b1a59af89e283c20c21eb6a4cc79687b0b0060b5c7 |
| data_ref.rs | 13fd111d97d3988b0b804d78c40806e0e3321b59c5091770c59887fdaa4bf763 |
| each.rs | 87db0d3ab354e0bc0b87a2863159ddcf65e56c4258709ea198c332d0d753b510 |
| flow.rs | b78254f7546f9d33b11492bb2be7777a0334c5abd27045538fac4ffd233a6dd4 |
| identity.rs | d88c475ca879b87d94f56c7e147ab2ac3fbce5cdd617b097d1a88bf45af61f3c |
| internal_error.rs | 9c71802945d8bef30397e73a472a4db5e03a8c315e97b292977a28a66f2e4123 |
| loop_orchestrator.rs | 89724d6b28308ba0248c86929b3be60b0be7ee14987a52f0391fee1cead0be9a |
| match_orchestrator.rs | ff10108c2426a01f04ff1ab3055883c3a980774a83e346f40617f611b3e7fda8 |
| node.rs | 4162c17e45102a4b270f64ea36e8c2b025479168fb3600604ed6ccd6e5abd7aa |
| orchestrator.rs | 407038f7838376591fe35e64bdf24795289875b8f6d3785cf729ca416494fff0 |
| ref_id.rs | 426201191b93fbdc25130eddafd0a04b4b3e736f096171ad98016fe088cc26a5 |
| root_signature.rs | d78d5f5057f9e722dc43ba7dae9d22128c801d027981168a67b980582b5514c6 |
| runtime.rs | cb48af6022adf7415119f7558893e08f124ccf5abe49f33f64266386ad5c3b11 |
| signature.rs | 4fc7f7349b485b155282a4bc8485d85026b6802e372608a83e99ad013766fb02 |

原 scope.rs：`44c52cd0eeeaaefea11f1d35502f4d74235cb17c86bf861b5a37694858de7d43`。

Probe scope.rs：`f83dd63c3e5c159a369f4e7ecc4515cf33dccd19f8972bd4cdede69c42ae8ce7`。

src/error.rs 为原 ScopeError 提供本 crate 内的 std::error::Error bridge，保留原错误对象；没有改写原诊断枚举或源文件。Root / 控制 / 调用接口是 src/flow.rs 中的新 Probe 适配，不宣称复用了原完整 Invocation 协议。

独立 crate 当前不含指向原工程的源码 path 引用。因此复跑只依赖本项目快照、锁定测试依赖和所声明工具链。对正式工程的迁移、Core 扩展审定和真实 Context / Invocation 整合均未执行。
