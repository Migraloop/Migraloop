# Operations

Operator 在生产环境运行 Deployments 时应预期的 day-2 行为。若干控制项已在 ADRs 与领域 glossary 中规范；把本章当成运维契约，并在实现到位时以 `migraloop status` / logs 核对。

## Schema Change Handling

Source DDL 会按每条 Pipeline 的依赖分类（ADR-0009）：

| 影响 | 平台行为 |
| --- | --- |
| 不影响该 Pipeline | 继续处理；schema 可稍后追上 |
| 影响 Pipeline 但 apply 仍安全 | 继续处理 |
| 阻挡安全 apply（重试无法前进） | **警告并 pause** 受影响的 Pipeline(s) |

此 pause 规则用于 **stream-wide blockers**，不是单行 poison data。

## Poison Change Handling

当单个 change 或 Output Identity 反复失败，但流其余部分仍可继续时（ADR-0015）：

1. 有界重试
2. **Quarantine** 该 change/identity
3. **Alert** Operators
4. **让 Pipeline 继续跑**

被 quarantine 的 keys 在修复或重试前保持 unhealthy / not aligned—绝不默默跳过。不要预期单行坏数据就 pause 整条 Pipeline。

## Backpressure

当 Platform Store apply、Derived maintenance 或 Target Delivery 跟不上时（ADR-0020）：

- 各阶段使用 **bounded queues** 并放慢 capture/apply
- Lag 仍会显示在 Sync Health / Delivery Health（以及暴露时的 metrics）
- 拒绝无界内存缓冲 / 把 OOM 当 backpressure
- 只因 Target 慢就 pause 整条 Pipeline **不是**默认行为

Operator 依可见 lag 行动（扩容 Target、降低负载、检查 Delivery 错误）—pause 留给真正的 blocker。

## Platform Store Guardrails

随附的 PostgreSQL Platform Store 带有安全默认与产品强制的下限（ADR-0010）。跨越安全门槛（例如可用磁盘）时必须 **只警告**—平台不会只因资源压力就自动 pause。Postgres 备份仍是 Operator 的责任。

## Upgrades

升级必须 **backward compatible**（ADR-0014）：

- Platform Store schema 变更以启动时应用的版本化 migrations 出货（`migraloop run` / `migraloop migrate`）
- 较新的 app 必须能继续既有 Deployments 与可接受的旧配置，而不是 wipe-and-rebuild
- 单 instance 升级期间允许短暂 sync pause；不得丢失 checkpoint/数据
- v1 不要求支持 downgrade

建议升级循环：

1. `migraloop status` — 记下 checkpoints 与健康
2. 滚动新的 app image / binary
3. 确认 migrations（`status` 中的 `Schema version`）
4. `migraloop sync` / 监视 Sync Health 与 Delivery Health

## 重启后 resume

持久的 capture 与 Delivery 进度存在 Platform Store。进程重启后，`migraloop sync` 会从存放的 checkpoint（exclusive）继续 Incremental Capture 并接续 Delivery—Operator 不应依赖仅存在本地的 recovery 文件。

## 相关章节

- 健康解读：[Observability](observability.md)
- 安装 / 单 instance 模型：[Deployment](deployment.md)
- CLI 动词：[CLI 与 Config 参考](cli-and-config.md)
