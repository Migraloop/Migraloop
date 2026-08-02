# Observability

Operator 需要 Sync / Delivery 信号、structured logs，以及（随 Observability Surface 落地）附可告警 failure counters 的 Prometheus metrics（ADR-0008）。Distributed tracing / 厂商 APM 属后续可选。

## 先跑这个

```bash
migraloop status
```

`status` 是当前主要的 Operator 循环。它会报告：

- **Platform Store** 可达性 / 健康与 schema version
- 每个 **Deployment**（Source/Target 标识、LogMiner 机制：contract 或 OCI）
- 每条 **Pipeline**（mode、source 表、target collection、Delivery status）
- 每个 **Base Dataset**（status、行数、列、省略的不支持类型、Initial Load / cutover watermarks、含 appliedChanges / lag / checkpoint 的 **Sync Health**、含 checked/mismatched 计数的 **Source Alignment**）
- 每条 Pipeline 的 **Delivery Health**（已应用变更 / status；有 Poison Change quarantine 时为 `unhealthy`；有 blocking Schema Change pause 时为 `paused`）
- 作用中的 **Quarantine** 行（Output Identity、change id、attempts、last error — unhealthy / not aligned）
- Transform Pipelines 的 **Derived Datasets**（若有）

第一次 sync 后 Operator 常看的健康迹象：

- Platform Store：`healthy`
- Base Dataset status 从 Initial Load 进入 incremental apply
- Sync Health lag 趋向追上（不是长期单向增长）
- 已配置 Target Binding 的 Delivery Health 显示成功应用（`ok`，不是 `unhealthy` quarantine）
- Quarantine：`(none)`（除非刻意留下被 quarantine 的 poison identity）
- Schema Change：`(none)`（除非刻意留下 blocking DDL pause）

## 更深的检查命令

| 命令 | 用途 |
| --- | --- |
| `migraloop base --table <TABLE>` | 抽样 Platform Store 中的 Base Dataset 行 |
| `migraloop target --collection <name>` | 抽样已 Deliver 的 MongoDB documents |
| `migraloop derived --pipeline <name>` | 抽样 Derived Dataset 行 |

多个 Deployments 共用 table/collection/pipeline 名称时，加上 `--deployment <name>`。

## Sync Health vs Delivery Health

- **Sync Health** — 从 Source capture 到 Base Dataset 是否跟上且成功应用。必要但不充分证明 Base 匹配 Source。
- **Source Alignment** — 该 Base 上次 Source Alignment Check 结果（`unknown` / `aligned` / `partial`）。在把 Base 当作 Drift baseline 前运行 `migraloop align`（resource-gated；用 Source reads 修复 Base；从不写入 Source）。`partial` 表示上次检查碰到 `--max-rows` budget。
- **Delivery Health** — Pipeline 的 Target Binding change stream 是否跟上且成功应用。对 non-Managed 字段的编辑与此信号无关。

## Logs 与 metrics

- App/CLI 在 Initial Load、Incremental Capture、Delivery 与失败时输出结构化运维信息（`migraloop` 进程 / container logs 的 stdout/stderr）。
- Prometheus scrape endpoint 与告警 counters 属 Observability Surface **契约**（ADR-0008），尚非主要 Operator 接口—在你的 build 提供 metrics 之前，请使用 `status` 的 lag/checkpoint/error 行。

## 相关章节

- 短路径：[从这里开始](start-here.md)
- Day-2 失败模式：[Operations](operations.md)
- 命令标志：[CLI 与 Config 参考](cli-and-config.md)
