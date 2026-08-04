# Observability

Operator 需要 Sync / Delivery 信号、structured logs，以及（随 Observability Surface 落地）附可告警 failure counters 的 Prometheus metrics（ADR-0008）。Distributed tracing / 厂商 APM 属后续可选。

## 先跑这个

```bash
migraloop status
```

`status` 是当前主要的 Operator 循环。它会报告：

- **Platform Store** 可达性 / 健康与 schema version（Platform Store Guardrails：过低的 Postgres 设置会被拒绝；可用磁盘低于 1 GiB 时打印 warn-only `WARN` — 绝不自动 pause Pipelines）
- 每个 **Deployment**（Source/Target 标识、LogMiner 机制：contract 或 OCI）
- 每条 **Pipeline**（mode、source 表、target collection、Delivery status）
- 每个 **Base Dataset**（status、行数、列、省略的不支持类型、Initial Load / cutover watermarks、**Sync Health** — `unknown` / `ok` / `lagging` / `failed` — 含 appliedChanges / lag / checkpoint、含 checked/mismatched 计数的 **Source Alignment**）
- 每条 Pipeline 的 **Delivery Health**（已应用变更 / lag / status；有 Poison Change quarantine 时为 `unhealthy`；有 blocking Schema Change pause 时为 `paused`；Downstream backpressure 下 lag 会上升但不会 pause Pipeline）
- 作用中的 **Quarantine** 行（Output Identity、change id、attempts、last error — unhealthy / not aligned）
- Transform Pipelines 的 **Derived Datasets**（若有）

第一次 sync 后 Operator 常看的健康迹象：

- Platform Store：`healthy`
- Base Dataset status 从 Initial Load 进入 incremental apply
- Sync Health 为 `ok`（或由 `lagging` 趋向 `ok`）— lag 不是长期单向增长
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

- **Sync Health** — 从 Source capture 到 Base Dataset 是否跟上且成功应用（Incremental 进度后 lag 为 0 时为 `ok`；仍有 Source backlog 时为 `lagging`；耐久 capture/apply 失败为 `failed`；尚未有 Incremental 进度为 `unknown`）。必要但不充分证明 Base 匹配 Source。`status` 与 Prometheus 从同一个 Observability assembly 导出这些 labels。
- **Source Alignment** — 该 Base 上次 Source Alignment Check 结果（`unknown` / `aligned` / `partial`）。在把 Base 当作 Drift baseline 前运行 `migraloop align`（resource-gated；用 Source reads 修复 Base；从不写入 Source）。`partial` 表示上次检查碰到 `--max-rows` budget。
- **Delivery Health** — Pipeline 的 Target Binding change stream 是否跟上且成功应用。对 non-Managed 字段的编辑与此信号无关。Downstream backpressure 下，`lag=` 反映从 capture resume position 起算的剩余 pending Delivery 工作（ADR-0020）—不是整条 Pipeline pause。Capture 一次仍最多只 materialize 一个 bounded queue window。
- **Drift** — 该 Pipeline 上次 Drift Check 结果（`unknown` / `ok` / `partial`）。在 Alignment 之后运行 `migraloop drift`（resource-gated；默认 Managed-field auto-repair；忽略 non-Managed fields）。`partial` 表示上次检查碰到 `--max-rows` budget。

## Logs 与 metrics

- App/CLI 在 Initial Load、Incremental Capture、Delivery、Backpressure、Poison Change quarantine、blocking Schema Change，以及 Platform Store 可用磁盘警告会发出 **structured JSON** operator event lines（并保留 human-readable 对应行）（`migraloop` 进程 / container logs 的 stdout/stderr）。请查找 `"event":"…"` 字段，例如 `initial_load_progress`、`initial_load_paused`、`initial_load_backoff`、`initial_load_complete`、`incremental_capture`、`delivery_complete`、`backpressure`、`poison_quarantine`、`schema_change_blocked`、`platform_store_disk_warn`。
- `migraloop run` 会对已应用 Pipelines 持续执行 Incremental Capture → Delivery，并在同一个 single active instance 上于 `http://<metrics-addr>/metrics` 提供 Prometheus scrape endpoint（默认 `0.0.0.0:9090`，可用 `--metrics-addr` / `MIGRALOOP_METRICS_ADDR` 覆盖）。Compose 会公布 host port `9090`。Metrics 包含 Sync/Delivery lag（`migraloop_sync_lag`、`migraloop_delivery_lag`）、Pipeline pause、可告警 failure gauges（`migraloop_quarantined_changes`、`migraloop_failures`，皆自耐久 Platform Store state 读取），以及 Platform Store disk gauges（`migraloop_platform_store_disk_free_bytes`、`migraloop_platform_store_disk_warn` — warn-only；绝不自动 pause）。
- `status` 仍是 Operator 解读 lag/checkpoint/error 的主要循环；用 scrape `/metrics` 做 alerting 与 dashboards。

## 相关章节

- 短路径：[从这里开始](start-here.md)
- Day-2 失败模式：[Operations](operations.md)
- 命令标志：[CLI 与 Config 参考](cli-and-config.md)
