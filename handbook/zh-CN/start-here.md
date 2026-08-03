# 从这里开始

从安装到第一条 Pipeline，再到 Sync Health / Delivery Health 检查的短路径。细节在各功能章节—把本页当主轴，而不是第二本手册。

## 读者

- **Operator**：安装并运行 **Deployment**、编写 **Pipeline**、监视健康状态。
- **Developer** 若要设置 monorepo，请直接看 [Developer 本地设置](developer-local-setup.md)。

v1 第一组引擎是 **Oracle → MongoDB**。一个 Deployment 恰好配对一个 Source System 与一个 Target System。

## 1. 安装 app 与 Platform Store

默认安装是 **一套 compose、两个 container**：`migraloop` app 与 PostgreSQL **Platform Store**。

```bash
docker compose up -d --build
```

Compose 会为 app 设置 `MIGRALOOP_PLATFORM_STORE_URL`。entrypoint 执行 `migraloop run`（启动时 migrate，在 port `9090` 提供 Prometheus `/metrics`，然后保持进程）。

若要可丢弃的 **Local Sync Lab** Fixture（Oracle + MongoDB + Platform Store + app，无默认 Deployment/Pipelines）：`migraloop lab up` / `status` / `down`。`lab status` 会回报 Fixture 就绪状态，以及哪个 Scenario Namespace 为 active 或 leftover（或 `(none)`）。可选的 **Lab Scenarios**（catalog 来自 `lab/scenarios/<id>/recipe.yaml`；例如 `migraloop lab scenario list` / `run direct-pipeline` / `run rt-project` / `run rt-filter` / `run rt-field-ops` / `run transform-pipeline` / `run concurrent-source-workload` / `run bulk-load` / `run idempotent-redelivery` / `run pause-resume` / `run remove-pipeline` / `run change-pipeline` / `run poison-quarantine` / `run schema-change-pause` / `run source-alignment` / `run drift-check` / `run bounded-backpressure` / `run observability-surface` / `run platform-store-guardrails` / `run backward-compatible-upgrades` / `run initial-load-throttled`）会在 Scenario Namespace 内走真实 apply/sync；Scenario `run` 会在 apply/sync 前拒绝非 Lab／生产环境的 Source/Target engine 绑定。重跑会先 wipe Namespace，另可用 `scenario remove` / `--auto-remove` 清理。若要在 Scenario recipes 之外做 DB-level restore/load，请用 `lab/escape-hatch/` 搭配 Lab 连接细节，再接普通 `apply`／`status`／inspect—同样不是 Release Quality Gate。手动验证（ADR-0025）。嵌套 Docker／**Cursor Cloud** storage-driver 说明（`fuse-overlayfs` 或 `vfs`）：见 [Developer local setup](developer-local-setup.md) 与 [Deployment](deployment.md)。另见 [CLI 与 Config 参考](cli-and-config.md)。

细节：[Deployment](deployment.md) · 标志与环境变量：[CLI 与 Config 参考](cli-and-config.md) · 密钥/TLS：[Security](security.md)

## 2. 准备 Source System 与 Target System

在 apply/sync 之前：

1. 满足 Oracle **Source Prerequisites**（supplemental logging、redo retention）与 **Required Privileges** — [Source System](source-system.md)。
2. 准备 Delivery 账号可写入的 MongoDB **Target System** — [Target System](target-system.md)。
3. Source System / Target System 密码只能用 secret reference（`fromEnv` / `fromFile` / `fromDockerSecret`）— [Security](security.md)。

## 3. 应用含第一条 Pipeline 的 Deployment

编写声明式 YAML/JSON Deployment（`apiVersion: migraloop.dev/v1`、`kind: Deployment`），包含 `spec.source`、`spec.target`，以及至少一条 Pipeline。然后：

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=...   # 名称须符合你的 secret refs
export MONGO_PASSWORD=...

migraloop apply -f deployment.yaml
```

`apply` 会验证配置、在 Pipeline 引用表时检查 Source Prerequisites、视需要执行 **Initial Load** 写入 **Base Dataset**，并把 Pipelines 记录到 Platform Store。

- Direct Pipeline（一张 source 表 → Target Binding）：[Pipeline](pipeline.md)
- Transform Pipeline（声明式 Rich Transform + Output Identity）：[Rich Transform](rich-transform.md)

## 4. 运行 Incremental Capture 与 Delivery

```bash
migraloop sync
```

`sync` 会从持久 checkpoint 继续 **Incremental Capture**（Oracle LogMiner）写入 Base Datasets、维护 Transform Pipeline 的 Derived Datasets，并把 Managed 字段 **Deliver** 到 MongoDB。

## 5. 检查 Sync Health 与 Delivery Health

```bash
migraloop status
```

查看 Platform Store 健康、Deployments、Pipelines、Base Dataset cutover/lag、**Sync Health** 与 **Delivery Health**。更细的检查：

```bash
migraloop base --table ORDERS
migraloop target --collection orders
migraloop derived --pipeline orders_by_customer   # Transform Pipelines
```

如何解读信号：[Observability](observability.md) · 日常运维：[Operations](operations.md)

## 章节地图

| 下一步 | 章节 |
| --- | --- |
| Source + Target 配对、安装形态 | [Deployment](deployment.md) |
| Oracle 连接、prerequisites、类型 | [Source System](source-system.md) |
| MongoDB Target Binding / Managed Columns | [Target System](target-system.md) |
| Direct / Transform Pipelines | [Pipeline](pipeline.md) |
| 声明式 operators 与 Affect Analysis | [Rich Transform](rich-transform.md) |
| Health、status、metrics 契约 | [Observability](observability.md) |
| Schema / poison / backpressure / upgrades | [Operations](operations.md) |
| 命令、标志、配置字段、环境变量 | [CLI 与 Config 参考](cli-and-config.md) |
| Secrets-by-reference 与 TLS | [Security](security.md) |
| Clone、build、本地测试 | [Developer 本地设置](developer-local-setup.md) |
