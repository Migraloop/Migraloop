# Deployment

一个 **Deployment** 恰好配对 **一个 Source System** 与 **一个 Target System**，并承载其间一或多条 **Pipeline**。若要不同的数据库配对，请另建 Deployment—不要在同一个 Deployment 内做多数据库 fan-in。

## 安装形态（v1）

默认接近生产环境的安装是 **一次安装、两个 container**：

| Service | 角色 |
| --- | --- |
| `platform-store` | 随附的 PostgreSQL **Platform Store**（引擎由产品锁定） |
| `app` | `migraloop` binary（`Dockerfile` 构建 release `migraloop-app`） |

在 repo 根目录启动：

```bash
docker compose up -d --build
```

Compose 会把 `MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@platform-store:5432/migraloop` 注入 app，并执行 `migraloop run`。可调整 Postgres volumes/resources，但不要更换 store 引擎。

若在 host 上对已 publish 的 store port `5432` 使用 Operator CLI：

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
migraloop migrate   # 若未使用 `run`
migraloop apply -f deployment.yaml
migraloop status
```

## Local Sync Lab Fixture

若要在可丢弃的真实堆栈上手动端到端验证（ADR-0025），使用 **Local Sync Lab** Fixture。它在既有 Platform Store + app 安装形态旁，再布署 Lab 用的 Oracle 与 MongoDB：

```bash
migraloop lab up      # 在 repo 根目录（或传 --lab-dir）
migraloop lab status  # Fixture 就绪状态 + 连接细节；没有默认 Pipeline
migraloop lab scenario list
migraloop lab scenario run direct-pipeline   # 需要 host Instant Client（LD_LIBRARY_PATH）
migraloop lab scenario run rt-project   # Rich Transform project → Derived → Delivery
migraloop lab scenario run rt-filter   # Rich Transform filter → Derived → Delivery
migraloop lab scenario run transform-pipeline   # 多表 Transform → Derived → Delivery
migraloop lab scenario run concurrent-source-workload   # Scenario 内并行 Source contention
migraloop lab scenario run bulk-load   # ~100k Source inserts + lag/throughput/duration thresholds
migraloop lab scenario remove direct-pipeline   # 清除 Namespace，不重跑
# 或：migraloop lab scenario run direct-pipeline --auto-remove
migraloop lab down    # 移除 containers 与 volumes
```

Compose 定义：`lab/compose.yaml`（project `migraloop-lab`）。Lab `app` image（`lab/Dockerfile`）会复制 host 建好的 `migraloop` binary，避免在 Docker 内重编；`migraloop lab up` 若缺少 binary 会先构建。Lab Oracle init 会启用 ARCHIVELOG 与 database supplemental logging 以供 LogMiner；**不会**预先套用任何 Deployment 或 Pipelines—那些来自 Lab Scenario 或你自己的 `migraloop apply`。Catalog Scenarios 包装在 `lab/scenarios/<id>/`（`recipe.yaml` + `deployment.yaml`）；`migraloop lab scenario list` 反映那些可选 recipes。目前 catalog 包含 `direct-pipeline`（Direct Pipeline insert/update/delete）、`rt-project`（Rich Transform `project`）、`rt-filter`（Rich Transform `filter`）、`transform-pipeline`（多表 customers + orders，Rich Transform `groupBy`/`sum` → Derived → Delivery），`concurrent-source-workload`（相同多表形状，但在单一 Scenario 内以 recipe 驱动并行 Source sessions；跨 Scenario 并行仍禁止），以及 `bulk-load`（约 100k Source inserts，经 Initial Load，并以可失败的 lag／throughput／duration thresholds 等权检查）。`migraloop lab scenario list` 会回报 catalog-complete 与已出货 capability gaps（`lab/scenarios/COVERAGE.md`）。各自会准备 Scenario Namespace、以真实 product path 套用，并默认保留 Namespace 供实时 `base`/`derived`/`target` 检查。重跑同一 Scenario 会先完整移除 Namespace 再重建；`scenario remove` 与 `--auto-remove` 分别提供手动与 opt-in 清理。与上方默认双 container 安装（root `Dockerfile`）、以及 CI 使用的 contract/stub harness／Release Quality Gate 都不同—由 operator 选择 Scenarios；完整 catalog 不是 CI release gate（ADR-0025）。Feature-time 编写路径：[Developer local setup](developer-local-setup.md)。

资源提醒：Lab Oracle（Free）通常需要数 GB RAM，第一次拉 image／开机可能要数分钟。Lab Compose 使用 `network_mode: host`，以便在 bridge 网络被挡的嵌套 Docker 环境仍可运行。若嵌套 Docker 在 overlay whiteout 解压失败，可改用 dockerd `storage-driver: vfs`（并关闭 containerd snapshotter）。

## Runtime 模型

- v1 以 **一个 active app instance**（内部可并行）加上 Platform Store 运行。
- 所有持久 Deployment 状态（Pipelines、Base/Derived Datasets、checkpoints）存在 Platform Store，替换 instance 才能续跑。
- 自动 multi-instance failover 属后续阶段；active processing 保持 single-leader（非 multi-writer）。

## 声明 Deployment

配置为 YAML 或 JSON。必要顶层字段：

```yaml
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: db.example.com
    port: 1521
    database: ORCL
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
    timezone: Asia/Shanghai        # 可选；naive DATE/TIMESTAMP 后备
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines: []                    # 见 Pipeline 章节
```

v1 要求 `source.kind: oracle` 与 `target.kind: mongodb`。密码必须是 secret reference—禁止明文。

以 `migraloop apply -f <file>` 应用。`pipelines` 为空时只应用 Deployment metadata（尚不 capture）。

## 相关章节

- Source 连接与 prerequisites：[Source System](source-system.md)
- Target Binding 与 Delivery：[Target System](target-system.md)
- Deployment 内的 Pipelines：[Pipeline](pipeline.md)
- 完整字段/标志清单：[CLI 与 Config 参考](cli-and-config.md)
