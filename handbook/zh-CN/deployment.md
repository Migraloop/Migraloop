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

Compose 会把 `MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@platform-store:5432/migraloop` 注入 app，并执行 `migraloop run`（对已应用 Pipelines 做 continuous Incremental Capture + Delivery，并经 `MIGRALOOP_METRICS_ADDR` 在 host port `9090` 提供 Prometheus `/metrics`）。请把 Source/Target secret refs 注入 app 环境，让 continuous Sync 能运行。随附 Postgres 带有 Platform Store Guardrails 安全默认（`shared_buffers=128MB`、`work_mem=8MB`、`maintenance_work_mem=128MB`、`max_connections=100`）；store data volume 也会以 read-only 挂进 app（`MIGRALOOP_PLATFORM_STORE_DATA_DIR`）供可用磁盘警告探测。可向上调整 Postgres volumes/resources；不要更换 store 引擎，也不要把设置降到产品下限以下（见 [Operations](operations.md)）。

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
migraloop lab status  # Fixture 就绪状态 + active/leftover Scenario Namespace + 连接细节；没有默认 Pipeline
migraloop lab scenario list
migraloop lab scenario run direct-pipeline   # 需要 host Instant Client（LD_LIBRARY_PATH）
migraloop lab scenario run rt-project   # Rich Transform project → Derived → Delivery
migraloop lab scenario run rt-filter   # Rich Transform filter → Derived → Delivery
migraloop lab scenario run rt-field-ops   # Rich Transform addFields/rename/remove → Derived → Delivery
migraloop lab scenario run rt-equilookup   # Rich Transform equiLookup multi-Base → Derived → Delivery
migraloop lab scenario run rt-union        # Rich Transform union multi-Base → Derived → Delivery
migraloop lab scenario run rt-unwind   # Rich Transform unwind → Derived → Delivery
migraloop lab scenario run rt-distinct-addtoset   # Rich Transform distinct/addToSet + Maintenance State → Derived → Delivery
migraloop lab scenario run transform-pipeline   # 多表 groupBy sum/count/min/max/avg → Derived → Delivery
migraloop lab scenario run concurrent-source-workload   # Scenario 内并行 Source contention
migraloop lab scenario run bulk-load   # ~100k Source inserts + lag/throughput/duration thresholds
migraloop lab scenario run idempotent-redelivery   # duplicate-safe Delivery re-apply
migraloop lab scenario run pause-resume   # pause/resume CLI verbs + catch-up Delivery
migraloop lab scenario run remove-pipeline # remove CLI 动词；仍被引用的 Shared Base 会保留
migraloop lab scenario run change-pipeline # 通过 apply 做 Pipeline revision；Derived rebuild／metadata-only 可跳过
migraloop lab scenario run poison-quarantine # quarantine poison identity；Pipeline 继续；status unhealthy
migraloop lab scenario run schema-change-pause # blocking DDL warn+pause；status Schema Change（非 quarantine）
migraloop lab scenario run source-alignment # Base≠Source 检测 + 仅修复 Base；resource-gated max-rows
migraloop lab scenario run drift-check # Managed Target drift 检测 + Managed auto-repair；保留 non-Managed
migraloop lab scenario run bounded-backpressure # Downstream 变慢 → bounded queues + 可见 lag；不 pause
migraloop lab scenario run observability-surface # structured logs + /metrics lag/failures + status health
migraloop lab scenario run platform-store-guardrails # Guardrails 拒绝过低设置；可用磁盘仅 WARN（不自动 pause）
migraloop lab scenario run initial-load-throttled # Chunked／rate-limited／pausable Initial Load
migraloop lab scenario run backward-compatible-upgrades # 升级 migrate 保留 Deployments；较旧 SemVer config 可套用且无需 wipe-rebuild
migraloop lab scenario remove direct-pipeline   # 清除 Namespace，不重跑
# 或：migraloop lab scenario run direct-pipeline --auto-remove
migraloop lab down    # 移除 containers 与 volumes
```

Compose 定义：`lab/compose.yaml`（project `migraloop-lab`）。Lab `app` image（`lab/Dockerfile`）会复制 host 建好的 `migraloop` binary，避免在 Docker 内重编；`migraloop lab up` 若缺少 binary 会先构建。Lab Oracle init 会启用 ARCHIVELOG 与 database supplemental logging 以供 LogMiner；**不会**预先套用任何 Deployment 或 Pipelines—那些来自 Lab Scenario 或你自己的 `migraloop apply`。Catalog Scenarios 包装在 `lab/scenarios/<id>/`（`recipe.yaml` + `deployment.yaml`）；recipe-driven runner 以 recipe 的 `workload`／`checks`／`thresholds` 为接口。`migraloop lab scenario list` 反映那些可选 recipes。目前 catalog 包含 `direct-pipeline`（Direct Pipeline insert/update/delete）、`rt-project`（Rich Transform `project`）、`rt-filter`（Rich Transform `filter`）、`rt-field-ops`（Rich Transform `addFields`/`rename`/`remove`）、`rt-equilookup`（Rich Transform `equiLookup` multi-Base）、`rt-union`（Rich Transform `union` multi-Base）、`rt-unwind`（Rich Transform `unwind`）、`rt-distinct-addtoset`（Rich Transform `distinct`/`addToSet` + Maintenance State）、`transform-pipeline`（多表 customers + orders，Rich Transform `groupBy` sum/count/min/max/avg → Derived → Delivery），`concurrent-source-workload`（相同多表形状，但在单一 Scenario 内以 recipe 驱动并行 Source sessions；跨 Scenario 并行仍禁止），`bulk-load`（约 100k Source inserts，经 Initial Load，并以可失败的 lag／throughput／duration thresholds 等权检查），、`idempotent-redelivery`（在真实 apply path 上做 duplicate-safe／idempotent re-Delivery，验证 Managed Target 结果），以及 `pause-resume`（pause/resume CLI 动词：一条 Pipeline 停止 Delivery、另一条继续；resume 自耐久 Base catch-up），以及 `remove-pipeline`（remove CLI 动词：停止 Delivery；仍被引用的 Shared Base 保留），以及 `change-pipeline`（通过 `apply` 做 Pipeline revision：暂停旧 Delivery → 重建该 Pipeline 的 Derived／重新 Delivery；Shared Bases 不重建；仅 `description` 的 metadata-only 可跳过 rebuild），以及 `poison-quarantine`（有界 Delivery 重试后 quarantine 单个 poison Output Identity 并 ALERT，Pipeline 继续；`status` 显示 Delivery Health unhealthy / not aligned），与 `schema-change-pause`（blocking DDL warn+pause；`status` 显示 Delivery Health paused + Schema Change，与 poison quarantine 不同），以及 `source-alignment`（Source Alignment Check 检测 Base≠Source、仅从 Source reads 修复 Base、resource-gated `--max-rows`），以及 `drift-check`（Drift Check 检测 Managed-field Target drift、默认 Managed auto-repair、保留 non-Managed、resource-gated `--max-rows`），以及 `bounded-backpressure`（Downstream Delivery 变慢时使用 bounded Incremental queues，Sync/Delivery lag 可见；不因单纯变慢而 pause Pipeline），以及 `observability-surface`（structured JSON operator logs、Prometheus `/metrics` lag/failures、`status` 上的 Sync/Delivery Health），以及 `platform-store-guardrails`（Platform Store Guardrails 拒绝过低设置；可用磁盘仅 WARN + `platform_store_disk_warn` — 不自动 pause），以及 `backward-compatible-upgrades`、`initial-load-throttled`（升级 migrate 保留 Deployments；较旧 SemVer-compatible `apiVersion` 可套用且无需 wipe-rebuild）。`migraloop lab scenario list` 会回报 catalog-complete 与已出货 capability gaps（`lab/scenarios/COVERAGE.md`）。各自会准备 Scenario Namespace、以真实 product path 套用（仅针对 Lab Fixture engines—Scenario `run` 会拒绝客户／生产环境 Source/Target 绑定），并默认保留 Namespace 供实时 `base`/`derived`/`target` 检查。重跑同一 Scenario 会先完整移除 Namespace 再重建；`scenario remove` 与 `--auto-remove` 分别提供手动与 opt-in 清理。与上方默认双 container 安装（root `Dockerfile`）、以及 CI 使用的 contract/stub harness／Release Quality Gate 都不同—由 operator 选择 Scenarios；完整 catalog 不是 CI release gate（ADR-0025）。Feature-time 编写路径：[Developer local setup](developer-local-setup.md)。

资源提醒：Lab Oracle（Free）通常需要数 GB RAM，第一次拉 image／开机可能要数分钟。Lab Compose 使用 `network_mode: host`，以便在 bridge 网络被挡的嵌套 Docker 环境仍可运行。若嵌套 Docker 在 overlay whiteout 解压失败，需改用非 overlay 的 dockerd storage driver——`fuse-overlayfs` 或 `vfs`——并关闭 containerd snapshotter。**Cursor Cloud** agent 会由 `.cursor/environment.json` 套用该配方（通过 `.cursor/cloud-dind-*.sh` 使用 `fuse-overlayfs`）；其他嵌套主机可手动套用相同的 `daemon.json` 形状。

### DB-level restore / load escape hatch

当你需要在 Lab Scenario recipe **之外**加载或还原数据—SQL dump、临时 inserts，或 Mongo seed/restore—请使用可丢弃的 Lab engines，以及 `migraloop lab status`（或 `lab up`）打印的连接细节。这是通往真实堆栈的 escape hatch，**不是**第二套 Scenario 编写模型，也**不是** Release Quality Gate／CI。

示例位于 `lab/escape-hatch/`（`oracle-load.sql`、`mongo-load.js`，以及仅绑定 Lab 的 `deployment.yaml`，以便接回普通 product path）。没有 `recipe.yaml`，此流程也**不要**执行 `migraloop lab scenario …`。

```bash
migraloop lab up
migraloop lab status   # 复制 Lab Oracle / Mongo / Platform Store 细节

# 加载 Lab Oracle（compose exec — 不需要 BYO 生产环境 Source）
docker compose -f lab/compose.yaml -p migraloop-lab exec -T oracle \
  sqlplus -s SYNC_USER/lab_oracle@FREEPDB1 < lab/escape-hatch/oracle-load.sql

# 加载 Lab Mongo（Target 端 seed／restore 式查看）
docker compose -f lab/compose.yaml -p migraloop-lab exec -T mongo \
  mongosh --quiet --host 127.0.0.1 -u migraloop -p lab_mongo \
  --authenticationDatabase admin lab < lab/escape-hatch/mongo-load.js

# 接回真实 product path（apply / status / base / target / sync）
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=lab_oracle
export MONGO_PASSWORD=lab_mongo
# 对 Lab Oracle 做 apply/sync 需要 host Instant Client：
# export LD_LIBRARY_PATH=/path/to/instantclient
migraloop apply -f lab/escape-hatch/deployment.yaml
migraloop status
migraloop base --table LAB_ESCAPE_CUSTOMERS
migraloop target --collection lab_escape_customers   # Delivery 跑完后
# Steady-state Sync 在 Lab `migraloop run` 内持续进行；可选 one-shot：
migraloop sync                                       # Lab / Operator Incremental Capture catch-up
```

**可选的 dump 工具还原**（相同 Lab 连接细节；仍不是 Scenario／不是 CI）。因 Lab Compose 使用 `network_mode: host`，host 工具可连 `127.0.0.1` Lab ports：

```bash
# MongoDB archive → Lab Mongo（示例；按 dump 路径／ns 调整）
mongorestore \
  --uri 'mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin' \
  --archive=your-lab-seed.archive

# Oracle Data Pump → Lab Oracle（需能连到 Lab 的 client；
# 可丢弃 image 的 SYS 密码为 lab_oracle_sys — 仅供 Lab）
impdp SYNC_USER/lab_oracle@//127.0.0.1:1521/FREEPDB1 \
  DUMPFILE=your_lab_seed.dmp DIRECTORY=DATA_PUMP_DIR TABLE_EXISTS_ACTION=REPLACE
```

若 dump restore 进 Lab Oracle 且之后要 sync，请套用 table-level supplemental logging（同 `oracle-load.sql`），再以绑定 Lab 的 Deployment 走普通 `migraloop apply`／`status`／`base`／`target`／`sync`。非 Delivery-managed 的 Target 端 load/restore 请用 Lab Mongo URI 以 mongosh 查看；`migraloop target` 用于 product Delivery 之后的 collections。

请只在可丢弃 Fixture 上加载—绝不要把此 escape hatch 指向客户／生产环境 engines。若要打包好的 correctness + metrics recipe，请优先用 Lab Scenarios；当你已有 SQL／JS／dumps 要自行放入 Lab 数据库时，再用此路径。

## Runtime 模型

- v1 以 **一个 active app instance**（内部可并行）加上 Platform Store 运行。
- 所有持久 Deployment 状态（Pipelines、Base/Derived Datasets、checkpoints）存在 Platform Store，替换 instance 才能续跑。
- 自动 multi-instance failover 属后续阶段；active processing 保持 single-leader（非 multi-writer）。
- Deployment **runtime** 拥有 apply／Sync／Delivery／lifecycle／checks；Operator CLI 是薄 adapter。新的 Source 或 Target engine 接在 `SourceEngine`／`TargetEngine`，不重塑这些概念——见 [Developer 本地设置](developer-local-setup.md#新增-source-或-target-enginedeveloper-checklist)。

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
    timezone: Asia/Shanghai        # 可选 IANA 或 ±HH:MM；naive DATE/TIMESTAMP 后备
    # tls:                         # 可选；省略即 cleartext Lab/开发
    #   enabled: true
    #   walletLocation: /etc/oracle/wallet
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
    # tls:
    #   enabled: true
    #   caFile: /etc/migraloop/certs/mongo-ca.pem
  pipelines: []                    # 见 Pipeline 章节
```

v1 要求 `source.kind: oracle` 与 `target.kind: mongodb`。密码必须是 secret reference—禁止明文。可选的 `tls` 块（以及 Platform Store 在 `MIGRALOOP_PLATFORM_STORE_URL` 上的 `sslmode`）见 [Security](security.md) 与 [CLI 与 Config](cli-and-config.md)。

以 `migraloop apply -f <file>` 应用。`pipelines` 为空时只应用 Deployment metadata（尚不 capture）。

## 相关章节

- Source 连接与 prerequisites：[Source System](source-system.md)
- Target Binding 与 Delivery：[Target System](target-system.md)
- Deployment 内的 Pipelines：[Pipeline](pipeline.md)
- Secrets 与 TLS：[Security](security.md)
- 完整字段/标志清单：[CLI 与 Config 参考](cli-and-config.md)
