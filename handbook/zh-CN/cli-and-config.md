# CLI 与 Config 参考

给 Operator 的命令、标志、环境变量与 Deployment 配置字段。

## Binary

```text
migraloop <subcommand> [flags]
```

由 `crates/app` 构建（`Dockerfile` release binary）。所有与 Platform Store 通信的 Operator 子命令都接受 `--platform-store-url` 或下方环境变量。

## Operator CLI 子命令

`migraloop` Operator CLI 当前提供这些子命令：

### `migrate`

应用版本化 Platform Store schema migrations。

```bash
migraloop migrate --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `apply`

应用声明式 Deployment 配置（YAML 或 JSON）。验证 secrets-by-reference、Source/Target kinds、Pipeline specs、（当 Pipelines 引用表时）Source Prerequisites，视需要执行 schema discovery + Initial Load，并 upsert Deployment/Pipeline 状态。

在真实 Oracle Source host（非 `contract`/`stub`）上，apply 会通过 OCI 从 live Source 做 schema discovery 与 Initial Load（需要 Instant Client；见 [Source System](source-system.md)）。contract/stub host 仍使用进程内 fixture catalog（CI 切片）。

```bash
migraloop apply --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" -f deployment.yaml
```

| 标志 | 含义 |
| --- | --- |
| `-f`, `--file` | Deployment 配置路径 |

### `status`

报告 Platform Store 健康、Deployments、Pipelines、Base Datasets、Sync Health、Delivery Health 与 Derived Datasets。

```bash
migraloop status --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `base`

查看某个 Source 表的 Base Dataset 行。

```bash
migraloop base --table ORDERS [--deployment oracle-to-mongo]
```

| 标志 | 含义 |
| --- | --- |
| `--table` | Source 表名（必要） |
| `--deployment` | 多个 Bases 共用表名时消歧义 |

### `target`

查看某个 Pipeline collection 的 Target 文档。

```bash
migraloop target --collection orders [--deployment oracle-to-mongo]
```

| 标志 | 含义 |
| --- | --- |
| `--collection` | Target collection 名称（必要） |
| `--deployment` | 共用 collection 名称时消歧义 |

### `derived`

查看 Transform Pipeline 的 Derived Dataset 行。

```bash
migraloop derived --pipeline orders_by_customer [--deployment oracle-to-mongo]
```

| 标志 | 含义 |
| --- | --- |
| `--pipeline` | Pipeline 名称（必要） |
| `--deployment` | 共用 Pipeline 名称时消歧义 |

### `sync`

运行 Incremental Capture 写入 Base Datasets、维护 Derived Datasets，然后 Delivery。

Oracle Incremental Capture 一律走 LogMiner：真实 host 使用 **LogMiner (OCI)**；`host: contract` / `stub` 使用进程内 contract harness。真实 host **不会** silent fallback 到 stub catalog。缺少 Instant Client 或 OCI 失败时会以 LogMiner/OCI 名称 fail fast。

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `run`

启动时 migrate，然后保持 app 进程运行（compose 默认 command）。

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `lab`

Local Sync Lab Fixture 与 Lab Scenarios（ADR-0025）。布署可丢弃的真实堆栈—Oracle Source（Lab 已满足的 Source Prerequisites）、MongoDB Target、Platform Store 与 app。Bring-up **不会**套用 sample Deployment 或 Pipelines。Operator 接着可 list/run 可选的 **Lab Scenarios**，在 **Scenario Namespace** 内以真实 product path 套用 Deployment 并驱动 Sync/Delivery。需要 Docker Compose 与 repo 的 `lab/` 目录（或 `--lab-dir`）。Scenario 的 `apply`/`sync` 需要 host 上的 Oracle Instant Client（`LD_LIBRARY_PATH`）。

```bash
migraloop lab up [--lab-dir lab]
migraloop lab status [--lab-dir lab]
migraloop lab down [--lab-dir lab]
migraloop lab scenario list [--lab-dir lab]
migraloop lab scenario run <scenario-id> [--lab-dir lab] [--auto-remove]
migraloop lab scenario remove <scenario-id> [--lab-dir lab]
```

| Subcommand | 含义 |
| --- | --- |
| `up` | 启动可丢弃 Fixture；就绪时打印连接细节 |
| `status` | 报告 Fixture 就绪状态（engines + Oracle prerequisites + Platform Store），以及哪个 Scenario Namespace 为 **active**（run 进行中）或 **leftover**（run 结束后保留），或各自为 `(none)`。在你套用配置或运行 Lab Scenario 之前也会显示 `Deployment: (none)` / `Pipeline: (none)` — 请用 Scenario run / leftover 行判断，不必从那些行自行猜测 |
| `down` | 拆除 containers 与 volumes |
| `scenario list` | 按 `--lab-dir` 磁盘上的 recipe 列出可选 Lab Scenarios（`lab/scenarios/<id>/recipe.yaml` + `deployment.yaml`，且已注册 runner）。summary 来自各 recipe—例如 `direct-pipeline`、`rt-project`、`rt-filter`、`transform-pipeline`、`concurrent-source-workload`、`bulk-load`、`idempotent-redelivery`。list 也会回报已出货 capability 覆盖（complete vs gaps；见 `lab/scenarios/COVERAGE.md`） |
| `scenario run` | 按 id 运行一个 Lab Scenario。若已有 Scenario 正在运行则拒绝。若 Source/Target 不是 Lab Fixture engines 也会拒绝（客户／生产数据库不在 Lab 范围—那些请用普通的 `apply`/`sync`）。重跑同一 Scenario 会先完整移除其 Namespace 再重建。回报 pass/fail 以及 `duration_ms`、rows/throughput、lag，以及 Scenario 定义的 thresholds（例如 settle time，或 bulk-load 的 lag／throughput／duration，若有）（correctness 与 operational metrics 等权）。`rt-project` / `rt-filter` 覆盖已出货 Rich Transform `project` 与 `filter` operators；`concurrent-source-workload` 在单一 Scenario 内跑并行 Source sessions；`bulk-load` 会 bulk-insert 约 100k Source rows，且 metric thresholds 可独立于 correctness 让 run 失败；`idempotent-redelivery` 会强制对同一批 Output Identities 做 duplicate-safe re-Delivery，并检查 Managed Target 结果仍正确。第二个 Scenario run 仍会被拒绝。默认 keep-on-finish 保留 Namespace 供实时 `base`/`derived`/`target` 检查；成功后若要删除可传 `--auto-remove` |
| `scenario remove` | 完整移除 Scenario Namespace（Source tables、Target collections、Platform Store Deployment），且不启动 run。若已有 Scenario 作用中则拒绝。已不存在时为 idempotent |

| Flag | 含义 |
| --- | --- |
| `--lab-dir` | 含 Lab `compose.yaml` 的目录（默认：`lab`） |
| `--auto-remove` | 仅用于 `scenario run`：成功结束后完整移除 Scenario Namespace（opt-in；失败时仍保留 Namespace 以便调试） |

Lab 是手动验证—不是 Release Quality Gate，也不是 contract/stub LogMiner harness。可选的 Scenario catalog 是 feature-time 完整度表面（ADR-0025），不是 CI suite：不要新增会跑完整 catalog 的 release-gate job。Scenario recipe 惯例、编写路径，以及已出货 capability 覆盖 gaps 见 [Developer local setup](developer-local-setup.md)、`lab/scenarios/README.md` 与 `lab/scenarios/COVERAGE.md`。若要在 Scenario recipes **之外**做 DB-level restore/load（用 `lab status` 连接细节对 Lab Oracle/Mongo 跑 SQL/mongosh/dumps，再接普通 `apply`／`status`／inspect／`sync`），见 `lab/escape-hatch/` 与 [Deployment](deployment.md)—该 escape hatch 不是第二套 Scenario 模型，也不是 CI。

## 公开环境变量契约

| 变量 | 含义 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Operator CLI 与 compose `app` 使用的 Platform Store 连接 URL（`postgres://...`） |
| 配置中 `fromEnv` 引用的密钥环境变量名 | 你在 `password.fromEnv` 写的任何名称（例如 `ORACLE_PASSWORD`、`MONGO_PASSWORD`）在 apply/sync 时必须存在于进程环境 |
| `LD_LIBRARY_PATH` | 真实 Oracle host：Oracle Instant Client libraries 目录（apply/sync runtime 需要；`contract`/`stub` 不使用） |
| Lab disposable defaults | `migraloop lab up` 之后：`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store URL `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Mongo URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`（仅本地 Lab） |

### Contract-harness Source Prerequisite probes（仅 host `stub` / `contract`）

进程内 LogMiner harness 的环境变量名称与默认见 [Source System](source-system.md)（`MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_REDO_RETENTION_HOURS`）。

## Deployment 配置契约

| 字段 | 必要 | 说明 |
| --- | --- | --- |
| `apiVersion` | 是 | `migraloop.dev/v1` |
| `kind` | 是 | `Deployment` |
| `metadata.name` | 是 | 非空 Deployment 名称 |
| `spec.source` | 是 | 见下 |
| `spec.target` | 是 | 见下 |
| `spec.pipelines` | 否 | 默认 `[]`（只应用 Deployment） |

### `spec.source` / `spec.target`

| 字段 | Source | Target | 说明 |
| --- | --- | --- | --- |
| `kind` | `oracle` | `mongodb` | v1 固定配对 |
| `host` | 是 | 是 | Source `stub`/`contract` → LogMiner harness + fixture Initial Load；其他 host → live OCI Initial Load + LogMiner |
| `port` | 是 | 是 | 有效 TCP port |
| `database` | 是 | 是 | |
| `username` | 是 | 是 | 省略 Pipeline `source.schema` 时，也作为默认 Oracle schema/owner |
| `password` | 是 | 是 | 恰好一个 `fromEnv`、`fromFile`、`fromDockerSecret` |
| `timezone` | 可选 | n/a | IANA 或 `±HH:MM`，供 naive 时间 |

Docker secrets 从 `/run/secrets/<name>` 解析。

### Pipeline 项（`spec.pipelines[]`）

| 字段 | 说明 |
| --- | --- |
| `name` | 非空 |
| `mode` | `direct` 或 `transform` |
| `source.table` | 必要；可选 `source.schema`（live Oracle owner；默认为 Source `username`） |
| `target.collection` | Target Binding；仅 Base-only 实验可省略 |
| `fields` | 字段 → `{ as: string \| omit }` 的映射 |
| `outputIdentity` | `transform` 必要 |
| `transform` | 声明式步骤；`transform` mode 必要；`direct` 禁止 |

最小 Direct 示例：

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
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: orders
      mode: direct
      source:
        table: ORDERS
      target:
        collection: orders
```

## 相关章节

- 短路径：[从这里开始](start-here.md)
- Secrets 与 TLS：[Security](security.md)
- 章节深读：[Deployment](deployment.md)、[Pipeline](pipeline.md)、[Source System](source-system.md)
