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

应用声明式 Deployment 配置（YAML 或 JSON）。验证 secrets-by-reference、Source/Target kinds、Pipeline specs、（当 Pipelines 引用表时）Source Prerequisites，视需要执行 Initial Load，并 upsert Deployment/Pipeline 状态。

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

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `run`

启动时 migrate，然后保持 app 进程运行（compose 默认 command）。

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

## 公开环境变量契约

| 变量 | 含义 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Operator CLI 与 compose `app` 使用的 Platform Store 连接 URL（`postgres://...`） |
| 配置中 `fromEnv` 引用的密钥环境变量名 | 你在 `password.fromEnv` 写的任何名称（例如 `ORACLE_PASSWORD`、`MONGO_PASSWORD`）在 apply/sync 时必须存在于进程环境 |

### Contract-harness Source Prerequisite probes（仅 host `stub` / `contract`）

| 变量 | 含义 | 默认 |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | `on` / `off` | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`、空字符串，或逗号分隔表 | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | 报告的 redo retention 小时数 | `72` |

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
| `host` | 是 | 是 | Source `stub`/`contract` → LogMiner harness |
| `port` | 是 | 是 | 有效 TCP port |
| `database` | 是 | 是 | |
| `username` | 是 | 是 | |
| `password` | 是 | 是 | 恰好一个 `fromEnv`、`fromFile`、`fromDockerSecret` |
| `timezone` | 可选 | n/a | IANA 或 `±HH:MM`，供 naive 时间 |

Docker secrets 从 `/run/secrets/<name>` 解析。

### Pipeline 项（`spec.pipelines[]`）

| 字段 | 说明 |
| --- | --- |
| `name` | 非空 |
| `mode` | `direct` 或 `transform` |
| `source.table` | 必要；可选 `source.schema` |
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
