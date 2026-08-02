# Source System

**Source System** 是平台从中 capture 的用户数据库。v1 以 **Oracle** 搭配 **LogMiner** 做 **Incremental Capture**。连接标识是 `kind` + host/port/database/username，外加 password secret reference。

## 连接形态

在 Deployment 配置的 `spec.source` 下：

| 字段 | 含义 |
| --- | --- |
| `kind` | v1 必须为 `oracle` |
| `host` | Oracle host。特殊值 `contract` 或 `stub` 会选用进程内 LogMiner contract harness（测试 / 本地切片）—不是真实 Oracle |
| `port` | TCP port（通常 `1521`） |
| `database` | Service / database 名称 |
| `username` | Sync 账号（最小 Required Privileges；不是默认就要 admin） |
| `password` | Secret reference：`fromEnv`、`fromFile` 或 `fromDockerSecret` |
| `timezone` | 可选 IANA 名称或 Oracle 风格 offset（`+09:00`）。在 naive DATE/TIMESTAMP 需要解读且 Source DB timezone 不可读时使用 |

真实 Oracle host 的 **Initial Load**（schema discovery + snapshot）与 **LogMiner Incremental Capture** 都走 **OCI** 路径。若 runtime 没有 Oracle Instant Client / OCI libraries，apply/sync 会以 LogMiner/OCI 名称 fail fast—不会默默退回 stub catalog。对 live Source 执行前请安装 Instant Client（Basic 或 Basic Light），并将 `LD_LIBRARY_PATH` 指向其目录。

在 live Source 上，Pipeline 的 `source.schema` 选择 Oracle owner；省略时平台以 Source `username`（大写）作为默认 schema。contract/stub harness 会忽略 schema，仅在 CI 切片使用 fixture catalog—不是 Lab／真实路径的定义真相。

## Source Prerequisites（Oracle / LogMiner）

在 **Initial Load** 或 **Incremental Capture** 之前，平台会验证 **Source Prerequisites**；未满足时以清楚错误 **fail fast**（ADR-0021）。平台**不会**自动修改 Source System 设置来「修好」这些检查。

### 1. Database supplemental logging

在 database 层启用 minimum supplemental logging：

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

没有这个设置，LogMiner 无法可靠重建 change vectors。

### 2. Table-level key supplemental logging

对每一张被 Pipeline 引用的表，启用 PRIMARY KEY 或 ALL COLUMNS supplemental logging：

```sql
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
-- 或当表没有可用 PK / 需要完整 before-images 时：
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

缺少 table-level logging 会导致该表 Incremental Capture 不完整或不正确。

### 3. 足够的 redo / archive retention

至少保留 **24 小时** redo（online + archived），让 Initial Load overlap、Incremental Capture lag，以及进程重启后的 resume 仍能读到需要的变更历史。按你的 Oracle edition 配置 archive destination retention / FRA policy。

Live OCI probe 要求 **ARCHIVELOG** 模式。可读时会报告可用 archived-redo 时间跨度；若跨度仍短于 24 小时（例如刚备妥的 Lab Source），但已配置 `db_recovery_file_dest` 或 `log_archive_dest_1`，probe 会视为符合文档化底线。**NOARCHIVELOG** 会 fail fast。若 redo 在平台消费前就过期，变更会丢失—平台宁可不跑，也不做不完整 capture。

### Operator 工作流程

1. 以 DBA / 具备权限的 Operator 在 Source System 上应用上述 SQL（或等效设置）。
2. 确认 sync 用户的 grants（见下方 Required Privileges）。
3. 运行 `migraloop apply` / `migraloop sync`。未满足的 prerequisites 会在运行前失败并指出缺什么。
4. 修好指名的 Oracle 设置后重跑。平台绝不会自动执行 `ALTER DATABASE` / `ALTER TABLE` 来「修复」失败。

**Local Sync Lab：** `migraloop lab up` 会布署可丢弃的 Oracle Source，并已满足 Lab 使用所需的 database-level prerequisites（ARCHIVELOG + database supplemental logging + sync-user grants）。当 Lab Scenario（或你）创建 Pipeline 引用的表时，仍须套用 table-level supplemental logging—例如 `migraloop lab scenario run direct-pipeline`、`transform-pipeline` 或 `concurrent-source-workload` 会在其 Scenario Namespace 表加上 `SUPPLEMENTAL LOG DATA (ALL) COLUMNS`，再走真实 `apply` / LogMiner `sync`（需要 host Instant Client / `LD_LIBRARY_PATH`）。重跑同一 Scenario 会先 drop 再重建那些 Namespace 表；`lab scenario remove` 可在不重跑的情况下清除。Lab 不会变更客户／生产环境数据库。

### Contract LogMiner harness（测试 / 本地切片）

当 Source `host` 为 `contract` 或 `stub` 时，Incremental Capture 使用进程内 **LogMiner contract harness**。该 harness 的 prerequisite probes 由环境变量驱动（只读；从不变更数据库）：

| 变量 | 含义 | 默认 |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | database supplemental logging 的 `on` / `off` | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`、空字符串，或已启用 PK/ALL logging 的逗号分隔表 | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | 报告的 redo retention（小时） | `72` |

## Required Privileges

Sync 账号需要足以执行 **Initial Load**、**Incremental Capture**（LogMiner session 与相关 dictionary/redo 读取）、Pipeline 引用表的 schema discovery，以及 alignment 类读取的权限—不是只能用 superuser（ADR-0016）。

实务上账号必须能：

- 对 Pipeline 引用的表（与 schema）做 Initial Load 所需的 `SELECT`
- 打开 LogMiner / 读取 Incremental Capture 所需的 redo contents views
- 读取 supplemental-logging 与 schema probe 所需的 data-dictionary metadata

在你的 Oracle edition 上授予能满足上述职责的最小集合。Admin/DBA 可用于 lab，但不得当成生产环境文档默认。

## Supported Source Types（v1）

Schema discovery 之后，Sync 只把 allow-list 内的 Oracle 类型转入 Platform Store（ADR-0018、ADR-0023）：

- **Allow-list：** `NUMBER`（precision/scale 规则）、`FLOAT` / `BINARY_FLOAT` / `BINARY_DOUBLE`、`CHAR` / `NCHAR` / `VARCHAR2` / `NVARCHAR2`、`DATE`、`TIMESTAMP`（含 WITH TIME ZONE / LOCAL TIME ZONE）、`RAW`（有 size cap），以及上述的 nullable 形式。
- **Out of scope：** `BLOB`、`CLOB`、`NCLOB`、`BFILE`、`LONG` / `LONG RAW`、`XMLType`、object types、nested tables / VARRAYs、`ROWID` / `UROWID` 与其他特殊类型。

不支持的列会从 Base Dataset **省略**（表仍会 sync）；省略情况可在 `migraloop status` 看到。若 Pipeline 需要不支持列则无法使用—绝不做默默 coercion。

**NUMBER：** 在安全时映射到保精度的 Mongo 类型（`NumberLong` / `Decimal128`）。Schema 不安全的 NUMBER 列必须在配置时以 Pipeline `fields`（`as: string` 或 `as: omit`）解决—不是在 runtime 逐行 quarantine。

**时间类型：** 平台内部使用 UTC。带时区值会变成绝对瞬间。Naive DATE/TIMESTAMP 在可读时使用 Source DB timezone，否则使用配置的 Source `timezone`。

## 哪些表会被 capture

Sync 按 **Pipeline 引用** 选择表—不是整 schema mirror。每张纳入的表在 Deployment 内至多一个共用 **Base Dataset**（完整 supported-type 行），供所有需要它的 Pipeline 重用。新增被引用的表只对该表做 **table-level Initial Load**。

## Live Oracle 验证（CLI operator seam）

在真实 Oracle Source 上（已安装 Instant Client，且 Source Prerequisites 已满足），Operator 可不经 mock 验证 Sync→Delivery：

1. 将 `spec.source.host` / `port` / `database` / `username` 指向 live Source（不要用 `contract`/`stub`）。
2. `migraloop apply -f deployment.yaml` — Initial Load 从 live 表读入 Base Datasets，并把 Direct Pipeline Deliver 到 MongoDB。
3. 在已启用 supplemental logging 的情况下变更 Source 行（`INSERT` / `UPDATE` / `DELETE`）。
4. `migraloop sync` — LogMiner (OCI) Incremental Capture 套用变更；MongoDB 上的 Managed 字段会反映这些变更。
5. 用 `migraloop status`、`migraloop base --table <TABLE>`、`migraloop target --collection <NAME>` 检视。

若有可用的 live Oracle，Developer 也可跑 gated seam test：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
export MIGRALOOP_LIVE_ORACLE_HOST=127.0.0.1
export MIGRALOOP_LIVE_ORACLE_PORT=1521
export MIGRALOOP_LIVE_ORACLE_SERVICE=FREEPDB1
export MIGRALOOP_LIVE_ORACLE_USER=SYNC_USER
export ORACLE_PASSWORD=...
cargo test -p migraloop-app --test cli_live_oracle_direct -- --ignored --nocapture
```

## 相关章节

- 与 Target 配对：[Deployment](deployment.md)
- 引用表的 Pipelines：[Pipeline](pipeline.md)
- Secrets 与 TLS：[Security](security.md)
- Developer 机器上的 Instant Client：[Developer 本地设置](developer-local-setup.md)
