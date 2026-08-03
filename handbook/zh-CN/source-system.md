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
| `tls` | 可选。设 `enabled: true` 以使用 TCPS；用 `walletLocation` 指向 Instant Client wallet 目录（`caFile` 会被拒绝—Oracle 此处不使用 PEM CA 文件）。仅路径—禁止 inline PEM。见 [Security](security.md) |

真实 Oracle host 的 **Initial Load**（schema discovery + chunked snapshot）与 **LogMiner Incremental Capture** 都走 **OCI** 路径。Initial Load 以 PK 排序的 `OFFSET`/`FETCH` window 读取（由 `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE` 限制），而不是一次 unbounded full-table slam；见 [Operations](operations.md) 与 [CLI & Config](cli-and-config.md)。若 runtime 没有 Oracle Instant Client / OCI libraries，apply/sync 会以 LogMiner/OCI 名称 fail fast—不会默默退回 stub catalog。当 `tls.enabled: true` 时，连接字符串使用 TCPS，配置错误会明确失败（不静默回退 cleartext）。对 live Source 执行前请安装 Instant Client（Basic 或 Basic Light），并将 `LD_LIBRARY_PATH` 指向其目录。

在 live Source 上，Pipeline 的 `source.schema` 选择 Oracle owner；省略时平台以 Source `username`（大写）作为默认 schema。contract/stub harness 会忽略 schema，仅在 CI 切片使用**注入的 contract Source catalog**（`MIGRALOOP_CONTRACT_SOURCE_CATALOG` JSON 供 schema discovery + Initial Load；`MIGRALOOP_INJECT_LOGMINER_CONTENTS` 供 Incremental Capture）—不是 binary 内建的业务表 catalog、不是 Lab／真实路径的定义真相，也不是受支持的 production Source 机制。

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

**Local Sync Lab：** `migraloop lab up` 会布署可丢弃的 Oracle Source，并已满足 Lab 使用所需的 database-level prerequisites（ARCHIVELOG + database supplemental logging + sync-user grants）。`migraloop lab status` 会回报 Fixture 就绪状态，并标出 active 或 leftover 的 Scenario Namespace（或 `(none)`）。当 Lab Scenario（或你）创建 Pipeline 引用的表时，仍须套用 table-level supplemental logging—例如 `migraloop lab scenario run direct-pipeline`、`rt-project`、`rt-filter`、`rt-field-ops`、`transform-pipeline`、`concurrent-source-workload`、`bulk-load`、`idempotent-redelivery`、`pause-resume`、`remove-pipeline`、`change-pipeline`、`poison-quarantine`、`schema-change-pause` 、`source-alignment` 、`drift-check`、`bounded-backpressure`、`observability-surface`、`platform-store-guardrails`、、`backward-compatible-upgrades`、或 `initial-load-throttled`（各自包装在 `lab/scenarios/<id>/`，含 `recipe.yaml`）会在其 Scenario Namespace 表加上 `SUPPLEMENTAL LOG DATA (ALL) COLUMNS`，再走真实 `apply`（若 Scenario 驱动 Incremental Capture 则含 LogMiner `sync`；需要 host Instant Client / `LD_LIBRARY_PATH`）。重跑同一 Scenario 会先 drop 再重建那些 Namespace 表；`lab scenario remove` 可在不重跑的情况下清除。若要在 Scenario recipes 之外做 DB-level restore/load，请用 `lab/escape-hatch/oracle-load.sql`（含 table supplemental logging）搭配 Lab 连接细节，再接普通 `apply`／`sync`—不是第二套 Scenario 模型，也不是 CI。Lab 不会变更客户／生产环境数据库—Scenario `run` 会在 apply/sync 前拒绝非 Lab Fixture engines 的 Source/Target 绑定—且 Scenario catalog 为手动验证（不是 Release Quality Gate／CI suite）。嵌套 Docker／**Cursor Cloud** dockerd storage-driver 说明（`fuse-overlayfs` 或 `vfs`）：见 [Developer local setup](developer-local-setup.md) 与 [Deployment](deployment.md)。

### Contract LogMiner harness（测试 / 本地切片）

当 Source `host` 为 `contract` 或 `stub` 时，Incremental Capture 使用进程内 **LogMiner contract harness**。该 harness 的 prerequisite probes 由环境变量驱动（只读；从不变更数据库）：

| 变量 | 含义 | 默认 |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | database supplemental logging 的 `on` / `off` | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`（当前 contract Source catalog 内所有表）、空字符串，或已启用 PK/ALL logging 的逗号分隔表 | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | 报告的 redo retention（小时） | `72` |
| `MIGRALOOP_CONTRACT_SOURCE_CATALOG` | contract catalog 表的 JSON 文件路径，供 schema discovery + Initial Load（仅 CI／本地切片）。harness host 需要表时必须提供；未设置即为空 catalog | 未设置（空 catalog） |

## Required Privileges

Oracle sync 账号的具体 grants（ADR-0016）。Sync、Prerequisites probe 或 Delivery 配对 **不需要 DBA / SYSDBA**—请使用下方最小授权的专用 sync 用户。Source Prerequisites DDL（`ALTER DATABASE` / `ALTER TABLE` supplemental logging、ARCHIVELOG）由 DBA 另行套用；**不属于** sync 账号的 Required Privileges。

### v1 必要（Initial Load + LogMiner Incremental Capture + Prerequisites）

授予 Deployment `spec.source.username`（替换 `SYNC_USER` 与表名）：

```sql
-- Session
GRANT CREATE SESSION TO SYNC_USER;
GRANT ALTER SESSION TO SYNC_USER;

-- Initial Load + alignment 类读取（每个 Pipeline 引用表各一次）
GRANT SELECT ON <schema>.<table> TO SYNC_USER;

-- LogMiner Incremental Capture
GRANT LOGMINING TO SYNC_USER;              -- 若该 edition 有此 privilege
GRANT SELECT ANY TRANSACTION TO SYNC_USER;
GRANT EXECUTE_CATALOG_ROLE TO SYNC_USER;   -- DBMS_LOGMNR / DBMS_LOGMNR_D
GRANT SELECT_CATALOG_ROLE TO SYNC_USER;    -- capture 与 probe 使用的 dictionary + V$
```

`SELECT_CATALOG_ROLE` 覆盖 schema discovery、supplemental-logging probe、redo-retention probe 与 LogMiner contents 所需的 dictionary / fixed views，包括：

| 对象 | 用途 |
| --- | --- |
| `ALL_TAB_COLUMNS`、`ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS` | Schema discovery + primary-key identity |
| `ALL_LOG_GROUPS` | 表级 supplemental-logging Prerequisites |
| `V$DATABASE` | ARCHIVELOG / DB supplemental logging / current SCN |
| `V$ARCHIVED_LOG`、`V$PARAMETER` | Redo retention / archive-destination Prerequisites |
| `V$LOGMNR_CONTENTS`（及相关 LogMiner fixed views） | Incremental Capture |

若安全策略禁止 `SELECT_CATALOG_ROLE` / `EXECUTE_CATALOG_ROLE`，改授同等能力的更窄集合：

```sql
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO SYNC_USER;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO SYNC_USER;
GRANT SELECT ON V_$DATABASE TO SYNC_USER;
GRANT SELECT ON V_$ARCHIVED_LOG TO SYNC_USER;
GRANT SELECT ON V_$LOG TO SYNC_USER;
GRANT SELECT ON V_$LOGFILE TO SYNC_USER;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO SYNC_USER;
GRANT SELECT ON V_$PARAMETER TO SYNC_USER;
-- 外加上方的 Pipeline 表 SELECT 与 CREATE/ALTER SESSION
```

更窄路径上的 dictionary views：

- `ALL_TAB_COLUMNS`、`ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS`—sync 用户已能 `SELECT` 的表通常可直接读取（表级 `SELECT` 到位后无需额外 grant）。
- `ALL_LOG_GROUPS`—表级 supplemental-logging Prerequisites 需要。若你的 edition 在没有 catalog 访问时看不到这些行，请保留 `SELECT_CATALOG_ROLE`（或 DBA 提供的同等 dictionary grant）；单靠 `V_$…` 清单无法取代。

Edition 备注：部分旧版可能没有 `LOGMINING`—改用 `DBMS_LOGMNR` / `DBMS_LOGMNR_D` 的 `EXECUTE` 加上 `V_$…` SELECT。角色名称可能随 Oracle 版本而异；以上能力清单才是契约。

### 选用／生产 Sync 不需要

| Grant | 状态 |
| --- | --- |
| `CREATE TABLE`、`UNLIMITED TABLESPACE` | **仅 Lab**—Local Sync Lab Scenario 以 `SYNC_USER` 创建 Namespace 表。生产 sync 账号 **不需要** DDL。 |
| `DBA`、`SYSDBA`、`SELECT ANY TABLE` | **不需要。** Lab 或紧急破窗可用；不得当成生产环境文档默认。 |
| Source Prerequisites DDL（ARCHIVELOG、supplemental logging） | 由 **DBA** 账号套用—见上方 Source Prerequisites—不是 sync 用户。 |

**Local Sync Lab：** `lab/oracle/init/01-lab-source-prerequisites.sh` 授予必要集合，外加 Lab DDL（`CREATE TABLE` / `UNLIMITED TABLESPACE`）让 Scenario 拥有 Namespace 对象。将 Lab grants 视为生产 Required Privileges 的超集，而非生产默认。

此账号的密钥与 TLS：[Security](security.md)。Target Delivery 账号：[Target System](target-system.md)。

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
- Secrets、TLS 与 privilege 指引：[Security](security.md#required-privileges-pointer)
- MongoDB Delivery grants：[Target System](target-system.md#required-privileges-target)
- Developer 机器上的 Instant Client：[Developer 本地设置](developer-local-setup.md)
