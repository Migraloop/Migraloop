# Source System

**Source System** 是平台從中 capture 的使用者資料庫。v1 以 **Oracle** 搭配 **LogMiner** 做 **Incremental Capture**。連線識別是 `kind` + host/port/database/username，外加 password secret reference。

## 連線型態

在 Deployment 設定的 `spec.source` 下：

| 欄位 | 意義 |
| --- | --- |
| `kind` | v1 必須為 `oracle` |
| `host` | Oracle host。特殊值 `contract` 或 `stub` 會選用行程內 LogMiner contract harness（測試 / 本機切片）—不是真實 Oracle |
| `port` | TCP port（通常 `1521`） |
| `database` | Service / database 名稱 |
| `username` | Sync 帳號（最小 Required Privileges；不是預設就要 admin） |
| `password` | Secret reference：`fromEnv`、`fromFile` 或 `fromDockerSecret` |
| `timezone` | 可選 IANA 名稱或 Oracle 風格 offset（`+09:00` / `±HH:MM`）。`apply` 時接受。在 naive DATE/TIMESTAMP 需要解讀且 Source DB timezone 不可讀時使用 |
| `tls` | 可選。設 `enabled: true` 以使用 TCPS；用 `walletLocation` 指向 Instant Client wallet 目錄（`caFile` 會被拒絕—Oracle 此處不使用 PEM CA 檔）。僅路徑—禁止 inline PEM。見 [Security](security.md) |

真實 Oracle host 的 **Initial Load**（schema discovery + chunked snapshot）與 **LogMiner Incremental Capture** 都走 **OCI** 路徑。Initial Load 以 PK 排序的 `OFFSET`/`FETCH` window 讀取（由 typed ApplyOptions／Operator env `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE` 限制），而不是一次 unbounded full-table slam；見 [Operations](operations.md) 與 [CLI & Config](cli-and-config.md)。若 runtime 沒有 Oracle Instant Client / OCI libraries，apply/sync 會以 LogMiner/OCI 名稱 fail fast—不會默默退回 stub catalog。當 `tls.enabled: true` 時，連線字串使用 TCPS，設定錯誤會明確失敗（不靜默回退 cleartext）。對 live Source 執行前請安裝 Instant Client（Basic 或 Basic Light），並將 `LD_LIBRARY_PATH` 指向其目錄。

LogMiner Incremental Capture 會投影 `RS_ID` 與 `SSN`（連同 SCN），讓共享同一 SCN 的多列 contents 保持可區分、有序，且在行程重啟或 bounded capture window 後仍可 resume-safe—Platform Store 的 dedupe 與 checkpoint 不得跳過尚未套用的 same-SCN peers（prefer duplicates over gaps；見 [Operations](operations.md)）。

在 live Source 上，Pipeline 的 `source.schema` 選擇 Oracle owner；省略時平台以 Source `username`（大寫）作為預設 schema。contract/stub harness 會忽略 schema，僅在 CI 切片使用**注入的 contract Source catalog**（`MIGRALOOP_CONTRACT_SOURCE_CATALOG` JSON 供 schema discovery + Initial Load；`MIGRALOOP_INJECT_LOGMINER_CONTENTS` 供 Incremental Capture）—不是 binary 內建的業務資料表 catalog、不是 Lab／真實路徑的定義真相，也不是受支援的 production Source 機制。

## Source Prerequisites（Oracle / LogMiner）

在 **Initial Load** 或 **Incremental Capture** 之前，平台會驗證 **Source Prerequisites**；未滿足時以清楚錯誤 **fail fast**（ADR-0021）。平台**不會**自動修改 Source System 設定來「修好」這些檢查。

### 1. Database supplemental logging

在 database 層啟用 minimum supplemental logging：

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

沒有這個設定，LogMiner 無法可靠重建 change vectors。

### 2. Table-level key supplemental logging

對每一張被 Pipeline 參照的資料表，啟用 PRIMARY KEY 或 ALL COLUMNS supplemental logging：

```sql
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
-- 或當資料表沒有可用 PK / 需要完整 before-images 時：
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

缺少 table-level logging 會導致該表 Incremental Capture 不完整或不正確。

### 3. 足夠的 redo / archive retention

至少保留 **24 小時** redo（online + archived），讓 Initial Load overlap、Incremental Capture lag，以及行程重啟後的 resume 仍能讀到需要的變更歷史。依你的 Oracle edition 設定 archive destination retention / FRA policy。

Live OCI probe 要求 **ARCHIVELOG** 模式。可讀時會回報可用 archived-redo 時間跨度；若跨度仍短於 24 小時（例如剛備妥的 Lab Source），但已設定 `db_recovery_file_dest` 或 `log_archive_dest_1`，probe 會視為符合文件化底線。**NOARCHIVELOG** 會 fail fast。若 redo 在平台消費前就過期，變更會遺失—平台寧可不跑，也不做不完整 capture。

### Operator 工作流程

1. 以 DBA / 具備權限的 Operator 在 Source System 上套用上述 SQL（或等效設定）。
2. 確認 sync 使用者的 grants（見下方 Required Privileges）。
3. 執行 `migraloop apply` / continuous `migraloop run`（或 one-shot `migraloop sync`）。未滿足的 prerequisites 會在執行前失敗並指出缺什麼。
4. 修好指名的 Oracle 設定後再重跑。平台絕不會自動執行 `ALTER DATABASE` / `ALTER TABLE` 來「修復」失敗。

**Local Sync Lab：** `migraloop lab up` 會佈建可拋棄的 Oracle Source，並已滿足 Lab 使用所需的 database-level prerequisites（ARCHIVELOG + database supplemental logging + sync-user grants）。`migraloop lab status` 會回報 Fixture 就緒狀態，並標出 active 或 leftover 的 Scenario Namespace（或 `(none)`）。當 recipe-driven Lab Scenario 執行（共用 `product_path` + `namespace.lifecycle` 負責 Namespace wipe／seed；executable `checks.correctness` 負責 isomorphic inspect；poison／backpressure Lab escapes 使用 typed SyncOptions CLI flags，Initial Load knobs 使用 typed ApplyOptions；或你）建立 Pipeline 參照的資料表時，仍須套用 table-level supplemental logging—例如 `migraloop lab scenario run direct-pipeline`、`rt-project`、`rt-filter`、`rt-field-ops`、`rt-equilookup`、`rt-union`、`rt-unwind`、`rt-distinct-addtoset`、`transform-pipeline`（`$group` sum/count/min/max/avg；僅 Aggregation `$…` 撰寫—見 [Rich Transform](rich-transform.md)）、`concurrent-source-workload`、`change-ordering`、`bulk-load`、`mega-mix`、`idempotent-redelivery`、`pause-resume`、`remove-pipeline`、`change-pipeline`、`poison-quarantine`、`schema-change-pause` 、`source-alignment` 、`drift-check`、`bounded-backpressure`、`observability-surface`、`platform-store-guardrails`、、`backward-compatible-upgrades`、或 `initial-load-throttled`（各自包裝在 `lab/scenarios/<id>/`，含 `recipe.yaml`）會在其 Scenario Namespace 資料表加上 `SUPPLEMENTAL LOG DATA (ALL) COLUMNS`，再走真實 `apply`（若 Scenario 驅動 Incremental Capture 則含 LogMiner `sync`；需要 host Instant Client / `LD_LIBRARY_PATH`）。重跑同一 Scenario 會先 drop 再重建那些 Namespace 資料表；`lab scenario remove` 可在不重跑的情況下清除。若要在 Scenario recipes 之外做 DB-level restore/load，請用 `lab/escape-hatch/oracle-load.sql`（含 table supplemental logging）搭配 Lab 連線細節，再接一般 `apply`／`sync`—不是第二套 Scenario 模型，也不是 CI。Lab 不會變更客戶／正式環境資料庫—Scenario `run` 會在 apply/sync 前拒絕非 Lab Fixture engines 的 Source/Target 綁定—且 Scenario catalog 為手動驗證（不是 Release Quality Gate／CI suite）。Lab Fixture 提高 Oracle `shm_size` 以支援吞吐證據；`migraloop capacity-estimate` 永不修改 Source 設定（ADR-0031）。巢狀 Docker／**Cursor Cloud** dockerd storage-driver 說明（`fuse-overlayfs` 或 `vfs`）：見 [Developer local setup](developer-local-setup.md) 與 [Deployment](deployment.md)。

### Contract LogMiner harness（測試 / 本機切片）

當 Source `host` 為 `contract` 或 `stub` 時，Incremental Capture 使用行程內 **LogMiner contract harness**。該 harness 的 prerequisite probes 由環境變數驅動（唯讀；從不變更資料庫）：

| 變數 | 意義 | 預設 |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | database supplemental logging 的 `on` / `off` | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`（目前 contract Source catalog 內所有資料表）、空字串，或已啟用 PK/ALL logging 的逗號分隔資料表 | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | 回報的 redo retention（小時） | `72` |
| `MIGRALOOP_CONTRACT_SOURCE_CATALOG` | contract catalog 資料表 JSON 檔路徑，供 schema discovery + Initial Load（僅 CI／本機切片）。harness host 需要資料表時必須提供；未設定即為空 catalog | 未設定（空 catalog） |

## Required Privileges

Oracle sync 帳號的具體 grants（ADR-0016）。Sync、Prerequisites probe 或 Delivery 配對 **不需要 DBA / SYSDBA**—請使用下方最小授權的專用 sync 使用者。Source Prerequisites DDL（`ALTER DATABASE` / `ALTER TABLE` supplemental logging、ARCHIVELOG）由 DBA 另行套用；**不屬於** sync 帳號的 Required Privileges。

### v1 必要（Initial Load + LogMiner Incremental Capture + Prerequisites）

授予 Deployment `spec.source.username`（替換 `SYNC_USER` 與資料表名稱）：

```sql
-- Session
GRANT CREATE SESSION TO SYNC_USER;
GRANT ALTER SESSION TO SYNC_USER;

-- Initial Load + alignment 類讀取（每個 Pipeline 參照資料表各一次）
GRANT SELECT ON <schema>.<table> TO SYNC_USER;

-- LogMiner Incremental Capture
GRANT LOGMINING TO SYNC_USER;              -- 若該 edition 有此 privilege
GRANT SELECT ANY TRANSACTION TO SYNC_USER;
GRANT EXECUTE_CATALOG_ROLE TO SYNC_USER;   -- DBMS_LOGMNR / DBMS_LOGMNR_D
GRANT SELECT_CATALOG_ROLE TO SYNC_USER;    -- capture 與 probe 使用的 dictionary + V$
```

`SELECT_CATALOG_ROLE` 涵蓋 schema discovery、supplemental-logging probe、redo-retention probe 與 LogMiner contents 所需的 dictionary / fixed views，包括：

| 物件 | 用途 |
| --- | --- |
| `ALL_TAB_COLUMNS`、`ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS` | Schema discovery + primary-key identity |
| `ALL_LOG_GROUPS` | 資料表層級 supplemental-logging Prerequisites |
| `V$DATABASE` | ARCHIVELOG / DB supplemental logging / current SCN |
| `V$ARCHIVED_LOG`、`V$PARAMETER` | Redo retention / archive-destination Prerequisites |
| `V$LOGMNR_CONTENTS`（及相關 LogMiner fixed views） | Incremental Capture |

若安全政策禁止 `SELECT_CATALOG_ROLE` / `EXECUTE_CATALOG_ROLE`，改授同等能力的較窄集合：

```sql
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO SYNC_USER;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO SYNC_USER;
GRANT SELECT ON V_$DATABASE TO SYNC_USER;
GRANT SELECT ON V_$ARCHIVED_LOG TO SYNC_USER;
GRANT SELECT ON V_$LOG TO SYNC_USER;
GRANT SELECT ON V_$LOGFILE TO SYNC_USER;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO SYNC_USER;
GRANT SELECT ON V_$PARAMETER TO SYNC_USER;
-- 外加上方的 Pipeline 資料表 SELECT 與 CREATE/ALTER SESSION
```

較窄路徑上的 dictionary views：

- `ALL_TAB_COLUMNS`、`ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS`—sync 使用者已能 `SELECT` 的資料表通常可直接讀取（表級 `SELECT` 到位後無需額外 grant）。
- `ALL_LOG_GROUPS`—資料表層級 supplemental-logging Prerequisites 需要。若你的 edition 在沒有 catalog 存取時看不到這些列，請保留 `SELECT_CATALOG_ROLE`（或 DBA 提供的同等 dictionary grant）；單靠 `V_$…` 清單無法取代。

Edition 備註：部分舊版可能沒有 `LOGMINING`—改用 `DBMS_LOGMNR` / `DBMS_LOGMNR_D` 的 `EXECUTE` 加上 `V_$…` SELECT。角色名稱可能隨 Oracle 版本而異；以上能力清單才是契約。

### 選用／正式 Sync 不需要

| Grant | 狀態 |
| --- | --- |
| `CREATE TABLE`、`UNLIMITED TABLESPACE` | **僅 Lab**—Local Sync Lab Scenario 以 `SYNC_USER` 建立 Namespace 資料表。正式 sync 帳號 **不需要** DDL。 |
| `DBA`、`SYSDBA`、`SELECT ANY TABLE` | **不需要。** Lab 或緊急破窗可用；不得當成正式環境文件預設。 |
| Source Prerequisites DDL（ARCHIVELOG、supplemental logging） | 由 **DBA** 帳號套用—見上方 Source Prerequisites—不是 sync 使用者。 |

**Local Sync Lab：** `lab/oracle/init/01-lab-source-prerequisites.sh` 授予必要集合，外加 Lab DDL（`CREATE TABLE` / `UNLIMITED TABLESPACE`）讓 Scenario 擁有 Namespace 物件。將 Lab grants 視為正式 Required Privileges 的超集，而非正式預設。

此帳號的密鑰與 TLS：[Security](security.md)。Target Delivery 帳號：[Target System](target-system.md)。

## Supported Source Types（v1）

Schema discovery 之後，Sync 只把 allow-list 內的 Oracle 型別轉入 Platform Store（ADR-0018、ADR-0023）：

- **Allow-list：** `NUMBER`（precision/scale 規則）、`FLOAT` / `BINARY_FLOAT` / `BINARY_DOUBLE`、`CHAR` / `NCHAR` / `VARCHAR2` / `NVARCHAR2`、`DATE`、`TIMESTAMP`（含 WITH TIME ZONE / LOCAL TIME ZONE）、`RAW`（有 size cap），以及上述的 nullable 形式。
- **Out of scope：** `BLOB`、`CLOB`、`NCLOB`、`BFILE`、`LONG` / `LONG RAW`、`XMLType`、object types、nested tables / VARRAYs、`ROWID` / `UROWID` 與其他特殊型別。

不支援的欄位會從 Base Dataset **省略**（資料表仍會 sync）；省略情況可在 `migraloop status` 看到。若 Pipeline 需要不支援欄位則無法使用—絕不做默默 coercion。

**NUMBER：** 在安全時對應到保精度的 Mongo 型別（`NumberLong` / `Decimal128`）。Capture JSON 對 `scale > 0` 會保留固定小數位（例如 scale 2 的 `10` 會變成 `"10.00"`），讓 Managed decimals 對 Delivery 安全。Schema 不安全的 NUMBER 欄位必須在設定時以 Pipeline `fields`（`as: string` 或 `as: omit`）解決—不是在 runtime 逐列 quarantine。

**時間型別：** 平台內部使用 UTC。具時區值會變成絕對瞬間。Naive DATE/TIMESTAMP 在可讀時使用 Source DB timezone，否則使用設定的 Source `timezone`（IANA 名稱或 Oracle 風格 `±HH:MM`）。

## 哪些資料表會被 capture

Sync 依 **Pipeline 參照** 選擇資料表—不是整 schema mirror。每張納入的表在 Deployment 內至多一個共用 **Base Dataset**（完整 supported-type 列），供所有需要它的 Pipeline 重用。新增被參照的表只對該表做 **table-level Initial Load**。

## Live Oracle 驗證（CLI operator seam）

在真實 Oracle Source 上（已安裝 Instant Client，且 Source Prerequisites 已滿足），Operator 可不經 mock 驗證 Sync→Delivery：

1. 將 `spec.source.host` / `port` / `database` / `username` 指向 live Source（不要用 `contract`/`stub`）。
2. `migraloop apply -f deployment.yaml` — Initial Load 從 live 表讀入 Base Datasets，並把 Direct Pipeline Deliver 到 MongoDB。
3. 在已啟用 supplemental logging 的情況下變更 Source 列（`INSERT` / `UPDATE` / `DELETE`）。
4. `migraloop sync` — LogMiner (OCI) Incremental Capture 套用變更；MongoDB 上的 Managed 欄位會反映這些變更。
5. 用 `migraloop status`、`migraloop base --table <TABLE>`、`migraloop target --collection <NAME>` 檢視。

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

## 新增另一個 Source engine（Developers）

v1 僅出貨 Oracle LogMiner。新的 Source kind 實作 `SourceEngine`，discovered columns 使用 engine-agnostic `data_type`／shared `ColumnShape`；Oracle allow-list、size caps、與 type-brand 命名留在 adapter-private（ADR-0018）。NUMBER→Mongo classification 只放在 `migraloop-types` 的 `ColumnShape` 旁（ADR-0023）——不要在 capture／delivery 再留 twin helpers。另需補 prerequisites／文件、Lab Scenario，以及 CI contract twin，且不重塑 Sync／Rich Transform／Delivery／runtime 概念——見 [Developer 本機設定 — 新增 Source 或 Target engine](developer-local-setup.md#新增-source-或-target-enginedeveloper-checklist)。

## 相關章節

- 與 Target 配對：[Deployment](deployment.md)
- 參照資料表的 Pipelines：[Pipeline](pipeline.md)
- Secrets、TLS 與 privilege 指引：[Security](security.md#required-privileges-pointer)
- MongoDB Delivery grants：[Target System](target-system.md#required-privileges-target)
- Developer 機器上的 Instant Client：[Developer 本機設定](developer-local-setup.md)
