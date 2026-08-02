# Deployment

一個 **Deployment** 恰好配對 **一個 Source System** 與 **一個 Target System**，並承載其間一或多條 **Pipeline**。若要不同的資料庫配對，請另建 Deployment—不要在同一個 Deployment 內做多資料庫 fan-in。

## 安裝形態（v1）

預設接近正式環境的安裝是 **一次安裝、兩個 container**：

| Service | 角色 |
| --- | --- |
| `platform-store` | 隨附的 PostgreSQL **Platform Store**（引擎由產品鎖定） |
| `app` | `migraloop` binary（`Dockerfile` 建置 release `migraloop-app`） |

在 repo 根目錄啟動：

```bash
docker compose up -d --build
```

Compose 會把 `MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@platform-store:5432/migraloop` 注入 app，並執行 `migraloop run`。可調整 Postgres volumes/resources，但不要更換 store 引擎。

若在 host 上對已 publish 的 store port `5432` 使用 Operator CLI：

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
migraloop migrate   # 若未使用 `run`
migraloop apply -f deployment.yaml
migraloop status
```

## Local Sync Lab Fixture

若要在可拋棄的真實堆疊上手動端到端驗證（ADR-0025），使用 **Local Sync Lab** Fixture。它在既有 Platform Store + app 安裝形態旁，再佈建 Lab 用的 Oracle 與 MongoDB：

```bash
migraloop lab up      # 在 repo 根目錄（或傳 --lab-dir）
migraloop lab status  # Fixture 就緒狀態 + active/leftover Scenario Namespace + 連線細節；沒有預設 Pipeline
migraloop lab scenario list
migraloop lab scenario run direct-pipeline   # 需要 host Instant Client（LD_LIBRARY_PATH）
migraloop lab scenario run rt-project   # Rich Transform project → Derived → Delivery
migraloop lab scenario run rt-filter   # Rich Transform filter → Derived → Delivery
migraloop lab scenario run transform-pipeline   # 多表 Transform → Derived → Delivery
migraloop lab scenario run concurrent-source-workload   # Scenario 內平行 Source contention
migraloop lab scenario run bulk-load   # ~100k Source inserts + lag/throughput/duration thresholds
migraloop lab scenario run idempotent-redelivery   # duplicate-safe Delivery re-apply
migraloop lab scenario run pause-resume   # pause/resume CLI verbs + catch-up Delivery
migraloop lab scenario remove direct-pipeline   # 清除 Namespace，不重跑
# 或：migraloop lab scenario run direct-pipeline --auto-remove
migraloop lab down    # 移除 containers 與 volumes
```

Compose 定義：`lab/compose.yaml`（project `migraloop-lab`）。Lab `app` image（`lab/Dockerfile`）會複製 host 建好的 `migraloop` binary，避免在 Docker 內重編；`migraloop lab up` 若缺少 binary 會先建置。Lab Oracle init 會啟用 ARCHIVELOG 與 database supplemental logging 以供 LogMiner；**不會**預先套用任何 Deployment 或 Pipelines—那些來自 Lab Scenario 或你自己的 `migraloop apply`。Catalog Scenarios 包裝在 `lab/scenarios/<id>/`（`recipe.yaml` + `deployment.yaml`）；`migraloop lab scenario list` 反映那些可選 recipes。目前 catalog 包含 `direct-pipeline`（Direct Pipeline insert/update/delete）、`rt-project`（Rich Transform `project`）、`rt-filter`（Rich Transform `filter`）、`transform-pipeline`（多表 customers + orders，Rich Transform `groupBy`/`sum` → Derived → Delivery），`concurrent-source-workload`（相同多表形狀，但在單一 Scenario 內以 recipe 驅動平行 Source sessions；跨 Scenario 並行仍禁止），`bulk-load`（約 100k Source inserts，經 Initial Load，並以可失敗的 lag／throughput／duration thresholds 等權檢查），、`idempotent-redelivery`（在真實 apply path 上做 duplicate-safe／idempotent re-Delivery，驗證 Managed Target 結果），以及 `pause-resume`（pause/resume CLI 動詞：一條 Pipeline 停止 Delivery、另一條繼續；resume 自耐久 Base catch-up）。`migraloop lab scenario list` 會回報 catalog-complete 與已出貨 capability gaps（`lab/scenarios/COVERAGE.md`）。各自會準備 Scenario Namespace、以真實 product path 套用（僅針對 Lab Fixture engines—Scenario `run` 會拒絕客戶／正式環境 Source/Target 綁定），並預設保留 Namespace 供即時 `base`/`derived`/`target` 檢查。重跑同一 Scenario 會先完整移除 Namespace 再重建；`scenario remove` 與 `--auto-remove` 分別提供手動與 opt-in 清理。與上方預設雙 container 安裝（root `Dockerfile`）、以及 CI 使用的 contract/stub harness／Release Quality Gate 都不同—由 operator 選擇 Scenarios；完整 catalog 不是 CI release gate（ADR-0025）。Feature-time 撰寫路徑：[Developer local setup](developer-local-setup.md)。

資源提醒：Lab Oracle（Free）通常需要數 GB RAM，第一次拉 image／開機可能要數分鐘。Lab Compose 使用 `network_mode: host`，以便在 bridge 網路被擋的巢狀 Docker 環境仍可運作。若巢狀 Docker 在 overlay whiteout 解壓失敗，可改用 dockerd `storage-driver: vfs`（並關閉 containerd snapshotter）。

### DB-level restore / load escape hatch

當你需要在 Lab Scenario recipe **之外**載入或還原資料—SQL dump、臨時 inserts，或 Mongo seed/restore—請使用可拋棄的 Lab engines，以及 `migraloop lab status`（或 `lab up`）印出的連線細節。這是通往真實堆疊的 escape hatch，**不是**第二套 Scenario 撰寫模型，也**不是** Release Quality Gate／CI。

範例位於 `lab/escape-hatch/`（`oracle-load.sql`、`mongo-load.js`，以及僅綁定 Lab 的 `deployment.yaml`，以便接回一般 product path）。沒有 `recipe.yaml`，此流程也**不要**執行 `migraloop lab scenario …`。

```bash
migraloop lab up
migraloop lab status   # 複製 Lab Oracle / Mongo / Platform Store 細節

# 載入 Lab Oracle（compose exec — 不需要 BYO 正式環境 Source）
docker compose -f lab/compose.yaml -p migraloop-lab exec -T oracle \
  sqlplus -s SYNC_USER/lab_oracle@FREEPDB1 < lab/escape-hatch/oracle-load.sql

# 載入 Lab Mongo（Target 端 seed／restore 式檢視）
docker compose -f lab/compose.yaml -p migraloop-lab exec -T mongo \
  mongosh --quiet --host 127.0.0.1 -u migraloop -p lab_mongo \
  --authenticationDatabase admin lab < lab/escape-hatch/mongo-load.js

# 接回真實 product path（apply / status / base / target / sync）
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=lab_oracle
export MONGO_PASSWORD=lab_mongo
# 對 Lab Oracle 做 apply/sync 需要 host Instant Client：
# export LD_LIBRARY_PATH=/path/to/instantclient
migraloop apply -f lab/escape-hatch/deployment.yaml
migraloop status
migraloop base --table LAB_ESCAPE_CUSTOMERS
migraloop target --collection lab_escape_customers   # Delivery 跑完後
migraloop sync                                       # 可選的 Incremental Capture 後續
```

**可選的 dump 工具還原**（相同 Lab 連線細節；仍不是 Scenario／不是 CI）。因 Lab Compose 使用 `network_mode: host`，host 工具可連 `127.0.0.1` Lab ports：

```bash
# MongoDB archive → Lab Mongo（範例；依 dump 路徑／ns 調整）
mongorestore \
  --uri 'mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin' \
  --archive=your-lab-seed.archive

# Oracle Data Pump → Lab Oracle（需能連到 Lab 的 client；
# 可拋棄 image 的 SYS 密碼為 lab_oracle_sys — 僅供 Lab）
impdp SYNC_USER/lab_oracle@//127.0.0.1:1521/FREEPDB1 \
  DUMPFILE=your_lab_seed.dmp DIRECTORY=DATA_PUMP_DIR TABLE_EXISTS_ACTION=REPLACE
```

若 dump restore 進 Lab Oracle 且之後要 sync，請套用 table-level supplemental logging（同 `oracle-load.sql`），再以綁定 Lab 的 Deployment 走一般 `migraloop apply`／`status`／`base`／`target`／`sync`。非 Delivery-managed 的 Target 端 load/restore 請用 Lab Mongo URI 以 mongosh 檢視；`migraloop target` 用於 product Delivery 之後的 collections。

請只在可拋棄 Fixture 上載入—絕不要把此 escape hatch 指向客戶／正式環境 engines。若要打包好的 correctness + metrics recipe，請優先用 Lab Scenarios；當你已有 SQL／JS／dumps 要自行放入 Lab 資料庫時，再用此路徑。

## Runtime 模型

- v1 以 **一個 active app instance**（內部可並行）加上 Platform Store 執行。
- 所有耐久 Deployment 狀態（Pipelines、Base/Derived Datasets、checkpoints）存在 Platform Store，替換 instance 才能續跑。
- 自動 multi-instance failover 屬後續階段；active processing 維持 single-leader（非 multi-writer）。

## 宣告 Deployment

設定為 YAML 或 JSON。必要頂層欄位：

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
    timezone: Asia/Taipei          # 可選；naive DATE/TIMESTAMP 後備
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines: []                    # 見 Pipeline 章節
```

v1 要求 `source.kind: oracle` 與 `target.kind: mongodb`。密碼必須是 secret reference—禁止明文。

以 `migraloop apply -f <file>` 套用。`pipelines` 為空時只套用 Deployment metadata（尚不 capture）。

## 相關章節

- Source 連線與 prerequisites：[Source System](source-system.md)
- Target Binding 與 Delivery：[Target System](target-system.md)
- Deployment 內的 Pipelines：[Pipeline](pipeline.md)
- 完整欄位/旗標清單：[CLI 與 Config 參考](cli-and-config.md)
