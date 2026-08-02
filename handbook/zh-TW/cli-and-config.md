# CLI 與 Config 參考

給 Operator 的指令、旗標、環境變數與 Deployment 設定欄位。

## Binary

```text
migraloop <subcommand> [flags]
```

由 `crates/app` 建置（`Dockerfile` release binary）。所有與 Platform Store 通訊的 Operator 子指令都接受 `--platform-store-url` 或下方環境變數。

## Operator CLI 子指令

`migraloop` Operator CLI 目前提供這些子指令：

### `migrate`

套用版本化 Platform Store schema migrations。

```bash
migraloop migrate --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `apply`

套用宣告式 Deployment 設定（YAML 或 JSON）。驗證 secrets-by-reference、Source/Target kinds、Pipeline specs、（當 Pipelines 參照資料表時）Source Prerequisites，視需要執行 schema discovery + Initial Load，並 upsert Deployment/Pipeline 狀態。

在真實 Oracle Source host（非 `contract`/`stub`）上，apply 會透過 OCI 從 live Source 做 schema discovery 與 Initial Load（需要 Instant Client；見 [Source System](source-system.md)）。contract/stub host 仍使用行程內 fixture catalog（CI 切片）。

```bash
migraloop apply --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" -f deployment.yaml
```

| 旗標 | 意義 |
| --- | --- |
| `-f`, `--file` | Deployment 設定路徑 |

### `status`

回報 Platform Store 健康、Deployments、Pipelines、Base Datasets、Sync Health、Delivery Health 與 Derived Datasets。

```bash
migraloop status --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `base`

檢視某個 Source 資料表的 Base Dataset 列。

```bash
migraloop base --table ORDERS [--deployment oracle-to-mongo]
```

| 旗標 | 意義 |
| --- | --- |
| `--table` | Source 資料表名稱（必要） |
| `--deployment` | 多個 Bases 共用表名時消歧義 |

### `target`

檢視某個 Pipeline collection 的 Target 文件。

```bash
migraloop target --collection orders [--deployment oracle-to-mongo]
```

| 旗標 | 意義 |
| --- | --- |
| `--collection` | Target collection 名稱（必要） |
| `--deployment` | 共用 collection 名稱時消歧義 |

### `derived`

檢視 Transform Pipeline 的 Derived Dataset 列。

```bash
migraloop derived --pipeline orders_by_customer [--deployment oracle-to-mongo]
```

| 旗標 | 意義 |
| --- | --- |
| `--pipeline` | Pipeline 名稱（必要） |
| `--deployment` | 共用 Pipeline 名稱時消歧義 |

### `sync`

執行 Incremental Capture 寫入 Base Datasets、維護 Derived Datasets，然後 Delivery。

Oracle Incremental Capture 一律走 LogMiner：真實 host 使用 **LogMiner (OCI)**；`host: contract` / `stub` 使用行程內 contract harness。真實 host **不會** silent fallback 到 stub catalog。缺少 Instant Client 或 OCI 失敗時會以 LogMiner/OCI 名稱 fail fast。

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `run`

啟動時 migrate，然後維持 app 行程運作（compose 預設 command）。

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `lab`

Local Sync Lab Fixture 與 Lab Scenarios（ADR-0025）。佈建可拋棄的真實堆疊—Oracle Source（Lab 已滿足的 Source Prerequisites）、MongoDB Target、Platform Store 與 app。Bring-up **不會**套用 sample Deployment 或 Pipelines。Operator 接著可 list/run 可選的 **Lab Scenarios**，在 **Scenario Namespace** 內以真實 product path 套用 Deployment 並驅動 Sync/Delivery。需要 Docker Compose 與 repo 的 `lab/` 目錄（或 `--lab-dir`）。Scenario 的 `apply`/`sync` 需要 host 上的 Oracle Instant Client（`LD_LIBRARY_PATH`）。

```bash
migraloop lab up [--lab-dir lab]
migraloop lab status [--lab-dir lab]
migraloop lab down [--lab-dir lab]
migraloop lab scenario list [--lab-dir lab]
migraloop lab scenario run <scenario-id> [--lab-dir lab] [--auto-remove]
migraloop lab scenario remove <scenario-id> [--lab-dir lab]
```

| Subcommand | 意義 |
| --- | --- |
| `up` | 啟動可拋棄 Fixture；就緒時印出連線細節 |
| `status` | 回報 Fixture 就緒狀態（engines + Oracle prerequisites + Platform Store）。在你套用設定或執行 Lab Scenario 之前顯示 `Deployment: (none)` / `Pipeline: (none)` |
| `down` | 拆除 containers 與 volumes |
| `scenario list` | 列出 catalog 中可選的 Lab Scenarios（例如 `direct-pipeline`、`transform-pipeline`、`concurrent-source-workload`） |
| `scenario run` | 依 id 執行一個 Lab Scenario。若已有 Scenario 正在執行則拒絕。重跑同一 Scenario 會先完整移除其 Namespace 再重建。回報 pass/fail 以及 `duration_ms`、rows/throughput，以及 Scenario 定義的 thresholds（例如 settle time，若有）（correctness 與 operational metrics 等權）。`concurrent-source-workload` 在單一 Scenario 內跑平行 Source sessions；第二個 Scenario run 仍會被拒絕。預設 keep-on-finish 保留 Namespace 供即時 `base`/`derived`/`target` 檢查；成功後若要刪除可傳 `--auto-remove` |
| `scenario remove` | 完整移除 Scenario Namespace（Source tables、Target collections、Platform Store Deployment），且不啟動 run。若已有 Scenario 作用中則拒絕。已不存在時為 idempotent |

| Flag | 意義 |
| --- | --- |
| `--lab-dir` | 含 Lab `compose.yaml` 的目錄（預設：`lab`） |
| `--auto-remove` | 僅用於 `scenario run`：成功結束後完整移除 Scenario Namespace（opt-in；失敗時仍保留 Namespace 以便除錯） |

Lab 是手動驗證—不是 Release Quality Gate，也不是 contract/stub LogMiner harness。見 [Deployment](deployment.md) 與 [Developer local setup](developer-local-setup.md)。

## 公開環境變數契約

| 變數 | 意義 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Operator CLI 與 compose `app` 使用的 Platform Store 連線 URL（`postgres://...`） |
| 設定中 `fromEnv` 參照的密鑰環境變數名 | 你在 `password.fromEnv` 寫的任何名稱（例如 `ORACLE_PASSWORD`、`MONGO_PASSWORD`）在 apply/sync 時必須存在於行程環境 |
| `LD_LIBRARY_PATH` | 真實 Oracle host：Oracle Instant Client libraries 目錄（apply/sync runtime 需要；`contract`/`stub` 不使用） |
| Lab disposable defaults | `migraloop lab up` 之後：`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store URL `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Mongo URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`（僅本機 Lab） |

### Contract-harness Source Prerequisite probes（僅 host `stub` / `contract`）

行程內 LogMiner harness 的環境變數名稱與預設見 [Source System](source-system.md)（`MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_REDO_RETENTION_HOURS`）。

## Deployment 設定契約

| 欄位 | 必要 | 說明 |
| --- | --- | --- |
| `apiVersion` | 是 | `migraloop.dev/v1` |
| `kind` | 是 | `Deployment` |
| `metadata.name` | 是 | 非空 Deployment 名稱 |
| `spec.source` | 是 | 見下 |
| `spec.target` | 是 | 見下 |
| `spec.pipelines` | 否 | 預設 `[]`（只套用 Deployment） |

### `spec.source` / `spec.target`

| 欄位 | Source | Target | 說明 |
| --- | --- | --- | --- |
| `kind` | `oracle` | `mongodb` | v1 固定配對 |
| `host` | 是 | 是 | Source `stub`/`contract` → LogMiner harness + fixture Initial Load；其他 host → live OCI Initial Load + LogMiner |
| `port` | 是 | 是 | 有效 TCP port |
| `database` | 是 | 是 | |
| `username` | 是 | 是 | 省略 Pipeline `source.schema` 時，也作為預設 Oracle schema/owner |
| `password` | 是 | 是 | 恰好一個 `fromEnv`、`fromFile`、`fromDockerSecret` |
| `timezone` | 可選 | n/a | IANA 或 `±HH:MM`，供 naive 時間 |

Docker secrets 從 `/run/secrets/<name>` 解析。

### Pipeline 項目（`spec.pipelines[]`）

| 欄位 | 說明 |
| --- | --- |
| `name` | 非空 |
| `mode` | `direct` 或 `transform` |
| `source.table` | 必要；可選 `source.schema`（live Oracle owner；預設為 Source `username`） |
| `target.collection` | Target Binding；僅 Base-only 實驗可省略 |
| `fields` | 欄位 → `{ as: string \| omit }` 的對應 |
| `outputIdentity` | `transform` 必要 |
| `transform` | 宣告式步驟；`transform` mode 必要；`direct` 禁止 |

最小 Direct 範例：

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

## 相關章節

- 短路徑：[從這裡開始](start-here.md)
- Secrets 與 TLS：[Security](security.md)
- 章節深讀：[Deployment](deployment.md)、[Pipeline](pipeline.md)、[Source System](source-system.md)
