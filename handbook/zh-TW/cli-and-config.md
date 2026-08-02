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

套用宣告式 Deployment 設定（YAML 或 JSON）。驗證 secrets-by-reference、Source/Target kinds、Pipeline specs、（當 Pipelines 參照資料表時）Source Prerequisites，視需要執行 Initial Load，並 upsert Deployment/Pipeline 狀態。

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

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `run`

啟動時 migrate，然後維持 app 行程運作（compose 預設 command）。

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

## 公開環境變數契約

| 變數 | 意義 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Operator CLI 與 compose `app` 使用的 Platform Store 連線 URL（`postgres://...`） |
| 設定中 `fromEnv` 參照的密鑰環境變數名 | 你在 `password.fromEnv` 寫的任何名稱（例如 `ORACLE_PASSWORD`、`MONGO_PASSWORD`）在 apply/sync 時必須存在於行程環境 |

### Contract-harness Source Prerequisite probes（僅 host `stub` / `contract`）

| 變數 | 意義 | 預設 |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | `on` / `off` | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`、空字串，或逗號分隔資料表 | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | 回報的 redo retention 小時數 | `72` |

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
| `host` | 是 | 是 | Source `stub`/`contract` → LogMiner harness |
| `port` | 是 | 是 | 有效 TCP port |
| `database` | 是 | 是 | |
| `username` | 是 | 是 | |
| `password` | 是 | 是 | 恰好一個 `fromEnv`、`fromFile`、`fromDockerSecret` |
| `timezone` | 可選 | n/a | IANA 或 `±HH:MM`，供 naive 時間 |

Docker secrets 從 `/run/secrets/<name>` 解析。

### Pipeline 項目（`spec.pipelines[]`）

| 欄位 | 說明 |
| --- | --- |
| `name` | 非空 |
| `mode` | `direct` 或 `transform` |
| `source.table` | 必要；可選 `source.schema` |
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
