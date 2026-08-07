# Security

Operator 如何為 Source System、Target System 與 Platform Store 提供憑證並保護連線。

## Secrets by reference

憑證**不得**以明文存在 Pipeline/Deployment 文件，也不得把已解析的密鑰值寫進 Platform Store 列（ADR-0006）。v1 接受來自：

| 參照形式 | 設定形狀 | 解析方式 |
| --- | --- | --- |
| 環境變數 | `password: { fromEnv: NAME }` | apply/sync 時的 `std::env` |
| 掛載檔案 | `password: { fromFile: /path/to/secret }` | 檔案內容（去掉尾端換行） |
| Docker secret | `password: { fromDockerSecret: name }` | `/run/secrets/<name>` |

必須恰好設定 **一個** `fromEnv`、`fromFile` 或 `fromDockerSecret`。明文 password 字串會讓設定驗證以清楚錯誤失敗。

範例：

```yaml
password:
  fromEnv: ORACLE_PASSWORD
```

外部密鑰管理（Vault / cloud KMS）可於後續加入；若你在 runtime 注入 env 或檔案，v1 不要求它們也能安全上線。

Compose 中的 Platform Store URL 可能為隨附 lab 風格 store 內嵌本機密碼—正式環境的 store 憑證請用與任何 Postgres DSN 相同的方式保護（env / orchestrator secrets），且不要把 Source/Target 密碼貼進 YAML。

**Local Sync Lab** 可拋棄預設（`migraloop lab up`）刻意方便本機開發，並在 bring-up 後印出（`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store `migraloop`/`migraloop`）；`migraloop lab status` 也會一併顯示這些 Lab-only 連線細節與 active/leftover Scenario Namespace 狀態。Lab Compose 會把相同的 Lab-only secrets 注入 Fixture `app`，讓 continuous `migraloop run` Sync 能開啟 Source 並 Deliver。`lab scenario run` 期間 Lab 會暫停該 `app`，改由 host Scenario Sync 使用 Operator shell 環境中的相同 Lab-only secret refs，結束後再恢復 `app`。Recipe-driven Lab Scenario 執行（Transform Scenarios 的 `transform` 僅使用 Aggregation `$…` stages—見 [Rich Transform](rich-transform.md)；catalog Scenarios 宣告共用 `product_path` 步驟；`namespace.lifecycle` 負責 wipe／seed；thin hooks 負責 rare escapes；executable `checks.correctness`；poison／backpressure fault knobs 使用 typed SyncOptions CLI flags，Initial Load throttle／pause／store-delay knobs 使用 typed ApplyOptions CLI flags）與 Namespace 清理（`migraloop lab scenario run direct-pipeline|rt-project|rt-filter|rt-field-ops|rt-equilookup|rt-union|rt-unwind|rt-distinct-addtoset|transform-pipeline (groupBy sum/count/min/max/avg)|concurrent-source-workload|change-ordering|bulk-load|idempotent-redelivery|pause-resume|remove-pipeline|change-pipeline|poison-quarantine|schema-change-pause|source-alignment|drift-check|bounded-backpressure|observability-surface|platform-store-guardrails|backward-compatible-upgrades|initial-load-throttled …`、`remove`、`--auto-remove`）會以相同的 Lab-only secret references，對可拋棄堆疊跑真實 `apply`/`sync` 與 Fixture DB 清理（Scenario recipes 位於 `lab/scenarios/<id>/`）。DB-level restore/load escape hatch（`lab/escape-hatch/`）使用相同印出的 Lab credentials，對可拋棄 engines 跑 compose-exec sqlplus/mongosh（或 dump 工具），再接一般 `apply`／`sync`；它不是 Scenario，也絕不可指向客戶／正式環境資料庫。Lab secrets 僅供 Lab；絕不要把 Lab 指令或 Scenario 設定指向客戶正式環境資料庫。CLI 會在 Scenario `run` 強制此規則：Source/Target 若不是 Lab Fixture engines，會在 apply/sync 之前被拒絕—真實 Deployments 仍走一般的 `migraloop apply` / `migraloop sync`。`migraloop capacity-estimate` 僅供建議，永不修改 Source 或 Target 資料庫設定（ADR-0031）。巢狀 Docker／**Cursor Cloud** Lab bring-up storage-driver 說明：見 [Developer local setup](developer-local-setup.md) 與 [Deployment](deployment.md)。

## TLS / Connection Security

TLS **支援** Source、Target 與 Platform Store 連線，並在正式環境 **建議啟用**（ADR-0017）。本機/開發或明確選擇的環境仍允許 cleartext—v1 不會對每個非 TLS 連線硬性失敗。一旦請求 TLS，設定錯誤會在 apply/run 明確失敗，**不會靜默回退到 cleartext**。

### Source / Target（`spec.source.tls` / `spec.target.tls`）

各系統可選的區塊。省略該區塊（或設 `enabled: false`）即為 cleartext Lab/開發。

| 欄位 | Source（Oracle） | Target（MongoDB） | 說明 |
| --- | --- | --- | --- |
| `enabled` | `true` 要求 TCPS | `true` 要求 Mongo TLS | 省略時預設：停用（允許 cleartext） |
| `caFile` | **無效**（apply 會拒絕；請用 `walletLocation`） | CA 路徑（`tlsCAFile`） | 僅檔案系統路徑—絕不要把 PEM 貼進 YAML 或 `password` |
| `walletLocation` | Instant Client wallet 目錄 | **無效**（apply 會拒絕） | Oracle `MY_WALLET_DIRECTORY` |
| `insecureSkipVerify` | 可選（`SSL_SERVER_DN_MATCH=no`） | 可選（允許無效憑證） | 僅供開發/Lab；正式環境保持 `false` |

範例（路徑是參照，不是密鑰本體）：

```yaml
source:
  # ...
  tls:
    enabled: true
    walletLocation: /etc/oracle/wallet
target:
  # ...
  tls:
    enabled: true
    caFile: /etc/migraloop/certs/mongo-ca.pem
```

`migraloop status` 會顯示非密鑰的 TLS 旗標/路徑（`tls=enabled|disabled`、`caFile=…`、`walletLocation=…`），絕不印出 PEM 本體或密碼。

### Platform Store

在 `MIGRALOOP_PLATFORM_STORE_URL` 以 Postgres libpq 風格查詢參數設定 TLS：

| 參數 | 用途 |
| --- | --- |
| `sslmode=require` / `verify-ca` / `verify-full` | 要求 TLS（不回退 cleartext） |
| `sslmode=prefer` / `disable`（或省略） | 方便本機/開發的 cleartext |
| `sslrootcert=/path/to/ca.pem` | 驗證模式用的 CA 檔 |

範例：`postgres://migraloop:***@db:5432/migraloop?sslmode=require&sslrootcert=/run/certs/pg-ca.pem`

Operator 指引：

- 正式網路中優先為 Oracle、MongoDB、Postgres 使用可 TLS 的連線路徑
- 密鑰材料與憑證 PEM 本體不要進 shell history 或已提交的設定—使用掛載路徑與 secret references
- 限制 Source/Target 帳號的 Required Privileges（具體 grants 見下方）

## Required Privileges (pointer)

ADR-0016：記載並偏好剛好足以運作的最小權限—不是預設就要 DBA/admin。各引擎的具體 grants 放在連線章節：

| 帳號 | 章節 | 涵蓋 |
| --- | --- | --- |
| Oracle Source sync 使用者 | [Source System → Required Privileges](source-system.md#required-privileges) | Initial Load、LogMiner Incremental Capture、Prerequisites probe、schema discovery；必要 vs 僅 Lab vs 由 DBA 套用的 Prerequisites DDL |
| MongoDB Target Delivery 使用者 | [Target System → Required Privileges](target-system.md#required-privileges-target) | Delivery upsert/delete、Target 檢視；`readWrite` vs collection 範圍自訂 role；Lab root 不是正式預設 |

這些帳號只能用 secret reference（`fromEnv` / `fromFile` / `fromDockerSecret`）寫入 Deployment 設定—YAML 中禁止明文密碼。

## 公開環境變數面

| 變數 | 敏感度 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | 使用密碼 DSN 時含 store 憑證—以 orchestrator secrets 注入 |
| `fromEnv` 使用的名稱 | 密鑰值—永不提交 |

## 相關章節

- 設定形狀：[CLI 與 Config 參考](cli-and-config.md)
- 安裝預設：[Deployment](deployment.md)
- 本機 compose 密碼：[Developer 本機設定](developer-local-setup.md)
- 新增 Source／Target engine（Developer checklist）：[Developer 本機設定](developer-local-setup.md#新增-source-或-target-enginedeveloper-checklist)
- Oracle sync grants：[Source System](source-system.md#required-privileges)
- MongoDB Delivery grants：[Target System](target-system.md#required-privileges-target)
