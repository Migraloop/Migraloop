# Observability

Operator 需要 Sync / Delivery 訊號、structured logs，以及（隨 Observability Surface 落地）附可告警 failure counters 的 Prometheus metrics（ADR-0008）。Distributed tracing / 廠商 APM 屬後續可選。

## 先跑這個

```bash
migraloop status
```

`status` 是目前主要的 Operator 迴圈。它會回報：

- **Platform Store** 可連線性 / 健康與 schema version（Platform Store Guardrails：過低的 Postgres 設定會被拒絕；可用磁碟低於 1 GiB 時印出 warn-only `WARN` — 絕不自動 pause Pipelines）
- 每個 **Deployment**（Source/Target 識別、LogMiner 機制：contract 或 OCI）
- 每條 **Pipeline**（mode、source 資料表、target collection、Delivery status）
- 每個 **Base Dataset**（status、列數、欄位、省略的不支援型別、Initial Load / cutover watermarks、**Sync Health** — `unknown` / `ok` / `lagging` / `failed` — 含 appliedChanges / lag / checkpoint、含 checked/mismatched 計數的 **Source Alignment**）
- 每條 Pipeline 的 **Delivery Health**（已套用變更 / lag / status；有 Poison Change quarantine 時為 `unhealthy`；有 blocking Schema Change pause 時為 `paused`；Downstream backpressure 下 lag 會上升但不會 pause Pipeline）
- 作用中的 **Quarantine** 列（Output Identity、change id、attempts、last error — unhealthy / not aligned）
- Transform Pipelines 的 **Derived Datasets**（若有）

第一次 sync 後 Operator 常看的健康跡象：

- Platform Store：`healthy`
- Base Dataset status 從 Initial Load 進入 incremental apply
- Sync Health 為 `ok`（或由 `lagging` 趨向 `ok`）— lag 不是長期單向成長
- 已設定 Target Binding 的 Delivery Health 顯示成功套用（`ok`，不是 `unhealthy` quarantine）
- Quarantine：`(none)`（除非刻意留下被 quarantine 的 poison identity）
- Schema Change：`(none)`（除非刻意留下 blocking DDL pause）

## 更深的檢查指令

| 指令 | 用途 |
| --- | --- |
| `migraloop base --table <TABLE>` | 抽樣 Platform Store 中的 Base Dataset 列 |
| `migraloop target --collection <name>` | 抽樣已 Deliver 的 MongoDB documents |
| `migraloop derived --pipeline <name>` | 抽樣 Derived Dataset 列 |

多個 Deployments 共用 table/collection/pipeline 名稱時，加上 `--deployment <name>`。

## Sync Health vs Delivery Health

- **Sync Health** — 從 Source capture 到 Base Dataset 是否跟上且成功套用（Incremental 進度後 lag 為 0 時為 `ok`；仍有 Source backlog 時為 `lagging`；耐久 capture/apply 失敗為 `failed`；尚未有 Incremental 進度為 `unknown`）。必要但不充分證明 Base 符合 Source。`status` 與 Prometheus 從同一個 Observability assembly 導出這些 labels。
- **Source Alignment** — 該 Base 上次 Source Alignment Check 結果（`unknown` / `aligned` / `partial`）。在把 Base 當作 Drift baseline 前執行 `migraloop align`（resource-gated；用 Source reads 修復 Base；從不寫入 Source）。`partial` 表示上次檢查碰到 `--max-rows` budget。
- **Delivery Health** — Pipeline 的 Target Binding change stream 是否跟上且成功套用。對 non-Managed 欄位的編輯與此訊號無關。Downstream backpressure 下，`lag=` 反映從 capture resume position 起算的剩餘 pending Delivery 工作（ADR-0020）—不是整條 Pipeline pause。Capture 一次仍最多只 materialize 一個 bounded queue window。
- **Drift** — 該 Pipeline 上次 Drift Check 結果（`unknown` / `ok` / `partial`）。在 Alignment 之後執行 `migraloop drift`（resource-gated；預設 Managed-field auto-repair；忽略 non-Managed fields）。`partial` 表示上次檢查碰到 `--max-rows` budget。

## Logs 與 metrics

- App/CLI 在 Initial Load、Incremental Capture、Delivery、Backpressure、Poison Change quarantine、blocking Schema Change，以及 Platform Store 可用磁碟警告會發出 **structured JSON** operator event lines（並保留 human-readable 對應行）（`migraloop` 行程 / container logs 的 stdout/stderr）。請找 `"event":"…"` 欄位，例如 `initial_load_progress`、`initial_load_paused`、`initial_load_backoff`、`initial_load_complete`、`incremental_capture`、`delivery_complete`、`backpressure`、`poison_quarantine`、`schema_change_blocked`、`platform_store_disk_warn`。
- `migraloop run` 會對已套用 Pipelines 持續執行 Incremental Capture → Delivery，並在同一個 single active instance 上於 `http://<metrics-addr>/metrics` 提供 Prometheus scrape endpoint（預設 `0.0.0.0:9090`，可用 `--metrics-addr` / `MIGRALOOP_METRICS_ADDR` 覆寫）。Compose 會公布 host port `9090`。Metrics 包含 Sync/Delivery lag（`migraloop_sync_lag`、`migraloop_delivery_lag`）、Pipeline pause、可告警 failure gauges（`migraloop_quarantined_changes`、`migraloop_failures`，皆自耐久 Platform Store state 讀取），以及 Platform Store disk gauges（`migraloop_platform_store_disk_free_bytes`、`migraloop_platform_store_disk_warn` — warn-only；絕不自動 pause）。
- `status` 仍是 Operator 解讀 lag/checkpoint/error 的主要迴圈；用 scrape `/metrics` 做 alerting 與 dashboards。

## 相關章節

- 短路徑：[從這裡開始](start-here.md)
- Day-2 失敗模式：[Operations](operations.md)
- 指令旗標：[CLI 與 Config 參考](cli-and-config.md)
