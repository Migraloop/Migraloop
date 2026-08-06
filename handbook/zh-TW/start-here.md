# 從這裡開始

從安裝到第一條 Pipeline，再到 Sync Health / Delivery Health 檢查的短路徑。細節在各功能章節—把本頁當主軸，而不是第二本手冊。

## 讀者

- **Operator**：安裝並執行 **Deployment**、撰寫 **Pipeline**、監看健康狀態。
- **Developer** 若要設定 monorepo，請直接看 [Developer 本機設定](developer-local-setup.md)。新增 Source 或 Target engine 請用該頁 checklist（interface + prerequisites／文件 + Lab Scenario + CI contract twin）——不要重寫 Sync／Rich Transform／Delivery／runtime。

v1 第一組引擎是 **Oracle → MongoDB**。一個 Deployment 恰好配對一個 Source System 與一個 Target System。

## 1. 安裝 app 與 Platform Store

預設安裝是 **一套 compose、兩個 container**：`migraloop` app 與 PostgreSQL **Platform Store**。

```bash
docker compose up -d --build
```

Compose 會為 app 設定 `MIGRALOOP_PLATFORM_STORE_URL`。entrypoint 執行 `migraloop run`（啟動時 migrate，對已套用的 Deployments/Pipelines 持續執行 Incremental Capture → Affect Analysis → Delivery，在 port `9090` 提供 Prometheus `/metrics`，然後維持行程）。Pipeline 使用的 Source/Target secret refs（例如 `ORACLE_PASSWORD` / `MONGO_PASSWORD`）必須存在於 app 行程環境中，continuous Sync 才能開啟 Source 並 Deliver。

若要可拋棄的 **Local Sync Lab** Fixture（Oracle + MongoDB + Platform Store + app，無預設 Deployment/Pipelines）：`migraloop lab up` / `status` / `down`。`lab status` 會回報 Fixture 就緒狀態，以及哪個 Scenario Namespace 為 active 或 leftover（或 `(none)`）。可選的 **Lab Scenarios**（catalog 來自 `lab/scenarios/<id>/recipe.yaml`；執行走 recipe-driven runner 真實 product path—recipe `product_path` 步驟共用 Namespace lifecycle + prepare／apply／sync；`namespace.lifecycle` 負責 wipe／seed（可選 mutate SQL）；thin hooks 負責 rare escapes；`checks.correctness` 為可執行 inspect vocabulary；poison／delay／fail-after／queue-capacity 等 Lab escapes 使用 typed SyncOptions CLI flags，Initial Load throttle／pause／store-delay knobs 使用 typed ApplyOptions CLI flags（不以 process env 作為主要 adapter）；例如 `migraloop lab scenario list` / `run direct-pipeline` / `run rt-project` / `run rt-filter` / `run rt-field-ops` / `run rt-equilookup` / `run rt-union` / `run rt-unwind` / `run rt-distinct-addtoset` / `run transform-pipeline`（groupBy sum/count/min/max/avg） / `run concurrent-source-workload` / `run change-ordering` / `run bulk-load` / `run idempotent-redelivery` / `run pause-resume` / `run remove-pipeline` / `run change-pipeline` / `run poison-quarantine` / `run schema-change-pause` / `run source-alignment` / `run drift-check` / `run bounded-backpressure` / `run observability-surface` / `run platform-store-guardrails` / `run backward-compatible-upgrades` / `run initial-load-throttled`）會在 Scenario Namespace 內走真實 apply/sync（Transform Scenarios 以建議的 Aggregation／SQL-like DX 撰寫—見 [Rich Transform](rich-transform.md)；classic steps 仍 Upgrade Compatible；執行期間 Lab 會暫停 Fixture `app` 以獨占 host Sync，結束後再恢復）；Scenario `run` 會在 apply/sync 前拒絕非 Lab／正式環境的 Source/Target engine 綁定。重跑會先 wipe Namespace，另可用 `scenario remove` / `--auto-remove` 清理。若要在 Scenario recipes 之外做 DB-level restore/load，請用 `lab/escape-hatch/` 搭配 Lab 連線細節，再接一般 `apply`／`status`／inspect—同樣不是 Release Quality Gate。手動驗證（ADR-0025）。巢狀 Docker／**Cursor Cloud** storage-driver 說明（`fuse-overlayfs` 或 `vfs`）：見 [Developer local setup](developer-local-setup.md) 與 [Deployment](deployment.md)。另見 [CLI 與 Config 參考](cli-and-config.md)。

細節：[Deployment](deployment.md) · 旗標與環境變數：[CLI 與 Config 參考](cli-and-config.md) · 密鑰/TLS：[Security](security.md)

## 2. 準備 Source System 與 Target System

在 apply/sync 之前：

1. 滿足 Oracle **Source Prerequisites**（supplemental logging、redo retention）與 **Required Privileges** — [Source System](source-system.md)。
2. 準備 Delivery 帳號可寫入的 MongoDB **Target System** — [Target System](target-system.md)。
3. Source System / Target System 密碼只能用 secret reference（`fromEnv` / `fromFile` / `fromDockerSecret`）— [Security](security.md)。

## 3. 套用含第一條 Pipeline 的 Deployment

撰寫宣告式 YAML/JSON Deployment（`apiVersion: migraloop.dev/v1`、`kind: Deployment`），包含 `spec.source`、`spec.target`，以及至少一條 Pipeline。然後：

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=...   # 名稱須符合你的 secret refs
export MONGO_PASSWORD=...

migraloop apply -f deployment.yaml
```

`apply` 會驗證設定、在 Pipeline 參照資料表時檢查 Source Prerequisites、視需要執行 **Initial Load** 寫入 **Base Dataset**，並把 Pipelines 記錄到 Platform Store。

- Direct Pipeline（一張 source 表 → Target Binding）：[Pipeline](pipeline.md)
- Transform Pipeline（宣告式 Rich Transform + Output Identity）：[Rich Transform](rich-transform.md)

## 4. Continuous Sync（以及可選的 one-shot catch-up）

Steady-state Sync 由長駐 app 負責：`apply` 之後，compose / `migraloop run` 實體會持續從耐久 checkpoint 繼續 **Incremental Capture**（Oracle LogMiner）寫入 Base Datasets、維護 Transform Pipeline 的 Derived Datasets，並把 Managed 欄位 **Deliver** 到 MongoDB—不需要外部 sync scheduler。

One-shot catch-up（Lab scenarios、Operator 手動 drain，或 `run` 不是 active path 時）：

```bash
migraloop sync
```

`sync` 會跑同一條 Incremental Capture → Affect Analysis → Delivery 路徑一次後結束。Continuous Sync 請優先用執行中的 app；`sync` 留給 Lab 與 catch-up。

## 5. 檢查 Sync Health 與 Delivery Health

```bash
migraloop status
```

查看 Platform Store 健康、Deployments、Pipelines、Base Dataset cutover/lag、**Sync Health** 與 **Delivery Health**。更細的檢查：

```bash
migraloop base --table ORDERS
migraloop target --collection orders
migraloop derived --pipeline orders_by_customer   # Transform Pipelines
```

如何解讀訊號：[Observability](observability.md) · 日常維運：[Operations](operations.md)

## 章節地圖

| 下一步 | 章節 |
| --- | --- |
| Source + Target 配對、安裝形態 | [Deployment](deployment.md) |
| Oracle 連線、prerequisites、型別 | [Source System](source-system.md) |
| MongoDB Target Binding / Managed Columns | [Target System](target-system.md) |
| Direct / Transform Pipelines | [Pipeline](pipeline.md) |
| 宣告式 operators 與 Affect Analysis | [Rich Transform](rich-transform.md) |
| Health、status、metrics 契約 | [Observability](observability.md) |
| Schema / poison / backpressure / upgrades | [Operations](operations.md) |
| 指令、旗標、設定欄位、環境變數 | [CLI 與 Config 參考](cli-and-config.md) |
| Secrets-by-reference 與 TLS | [Security](security.md) |
| Clone、build、本機測試 | [Developer 本機設定](developer-local-setup.md) |
