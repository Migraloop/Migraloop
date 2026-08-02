# 從這裡開始

從安裝到第一條 Pipeline，再到 Sync Health / Delivery Health 檢查的短路徑。細節在各功能章節—把本頁當主軸，而不是第二本手冊。

## 讀者

- **Operator**：安裝並執行 **Deployment**、撰寫 **Pipeline**、監看健康狀態。
- **Developer** 若要設定 monorepo，請直接看 [Developer 本機設定](developer-local-setup.md)。

v1 第一組引擎是 **Oracle → MongoDB**。一個 Deployment 恰好配對一個 Source System 與一個 Target System。

## 1. 安裝 app 與 Platform Store

預設安裝是 **一套 compose、兩個 container**：`migraloop` app 與 PostgreSQL **Platform Store**。

```bash
docker compose up -d --build
```

Compose 會為 app 設定 `MIGRALOOP_PLATFORM_STORE_URL`。entrypoint 執行 `migraloop run`（啟動時 migrate，然後維持行程）。

若要可拋棄的 **Local Sync Lab** Fixture（Oracle + MongoDB + Platform Store + app，無預設 Deployment/Pipelines）：`migraloop lab up` / `status` / `down`。可選的 **Lab Scenarios**（例如 `migraloop lab scenario list` / `run direct-pipeline`）會在 Scenario Namespace 內走真實 apply/sync；重跑會先 wipe Namespace，另可用 `scenario remove` / `--auto-remove` 清理 — 見 [Deployment](deployment.md) 與 [CLI 與 Config 參考](cli-and-config.md)。

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

## 4. 執行 Incremental Capture 與 Delivery

```bash
migraloop sync
```

`sync` 會從耐久 checkpoint 繼續 **Incremental Capture**（Oracle LogMiner）寫入 Base Datasets、維護 Transform Pipeline 的 Derived Datasets，並把 Managed 欄位 **Deliver** 到 MongoDB。

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
