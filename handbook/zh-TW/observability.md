# Observability

Operator 需要 Sync / Delivery 訊號、structured logs，以及（隨 Observability Surface 落地）附可告警 failure counters 的 Prometheus metrics（ADR-0008）。Distributed tracing / 廠商 APM 屬後續可選。

## 先跑這個

```bash
migraloop status
```

`status` 是目前主要的 Operator 迴圈。它會回報：

- **Platform Store** 可連線性 / 健康與 schema version
- 每個 **Deployment**（Source/Target 識別、LogMiner 機制：contract 或 OCI）
- 每條 **Pipeline**（mode、source 資料表、target collection、Delivery status）
- 每個 **Base Dataset**（status、列數、欄位、省略的不支援型別、Initial Load / cutover watermarks、含 appliedChanges / lag / checkpoint 的 **Sync Health**）
- 每條 Pipeline 的 **Delivery Health**（已套用變更 / status）
- Transform Pipelines 的 **Derived Datasets**（若有）

第一次 sync 後 Operator 常看的健康跡象：

- Platform Store：`healthy`
- Base Dataset status 從 Initial Load 進入 incremental apply
- Sync Health lag 趨向追上（不是長期單向成長）
- 已設定 Target Binding 的 Delivery Health 顯示成功套用

## 更深的檢查指令

| 指令 | 用途 |
| --- | --- |
| `migraloop base --table <TABLE>` | 抽樣 Platform Store 中的 Base Dataset 列 |
| `migraloop target --collection <name>` | 抽樣已 Deliver 的 MongoDB documents |
| `migraloop derived --pipeline <name>` | 抽樣 Derived Dataset 列 |

多個 Deployments 共用 table/collection/pipeline 名稱時，加上 `--deployment <name>`。

## Sync Health vs Delivery Health

- **Sync Health** — 從 Source capture 到 Base Dataset 是否跟上且成功套用。必要但不充分證明與 source 位元級對齊（見領域文件中的 Source Alignment Check / 後續 Operations 深度）。
- **Delivery Health** — Pipeline 的 Target Binding change stream 是否跟上且成功套用。對 non-Managed 欄位的編輯與此訊號無關。

## Logs 與 metrics

- App/CLI 在 Initial Load、Incremental Capture、Delivery 與失敗時輸出結構化維運訊息（`migraloop` 行程 / container logs 的 stdout/stderr）。
- Prometheus scrape endpoint 與告警 counters 屬 Observability Surface **契約**（ADR-0008），尚非主要 Operator 介面—在你的 build 提供 metrics 之前，請使用 `status` 的 lag/checkpoint/error 行。

## 相關章節

- 短路徑：[從這裡開始](start-here.md)
- Day-2 失敗模式：[Operations](operations.md)
- 指令旗標：[CLI 與 Config 參考](cli-and-config.md)
