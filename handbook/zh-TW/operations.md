# Operations

Operator 在正式環境執行 Deployments 時應預期的 day-2 行為。下列若干控制項是 ADRs 與領域 glossary 記錄的 **產品契約**；每個控制項落地時，請以 `migraloop status` / logs 核對你的 build 實際暴露什麼。

## Schema Change Handling

Source DDL 會依每條 Pipeline 的相依性分類（ADR-0009）：

| 影響 | 預期平台行為 |
| --- | --- |
| 不影響該 Pipeline | 繼續處理；schema 可稍後追上 |
| 影響 Pipeline 但 apply 仍安全 | 繼續處理 |
| 阻擋安全 apply（重試無法前進） | **警告並 pause** 受影響的 Pipeline(s) |

此 pause 規則用於 **stream-wide blockers**，不是單一列的 poison data。專用 pause/resume CLI 動詞屬 control-plane 契約（見 [Pipeline](pipeline.md)）；在它們出貨前，把無法解除的 apply 失敗當成 `status` / logs 上的 Operator 可見錯誤，並只在設定中保留可執行的 Pipelines。

## Poison Change Handling

當單一 change 或 Output Identity 反覆失敗，但串流其餘部分仍可繼續時（ADR-0015），預期路徑是：

1. 有界重試
2. **Quarantine** 該 change/identity
3. **Alert** Operators
4. **讓 Pipeline 繼續跑**

被 quarantine 的 keys 在修復或重試前保持 unhealthy / not aligned—絕不默默略過。不要預期單列壞資料就 pause 整條 Pipeline。在 quarantine 出現在 `status` 之前，請從 apply 錯誤與 Delivery Health 觀察卡住的 identities。

## Backpressure

當 Platform Store apply、Derived maintenance 或 Target Delivery 跟不上時（ADR-0020）：

- 各階段使用 **bounded queues** 並放慢 capture/apply
- Lag 仍會顯示在 Sync Health / Delivery Health（以及暴露時的 metrics）
- 拒絕無界記憶體緩衝 / 把 OOM 當 backpressure
- 只因 Target 慢就 pause 整條 Pipeline **不是**預設行為

Operator 依可見 lag 行動（擴充 Target、降低負載、檢查 Delivery 錯誤）—pause 留給真正的 blocker。

## Platform Store Guardrails

隨附的 PostgreSQL Platform Store 帶有安全預設與產品強制的下限（ADR-0010）。跨越安全門檻（例如可用磁碟）時必須 **只警告**—平台不會只因資源壓力就自動 pause。Postgres 備份仍是 Operator 的責任。

## Upgrades

升級必須 **backward compatible**（ADR-0014）：

- Platform Store schema 變更以啟動時套用的版本化 migrations 出貨（`migraloop run` / `migraloop migrate`）
- 較新的 app 必須能繼續既有 Deployments 與可接受的舊設定，而不是 wipe-and-rebuild
- 單 instance 升級期間允許短暫 sync pause；不得遺失 checkpoint/資料
- v1 不要求支援 downgrade

建議升級迴圈：

1. `migraloop status` — 記下 checkpoints 與健康
2. 滾動新的 app image / binary
3. 確認 migrations（`status` 中的 `Schema version`）
4. `migraloop sync` / 監看 Sync Health 與 Delivery Health

## 重啟後 resume

耐久的 capture 與 Delivery 進度存在 Platform Store。行程重啟後，`migraloop sync` 會從存放的 checkpoint（exclusive）繼續 Incremental Capture 並接續 Delivery—Operator 不應依賴僅存在本機的 recovery 檔。

## 相關章節

- 健康解讀：[Observability](observability.md)
- 安裝 / 單 instance 模型：[Deployment](deployment.md)
- CLI 動詞：[CLI 與 Config 參考](cli-and-config.md)
