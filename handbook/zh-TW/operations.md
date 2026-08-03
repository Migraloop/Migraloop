# Operations

Operator 在正式環境執行 Deployments 時應預期的 day-2 行為。下列若干控制項是 ADRs 與領域 glossary 記錄的 **產品契約**；每個控制項落地時，請以 `migraloop status` / logs 核對你的 build 實際暴露什麼。

## Schema Change Handling

Source DDL 會依每條 Pipeline 的相依性分類（ADR-0009）：

| 影響 | 預期平台行為 |
| --- | --- |
| 不影響該 Pipeline | 繼續處理；schema 可稍後追上 |
| 影響 Pipeline 但 apply 仍安全 | 繼續處理 |
| 阻擋安全 apply（重試無法前進） | **警告並 pause** 受影響的 Pipeline(s) |

此 pause 規則用於 **stream-wide blockers**，不是單一列的 poison data。當 Incremental Capture 遇到會阻塞的 Source DDL 時，`migraloop sync` 會發出 Operator 可見的 **WARN**、持久化 Schema Change impact，並以與 `migraloop pause` 相同的耐久 pause 旗標 pause 受影響的 Pipeline(s)—不會走 quarantine。Unaffecting 或 non-blocking 的 schema changes 會繼續；`status` 會顯示 `Delivery Health: paused`，以及作用中的 blocking Schema Change 列（與 Poison Change quarantine 不同）。Operator 也可主動用 `migraloop pause --pipeline <name>` / `migraloop resume --pipeline <name>` pause/resume 一條 Pipeline，或以 `migraloop remove --pipeline <name>` 移除一條（見 [Pipeline](pipeline.md) 與 [CLI 與 Config](cli-and-config.md)），且不必重啟 Deployment。Resume 會清除該 Pipeline 作用中的 Schema Change impacts，並依耐久 Base/Derived 狀態做 catch-up Delivery。

## Poison Change Handling

當單一 change 或 Output Identity 反覆失敗，但串流其餘部分仍可繼續時（ADR-0015），預期路徑是：

1. 有界重試
2. **Quarantine** 該 change/identity
3. **Alert** Operators
4. **讓 Pipeline 繼續跑**

被 quarantine 的 keys 在修復或重試前保持 unhealthy / not aligned—絕不默默略過。不要預期單列壞資料就 pause 整條 Pipeline。有界 Delivery 重試後，`migraloop sync` 會持久化 quarantine、發出 Operator 可見的 **ALERT**，並繼續處理其他 changes；`migraloop status` 會顯示 `Delivery Health: unhealthy`，並把每個被 quarantine 的 Output Identity 標為 unhealthy / not aligned。


## Source Alignment Check

單靠 Sync Health 不能證明 Base 符合 Source。Operator 在把 Base Dataset 當作 Drift baseline 之前，應執行可排程、resource-gated 的 **Source Alignment Check**：

```bash
migraloop align [--table CUSTOMERS] [--max-rows 1000]
```

檢查最多讀取 `--max-rows` 筆 Source 列（預設 `1000`—不是全表 slam），以主鍵比對 Base，並在不一致時用這些 Source reads 修復 Base。**從不寫入 Source**。`status` 顯示上次執行的 `Source Alignment: aligned|partial|unknown` 與 checked/mismatched 計數（`partial` = budget 被截斷）。見 [CLI 與 Config](cli-and-config.md) 與 [Observability](observability.md)。

## Drift Check

單靠 Delivery Health 不能證明 Target 上的 Managed fields 符合平台 expected dataset。Operator 在 Direct Pipelines 完成 Source Alignment 後，應執行可排程、resource-gated 的 **Drift Check**，使 Base/Derived 成為可信 baseline：

```bash
migraloop drift [--pipeline customers] [--max-rows 1000]
```

檢查最多讀取 `--max-rows` 個 expected Output Identities（預設 `1000`—不是全表 slam），比對 Target 的 Managed fields，並預設以 Managed-only upsert **auto-repair** Managed drift。**non-Managed Target fields 會被忽略**且保持不動。不會在 Alignment baseline 之外再增加 Source load。`status` 顯示 `Drift: ok|partial|unknown` 與 checked/mismatched 計數（`partial` = budget 被截斷）。見 [CLI 與 Config](cli-and-config.md) 與 [Observability](observability.md)。

## Backpressure

當 Platform Store apply、Derived maintenance 或 Target Delivery 跟不上時（ADR-0020）：

- 各階段使用 **bounded queues**（預設 Incremental window `MIGRALOOP_SYNC_QUEUE_CAPACITY`，256）並放慢 capture/apply
- Sync Health 與 Delivery Health 都暴露目前 window 剩餘工作的 `lag=`；當 window 已滿或 Downstream 延遲時，`sync` 會印出 `Backpressure: queue_depth=… capacity=…`
- 拒絕無界記憶體緩衝 / 把 OOM 當 backpressure
- 只因 Target 慢就 pause 整條 Pipeline **不是**預設行為

Operator 依可見 lag 行動（擴充 Target、降低負載、檢查 Delivery 錯誤）—pause 留給真正的 blocker。Lab Scenario `bounded-backpressure` 可在可拋棄 Fixture 上演練此路徑。

## Platform Store Guardrails

隨附的 PostgreSQL Platform Store 帶有安全預設與產品強制的下限（ADR-0010 / issue #28）。Operator 可以提高設定；app 會 **拒絕** 過低值，使 migrate / status / sync / apply / run 以 Guardrails 訊息失敗，而不是在資源不足下繼續跑。

| 設定 | Compose 預設 | 產品下限（floor） |
| --- | --- | --- |
| `shared_buffers` | `128MB` | ≥ `64MB` |
| `work_mem` | `8MB` | ≥ `4MB` |
| `maintenance_work_mem` | `128MB` | ≥ `64MB` |
| `max_connections` | `100` | ≥ `20` |

根目錄 `compose.yaml` 與 `lab/compose.yaml` 會在 `platform-store` service 帶上這些預設。

**可用磁碟警告（warn-only）：** 當 Platform Store data volume 可用空間低於 **1 GiB** 時，產品會在 `migraloop status`（以及 sync / apply / run 路徑）印出 `WARN: …`、發出 structured event `platform_store_disk_warn`，並暴露 Prometheus gauges `migraloop_platform_store_disk_free_bytes` 與 `migraloop_platform_store_disk_warn`。跨越門檻 **不會** 自動 pause Pipelines—除非另有問題，Platform Store 仍維持 healthy；如何回應警告是 Operator 的責任。

**如何觀測可用磁碟：** compose 會把 store data volume 以 read-only 掛進 app 的 `/var/lib/migraloop/platform-store-data`，並設定 `MIGRALOOP_PLATFORM_STORE_DATA_DIR` 指向該路徑。當無法做 filesystem probe 時，Operator／orchestrator 也可改供 `MIGRALOOP_PLATFORM_STORE_FREE_DISK_BYTES`。

Lab Scenario `platform-store-guardrails` 可在可拋棄 Fixture 上演練隨附 Guardrail 下限、過低設定拒絕，以及 warn-only 可用磁碟行為。Postgres 備份仍是 Operator 的責任。

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
