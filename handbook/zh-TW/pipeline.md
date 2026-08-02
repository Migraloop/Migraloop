# Pipeline

**Pipeline** 是 **Deployment** 內的使用者定義流程，產出一個 target collection。Deployment 擁有 Source/Target 配對；Pipeline 擁有 mode、source 資料表參照、可選的 Rich Transform、Output Identity、Target Binding，以及欄位對應覆寫。

## Modes

| Mode | 行為 |
| --- | --- |
| `direct` | 無 Rich Transform。把一個 Base Dataset Deliver 到 Target Binding。Output Identity 預設來自 source primary key。 |
| `transform` | 宣告宣告式 **Rich Transform**，物化 **Derived Dataset** 再 Deliver。需要非空的 `outputIdentity` 與至少一個 transform operator。 |

## 宣告 Pipelines

Pipelines 位於 Deployment 文件的 `spec.pipelines`：

```yaml
pipelines:
  - name: orders_direct
    mode: direct
    source:
      table: ORDERS
      schema: APP                 # 可選
    target:
      collection: orders
    # 可選 Managed-field 覆寫（不安全的 NUMBER 等）
    fields:
      HUGE_AMOUNT:
        as: string               # 或 omit

  - name: orders_by_customer
    mode: transform
    source:
      table: ORDERS
    target:
      collection: orders_by_customer
    outputIdentity: [CUSTOMER_ID]
    transform:
      - groupBy:
          keys: [CUSTOMER_ID]
          aggregates:
            - op: sum
              field: AMOUNT
              as: TOTAL_AMOUNT
```

`apply` 會強制的驗證規則：

- `mode` 為 `direct` 或 `transform`
- Direct Pipelines 不得宣告 `transform`
- Transform Pipelines 需要 `outputIdentity` 與非空的宣告式 `transform`
- `fields` 的 key 把 source/Managed 欄位對應到 `{ as: string }` 或 `{ as: omit }`（ADR-0023）

Operator 形狀見 [Rich Transform](rich-transform.md)。

## Lifecycle（control plane）

產品模型：在不重啟整個 Deployment 的前提下 add、pause、resume、remove、change Pipelines（ADR-0007）。

**目前已出貨 CLI 上 Operator 的做法：**

1. 編輯宣告式 Deployment 文件（新增/變更/移除 Pipeline 項目）。
2. `migraloop apply -f deployment.yaml` — upsert Deployment + Pipeline 集合；對新參照的資料表做 table-level **Initial Load**；當 Transform 修訂需要時重建 Derived 輸出；無關的 Pipeline 變更不會重建共用 Base Datasets。
3. `migraloop sync` — 對活躍工作做 Incremental Capture + Delivery。
4. `migraloop status` / `base` / `target` / `derived` — 檢查進度與健康。

專用的 pause/resume 子指令仍屬 control-plane 契約；在它們成為一等 CLI 動詞之前，stream-wide blocker 請依 Operations 指引處理，並只在應該執行時把 Pipelines 留在宣告中。

## Capture 範圍

哪些 Source 資料表進入 Sync，由 Pipeline 的 `source.table` 參照決定。每張表在每個 Deployment 至多一個 Base Dataset，跨 Pipelines 共用。新表只做 table-level Initial Load。

## 相關章節

- Source prerequisites 與型別：[Source System](source-system.md)
- Target Binding / Managed fields：[Target System](target-system.md)
- Transform operators：[Rich Transform](rich-transform.md)
- 設定欄位參考：[CLI 與 Config 參考](cli-and-config.md)
