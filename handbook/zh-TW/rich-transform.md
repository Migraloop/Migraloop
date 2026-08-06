# Rich Transform

**Rich Transform** 是使用者定義、**宣告式**的轉換，只由平台能分析的 operators 組成。它只讀 **平台管理的** Base Datasets—從不把使用者的 Source 或 Target 當 compute engine。自由形式 SQL/JS scripts 會被拒絕，因為會讓 **Affect Analysis** 不可能。

## 何時使用

當 Target document 的粒度或形狀與單一 source 列不同時（filter、projection 或 aggregation），使用 **Transform Pipeline**（`mode: transform`）。若是一列 → 一份文件的複製，請優先用 **Direct Pipeline**。

Transform Pipelines 必須宣告：

- `outputIdentity` — Delivery insert/update/delete 用的穩定 key 欄位
- `transform` — 宣告式 operator 步驟的有序列表

## Authoring forms

Operator 可用下列任一形式撰寫同一組可分析 surface—兩者都正規化成同一 IR，供 **Affect Analysis** 使用：

1. **Aggregation / SQL-like DX（建議）** — 接近 MongoDB Aggregation 的 stages（`$project`、`$match`、`$lookup`、`$group`…），以及精簡 SQL 別名（`select`、`where`、`join`）。這是新 Pipeline 與出貨 Lab Scenarios 的支援撰寫路徑。
2. **Classic steps（Upgrade Compatibility）** — `project`、`filter`、`equiLookup`、`groupBy`…。使用 classic steps 的既有 Deployments 可持續 apply，無需強制改寫。

範例（建議的 Aggregation 形式）：

```yaml
transform:
  - $project:
      ID: 1
      NAME: 1
      ACTIVE: 1
  - $match:
      ACTIVE: 1
```

等效 SQL 別名：

```yaml
transform:
  - select:
      fields: [ID, NAME, ACTIVE]
  - where:
      field: ACTIVE
      eq: 1
```

受限 Aggregation stages 對應如下（無法分析的擴充仍會被拒絕）：

| Aggregation / SQL-like | Classic 等效 | 說明 |
| --- | --- | --- |
| `$project` / `select` | `project` | inclusion map（`FIELD: 1`）或 `{ fields: [...] }` |
| `$match` / `where` | `filter` | 僅單欄 equality |
| `$addFields` / `$set` | `addFields` | `"$field"` 複製或 `{ $literal: ... }`／JSON literal |
| `$unset` | `remove` | 欄位名或名稱陣列 |
| `$rename` | `rename` | `{ FROM: TO }` map |
| `$lookup` / `join` | `equiLookup` | 僅 equijoin—不可用 `pipeline` / `let` |
| `$unwind` | `unwind` | path 字串或 `{ path }`—不可用 `preserveNullAndEmptyArrays` |
| `$unionWith` | `union` | `coll` / `from` / 字串—不可用 nested `pipeline` |
| `$group` | `groupBy` / `distinct` / `addToSet` | `_id: "$KEY"`；accumulators `$sum`/`$count`/`$min`/`$max`/`$avg`/`$addToSet`。`$count` 需欄位 ref（`{ $count: "$ORDER_ID" }` = SQL `COUNT(field)`），不是 Mongo 空的 `{ $count: {} }` |

為可讀性，每個 Pipeline 建議只用一種形式；同一列表混用 classic 與 Aggregation 步驟在各自合法時也被允許。

### Migration notes

- **保留 classic YAML 不需 wipe。** 升級 app 不會強制 Operator 改寫 classic `project`／`filter`／`groupBy`／… Deployments（Upgrade Compatibility／ADR-0014）。Classic steps 仍可解析並 Sync。
- **新撰寫應使用 Aggregation DX。** Lab Scenarios 與 handbook 範例以 `$project`／`$match`／`$group`／… 為支援風格。
- **在設定中改寫形式屬於 Pipeline revision。** 撰寫的 `transform` JSON 會原樣儲存。以 IR 等效的 Aggregation YAML 取代 classic steps（或反向）會改變已儲存的宣告，因此 `migraloop apply` 會視為 **語意 Pipeline revision**：暫停該 Pipeline 舊的 Delivery、重建 Derived Dataset、重新 Deliver（含 delete reconciliation），再 resume。建議在已規劃 revision 視窗時遷移—或讓 classic Deployments 維持不變。
- **目錄中的能力名稱仍用 classic。** Coverage 列與 glossary 仍以 `project`／`equiLookup`／`groupBy` 稱呼可分析 surface；撰寫可用任一形式。

## v1 operator surface（已實作）

目前出貨的 parser 接受這些可分析 operators（Oracle → MongoDB 切片）—以下以建議的 Aggregation 形式說明。Classic 等效仍支援（見上方對應表）。

### `$project`

只保留列出的欄位（inclusion map 或 `{ fields: [...] }`）：

```yaml
- $project:
    ID: 1
    CUSTOMER_ID: 1
    AMOUNT: 1
```

### `$addFields` / `$set`

新增 Managed 欄位：JSON literal 或複製既有欄位：

```yaml
- $addFields:
    currency: USD
    displayName: "$customerName"
```

（literal 也可寫成 `{ $literal: USD }`。）

### `$rename`

重新命名欄位（`FROM` → `TO`）：

```yaml
- $rename:
    NAME: customerName
```

### `$unset`

從列中移除欄位（移除後對 Affect Analysis 視為 unused）：

```yaml
- $unset: [EMAIL, NOTES]
```

### `$match`

單欄 equality filter：

```yaml
- $match:
    STATUS: OPEN
```

### `$group`（aggregations）

Group keys 加上 aggregates。v1 aggregate ops：`$sum`、`$count`、`$min`、`$max`、`$avg`。
`$count` 計算被參考欄位的非 null 值（SQL `COUNT(field)`）。`$min`／`$max`／`$avg`／`$sum`
使用保留精度的 decimal 算術（非 IEEE double）。空 group 會省略；僅有 null 欄位值時
`$min`／`$max`／`$avg` 為 JSON `null`，而 `$count` 為 `0`、`$sum` 為 `0`。

```yaml
- $group:
    _id: "$CUSTOMER_ID"
    ORDER_COUNT:
      $count: "$ORDER_ID"
    MIN_AMOUNT:
      $min: "$AMOUNT"
    MAX_AMOUNT:
      $max: "$AMOUNT"
    AVG_AMOUNT:
      $avg: "$AMOUNT"
    TOTAL_AMOUNT:
      $sum: "$AMOUNT"
```

這些 aggregations **不會** 發明 Maintenance State：增量更新只從 Base 重算受影響的
Output Identities。Unused 欄位變更（例如 aggregates 讀 ORDER_ID/AMOUNT 時的 ADDRESS）
會跳過 Derived recompute。

### `$group` for distinct

每個唯一 key 一列 Derived（SQL `DISTINCT` 語意）—僅含 `_id` 的 `$group`。
Output Identity 通常對齊這些欄位。

```yaml
- $group:
    _id: "$CUSTOMER_ID"
```

### `$group` with `$addToSet`

依 `_id` 分組，並把唯一非 null 值收集成 JSON 陣列（Mongo 風格 `$addToSet`）。
陣列中的值順序是確定性的。

```yaml
- $group:
    _id: "$CUSTOMER_ID"
    AMOUNTS:
      $addToSet: "$AMOUNT"
```

Distinct 與 `$addToSet` **會** 建立 **Maintenance State**（Platform Store 中 per-identity／
per-member refcounts），讓 value-level Affect Analysis 能跳過無用的 Derived 更新—例如插入
已計入的重複 `CUSTOMER_ID`，或 set 中已存在的 `AMOUNT`。v1 每個 transform 最多允許一個
distinct 或 `$addToSet` operator。簡單的 `$group` sum/count/min/max/avg 仍不得發明
Maintenance State。

### `$lookup`

對同一 Deployment 內另一個 **Base Dataset** 做 left-outer equijoin。符合的 foreign 列會嵌成
`as` 下的陣列。Pipeline 的 `source.table` 是 left（primary）Base；`from` 命名 secondary Base
（Initial Load + Incremental Capture 兩者都會納入）。可選 `fromSchema` 覆寫 secondary schema
（預設為 Pipeline source schema）。

```yaml
- $lookup:
    from: ORDERS
    localField: ID
    foreignField: CUSTOMER_ID
    as: orders
```

請用帶相同 equijoin 欄位的受限 Aggregation `$lookup`／`join`（或 classic `equiLookup`）。
自由形式 Mongo `$lookup` 擴充（`pipeline`／`let`）會被拒絕，以維持 **Affect Analysis**
正確。任一侧 Base 的變更只更新受影響的 primary Output Identities；`$project` 後 unused 的
primary 欄位（例如 EMAIL）仍會跳過 recompute。嵌入的 foreign 列含完整 Base 欄位，因此
foreign 側欄位變更會重算相符 identities。

### `$unwind`

把陣列欄位展開成每個元素一列 Derived（1→N grain）。典型組合是 `$lookup` 再 `$unwind`，
讓 Delivery 能以 unwind 後的 Output Identity（例如 `ORDER_ID`）鍵結文件。

```yaml
- $unwind: "$orders"
```

當陣列元素是物件時，其欄位會 **merge 進 parent 列** 並移除該 path（利於 Delivery 的 flatten）。
純量元素則替換該 path 的值（Mongo 風格）。缺失、null 或空陣列不產生列。`preserveNullAndEmptyArrays`／
`includeArrayIndex` 等選項會被拒絕，讓 **Affect Analysis** 只展開受影響的 Output Identities—
包含陣列成員消失時的 deletes。

### `$unionWith`

把另一個 **Base Dataset** 串進串流（SQL `UNION ALL`／不含 nested pipeline 的 Mongo `$unionWith`）。
Pipeline 的 `source.table` 是 primary Base；`$unionWith` 名稱是 secondary Base（Initial Load +
Incremental Capture 兩者都會納入）。先前步驟已塑形的列在前；secondary Base 列原樣附加；之後的
步驟（例如 `$project`）兩邊都適用。可選 `fromSchema` 覆寫 secondary schema（預設為 Pipeline
source schema）。

```yaml
- $unionWith: WEST_CUSTOMERS
- $project:
    ID: 1
    NAME: 1
```

Nested `$unionWith` `pipeline` 擴充會被拒絕，以維持 **Affect Analysis** 正確。任一侧貢獻
Base 的變更只更新受影響的 Output Identities；後續 `$project` 後 unused 的欄位（例如 EMAIL）
仍會跳過 recompute。v1 不把 `$unionWith` 與 distinct／`$addToSet` 組合。請選擇在貢獻 Bases
之間仍保持唯一的 **Output Identity**—Delivery 對每個 identity upsert 一份 Target document
（SQL `UNION ALL` 列多重性不會為同一 key 建立多份 Mongo documents）。

## Output Identity

**Output Identity** 在 Target 上定位一份文件，供 Delivery 與 Drift Check 使用。它必須可從
transform 輸入決定—不可用隨機 UUID。對 aggregations，identity 通常對齊 `$group` 的 `_id` keys。

## Affect Analysis

**Affect Analysis** 依 transform 定義與進來的 Base change，決定哪些 Output Identities（若有）
需要 Derived recomputation。Unused 欄位不得觸發 recompute（例如只改 address 不得重算
sum-of-amount-by-customer）。對 distinct／`$addToSet`，Maintenance State 讓已計入的重複 key
或 set member（以及 delete 並非最後貢獻者時）可做 value-level skip。

當 Base 列的 `$group` key 變更時，Affect Analysis 會在套用變更 **之前** 讀取 Base 列，以便更新
舊與新的 Output Identities（調整或移除舊 identity；upsert 新的）。不得先覆寫 Base 再試圖
還原先前的 key。

對整個 Derived Dataset 做 steady-state 全量 recompute 不可接受。在正確時優先走
operator-equivalent fast paths；否則只從 platform Base 輸入重算受影響 identities。

檢查 Derived 列：

```bash
migraloop derived --pipeline orders_by_customer
```

## Related chapters

- Pipeline 宣告：[Pipeline](pipeline.md)
- Derived 輸出的 Delivery：[Target System](target-system.md)
- Transform Pipelines 的健康：[Observability](observability.md)
