# Rich Transform

**Rich Transform** 是使用者定義、**宣告式**的轉換，只由平台能分析的 operators 組成。它只讀 **平台管理的** Base Datasets—從不把使用者的 Source 或 Target 當 compute engine。自由形式 SQL/JS scripts 會被拒絕，因為會讓 **Affect Analysis** 不可能。

## 何時使用

當 Target document 的粒度或形狀與單一 source 列不同時（filter、projection 或 aggregation），使用 **Transform Pipeline**（`mode: transform`）。若是一列 → 一份文件的複製，請優先用 **Direct Pipeline**。

Transform Pipelines 必須宣告：

- `outputIdentity` — Delivery insert/update/delete 用的穩定 key 欄位
- `transform` — 宣告式 operator 步驟的有序列表

## Authoring forms（expand）

Operator 可用下列任一形式撰寫同一組可分析 surface—兩者都正規化成同一 IR，供 **Affect Analysis** 使用：

1. **Classic steps** — `project`、`filter`、`equiLookup`、`groupBy`…（下方範例；仍完全支援）。
2. **Aggregation / SQL-like DX** — 接近 MongoDB Aggregation 的 stages（`$project`、`$match`、`$lookup`、`$group`…），以及精簡 SQL 別名（`select`、`where`、`join`）。

範例（等同 classic `project` + `filter`）：

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

使用 classic steps 的既有 Deployments 可持續運作（Upgrade Compatibility）。為可讀性，每個 Pipeline 建議只用一種形式；同一列表混用 classic 與 Aggregation 步驟在各自合法時也被允許。

## v1 operator surface（已實作）

目前出貨的 parser 接受這些可分析 operators（Oracle → MongoDB 切片）—以下以 classic 形式說明；Aggregation 等效見 **Authoring forms**：

### `project`

只保留列出的欄位：

```yaml
- project:
    fields: [ID, CUSTOMER_ID, AMOUNT]
```

### `addFields`

新增 Managed 欄位：literal JSON `value`，或從既有欄位 `field` 複製（兩者擇一）：

```yaml
- addFields:
    fields:
      - as: currency
        value: USD
      - as: displayName
        field: customerName
```

### `rename`

重新命名欄位（`from` → `to`）：

```yaml
- rename:
    fields:
      - from: NAME
        to: customerName
```

### `remove`

從列中移除欄位（移除後對 Affect Analysis 視為未使用）：

```yaml
- remove:
    fields: [EMAIL, NOTES]
```

### `filter`

單一欄位的等值過濾：

```yaml
- filter:
    field: STATUS
    eq: OPEN
```

### `groupBy`

Group keys 加上 aggregates。v1 aggregate ops：`sum`、`count`、`min`、`max`、`avg`。
每個 aggregate 都需要 `field` 與 `as`。`count` 計算 `field` 的非 null 值數量
（SQL `COUNT(field)`）。`min` / `max` / `avg` / `sum` 使用精度保留的 decimal 算術
（非 IEEE double）。空 group 會被省略；若 `field` 全為 null，`min` / `max` / `avg`
為 JSON `null`，`count` 為 `0`，`sum` 為 `0`。

```yaml
- groupBy:
    keys: [CUSTOMER_ID]
    aggregates:
      - op: count
        field: ORDER_ID
        as: ORDER_COUNT
      - op: min
        field: AMOUNT
        as: MIN_AMOUNT
      - op: max
        field: AMOUNT
        as: MAX_AMOUNT
      - op: avg
        field: AMOUNT
        as: AVG_AMOUNT
      - op: sum
        field: AMOUNT
        as: TOTAL_AMOUNT
```

這些 aggregation **不會**額外發明 Maintenance State：incremental 更新只從 Base
重算受影響的 Output Identities。未使用欄位變更（例如 aggregates 讀 ORDER_ID/AMOUNT
時只改 ADDRESS）會略過 Derived 重算。

### `distinct`

依 `fields` 的唯一組合各產生一列 Derived（SQL `DISTINCT` 語意）。Output Identity
通常對應這些 fields。

```yaml
- distinct:
    fields: [CUSTOMER_ID]
```

### `addToSet`

依 `keys` 分組，把 `field` 的唯一非 null 值收集成 JSON 陣列 `as`（Mongo 風格
`$addToSet`）。陣列內的值以決定性順序排列。

```yaml
- addToSet:
    keys: [CUSTOMER_ID]
    field: AMOUNT
    as: AMOUNTS
```

`distinct` 與 `addToSet` **會**建立 **Maintenance State**（Platform Store 內的
per-identity / per-member refcounts），讓 value-level Affect Analysis 能略過無用的
Derived 更新—例如插入已計入的重複 `CUSTOMER_ID`，或 set 中已存在的 `AMOUNT`。v1
每個 transform 最多允許一個 `distinct` 或 `addToSet`。簡單的 `groupBy`
sum/count/min/max/avg 仍不得發明 Maintenance State。

### `equiLookup`

對同一 Deployment 內另一個 **Base Dataset** 做 left-outer equijoin。符合條件的
foreign rows 會嵌成陣列放在 `as` 之下。Pipeline 的 `source.table` 是左側（primary）
Base；`from` 命名 secondary Base（Initial Load 與 Incremental Capture 都會涵蓋兩者）。
可選的 `fromSchema` 覆寫 secondary schema（預設為 Pipeline source schema）。

```yaml
- equiLookup:
    from: ORDERS
    localField: ID
    foreignField: CUSTOMER_ID
    as: orders
```

請用 classic `equiLookup`，或帶相同 equijoin 欄位的受限 Aggregation `$lookup` / `join`。
自由形式的 Mongo `$lookup` 擴充（`pipeline` / `let`）會被拒絕，以便 **Affect Analysis**
保持正確。任一側 Base 變更只更新受影響的 primary Output Identities；未使用的 primary
欄位（例如 `project` 之後的 EMAIL）仍會略過重算。嵌入的 foreign rows 包含完整 Base
欄位，因此 foreign 側欄位變更會重算相符的 identities。

### `unwind`

把陣列欄位展開成每個元素一列 Derived（1→N 粒度）。常見組合是先 `equiLookup`
再 `unwind`，讓 Delivery 能以展開後的 Output Identity（例如 `ORDER_ID`）為文件鍵。

```yaml
- unwind:
    path: orders
```

當陣列元素是物件時，其欄位會**合併進 parent 列**，並移除陣列 path（利於 Delivery 的 flatten）。
純量元素則替換該 path 的值（Mongo 風格）。缺失、null 或空陣列不產生列。請用 classic
`unwind` 或 Aggregation `$unwind`（`"$path"` 或 `{ path }`）。`preserveNullAndEmptyArrays` /
`includeArrayIndex` 等選項會被拒絕，以便 **Affect Analysis** 只展開受影響的 Output
Identities—包括陣列成員消失時的 deletes。

### `union`

把另一個 **Base Dataset** 串接到串流（SQL `UNION ALL` / Mongo `$unionWith` 且無 nested
pipeline）。Pipeline 的 `source.table` 是 primary Base；`from` 命名 secondary Base
（Initial Load + Incremental Capture 涵蓋兩者）。先前步驟已塑造的列在前；secondary Base
列原樣接在後面；之後的步驟（例如 `project`）對兩邊都生效。可選的 `fromSchema` 覆寫
secondary schema（預設為 Pipeline source schema）。

```yaml
- union:
    from: WEST_CUSTOMERS
- project:
    fields: [ID, NAME]
```

請用 classic `union`，或受限 Aggregation `$unionWith`（`coll` / `from` / 字串名稱）。
巢狀 `$unionWith` `pipeline` 擴充會被拒絕，以便 **Affect Analysis** 保持正確。任一貢獻
Base 的變更只更新受影響的 Output Identities；後續 `project` 未使用的欄位（例如 EMAIL）仍會
skip 重算。v1 不允許 `union` 與 `distinct` / `addToSet` 併用。請選擇跨貢獻 Bases 仍唯一的
**Output Identity**—Delivery 對每個 identity upsert 一份 Target 文件（SQL `UNION ALL` 的列
多重性不會為同一 key 建立多份 Mongo 文件）。

## Output Identity

**Output Identity** 在 Target 上定位一份文件，供 Delivery 與 Drift Check 使用。必須可由 transform 輸入決定—不能用隨機 UUID。對 aggregation，identity 通常對應 `groupBy` keys。

## Affect Analysis

**Affect Analysis** 依 transform 定義與進來的 Base change，決定哪些 Output Identities（若有）需要 Derived 重算。未使用的欄位不得觸發重算（例如只改地址不應重算依客戶加總金額）。對 `distinct` / `addToSet`，Maintenance State 可在重複 key 或 set member 已計入時（以及 delete 並非最後一個貢獻者時）做 value-level skip。

當 Base 列的 `groupBy` key 變更時，Affect Analysis 會在套用 change **之前**讀取 Base 列，以便同時更新舊與新的 Output Identity（調整或移除舊 identity；upsert 新 identity）。不可先覆寫 Base 再嘗試找回先前的 key。

穩態下對整個 Derived Dataset 做 full recompute 不可接受。在正確時優先走 operator-equivalent 快速路徑；否則只對受影響 identities 從平台 Base 輸入重算。

檢查 Derived 列：

```bash
migraloop derived --pipeline orders_by_customer
```

## 相關章節

- Pipeline 宣告：[Pipeline](pipeline.md)
- Derived 輸出的 Delivery：[Target System](target-system.md)
- Transform Pipelines 的健康：[Observability](observability.md)
