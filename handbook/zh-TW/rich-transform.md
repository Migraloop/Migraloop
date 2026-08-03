# Rich Transform

**Rich Transform** 是使用者定義、**宣告式**的轉換，只由平台能分析的 operators 組成。它只讀 **平台管理的** Base Datasets—從不把使用者的 Source 或 Target 當 compute engine。自由形式 SQL/JS scripts 會被拒絕，因為會讓 **Affect Analysis** 不可能。

## 何時使用

當 Target document 的粒度或形狀與單一 source 列不同時（filter、projection 或 aggregation），使用 **Transform Pipeline**（`mode: transform`）。若是一列 → 一份文件的複製，請優先用 **Direct Pipeline**。

Transform Pipelines 必須宣告：

- `outputIdentity` — Delivery insert/update/delete 用的穩定 key 欄位
- `transform` — 宣告式 operator 步驟的有序列表

## v1 operator surface（已實作）

目前出貨的 parser 接受這些可分析 operators（Oracle → MongoDB 切片）：

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

Group keys 加上 aggregates。v1 aggregate op：`sum`。

```yaml
- groupBy:
    keys: [CUSTOMER_ID]
    aggregates:
      - op: sum
        field: AMOUNT
        as: TOTAL_AMOUNT
```

領域 roadmap 也提到 equiLookup、unwind、count/min/max/avg、distinct/addToSet、union 等 operators。在它們進入 CLI config parser 之前，請只宣告上面的 operators—不支援的 operator 名稱會讓 apply 失敗。

## Output Identity

**Output Identity** 在 Target 上定位一份文件，供 Delivery 與 Drift Check 使用。必須可由 transform 輸入決定—不能用隨機 UUID。對 aggregation，identity 通常對應 `groupBy` keys。

## Affect Analysis

**Affect Analysis** 依 transform 定義與進來的 Base change，決定哪些 Output Identities（若有）需要 Derived 重算。未使用的欄位不得觸發重算（例如只改地址不應重算依客戶加總金額）。Operator 語意決定 value-level 情況（例如 distinct/count 類更新）。

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
