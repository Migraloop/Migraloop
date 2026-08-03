# Rich Transform

**Rich Transform** 是用户定义、**声明式**的转换，只由平台能分析的 operators 组成。它只读 **平台管理的** Base Datasets—从不把用户的 Source 或 Target 当 compute engine。自由形式 SQL/JS scripts 会被拒绝，因为会让 **Affect Analysis** 不可能。

## 何时使用

当 Target document 的粒度或形状与单一 source 行不同时（filter、projection 或 aggregation），使用 **Transform Pipeline**（`mode: transform`）。若是一行 → 一份文档的复制，请优先用 **Direct Pipeline**。

Transform Pipelines 必须声明：

- `outputIdentity` — Delivery insert/update/delete 用的稳定 key 字段
- `transform` — 声明式 operator 步骤的有序列表

## v1 operator surface（已实现）

当前出货的 parser 接受这些可分析 operators（Oracle → MongoDB 切片）：

### `project`

只保留列出的字段：

```yaml
- project:
    fields: [ID, CUSTOMER_ID, AMOUNT]
```

### `addFields`

新增 Managed 字段：literal JSON `value`，或从既有字段 `field` 复制（二者择一）：

```yaml
- addFields:
    fields:
      - as: currency
        value: USD
      - as: displayName
        field: customerName
```

### `rename`

重命名字段（`from` → `to`）：

```yaml
- rename:
    fields:
      - from: NAME
        to: customerName
```

### `remove`

从行中移除字段（移除后对 Affect Analysis 视为未使用）：

```yaml
- remove:
    fields: [EMAIL, NOTES]
```

### `filter`

单字段等值过滤：

```yaml
- filter:
    field: STATUS
    eq: OPEN
```

### `groupBy`

Group keys 加上 aggregates。v1 aggregate ops：`sum`、`count`、`min`、`max`、`avg`。
每个 aggregate 都需要 `field` 与 `as`。`count` 计算 `field` 的非 null 值数量
（SQL `COUNT(field)`）。`min` / `max` / `avg` / `sum` 使用精度保留的 decimal 算术
（非 IEEE double）。空 group 会被省略；若 `field` 全为 null，`min` / `max` / `avg`
为 JSON `null`，`count` 为 `0`，`sum` 为 `0`。

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

这些 aggregation **不会**额外发明 Maintenance State：incremental 更新只从 Base
重算受影响的 Output Identities。未使用字段变更（例如 aggregates 读 ORDER_ID/AMOUNT
时只改 ADDRESS）会跳过 Derived 重算。

### `equiLookup`

对同一 Deployment 内另一个 **Base Dataset** 做 left-outer equijoin。匹配的
foreign rows 会嵌成数组放在 `as` 之下。Pipeline 的 `source.table` 是左侧（primary）
Base；`from` 命名 secondary Base（Initial Load 与 Incremental Capture 都会覆盖两者）。
可选的 `fromSchema` 覆盖 secondary schema（默认为 Pipeline source schema）。

```yaml
- equiLookup:
    from: ORDERS
    localField: ID
    foreignField: CUSTOMER_ID
    as: orders
```

自由形式的 Mongo `$lookup`（含 `pipeline` / `let` 扩展）会被拒绝—请用此声明式
形式，以便 **Affect Analysis** 保持正确。任一侧 Base 变更只更新受影响的 primary
Output Identities；未使用的 primary 字段（例如 `project` 之后的 EMAIL）仍会跳过重算。
嵌入的 foreign rows 包含完整 Base 字段，因此 foreign 侧字段变更会重算匹配的 identities。

领域 roadmap 也提到 unwind、distinct/addToSet、union 等 operators。在它们进入 CLI
config parser 之前，请只声明上面的 operators—不支持的 operator 名称会让 apply 失败。

## Output Identity

**Output Identity** 在 Target 上定位一份文档，供 Delivery 与 Drift Check 使用。必须可由 transform 输入决定—不能用随机 UUID。对 aggregation，identity 通常对应 `groupBy` keys。

## Affect Analysis

**Affect Analysis** 依 transform 定义与进来的 Base change，决定哪些 Output Identities（若有）需要 Derived 重算。未使用的字段不得触发重算（例如只改地址不应重算按客户加总金额）。Operator 语义决定 value-level 情况（例如 distinct/count 类更新）。

当 Base 行的 `groupBy` key 变更时，Affect Analysis 会在应用 change **之前**读取 Base 行，以便同时更新旧与新的 Output Identity（调整或移除旧 identity；upsert 新 identity）。不可先覆盖 Base 再尝试找回先前的 key。

稳态下对整个 Derived Dataset 做 full recompute 不可接受。在正确时优先走 operator-equivalent 快速路径；否则只对受影响 identities 从平台 Base 输入重算。

检查 Derived 行：

```bash
migraloop derived --pipeline orders_by_customer
```

## 相关章节

- Pipeline 声明：[Pipeline](pipeline.md)
- Derived 输出的 Delivery：[Target System](target-system.md)
- Transform Pipelines 的健康：[Observability](observability.md)
