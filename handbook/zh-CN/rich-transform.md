# Rich Transform

**Rich Transform** 是用户定义、**声明式**的转换，只由平台能分析的 operators 组成。它只读 **平台管理的** Base Datasets—从不把用户的 Source 或 Target 当 compute engine。自由形式 SQL/JS scripts 会被拒绝，因为会让 **Affect Analysis** 不可能。

## 何时使用

当 Target document 的粒度或形状与单一 source 行不同时（filter、projection 或 aggregation），使用 **Transform Pipeline**（`mode: transform`）。若是一行 → 一份文档的复制，请优先用 **Direct Pipeline**。

Transform Pipelines 必须声明：

- `outputIdentity` — Delivery insert/update/delete 用的稳定 key 字段
- `transform` — 声明式 operator 步骤的有序列表

## Authoring forms

Operator 可用下列任一形式编写同一组可分析 surface—两者都规范化成同一 IR，供 **Affect Analysis** 使用：

1. **Aggregation / SQL-like DX（建议）** — 接近 MongoDB Aggregation 的 stages（`$project`、`$match`、`$lookup`、`$group`…），以及精简 SQL 别名（`select`、`where`、`join`）。这是新 Pipeline 与出货 Lab Scenarios 的支持编写路径。
2. **Classic steps（Upgrade Compatibility）** — `project`、`filter`、`equiLookup`、`groupBy`…。使用 classic steps 的既有 Deployments 可持续 apply，无需强制改写。

示例（建议的 Aggregation 形式）：

```yaml
transform:
  - $project:
      ID: 1
      NAME: 1
      ACTIVE: 1
  - $match:
      ACTIVE: 1
```

等效 SQL 别名：

```yaml
transform:
  - select:
      fields: [ID, NAME, ACTIVE]
  - where:
      field: ACTIVE
      eq: 1
```

受限 Aggregation stages 对应如下（无法分析的扩展仍会被拒绝）：

| Aggregation / SQL-like | Classic 等效 | 说明 |
| --- | --- | --- |
| `$project` / `select` | `project` | inclusion map（`FIELD: 1`）或 `{ fields: [...] }` |
| `$match` / `where` | `filter` | 仅单字段 equality |
| `$addFields` / `$set` | `addFields` | `"$field"` 复制或 `{ $literal: ... }`／JSON literal |
| `$unset` | `remove` | 字段名或名称数组 |
| `$rename` | `rename` | `{ FROM: TO }` map |
| `$lookup` / `join` | `equiLookup` | 仅 equijoin—不可用 `pipeline` / `let` |
| `$unwind` | `unwind` | path 字符串或 `{ path }`—不可用 `preserveNullAndEmptyArrays` |
| `$unionWith` | `union` | `coll` / `from` / 字符串—不可用 nested `pipeline` |
| `$group` | `groupBy` / `distinct` / `addToSet` | `_id: "$KEY"`；accumulators `$sum`/`$count`/`$min`/`$max`/`$avg`/`$addToSet`。`$count` 需字段 ref（`{ $count: "$ORDER_ID" }` = SQL `COUNT(field)`），不是 Mongo 空的 `{ $count: {} }` |

为可读性，每个 Pipeline 建议只用一种形式；同一列表混用 classic 与 Aggregation 步骤在各自合法时也被允许。

### Migration notes

- **保留 classic YAML 不需 wipe。** 升级 app 不会强制 Operator 改写 classic `project`／`filter`／`groupBy`／… Deployments（Upgrade Compatibility／ADR-0014）。Classic steps 仍可解析；既有 Deployments 可持续 Sync。
- **新编写应使用 Aggregation DX。** Lab Scenarios 与 handbook 示例以 `$project`／`$match`／`$group`／… 为支持风格。
- **在配置中改写形式属于 Pipeline revision。** 编写的 `transform` JSON 会原样存储。以 IR 等效的 Aggregation YAML 取代 classic steps（或反向）会改变已存储的声明，因此 `migraloop apply` 会视为 **语义 Pipeline revision**：暂停该 Pipeline 旧的 Delivery、重建 Derived Dataset、重新 Deliver（含 delete reconciliation），再 resume。建议在已规划 revision 窗口时迁移—或让 classic Deployments 保持不变。
- **目录中的能力名称仍用 classic。** Coverage 行与 glossary 仍以 `project`／`equiLookup`／`groupBy` 称呼可分析 surface；编写可用任一形式。

## v1 operator surface（已实现）

当前出货的 parser 接受这些可分析 operators（Oracle → MongoDB 切片）—以下以建议的 Aggregation 形式说明。Classic 等效仍支持（见上方对应表）。

### `$project`

只保留列出的字段（inclusion map 或 `{ fields: [...] }`）：

```yaml
- $project:
    ID: 1
    CUSTOMER_ID: 1
    AMOUNT: 1
```

### `$addFields` / `$set`

新增 Managed 字段：JSON literal 或复制既有字段：

```yaml
- $addFields:
    currency: USD
    displayName: "$customerName"
```

（literal 也可写成 `{ $literal: USD }`。）

### `$rename`

重命名字段（`FROM` → `TO`）：

```yaml
- $rename:
    NAME: customerName
```

### `$unset`

从行中移除字段（移除后对 Affect Analysis 视为 unused）：

```yaml
- $unset: [EMAIL, NOTES]
```

### `$match`

单字段 equality filter：

```yaml
- $match:
    STATUS: OPEN
```

### `$group`（aggregations）

Group keys 加上 aggregates。v1 aggregate ops：`$sum`、`$count`、`$min`、`$max`、`$avg`。
`$count` 计算被引用字段的非 null 值（SQL `COUNT(field)`）。`$min`／`$max`／`$avg`／`$sum`
使用保留精度的 decimal 算术（非 IEEE double）。空 group 会省略；仅有 null 字段值时
`$min`／`$max`／`$avg` 为 JSON `null`，而 `$count` 为 `0`、`$sum` 为 `0`。

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

这些 aggregations **不会** 发明 Maintenance State：增量更新只从 Base 重算受影响的
Output Identities。Unused 字段变更（例如 aggregates 读 ORDER_ID/AMOUNT 时的 ADDRESS）
会跳过 Derived recompute。

### `$group` for distinct

每个唯一 key 一行 Derived（SQL `DISTINCT` 语义）—仅含 `_id` 的 `$group`。
Output Identity 通常对齐这些字段。

```yaml
- $group:
    _id: "$CUSTOMER_ID"
```

### `$group` with `$addToSet`

按 `_id` 分组，并把唯一非 null 值收集成 JSON 数组（Mongo 风格 `$addToSet`）。
数组中的值顺序是确定性的。

```yaml
- $group:
    _id: "$CUSTOMER_ID"
    AMOUNTS:
      $addToSet: "$AMOUNT"
```

Distinct 与 `$addToSet` **会** 建立 **Maintenance State**（Platform Store 中 per-identity／
per-member refcounts），让 value-level Affect Analysis 能跳过无用的 Derived 更新—例如插入
已计入的重复 `CUSTOMER_ID`，或 set 中已存在的 `AMOUNT`。v1 每个 transform 最多允许一个
distinct 或 `$addToSet` operator。简单的 `$group` sum/count/min/max/avg 仍不得发明
Maintenance State。

### `$lookup`

对同一 Deployment 内另一个 **Base Dataset** 做 left-outer equijoin。匹配的 foreign 行会嵌成
`as` 下的数组。Pipeline 的 `source.table` 是 left（primary）Base；`from` 命名 secondary Base
（Initial Load + Incremental Capture 两者都会纳入）。可选 `fromSchema` 覆盖 secondary schema
（默认为 Pipeline source schema）。

```yaml
- $lookup:
    from: ORDERS
    localField: ID
    foreignField: CUSTOMER_ID
    as: orders
```

请用带相同 equijoin 字段的受限 Aggregation `$lookup`／`join`（或 classic `equiLookup`）。
自由形式 Mongo `$lookup` 扩展（`pipeline`／`let`）会被拒绝，以维持 **Affect Analysis**
正确。任一侧 Base 的变更只更新受影响的 primary Output Identities；`$project` 后 unused 的
primary 字段（例如 EMAIL）仍会跳过 recompute。嵌入的 foreign 行含完整 Base 字段，因此
foreign 侧字段变更会重算匹配 identities。

### `$unwind`

把数组字段展开成每个元素一行 Derived（1→N grain）。典型组合是 `$lookup` 再 `$unwind`，
让 Delivery 能以 unwind 后的 Output Identity（例如 `ORDER_ID`）键结文档。

```yaml
- $unwind: "$orders"
```

当数组元素是对象时，其字段会 **merge 进 parent 行** 并移除该 path（利于 Delivery 的 flatten）。
标量元素则替换该 path 的值（Mongo 风格）。缺失、null 或空数组不产生行。`preserveNullAndEmptyArrays`／
`includeArrayIndex` 等选项会被拒绝，让 **Affect Analysis** 只展开受影响的 Output Identities—
包含数组成员消失时的 deletes。

### `$unionWith`

把另一个 **Base Dataset** 串进流（SQL `UNION ALL`／不含 nested pipeline 的 Mongo `$unionWith`）。
Pipeline 的 `source.table` 是 primary Base；`$unionWith` 名称是 secondary Base（Initial Load +
Incremental Capture 两者都会纳入）。先前步骤已塑形的行在前；secondary Base 行原样附加；之后的
步骤（例如 `$project`）两边都适用。可选 `fromSchema` 覆盖 secondary schema（默认为 Pipeline
source schema）。

```yaml
- $unionWith: WEST_CUSTOMERS
- $project:
    ID: 1
    NAME: 1
```

Nested `$unionWith` `pipeline` 扩展会被拒绝，以维持 **Affect Analysis** 正确。任一侧贡献
Base 的变更只更新受影响的 Output Identities；后续 `$project` 后 unused 的字段（例如 EMAIL）
仍会跳过 recompute。v1 不把 `$unionWith` 与 distinct／`$addToSet` 组合。请选择在贡献 Bases
之间仍保持唯一的 **Output Identity**—Delivery 对每个 identity upsert 一份 Target document
（SQL `UNION ALL` 行多重性不会为同一 key 创建多份 Mongo documents）。

## Output Identity

**Output Identity** 在 Target 上定位一份文档，供 Delivery 与 Drift Check 使用。它必须可从
transform 输入决定—不可用随机 UUID。对 aggregations，identity 通常对齐 `$group` 的 `_id` keys。

## Affect Analysis

**Affect Analysis** 依 transform 定义与进来的 Base change，决定哪些 Output Identities（若有）
需要 Derived recomputation。Unused 字段不得触发 recompute（例如只改 address 不得重算
sum-of-amount-by-customer）。对 distinct／`$addToSet`，Maintenance State 让已计入的重复 key
或 set member（以及 delete 并非最后贡献者时）可做 value-level skip。

当 Base 行的 `$group` key 变更时，Affect Analysis 会在应用变更 **之前** 读取 Base 行，以便更新
旧与新的 Output Identities（调整或移除旧 identity；upsert 新的）。不得先覆盖 Base 再试图
还原先前的 key。

对整个 Derived Dataset 做 steady-state 全量 recompute 不可接受。在正确时优先走
operator-equivalent fast paths；否则只从 platform Base 输入重算受影响 identities。

检查 Derived 行：

```bash
migraloop derived --pipeline orders_by_customer
```

## Related chapters

- Pipeline 声明：[Pipeline](pipeline.md)
- Derived 输出的 Delivery：[Target System](target-system.md)
- Transform Pipelines 的健康：[Observability](observability.md)
