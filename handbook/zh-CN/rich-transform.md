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

### `distinct`

按 `fields` 的唯一组合各产生一行 Derived（SQL `DISTINCT` 语义）。Output Identity
通常对应这些 fields。

```yaml
- distinct:
    fields: [CUSTOMER_ID]
```

### `addToSet`

按 `keys` 分组，把 `field` 的唯一非 null 值收集成 JSON 数组 `as`（Mongo 风格
`$addToSet`）。数组内的值以确定性顺序排列。

```yaml
- addToSet:
    keys: [CUSTOMER_ID]
    field: AMOUNT
    as: AMOUNTS
```

`distinct` 与 `addToSet` **会**创建 **Maintenance State**（Platform Store 内的
per-identity / per-member refcounts），让 value-level Affect Analysis 能跳过无用的
Derived 更新—例如插入已计入的重复 `CUSTOMER_ID`，或 set 中已存在的 `AMOUNT`。v1
每个 transform 最多允许一个 `distinct` 或 `addToSet`。简单的 `groupBy`
sum/count/min/max/avg 仍不得发明 Maintenance State。

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

### `unwind`

把数组字段展开成每个元素一行 Derived（1→N 粒度）。常见组合是先 `equiLookup`
再 `unwind`，让 Delivery 能以展开后的 Output Identity（例如 `ORDER_ID`）为文档键。

```yaml
- unwind:
    path: orders
```

当数组元素是对象时，其字段会**合并进 parent 行**，并移除数组 path（利于 Delivery 的 flatten）。
标量元素则替换该 path 的值（Mongo 风格）。缺失、null 或空数组不产生行。自由形式的
`$unwind` 以及 `preserveNullAndEmptyArrays` / `includeArrayIndex` 等选项会被拒绝，以便
**Affect Analysis** 只展开受影响的 Output Identities—包括数组成员消失时的 deletes。

### `union`

把另一个 **Base Dataset** 串联到流（SQL `UNION ALL` / Mongo `$unionWith` 且无 nested
pipeline）。Pipeline 的 `source.table` 是 primary Base；`from` 命名 secondary Base
（Initial Load + Incremental Capture 覆盖两者）。先前步骤已塑造的行在前；secondary Base
行原样接在后面；之后的步骤（例如 `project`）对两边都生效。可选的 `fromSchema` 覆盖
secondary schema（默认为 Pipeline source schema）。

```yaml
- union:
    from: WEST_CUSTOMERS
- project:
    fields: [ID, NAME]
```

自由形式的 Mongo `$unionWith`（含 `pipeline` / `coll` 扩展）会被拒绝—请用此声明式形状，
以便 **Affect Analysis** 保持正确。任一贡献 Base 的变更只更新受影响的 Output Identities；
后续 `project` 未使用的字段（例如 EMAIL）仍会 skip 重算。v1 不允许 `union` 与
`distinct` / `addToSet` 并用。请选择跨贡献 Bases 仍唯一的 **Output Identity**—Delivery
对每个 identity upsert 一份 Target 文档（SQL `UNION ALL` 的行多重性不会为同一 key 建立多份
Mongo 文档）。

## Output Identity

**Output Identity** 在 Target 上定位一份文档，供 Delivery 与 Drift Check 使用。必须可由 transform 输入决定—不能用随机 UUID。对 aggregation，identity 通常对应 `groupBy` keys。

## Affect Analysis

**Affect Analysis** 依 transform 定义与进来的 Base change，决定哪些 Output Identities（若有）需要 Derived 重算。未使用的字段不得触发重算（例如只改地址不应重算按客户加总金额）。对 `distinct` / `addToSet`，Maintenance State 可在重复 key 或 set member 已计入时（以及 delete 并非最后一个贡献者时）做 value-level skip。

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
