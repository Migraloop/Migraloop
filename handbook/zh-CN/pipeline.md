# Pipeline

**Pipeline** 是 **Deployment** 内的用户定义流程，产出一个 target collection。Deployment 拥有 Source/Target 配对；Pipeline 拥有 mode、source 表引用、可选的 Rich Transform、Output Identity、Target Binding，以及字段映射覆盖。

## Modes

| Mode | 行为 |
| --- | --- |
| `direct` | 无 Rich Transform。把一个 Base Dataset Deliver 到 Target Binding。Output Identity 默认来自 source primary key。 |
| `transform` | 声明声明式 **Rich Transform**，物化 **Derived Dataset** 再 Deliver。需要非空的 `outputIdentity` 与至少一个 transform operator。 |

## 声明 Pipelines

Pipelines 位于 Deployment 文档的 `spec.pipelines`：

```yaml
pipelines:
  - name: orders_direct
    mode: direct
    source:
      table: ORDERS
      schema: APP                 # 可选
    target:
      collection: orders
    # 可选 Managed-field 覆盖（不安全的 NUMBER 等）
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

`apply` 会强制的验证规则：

- `mode` 为 `direct` 或 `transform`
- Direct Pipelines 不得声明 `transform`
- Transform Pipelines 需要 `outputIdentity` 与非空的声明式 `transform`
- `fields` 的 key 把 source/Managed 字段映射到 `{ as: string }` 或 `{ as: omit }`（ADR-0023）

Operator 形状见 [Rich Transform](rich-transform.md)。

## Lifecycle（control plane）

产品模型：在不重启整个 Deployment 的前提下 add、pause、resume、remove、change Pipelines（ADR-0007）。

**当前已出货 CLI 上 Operator 的做法：**

1. 编辑声明式 Deployment 文档（新增/变更/移除 Pipeline 项）。
2. `migraloop apply -f deployment.yaml` — upsert Deployment + Pipeline 集合；对新引用的表做 table-level **Initial Load**；当 Transform 修订需要时重建 Derived 输出；无关的 Pipeline 变更不会重建共用 Base Datasets。
3. `migraloop sync` — 对活跃工作做 Incremental Capture + Delivery。
4. `migraloop status` / `base` / `target` / `derived` — 检查进度与健康。

专用的 pause/resume 子命令仍属 control-plane 契约；在它们成为一等 CLI 动词之前，stream-wide blocker 请按 Operations 指引处理，并只在应该运行时把 Pipelines 留在声明中。

## Capture 范围

哪些 Source 表进入 Sync，由 Pipeline 的 `source.table` 引用决定。每张表在每个 Deployment 至多一个 Base Dataset，跨 Pipelines 共用。新表只做 table-level Initial Load。

## 相关章节

- Source prerequisites 与类型：[Source System](source-system.md)
- Target Binding / Managed fields：[Target System](target-system.md)
- Transform operators：[Rich Transform](rich-transform.md)
- 配置字段参考：[CLI 与 Config 参考](cli-and-config.md)
