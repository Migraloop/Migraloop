# Pipeline

**Pipeline** 是 **Deployment** 内的用户定义流程，产出一个 target collection。Deployment 拥有 Source/Target 配对；Pipeline 拥有 mode、source 表引用、可选的 Rich Transform、Output Identity、Target Binding，以及字段映射覆盖。

## Modes

| Mode | 行为 |
| --- | --- |
| `direct` | 无 Rich Transform。把一个 Base Dataset Deliver 到 Target Binding。Output Identity 默认来自 source primary key。 |
| `transform` | 声明声明式 **Rich Transform**，物化 **Derived Dataset** 再 Deliver。需要非空的 `outputIdentity` 与至少一个 transform operator。 |

## 声明 Pipelines

Pipelines 位于 Deployment 文档的 `spec.pipelines`。外层 Deployment 的 `apiVersion` 必须是 major `1` 内 SemVer 较旧或相等（正式写法 `migraloop.dev/v1`；较旧可接受形式如 `migraloop.dev/v1.0.0` 仍可套用 Pipeline 声明且无需 wipe-rebuild — 见 [Operations](operations.md) Upgrades 与 [CLI 与 Config](cli-and-config.md)）：

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

Operator 形状见 [Rich Transform](rich-transform.md)（`project`、`addFields`、`rename`、`remove`、`filter`、`groupBy`（含 sum/count/min/max/avg））。

## Lifecycle（control plane）

产品模型：在不重启整个 Deployment 的前提下 add、pause、resume、remove、change Pipelines（ADR-0007）。

**当前已出货 CLI 上 Operator 的做法：**

1. 编辑声明式 Deployment 文档（新增/变更/移除 Pipeline 项）。
2. `migraloop apply -f deployment.yaml` — upsert Deployment + Pipeline 集合；对新引用的表做 table-level **Initial Load**；当 Pipeline 的语义声明变更时套用 **revision**（mode、Source table、Target Binding、fields、Output Identity 或 transform）：暂停该 Pipeline 的旧 Delivery，按需要重建其 Derived Dataset 并重新 Delivery（含消失 identity 的 delete reconciliation），然后继续 incremental。Shared Base Datasets 不会因 Pipeline revision 而重建。仅变更可选的 `description` 属 metadata-only，可跳过 rebuild／re-Delivery。无关 Pipeline 持续运行。
3. `migraloop sync` — 对活跃（未 pause）的 Pipelines 做 Incremental Capture + Delivery。
4. `migraloop pause --pipeline <name>` / `migraloop resume --pipeline <name>` — 在不重启 Deployment 的前提下，停止或继续单一 Pipeline 的 Delivery/processing。Pause 会耐久写入 Platform Store；resume 会按当前 Base/Derived 状态做 catch-up Delivery。其他 Pipelines 不受影响。`status` 会在该 Pipeline 及其 Delivery Health 上显示 `paused`。
5. `migraloop remove --pipeline <name>` — 在不重启 Deployment 的前提下停止该 Pipeline 并停止 Delivery。若其他 Pipelines 仍引用，Shared Base Datasets 会保留；不再被引用的 Bases 会被 prune。`status` 不再把该 Pipeline 列为 active。若要在之后的 `apply` 中持续省略它，也请从 declarative config 移除该 Pipeline 项。
6. `migraloop status` / `base` / `target` / `derived` — 检查进度与健康。

Stream-wide blockers（例如无法解除的 DDL）仍按 [Operations](operations.md) 的 pause 指引；Operator 主动 pause/resume/remove／通过 apply 的 change 则是刻意停止与 revision 的一等 control-plane 路径。

## Capture 范围

哪些 Source 表进入 Sync，由 Pipeline 的 `source.table` 引用决定。每张表在每个 Deployment 至多一个 Base Dataset，跨 Pipelines 共用。新表只做 table-level Initial Load。

Source/Target 的 TLS 与 secrets 属于外层 Deployment 的 `spec.source` / `spec.target`（不在 Pipeline 项上）—见 [Security](security.md) 与 [CLI 与 Config](cli-and-config.md)。

## 相关章节

- Source prerequisites 与类型：[Source System](source-system.md)
- Target Binding / Managed fields：[Target System](target-system.md)
- Transform operators：[Rich Transform](rich-transform.md)
- 配置字段参考：[CLI 与 Config 参考](cli-and-config.md)
