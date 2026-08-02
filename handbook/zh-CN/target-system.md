# Target System

**Target System** 是平台 **Deliver** 进去的用户数据库。v1 提供 **MongoDB** document Delivery。

## 连接形态

在 Deployment 配置的 `spec.target` 下：

| 字段 | 含义 |
| --- | --- |
| `kind` | v1 必须为 `mongodb` |
| `host` / `port` / `database` | MongoDB 连接标识 |
| `username` | 可对 bound collections 做 upsert/delete 的 Delivery 账号 |
| `password` | Secret reference（`fromEnv`、`fromFile` 或 `fromDockerSecret`） |

v1 不配置 Target timezone—Delivery 对时间型 Managed 字段写入 UTC datetime。

## Target Binding

会 Deliver 的每条 Pipeline 都要声明 **Target Binding**：输出写入哪个 collection。

```yaml
target:
  collection: orders
```

Binding（连同 Pipeline）也隐含 **Output Identity** 与 **Managed Columns**。Target collection 可以有 binding 以外的其他字段。

## Managed Columns / fields

**Managed Columns**（v1 为 document fields）是 Delivery 会写入的输出形状。

- 在 MongoDB 上，平台**不会**盘点 non-managed 字段—只是从不写入 Managed 集合以外的 key，因此其他字段保持不动。
- 当某个 **Output Identity** 在平台 dataset 中不再存在时，Delivery 可能 **删除整个 target document**。
- 可靠性是 **at-least-once 搭配 idempotent apply**：重试可能重写同一 identity；Managed 结果按 identity upsert/delete。

## Direct vs Transform Delivery

| Pipeline mode | Deliver 的内容 |
| --- | --- |
| `direct` | Base Dataset 行形状（flattened fields）。Source primary key 对应 document identity（`_id` 或配置的 id）。 |
| `transform` | Rich Transform 产生的 **Derived Dataset**。Operator 必须声明 `outputIdentity`。 |

检查已 Deliver 的文档：

```bash
migraloop target --collection orders
# 可选：名称冲突时加 --deployment <name>
```

## Required Privileges（Target）

Delivery 账号需要能在 bound collections 上 insert/update/delete（若你的运维模型允许，也可包含创建 collection）。偏好刚好足够 Deliver 的最小授权—不是默认 cluster-admin（ADR-0016）。

## Mapping 注意事项

Source allow-list 与 NUMBER/时间规则会影响 Managed 输出能出现什么—见 [Source System](source-system.md)。不安全的 NUMBER 字段必须先用 Pipeline `fields` 映射，`apply` 才会成功。

## 相关章节

- Deployment 配对：[Deployment](deployment.md)
- Pipeline modes 与 `fields`：[Pipeline](pipeline.md)
- Delivery Health：[Observability](observability.md)
- Secrets / TLS：[Security](security.md)
