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
| `tls` | 可选。设 `enabled: true` 以使用 Mongo TLS；`caFile` 为文件系统 CA 路径（不可 inline PEM）。见 [Security](security.md) |

v1 不配置 Target timezone—Delivery 对时间型 Managed 字段写入 UTC datetime（值已由 Source DB timezone 或 Deployment Source `timezone` 正规化；后者可为 IANA 名称或 Oracle 风格 `±HH:MM`）。

## Target Binding

会 Deliver 的每条 Pipeline 都要声明 **Target Binding**：输出写入哪个 collection。

```yaml
target:
  collection: orders
```

Binding（连同 Pipeline）也隐含 **Output Identity** 与 **Managed Columns**。Target collection 可以有 binding 以外的其他字段。

## Managed Columns / fields

**Managed Columns**（v1 为 document fields）是 Delivery 会写入的输出形状。Delivery 所有权依 Target kind 而异（ADR-0002）。**v1 仅提供 MongoDB document Delivery**；下列 relational 规则是给后续 relational Target Systems 的 design continuity—不是 v1 Delivery runtime。

### Document targets（v1：MongoDB）

- 平台**不会**盘点 non-managed 字段—只是从不写入 Managed 集合以外的 key，因此其他字段保持不动。
- 当某个 **Output Identity** 在平台 dataset 中不再存在时，Delivery 可能 **删除整个 target document**。

### Relational targets（未来）

在 relational Target Systems 上，Managed Columns 是平台必须在 target table **创建并维护的 schema**：

- Delivery **只创建／维护** table schema 中的 Managed Columns。
- Non-managed columns **不在 update 范围内**—平台不拥有、不变更、也不覆写它们。
- 当某个 **Output Identity** 消失时，Delivery 仍可能 **删除整行 target row**（按 Output Identity 的 full-row delete），与 document targets 相同。

### 可靠性

可靠性是 **at-least-once 搭配 idempotent apply**：重试可能重写同一 identity；Managed 结果按 identity upsert/delete。

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

## Required Privileges (Target)

MongoDB Delivery 账号的具体授权（ADR-0016）。**不需要 `root` / `clusterAdmin`**—请使用限定在 Target database（若运维模型允许，可再限缩到 bound collections）的专用用户。

### v1 Delivery + Target 检视必要

Delivery 会对每个 Pipeline 的 bound collection 按 Output Identity 做 upsert（`update` 且 `upsert: true`）与 `delete`，并以 `find` 支持 `migraloop target` 检视。在 `spec.target.database` 所指的 Target database 上，最小 role 为：

```javascript
use admin
db.createUser({
  user: "deliver_user",
  pwd: passwordPrompt(),  // 或由你的 secret-manager 注入
  roles: [
    { role: "readWrite", db: "<target_database>" }
  ]
})
```

该 database 上的 `readWrite` 包含其 collections 的 `find`、`insert`、`update`、`remove` 与 `createCollection`—足以支持 Delivery 与 CLI Target 检视（collection 可在首次写入时创建）。

### 更窄的自定义 role（选用）

若你已预先创建每个 bound collection，并希望用 collection 范围授权取代 database 级 `readWrite`：

```javascript
use <target_database>
db.createRole({
  role: "migraloopDeliver",
  privileges: [
    {
      resource: { db: "<target_database>", collection: "<bound_collection>" },
      actions: ["find", "insert", "update", "remove"]
    }
    // 每个 Pipeline Target Binding 重复 resource+actions
  ],
  roles: []
})
use admin
db.createUser({
  user: "deliver_user",
  pwd: passwordPrompt(),
  roles: [{ role: "migraloopDeliver", db: "<target_database>" }]
})
```

若首次 Delivery 必须创建尚不存在的 collection，请在 database 上加入 `createCollection`（或事先创建 collections）。

### 选用／不需要

| Privilege | 状态 |
| --- | --- |
| `root`、`clusterAdmin`、`dbAdminAnyDatabase` | Delivery **不需要**。Local Sync Lab 的可抛弃 Mongo 用户为 root 仅为 Fixture 方便—不是生产默认。 |
| `dropCollection` / `dropDatabase` | 产品 Delivery **不需要**（Lab Scenario 清理可能使用更宽的 Fixture 凭证）。 |

v1 连接字符串以 `authSource=admin` 验证（见 Delivery URI 构建）。请在部署预期的 auth database 创建用户，并把密码放在 secret reference（[Security](security.md)）。Source sync 账号 grants：[Source System](source-system.md)。

## Mapping 注意事项

Source allow-list 与 NUMBER/时间规则会影响 Managed 输出能出现什么—见 [Source System](source-system.md)。不安全的 NUMBER 字段必须先用 Pipeline `fields` 映射，`apply` 才会成功。NUMBER→Mongo classification 只有一个 shared home，位于 `ColumnShape` 旁（ADR-0023）；Operator 可见的 mapping 规则不变。

## 新增另一个 Target engine（Developers）

v1 仅出货 MongoDB document Delivery。新的 Target kind 实现 `TargetEngine`（外加 Delivery grants／文档、Lab Scenario，以及 CI contract twin），且不重塑 Sync／Rich Transform／Delivery／runtime 概念——见 [Developer 本地设置 — 新增 Source 或 Target engine](developer-local-setup.md#新增-source-或-target-enginedeveloper-checklist)。

## 相关章节

- Deployment 配对：[Deployment](deployment.md)
- Pipeline modes 与 `fields`：[Pipeline](pipeline.md)
- Delivery Health：[Observability](observability.md)
- Secrets、TLS 与 privilege 指引：[Security](security.md#required-privileges-pointer)
- Oracle sync grants：[Source System](source-system.md#required-privileges)
