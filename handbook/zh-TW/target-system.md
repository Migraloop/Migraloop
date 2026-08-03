# Target System

**Target System** 是平台 **Deliver** 進去的使用者資料庫。v1 提供 **MongoDB** document Delivery。

## 連線型態

在 Deployment 設定的 `spec.target` 下：

| 欄位 | 意義 |
| --- | --- |
| `kind` | v1 必須為 `mongodb` |
| `host` / `port` / `database` | MongoDB 連線識別 |
| `username` | 可對 bound collections 做 upsert/delete 的 Delivery 帳號 |
| `password` | Secret reference（`fromEnv`、`fromFile` 或 `fromDockerSecret`） |
| `tls` | 可選。設 `enabled: true` 以使用 Mongo TLS；`caFile` 為檔案系統 CA 路徑（不可 inline PEM）。見 [Security](security.md) |

v1 不設定 Target timezone—Delivery 對時間型 Managed 欄位寫入 UTC datetime（值已由 Source DB timezone 或 Deployment Source `timezone` 正規化；後者可為 IANA 名稱或 Oracle 風格 `±HH:MM`）。

## Target Binding

會 Deliver 的每條 Pipeline 都要宣告 **Target Binding**：輸出寫入哪個 collection。

```yaml
target:
  collection: orders
```

Binding（連同 Pipeline）也隱含 **Output Identity** 與 **Managed Columns**。Target collection 可以有 binding 以外的其他欄位。

## Managed Columns / fields

**Managed Columns**（v1 為 document fields）是 Delivery 會寫入的輸出形狀。Delivery 所有權依 Target kind 而異（ADR-0002）。**v1 僅提供 MongoDB document Delivery**；下列 relational 規則是給後續 relational Target Systems 的 design continuity—不是 v1 Delivery runtime。

### Document targets（v1：MongoDB）

- 平台**不會**盤點 non-managed 欄位—只是從不寫入 Managed 集合以外的 key，因此其他欄位維持不動。
- 當某個 **Output Identity** 在平台 dataset 中不再存在時，Delivery 可能 **刪除整個 target document**。

### Relational targets（未來）

在 relational Target Systems 上，Managed Columns 是平台必須在 target table **建立並維護的 schema**：

- Delivery **只建立／維護** table schema 中的 Managed Columns。
- Non-managed columns **不在 update 範圍內**—平台不擁有、不變更、也不覆寫它們。
- 當某個 **Output Identity** 消失時，Delivery 仍可能 **刪除整列 target row**（依 Output Identity 的 full-row delete），與 document targets 相同。

### 可靠度

可靠度是 **at-least-once 搭配 idempotent apply**：重試可能重寫同一 identity；Managed 結果依 identity upsert/delete。

## Direct vs Transform Delivery

| Pipeline mode | Deliver 的內容 |
| --- | --- |
| `direct` | Base Dataset 列形狀（flattened fields）。Source primary key 對應 document identity（`_id` 或設定的 id）。 |
| `transform` | Rich Transform 產生的 **Derived Dataset**。Operator 必須宣告 `outputIdentity`。 |

檢查已 Deliver 的文件：

```bash
migraloop target --collection orders
# 可選：名稱衝突時加 --deployment <name>
```

## Required Privileges (Target)

MongoDB Delivery 帳號的具體授權（ADR-0016）。**不需要 `root` / `clusterAdmin`**—請使用限定在 Target database（若維運模型允許，可再限縮到 bound collections）的專用使用者。

### v1 Delivery + Target 檢視必要

Delivery 會對每個 Pipeline 的 bound collection 依 Output Identity 做 upsert（`update` 且 `upsert: true`）與 `delete`，並以 `find` 支援 `migraloop target` 檢視。在 `spec.target.database` 所指的 Target database 上，最小 role 為：

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

該 database 上的 `readWrite` 包含其 collections 的 `find`、`insert`、`update`、`remove` 與 `createCollection`—足以支援 Delivery 與 CLI Target 檢視（collection 可在首次寫入時建立）。

### 較窄的自訂 role（選用）

若你已預先建立每個 bound collection，並希望用 collection 範圍授權取代 database 級 `readWrite`：

```javascript
use <target_database>
db.createRole({
  role: "migraloopDeliver",
  privileges: [
    {
      resource: { db: "<target_database>", collection: "<bound_collection>" },
      actions: ["find", "insert", "update", "remove"]
    }
    // 每個 Pipeline Target Binding 重複 resource+actions
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

若首次 Delivery 必須建立尚不存在的 collection，請在 database 上加入 `createCollection`（或事先建立 collections）。

### 選用／不需要

| Privilege | 狀態 |
| --- | --- |
| `root`、`clusterAdmin`、`dbAdminAnyDatabase` | Delivery **不需要**。Local Sync Lab 的可拋棄 Mongo 使用者為 root 僅為 Fixture 方便—不是正式預設。 |
| `dropCollection` / `dropDatabase` | 產品 Delivery **不需要**（Lab Scenario 清理可能使用較寬的 Fixture 憑證）。 |

v1 連線字串以 `authSource=admin` 驗證（見 Delivery URI 建構）。請在部署預期的 auth database 建立使用者，並把密碼放在 secret reference（[Security](security.md)）。Source sync 帳號 grants：[Source System](source-system.md)。

## Mapping 注意事項

Source allow-list 與 NUMBER/時間規則會影響 Managed 輸出能出現什麼—見 [Source System](source-system.md)。不安全的 NUMBER 欄位必須先用 Pipeline `fields` 對應，`apply` 才會成功。

## 相關章節

- Deployment 配對：[Deployment](deployment.md)
- Pipeline modes 與 `fields`：[Pipeline](pipeline.md)
- Delivery Health：[Observability](observability.md)
- Secrets、TLS 與 privilege 指引：[Security](security.md#required-privileges-pointer)
- Oracle sync grants：[Source System](source-system.md#required-privileges)
