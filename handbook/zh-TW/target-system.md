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

v1 不設定 Target timezone—Delivery 對時間型 Managed 欄位寫入 UTC datetime。

## Target Binding

會 Deliver 的每條 Pipeline 都要宣告 **Target Binding**：輸出寫入哪個 collection。

```yaml
target:
  collection: orders
```

Binding（連同 Pipeline）也隱含 **Output Identity** 與 **Managed Columns**。Target collection 可以有 binding 以外的其他欄位。

## Managed Columns / fields

**Managed Columns**（v1 為 document fields）是 Delivery 會寫入的輸出形狀。

- 在 MongoDB 上，平台**不會**盤點 non-managed 欄位—只是從不寫入 Managed 集合以外的 key，因此其他欄位維持不動。
- 當某個 **Output Identity** 在平台 dataset 中不再存在時，Delivery 可能 **刪除整個 target document**。
- 可靠度是 **at-least-once 搭配 idempotent apply**：重試可能重寫同一 identity；Managed 結果依 identity upsert/delete。

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

## Required Privileges（Target）

Delivery 帳號需要能在 bound collections 上 insert/update/delete（若你的維運模型允許，也可包含建立 collection）。偏好剛好足夠 Deliver 的最小授權—不是預設 cluster-admin（ADR-0016）。

## Mapping 注意事項

Source allow-list 與 NUMBER/時間規則會影響 Managed 輸出能出現什麼—見 [Source System](source-system.md)。不安全的 NUMBER 欄位必須先用 Pipeline `fields` 對應，`apply` 才會成功。

## 相關章節

- Deployment 配對：[Deployment](deployment.md)
- Pipeline modes 與 `fields`：[Pipeline](pipeline.md)
- Delivery Health：[Observability](observability.md)
- Secrets / TLS：[Security](security.md)
