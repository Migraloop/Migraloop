# Security

Operator 如何為 Source System、Target System 與 Platform Store 提供憑證並保護連線。

## Secrets by reference

憑證**不得**以明文存在 Pipeline/Deployment 文件，也不得把已解析的密鑰值寫進 Platform Store 列（ADR-0006）。v1 接受來自：

| 參照形式 | 設定形狀 | 解析方式 |
| --- | --- | --- |
| 環境變數 | `password: { fromEnv: NAME }` | apply/sync 時的 `std::env` |
| 掛載檔案 | `password: { fromFile: /path/to/secret }` | 檔案內容（去掉尾端換行） |
| Docker secret | `password: { fromDockerSecret: name }` | `/run/secrets/<name>` |

必須恰好設定 **一個** `fromEnv`、`fromFile` 或 `fromDockerSecret`。明文 password 字串會讓設定驗證以清楚錯誤失敗。

範例：

```yaml
password:
  fromEnv: ORACLE_PASSWORD
```

外部密鑰管理（Vault / cloud KMS）可於後續加入；若你在 runtime 注入 env 或檔案，v1 不要求它們也能安全上線。

Compose 中的 Platform Store URL 可能為隨附 lab 風格 store 內嵌本機密碼—正式環境的 store 憑證請用與任何 Postgres DSN 相同的方式保護（env / orchestrator secrets），且不要把 Source/Target 密碼貼進 YAML。

**Local Sync Lab** 可拋棄預設（`migraloop lab up`）刻意方便本機開發，並在 bring-up 後印出（`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store `migraloop`/`migraloop`）。Lab Scenario 執行與 Namespace 清理（`migraloop lab scenario run direct-pipeline|transform-pipeline|concurrent-source-workload|bulk-load …`、`remove`、`--auto-remove`）會以相同的 Lab-only secret references，對可拋棄堆疊跑真實 `apply`/`sync` 與 Fixture DB 清理（Scenario recipes 位於 `lab/scenarios/<id>/`）。僅供 Lab 使用；絕不要把 Lab 指令或 Scenario 設定指向客戶正式環境資料庫。

## TLS / Connection Security

TLS **支援** Source、Target 與 Platform Store 連線，並在正式環境 **建議啟用**（ADR-0017）。本機/開發或明確選擇的環境仍允許 cleartext—v1 不會對每個非 TLS 連線硬性失敗。

Operator 指引：

- 正式網路中優先為 Oracle、MongoDB、Postgres 使用可 TLS 的連線路徑
- 密鑰材料不要進 shell history 或已提交的設定
- 限制 Source/Target 帳號的 Required Privileges（見 [Source System](source-system.md) 與 [Target System](target-system.md)）

## 公開環境變數面

| 變數 | 敏感度 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | 使用密碼 DSN 時含 store 憑證—以 orchestrator secrets 注入 |
| `fromEnv` 使用的名稱 | 密鑰值—永不提交 |

## 相關章節

- 設定形狀：[CLI 與 Config 參考](cli-and-config.md)
- 安裝預設：[Deployment](deployment.md)
- 本機 compose 密碼：[Developer 本機設定](developer-local-setup.md)
