# Deployment

一個 **Deployment** 恰好配對 **一個 Source System** 與 **一個 Target System**，並承載其間一或多條 **Pipeline**。若要不同的資料庫配對，請另建 Deployment—不要在同一個 Deployment 內做多資料庫 fan-in。

## 安裝形態（v1）

預設接近正式環境的安裝是 **一次安裝、兩個 container**：

| Service | 角色 |
| --- | --- |
| `platform-store` | 隨附的 PostgreSQL **Platform Store**（引擎由產品鎖定） |
| `app` | `migraloop` binary（`Dockerfile` 建置 release `migraloop-app`） |

在 repo 根目錄啟動：

```bash
docker compose up -d --build
```

Compose 會把 `MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@platform-store:5432/migraloop` 注入 app，並執行 `migraloop run`。可調整 Postgres volumes/resources，但不要更換 store 引擎。

若在 host 上對已 publish 的 store port `5432` 使用 Operator CLI：

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
migraloop migrate   # 若未使用 `run`
migraloop apply -f deployment.yaml
migraloop status
```

## Runtime 模型

- v1 以 **一個 active app instance**（內部可並行）加上 Platform Store 執行。
- 所有耐久 Deployment 狀態（Pipelines、Base/Derived Datasets、checkpoints）存在 Platform Store，替換 instance 才能續跑。
- 自動 multi-instance failover 屬後續階段；active processing 維持 single-leader（非 multi-writer）。

## 宣告 Deployment

設定為 YAML 或 JSON。必要頂層欄位：

```yaml
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: db.example.com
    port: 1521
    database: ORCL
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
    timezone: Asia/Taipei          # 可選；naive DATE/TIMESTAMP 後備
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines: []                    # 見 Pipeline 章節
```

v1 要求 `source.kind: oracle` 與 `target.kind: mongodb`。密碼必須是 secret reference—禁止明文。

以 `migraloop apply -f <file>` 套用。`pipelines` 為空時只套用 Deployment metadata（尚不 capture）。

## 相關章節

- Source 連線與 prerequisites：[Source System](source-system.md)
- Target Binding 與 Delivery：[Target System](target-system.md)
- Deployment 內的 Pipelines：[Pipeline](pipeline.md)
- 完整欄位/旗標清單：[CLI 與 Config 參考](cli-and-config.md)
