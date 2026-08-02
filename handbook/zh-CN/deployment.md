# Deployment

一个 **Deployment** 恰好配对 **一个 Source System** 与 **一个 Target System**，并承载其间一或多条 **Pipeline**。若要不同的数据库配对，请另建 Deployment—不要在同一个 Deployment 内做多数据库 fan-in。

## 安装形态（v1）

默认接近生产环境的安装是 **一次安装、两个 container**：

| Service | 角色 |
| --- | --- |
| `platform-store` | 随附的 PostgreSQL **Platform Store**（引擎由产品锁定） |
| `app` | `migraloop` binary（`Dockerfile` 构建 release `migraloop-app`） |

在 repo 根目录启动：

```bash
docker compose up -d --build
```

Compose 会把 `MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@platform-store:5432/migraloop` 注入 app，并执行 `migraloop run`。可调整 Postgres volumes/resources，但不要更换 store 引擎。

若在 host 上对已 publish 的 store port `5432` 使用 Operator CLI：

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
migraloop migrate   # 若未使用 `run`
migraloop apply -f deployment.yaml
migraloop status
```

## Runtime 模型

- v1 以 **一个 active app instance**（内部可并行）加上 Platform Store 运行。
- 所有持久 Deployment 状态（Pipelines、Base/Derived Datasets、checkpoints）存在 Platform Store，替换 instance 才能续跑。
- 自动 multi-instance failover 属后续阶段；active processing 保持 single-leader（非 multi-writer）。

## 声明 Deployment

配置为 YAML 或 JSON。必要顶层字段：

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
    timezone: Asia/Shanghai        # 可选；naive DATE/TIMESTAMP 后备
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines: []                    # 见 Pipeline 章节
```

v1 要求 `source.kind: oracle` 与 `target.kind: mongodb`。密码必须是 secret reference—禁止明文。

以 `migraloop apply -f <file>` 应用。`pipelines` 为空时只应用 Deployment metadata（尚不 capture）。

## 相关章节

- Source 连接与 prerequisites：[Source System](source-system.md)
- Target Binding 与 Delivery：[Target System](target-system.md)
- Deployment 内的 Pipelines：[Pipeline](pipeline.md)
- 完整字段/标志清单：[CLI 与 Config 参考](cli-and-config.md)
