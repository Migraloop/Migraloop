# Security

Operator 如何为 Source System、Target System 与 Platform Store 提供凭证并保护连接。

## Secrets by reference

凭证**不得**以明文存在 Pipeline/Deployment 文档，也不得把已解析的密钥值写进 Platform Store 行（ADR-0006）。v1 接受来自：

| 引用形式 | 配置形状 | 解析方式 |
| --- | --- | --- |
| 环境变量 | `password: { fromEnv: NAME }` | apply/sync 时的 `std::env` |
| 挂载文件 | `password: { fromFile: /path/to/secret }` | 文件内容（去掉尾部换行） |
| Docker secret | `password: { fromDockerSecret: name }` | `/run/secrets/<name>` |

必须恰好设置 **一个** `fromEnv`、`fromFile` 或 `fromDockerSecret`。明文 password 字符串会让配置验证以清楚错误失败。

示例：

```yaml
password:
  fromEnv: ORACLE_PASSWORD
```

外部密钥管理（Vault / cloud KMS）可于后续加入；若你在 runtime 注入 env 或文件，v1 不要求它们也能安全上线。

Compose 中的 Platform Store URL 可能为随附 lab 风格 store 内嵌本地密码—生产环境的 store 凭证请用与任何 Postgres DSN 相同的方式保护（env / orchestrator secrets），且不要把 Source/Target 密码贴进 YAML。

**Local Sync Lab** 可丢弃默认（`migraloop lab up`）刻意方便本地开发，并在 bring-up 后打印（`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store `migraloop`/`migraloop`）；`migraloop lab status` 也会一并显示这些 Lab-only 连接细节与 active/leftover Scenario Namespace 状态。Lab Scenario 运行与 Namespace 清理（`migraloop lab scenario run direct-pipeline|rt-project|rt-filter|transform-pipeline|concurrent-source-workload|bulk-load|idempotent-redelivery|pause-resume|remove-pipeline|change-pipeline|poison-quarantine|schema-change-pause|source-alignment …`、`remove`、`--auto-remove`）会以相同的 Lab-only secret references，对可丢弃堆栈跑真实 `apply`/`sync` 与 Fixture DB 清理（Scenario recipes 位于 `lab/scenarios/<id>/`）。DB-level restore/load escape hatch（`lab/escape-hatch/`）使用相同打印的 Lab credentials，对可丢弃 engines 跑 compose-exec sqlplus/mongosh（或 dump 工具），再接普通 `apply`／`sync`；它不是 Scenario，也绝不可指向客户／生产环境数据库。Lab secrets 仅供 Lab；绝不要把 Lab 命令或 Scenario 配置指向客户生产环境数据库。CLI 会在 Scenario `run` 强制此规则：Source/Target 若不是 Lab Fixture engines，会在 apply/sync 之前被拒绝—真实 Deployments 仍走普通的 `migraloop apply` / `migraloop sync`。嵌套 Docker／**Cursor Cloud** Lab bring-up storage-driver 说明：见 [Developer local setup](developer-local-setup.md) 与 [Deployment](deployment.md)。

## TLS / Connection Security

TLS **支持** Source、Target 与 Platform Store 连接，并在生产环境 **建议启用**（ADR-0017）。本地/开发或明确选择的环境仍允许 cleartext—v1 不会对每个非 TLS 连接硬性失败。

Operator 指引：

- 生产网络中优先为 Oracle、MongoDB、Postgres 使用可 TLS 的连接路径
- 密钥材料不要进 shell history 或已提交的配置
- 限制 Source/Target 账号的 Required Privileges（见 [Source System](source-system.md) 与 [Target System](target-system.md)）

## 公开环境变量面

| 变量 | 敏感度 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | 使用密码 DSN 时含 store 凭证—以 orchestrator secrets 注入 |
| `fromEnv` 使用的名称 | 密钥值—永不提交 |

## 相关章节

- 配置形状：[CLI 与 Config 参考](cli-and-config.md)
- 安装默认：[Deployment](deployment.md)
- 本地 compose 密码：[Developer 本地设置](developer-local-setup.md)
