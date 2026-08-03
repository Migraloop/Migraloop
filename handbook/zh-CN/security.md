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

**Local Sync Lab** 可丢弃默认（`migraloop lab up`）刻意方便本地开发，并在 bring-up 后打印（`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store `migraloop`/`migraloop`）；`migraloop lab status` 也会一并显示这些 Lab-only 连接细节与 active/leftover Scenario Namespace 状态。Lab Scenario 运行与 Namespace 清理（`migraloop lab scenario run direct-pipeline|rt-project|rt-filter|rt-field-ops|transform-pipeline|concurrent-source-workload|bulk-load|idempotent-redelivery|pause-resume|remove-pipeline|change-pipeline|poison-quarantine|schema-change-pause|source-alignment|drift-check|bounded-backpressure|observability-surface|platform-store-guardrails|backward-compatible-upgrades|initial-load-throttled …`、`remove`、`--auto-remove`）会以相同的 Lab-only secret references，对可丢弃堆栈跑真实 `apply`/`sync` 与 Fixture DB 清理（Scenario recipes 位于 `lab/scenarios/<id>/`）。DB-level restore/load escape hatch（`lab/escape-hatch/`）使用相同打印的 Lab credentials，对可丢弃 engines 跑 compose-exec sqlplus/mongosh（或 dump 工具），再接普通 `apply`／`sync`；它不是 Scenario，也绝不可指向客户／生产环境数据库。Lab secrets 仅供 Lab；绝不要把 Lab 命令或 Scenario 配置指向客户生产环境数据库。CLI 会在 Scenario `run` 强制此规则：Source/Target 若不是 Lab Fixture engines，会在 apply/sync 之前被拒绝—真实 Deployments 仍走普通的 `migraloop apply` / `migraloop sync`。嵌套 Docker／**Cursor Cloud** Lab bring-up storage-driver 说明：见 [Developer local setup](developer-local-setup.md) 与 [Deployment](deployment.md)。

## TLS / Connection Security

TLS **支持** Source、Target 与 Platform Store 连接，并在生产环境 **建议启用**（ADR-0017）。本地/开发或明确选择的环境仍允许 cleartext—v1 不会对每个非 TLS 连接硬性失败。一旦请求 TLS，配置错误会在 apply/run 明确失败，**不会静默回退到 cleartext**。

### Source / Target（`spec.source.tls` / `spec.target.tls`）

各系统可选的块。省略该块（或设 `enabled: false`）即为 cleartext Lab/开发。

| 字段 | Source（Oracle） | Target（MongoDB） | 说明 |
| --- | --- | --- | --- |
| `enabled` | `true` 要求 TCPS | `true` 要求 Mongo TLS | 省略时默认：禁用（允许 cleartext） |
| `caFile` | **无效**（apply 会拒绝；请用 `walletLocation`） | CA 路径（`tlsCAFile`） | 仅文件系统路径—绝不要把 PEM 贴进 YAML 或 `password` |
| `walletLocation` | Instant Client wallet 目录 | **无效**（apply 会拒绝） | Oracle `MY_WALLET_DIRECTORY` |
| `insecureSkipVerify` | 可选（`SSL_SERVER_DN_MATCH=no`） | 可选（允许无效证书） | 仅供开发/Lab；生产环境保持 `false` |

示例（路径是引用，不是密钥本体）：

```yaml
source:
  # ...
  tls:
    enabled: true
    walletLocation: /etc/oracle/wallet
target:
  # ...
  tls:
    enabled: true
    caFile: /etc/migraloop/certs/mongo-ca.pem
```

`migraloop status` 会显示非密钥的 TLS 标志/路径（`tls=enabled|disabled`、`caFile=…`、`walletLocation=…`），绝不打印 PEM 本体或密码。

### Platform Store

在 `MIGRALOOP_PLATFORM_STORE_URL` 以 Postgres libpq 风格查询参数配置 TLS：

| 参数 | 用途 |
| --- | --- |
| `sslmode=require` / `verify-ca` / `verify-full` | 要求 TLS（不回退 cleartext） |
| `sslmode=prefer` / `disable`（或省略） | 方便本地/开发的 cleartext |
| `sslrootcert=/path/to/ca.pem` | 验证模式用的 CA 文件 |

示例：`postgres://migraloop:***@db:5432/migraloop?sslmode=require&sslrootcert=/run/certs/pg-ca.pem`

Operator 指引：

- 生产网络中优先为 Oracle、MongoDB、Postgres 使用可 TLS 的连接路径
- 密钥材料与证书 PEM 本体不要进 shell history 或已提交的配置—使用挂载路径与 secret references
- 限制 Source/Target 账号的 Required Privileges（具体 grants 见下方）

## Required Privileges (pointer)

ADR-0016：记载并偏好刚好足以运行的最小权限—不是默认就要 DBA/admin。各引擎的具体 grants 放在连接章节：

| 账号 | 章节 | 覆盖 |
| --- | --- | --- |
| Oracle Source sync 用户 | [Source System → Required Privileges](source-system.md#required-privileges) | Initial Load、LogMiner Incremental Capture、Prerequisites probe、schema discovery；必要 vs 仅 Lab vs 由 DBA 套用的 Prerequisites DDL |
| MongoDB Target Delivery 用户 | [Target System → Required Privileges](target-system.md#required-privileges-target) | Delivery upsert/delete、Target 检视；`readWrite` vs collection 范围自定义 role；Lab root 不是生产默认 |

这些账号只能用 secret reference（`fromEnv` / `fromFile` / `fromDockerSecret`）写入 Deployment 配置—YAML 中禁止明文密码。

## 公开环境变量面

| 变量 | 敏感度 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | 使用密码 DSN 时含 store 凭证—以 orchestrator secrets 注入 |
| `fromEnv` 使用的名称 | 密钥值—永不提交 |

## 相关章节

- 配置形状：[CLI 与 Config 参考](cli-and-config.md)
- 安装默认：[Deployment](deployment.md)
- 本地 compose 密码：[Developer 本地设置](developer-local-setup.md)
- Oracle sync grants：[Source System](source-system.md#required-privileges)
- MongoDB Delivery grants：[Target System](target-system.md#required-privileges-target)
