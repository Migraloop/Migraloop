# Developer 本地设置

在此模块化 Rust monorepo 中 clone、build、启动 Platform Store，并运行测试。给 Operator 的产品用法在其他 handbook 章节—本页是 Developer 路径。

## 前置需求

- 符合 `rust-toolchain.toml` 的 Rust toolchain（stable）
- Docker / Docker Compose（Platform Store 与可选的集成测试依赖）
- Git
- **可选（live Oracle Source）：** 在运行 `migraloop` 的机器上安装 Oracle Instant Client Basic 或 Basic Light，并将 `LD_LIBRARY_PATH` 指向 Instant Client 目录。真实 host 的 Initial Load 与 LogMiner (OCI) 需要它；`host: contract` / `stub` 的 CI 切片不需要。

## Clone 与 build

```bash
git clone https://github.com/Migraloop/Migraloop.git
cd Migraloop
cargo build -p migraloop-app
```

Workspace members：`crates/app`（binary `migraloop`）、`cli`、`capture`、`platform-store`、`transform`、`delivery`，以及 `ci/handbook`（Handbook guard）。

## 以 compose 启动 Platform Store

```bash
docker compose up -d platform-store
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
cargo run -p migraloop-app -- migrate
cargo run -p migraloop-app -- status
```

完整双 container stack（store + app `run`）：

```bash
docker compose up -d --build
```

Compose 默认凭证（`migraloop` / `migraloop`）仅供本地开发。

## Local Sync Lab Fixture

可丢弃的 Oracle + MongoDB + Platform Store + app，供手动 Sync→Delivery 验证（非 CI）：

```bash
cargo build -p migraloop-app
./target/debug/migraloop lab up
./target/debug/migraloop lab status
./target/debug/migraloop lab scenario list
# Scenario apply/sync 需要 Instant Client：export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop lab scenario run direct-pipeline
# 用 migraloop base / target 查看留下的 Scenario Namespace，或直接变更 Lab DB。
# 重跑会先 wipe Namespace；或：lab scenario remove direct-pipeline / run --auto-remove
./target/debug/migraloop lab down
```

Bring-up 后默认：Platform Store `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Oracle `SYNC_USER` / `lab_oracle` @ `FREEPDB1`、MongoDB URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`。Lab bring-up 不会套用 sample Deployments/Pipelines。需要 Docker Compose；`lab up` 若缺少 binary 会构建 `target/debug/migraloop`，再由 `lab/Dockerfile` 打包（Ubuntu 24.04 base 以对齐 host glibc）。Lab Compose 使用 `network_mode: host`。第一次 Oracle 开机可能要数分钟。嵌套 Docker whiteout 解压失败时可用 dockerd `storage-driver: vfs`。见 [CLI 与 Config](cli-and-config.md)（`lab`）与 [Deployment](deployment.md)。

## 测试

Unit/crate 测试：

```bash
cargo test -p migraloop-capture
cargo test -p migraloop-transform
cargo test -p migraloop-cli
```

`crates/app/tests` 下的集成测试通常需要可连接的 Postgres（以及常需要 MongoDB），通过：

| 变量 | 常见默认 |
| --- | --- |
| `MIGRALOOP_TEST_ADMIN_URL` | `postgres://migraloop:migraloop@127.0.0.1:5432/postgres` |
| `MIGRALOOP_TEST_MONGO_HOST` / `MIGRALOOP_TEST_MONGO_PORT` | `127.0.0.1` / `27017` |

```bash
cargo test -p migraloop-app
```

Live Oracle Direct Pipeline seam（默认 ignored；需要 Instant Client + 已备妥 Prerequisites 的 Source）：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
export MIGRALOOP_LIVE_ORACLE_HOST=...
export MIGRALOOP_LIVE_ORACLE_PORT=1521
export MIGRALOOP_LIVE_ORACLE_SERVICE=FREEPDB1
export MIGRALOOP_LIVE_ORACLE_USER=SYNC_USER
export ORACLE_PASSWORD=...
cargo test -p migraloop-app --test cli_live_oracle_direct -- --ignored --nocapture
```

Lab Fixture lifecycle seam（默认 ignored；需要 Docker Compose + Lab Oracle image）：

```bash
cargo test -p migraloop-app --test cli_lab_fixture -- --ignored --nocapture
```

Lab Scenario Direct Pipeline seam（默认 ignored；需要 Docker Lab Fixture + Instant Client）：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
```

Operator 的 apply/sync/inspect 验证步骤见 [Source System](source-system.md)。

## Handbook guard（文档 CI seam）

变更 Operator/Developer 可见行为或 handbook 页面时，运行与 CI 相同的 entrypoint：

```bash
cargo test -p handbook-guard
cargo run -p handbook-guard -- check \
  --handbook handbook \
  --touchpoints ci/handbook/touchpoints.json \
  --cli-source crates/cli/src/lib.rs \
  --cli-surface ci/handbook/cli-surface.txt
```

`handbook/en`、`handbook/zh-TW`、`handbook/zh-CN` 下的 locale trees 必须保持路径同构。英文为 canonical。

## 目录提醒

| 路径 | 读者 |
| --- | --- |
| `handbook/` | Operators + Developers（本 portal） |
| `CONTEXT.md`、`docs/adr/` | 领域 glossary 与工程 ADRs |
| `docs/agents/` | Agent skill contracts |
| `ci/handbook/` | Handbook guards 的机器配置 |

## 相关章节

- Operator 短路径：[从这里开始](start-here.md)
- Compose 安装形态：[Deployment](deployment.md)
- CLI surface：[CLI 与 Config 参考](cli-and-config.md)
