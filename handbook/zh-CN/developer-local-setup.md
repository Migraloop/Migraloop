# Developer 本地设置

在此模块化 Rust monorepo 中 clone、build、启动 Platform Store，并运行测试。给 Operator 的产品用法在其他 handbook 章节—本页是 Developer 路径。

## 前置需求

- 符合 `rust-toolchain.toml` 的 Rust toolchain（stable）
- Docker / Docker Compose（Platform Store 与可选的集成测试依赖）
- Git

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
