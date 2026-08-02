# Developer 本機設定

在此模組化 Rust monorepo 中 clone、build、啟動 Platform Store，並執行測試。給 Operator 的產品用法在其他 handbook 章節—本頁是 Developer 路徑。

## 前置需求

- 符合 `rust-toolchain.toml` 的 Rust toolchain（stable）
- Docker / Docker Compose（Platform Store 與可選的整合測試相依）
- Git

## Clone 與 build

```bash
git clone https://github.com/Migraloop/Migraloop.git
cd Migraloop
cargo build -p migraloop-app
```

Workspace members：`crates/app`（binary `migraloop`）、`cli`、`capture`、`platform-store`、`transform`、`delivery`，以及 `ci/handbook`（Handbook guard）。

## 以 compose 啟動 Platform Store

```bash
docker compose up -d platform-store
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
cargo run -p migraloop-app -- migrate
cargo run -p migraloop-app -- status
```

完整雙 container stack（store + app `run`）：

```bash
docker compose up -d --build
```

Compose 預設憑證（`migraloop` / `migraloop`）僅供本機開發。

## 測試

Unit/crate 測試：

```bash
cargo test -p migraloop-capture
cargo test -p migraloop-transform
cargo test -p migraloop-cli
```

`crates/app/tests` 下的整合測試通常需要可連線的 Postgres（以及常需要 MongoDB），透過：

| 變數 | 常見預設 |
| --- | --- |
| `MIGRALOOP_TEST_ADMIN_URL` | `postgres://migraloop:migraloop@127.0.0.1:5432/postgres` |
| `MIGRALOOP_TEST_MONGO_HOST` / `MIGRALOOP_TEST_MONGO_PORT` | `127.0.0.1` / `27017` |

```bash
cargo test -p migraloop-app
```

## Handbook guard（文件 CI seam）

變更 Operator/Developer 可見行為或 handbook 頁面時，執行與 CI 相同的 entrypoint：

```bash
cargo test -p handbook-guard
cargo run -p handbook-guard -- check \
  --handbook handbook \
  --touchpoints ci/handbook/touchpoints.json \
  --cli-source crates/cli/src/lib.rs \
  --cli-surface ci/handbook/cli-surface.txt
```

`handbook/en`、`handbook/zh-TW`、`handbook/zh-CN` 下的 locale trees 必須保持路徑同構。英文為 canonical。

## 目錄提醒

| 路徑 | 讀者 |
| --- | --- |
| `handbook/` | Operators + Developers（本 portal） |
| `CONTEXT.md`、`docs/adr/` | 領域 glossary 與工程 ADRs |
| `docs/agents/` | Agent skill contracts |
| `ci/handbook/` | Handbook guards 的機器設定 |

## 相關章節

- Operator 短路徑：[從這裡開始](start-here.md)
- Compose 安裝形態：[Deployment](deployment.md)
- CLI surface：[CLI 與 Config 參考](cli-and-config.md)
