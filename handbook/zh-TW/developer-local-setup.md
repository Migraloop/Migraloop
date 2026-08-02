# Developer 本機設定

在此模組化 Rust monorepo 中 clone、build、啟動 Platform Store，並執行測試。給 Operator 的產品用法在其他 handbook 章節—本頁是 Developer 路徑。

## 前置需求

- 符合 `rust-toolchain.toml` 的 Rust toolchain（stable）
- Docker / Docker Compose（Platform Store 與可選的整合測試相依）
- Git
- **可選（live Oracle Source）：** 在執行 `migraloop` 的機器上安裝 Oracle Instant Client Basic 或 Basic Light，並將 `LD_LIBRARY_PATH` 指向 Instant Client 目錄。真實 host 的 Initial Load 與 LogMiner (OCI) 需要它；`host: contract` / `stub` 的 CI 切片不需要。

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

## Local Sync Lab Fixture

可拋棄的 Oracle + MongoDB + Platform Store + app，供**手動** Sync→Delivery 驗證（ADR-0025）。與 **Release Quality Gate**／CI contract-stub harness 不同：由 operator 選擇 Lab Scenarios；**不要**把 Scenario catalog 當成 CI suite，也不要新增會跑完整 catalog 的 release-gate job。

```bash
cargo build -p migraloop-app
./target/debug/migraloop lab up
./target/debug/migraloop lab status
./target/debug/migraloop lab scenario list
# Scenario apply/sync 需要 Instant Client：export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop lab scenario run direct-pipeline
./target/debug/migraloop lab scenario run transform-pipeline
./target/debug/migraloop lab scenario run concurrent-source-workload
./target/debug/migraloop lab scenario run bulk-load
# 用 migraloop base / derived / target 檢視留下的 Scenario Namespace，或直接變更 Lab DB。
# 重跑會先 wipe Namespace；或：lab scenario remove <id> / run --auto-remove
./target/debug/migraloop lab down
```

Bring-up 後預設：Platform Store `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Oracle `SYNC_USER` / `lab_oracle` @ `FREEPDB1`、MongoDB URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`。Lab bring-up 不會套用 sample Deployments/Pipelines。需要 Docker Compose；`lab up` 若缺少 binary 會建置 `target/debug/migraloop`，再由 `lab/Dockerfile` 打包（Ubuntu 24.04 base 以對齊 host glibc）。Lab Compose 使用 `network_mode: host`。第一次 Oracle 開機可能要數分鐘。巢狀 Docker whiteout 解壓失敗時可用 dockerd `storage-driver: vfs`。見 [CLI 與 Config](cli-and-config.md)（`lab`）與 [Deployment](deployment.md)。

### 撰寫 Lab Scenario（feature-time coverage）

第一級 capability 在設計時就要一併規劃 Lab Scenario 覆蓋，否則視為未完成（ADR-0025）。開發 feature 時請走這條可重複路徑：

1. 建立 `lab/scenarios/<id>/`，包含：
   - `recipe.yaml` — catalog metadata：`id`、`summary`、**Scenario Namespace**（`source_tables`、`target_collections`、`deployment`、`pipelines`）、`workload`（`concurrency`：`serial`|`parallel`、有序 `steps`）、`checks.correctness`、可選的等權 `thresholds`（`max_settle_ms`、`max_lag`、`max_duration_ms`、`min_rows_per_s`）
   - `deployment.yaml` — 真實 product Deployment config（與 Operator `apply` 相同格式）
2. 在 `crates/cli/src/lab_scenario.rs` 實作 Namespace prepare/remove、Source workload、checks 與 thresholds，並向其他 runners 註冊 Scenario id。
3. 確認 `migraloop lab scenario list` 顯示新 id，且 **summary 來自 `recipe.yaml`**。Selectable catalog = 已註冊 runner，且在 `--lab-dir` 下同時有 recipe + deployment 檔。
4. 在 Lab Fixture 上手動驗證 `migraloop lab scenario run <id>`。list／控制面行為維持 always-on CLI-seam 測試；完整 Fixture run 維持 `#[ignore]` — 不是 Release Quality Gate。

Recipe 慣例與短清單亦見 `lab/scenarios/README.md`。

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

Live Oracle Direct Pipeline seam（預設 ignored；需要 Instant Client + 已備妥 Prerequisites 的 Source）：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
export MIGRALOOP_LIVE_ORACLE_HOST=...
export MIGRALOOP_LIVE_ORACLE_PORT=1521
export MIGRALOOP_LIVE_ORACLE_SERVICE=FREEPDB1
export MIGRALOOP_LIVE_ORACLE_USER=SYNC_USER
export ORACLE_PASSWORD=...
cargo test -p migraloop-app --test cli_live_oracle_direct -- --ignored --nocapture
```

Lab Fixture lifecycle seam（預設 ignored；需要 Docker Compose + Lab Oracle image）：

```bash
cargo test -p migraloop-app --test cli_lab_fixture -- --ignored --nocapture
```

Lab Scenario Direct Pipeline、多表 Transform Pipeline 、concurrent Source workload 與 bulk-load seams（預設 ignored；需要 Docker Lab Fixture + Instant Client）：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
```

Operator 的 apply/sync/inspect 驗證步驟見 [Source System](source-system.md)。

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
