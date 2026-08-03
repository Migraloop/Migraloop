# Developer 本機設定

在此模組化 Rust monorepo 中 clone、build、啟動 Platform Store，並執行測試。給 Operator 的產品用法在其他 handbook 章節—本頁是 Developer 路徑。

## 前置需求

- 符合 `rust-toolchain.toml` 的 Rust toolchain（stable）
- Docker / Docker Compose（Platform Store 與可選的整合測試相依）
- Git
- **可選（live Oracle Source）：** 在執行 `migraloop` 的機器上安裝 Oracle Instant Client Basic 或 Basic Light，並將 `LD_LIBRARY_PATH` 指向 Instant Client 目錄。真實 host 的 Initial Load 與 LogMiner (OCI) 需要它；`host: contract` / `stub` 的 CI 切片不需要。
- **可選（contract/stub CI 切片）：** `MIGRALOOP_CONTRACT_SOURCE_CATALOG` 可指向 JSON 檔，merge/override 行程內 contract Source catalog 以供 schema discovery + Initial Load（見 [Source System](source-system.md)／[CLI 與 Config](cli-and-config.md)）。預設命名 fixtures 仍供情境可讀性；這不是 production Source 機制。

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
./target/debug/migraloop lab status   # Fixture ready + Scenario run active/leftover/(none)
./target/debug/migraloop lab scenario list
# Scenario apply/sync 需要 Instant Client：export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop lab scenario run direct-pipeline
./target/debug/migraloop lab scenario run rt-project
./target/debug/migraloop lab scenario run rt-filter
./target/debug/migraloop lab scenario run transform-pipeline
./target/debug/migraloop lab scenario run concurrent-source-workload
./target/debug/migraloop lab scenario run bulk-load
./target/debug/migraloop lab scenario run idempotent-redelivery
./target/debug/migraloop lab scenario run pause-resume
./target/debug/migraloop lab scenario run remove-pipeline
./target/debug/migraloop lab scenario run change-pipeline
./target/debug/migraloop lab scenario run poison-quarantine
./target/debug/migraloop lab scenario run schema-change-pause
./target/debug/migraloop lab scenario run source-alignment
./target/debug/migraloop lab scenario run drift-check
./target/debug/migraloop lab scenario run bounded-backpressure
./target/debug/migraloop lab scenario run observability-surface
./target/debug/migraloop lab scenario run platform-store-guardrails
./target/debug/migraloop lab scenario run backward-compatible-upgrades
# keep-on-finish 後 lab status 會標出 leftover Namespace；也可用 base / derived / target 檢視。
# 重跑會先 wipe Namespace；或：lab scenario remove <id> / run --auto-remove
./target/debug/migraloop lab down
```

Bring-up 後預設：Platform Store `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Oracle `SYNC_USER` / `lab_oracle` @ `FREEPDB1`、MongoDB URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`。Lab bring-up 不會套用 sample Deployments/Pipelines。需要 Docker Compose；`lab up` 若缺少 binary 會建置 `target/debug/migraloop`，再由 `lab/Dockerfile` 打包（Ubuntu 24.04 base 以對齊 host glibc）。Lab Compose 使用 `network_mode: host`。第一次 Oracle 開機可能要數分鐘。巢狀 Docker whiteout 解壓失敗時，請使用 dockerd `storage-driver: fuse-overlayfs` 或 `vfs`（並關閉 containerd snapshotter）。在 **Cursor Cloud** 上，environment 的 `install`/`start` 已設定 `fuse-overlayfs` 並預熱 Lab images—session 就緒後直接跑 `migraloop lab up`。見 [CLI 與 Config](cli-and-config.md)（`lab`）與 [Deployment](deployment.md)。

### DB-level restore / load escape hatch

若要在 Scenario recipes **之外**載入資料（SQL／JS／dumps 進入 Lab Oracle 與／或 Lab Mongo），請用 `lab/escape-hatch/`，並搭配 `migraloop lab status` 的可拋棄 Fixture 連線細節。這不是 Lab Scenario（`recipe.yaml`／`lab scenario run`），也不是 Release Quality Gate。

```bash
./target/debug/migraloop lab up
docker compose -f lab/compose.yaml -p migraloop-lab exec -T oracle \
  sqlplus -s SYNC_USER/lab_oracle@FREEPDB1 < lab/escape-hatch/oracle-load.sql
docker compose -f lab/compose.yaml -p migraloop-lab exec -T mongo \
  mongosh --quiet --host 127.0.0.1 -u migraloop -p lab_mongo \
  --authenticationDatabase admin lab < lab/escape-hatch/mongo-load.js
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=lab_oracle MONGO_PASSWORD=lab_mongo
export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop apply -f lab/escape-hatch/deployment.yaml
./target/debug/migraloop status
./target/debug/migraloop base --table LAB_ESCAPE_CUSTOMERS
```

Operator 面向細節見 [Deployment](deployment.md)。CLI-seam 覆蓋：always-on package 檢查，以及 `crates/app/tests/cli_lab_escape_hatch.rs` 中預設 ignored 的 Fixture flow。

### 撰寫 Lab Scenario（feature-time coverage）

已出貨第一級 capability 的完整度階梯（ADR-0025 + ADR-0028）：**capability → Lab Scenario → 非 ignored 的 contract-path CI twin**。在手動 Lab Scenario 與 Release Quality Gate twin 都齊之前，該 capability 視為未完成。開發 feature 時請走這條可重複路徑：

1. 建立 `lab/scenarios/<id>/`，包含：
   - `recipe.yaml` — catalog metadata：`id`、`summary`、**Scenario Namespace**（`source_tables`、`target_collections`、`deployment`、`pipelines`）、`workload`（`concurrency`：`serial`|`parallel`、有序 `steps`）、`checks.correctness`、可選的等權 `thresholds`（`max_settle_ms`、`max_lag`、`max_duration_ms`、`min_rows_per_s`）
   - `deployment.yaml` — 真實 product Deployment config（與 Operator `apply` 相同格式），且只能綁定 Lab Fixture engines（`migraloop lab status` 所示的 `127.0.0.1` / `localhost` Oracle + Mongo endpoints）。Scenario `run` 會在 apply/sync 前拒絕非 Lab／正式環境 engine targets。
2. 在 `crates/cli/src/lab_scenario.rs` 實作 Namespace prepare/remove、Source workload、checks 與 thresholds，並向其他 runners 註冊 Scenario id。
3. 確認 `migraloop lab scenario list` 顯示新 id，且 **summary 來自 `recipe.yaml`**。Selectable catalog = 已註冊 runner，且在 `--lab-dir` 下同時有 recipe + deployment 檔。
4. 在 Lab Fixture 上手動驗證 `migraloop lab scenario run <id>`。list／控制面行為維持 always-on CLI-seam 測試；完整 Fixture run 維持 `#[ignore]` — 不是 Release Quality Gate 證據。
5. 在 `crates/app/tests` 新增**非 ignored** 的 contract-path CI twin（優先延伸既有 CLI／`migraloop-app` seams，走 contract/stub + Platform Store/Mongo）。更新 Lab↔CI 矩陣 `docs/rqg/CI_TWIN_COVERAGE.md`。**不要**為了「過 gate」而取消 ignore Lab Scenario／Fixture／live Oracle 測試，也**不要**新增會跑 Lab Scenario catalog 的 CI job。

Recipe 慣例與短清單亦見 `lab/scenarios/README.md`。已出貨 capability 的 Lab gaps：`lab/scenarios/COVERAGE.md`（亦由 `lab scenario list` 摘要）。同一批 capability 的 CI twin 列：`docs/rqg/CI_TWIN_COVERAGE.md`。

## Release Quality Gate

每個 PR／push 都必須讓四個平行 checks 全綠（ADR-0011、ADR-0028）。Handbook guard 維持獨立 workflow；其餘三個 jobs 在 `.github/workflows/release-quality-gate.yml`。自動化表面請稱為 **Release Quality Gate**／**contract-path CI twin**—絕不要叫「Mock Lab」，也不要把 Local Sync Lab 當成 gate。

| Check | 跑什麼 | 本地重現 |
| --- | --- | --- |
| **Handbook guard** | `cargo test -p handbook-guard` 加上 handbook check entrypoint | 見下方「Handbook guard」一節 |
| **rqg-unit** | workspace crate 測試，排除 `migraloop-app` 與 `handbook-guard`（不需 Postgres/Mongo） | `cargo test --workspace --exclude migraloop-app --exclude handbook-guard` |
| **rqg-integration** | 非 ignored 的 `migraloop-app` 測試（正確性、contract、fault、capability CI twins） | 下方 CI 對齊 env，再 `cargo test -p migraloop-app` |
| **rqg-perf** | contract/stub 上固定 Direct Pipeline microbench，對照 committed baseline（`allowed_regression_pct` 約 55，因應 hosted runner 噪訊；最多 3 次 attempts） | 下方 CI 對齊 env，再 `bash ci/rqg/run_direct_pipeline_microbench.sh` |

`rqg-integration` 與 `rqg-perf` 使用與 CI 相同的 service 憑證。執行那些 cargo／bash 指令前請設定：

| 變數 | CI／本地對齊值 |
| --- | --- |
| `MIGRALOOP_TEST_ADMIN_URL` | `postgres://migraloop:migraloop@127.0.0.1:5432/postgres` |
| `MIGRALOOP_TEST_MONGO_HOST` | `127.0.0.1` |
| `MIGRALOOP_TEST_MONGO_PORT` | `27017` |

這些 jobs 的 MongoDB 預期 root 帳密為 `deliver_user`／`mongo-secret-value`（`authSource=admin`）—與 app 整合測試硬編碼的預設相同。本地服務範例：

```bash
docker compose up -d platform-store   # Postgres 16；admin URL 如上
docker run -d --name migraloop-rqg-mongo -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=deliver_user \
  -e MONGO_INITDB_ROOT_PASSWORD=mongo-secret-value \
  mongo:7
# 部分 schema／Delivery probes 還需要：pip install pymongo
export MIGRALOOP_TEST_ADMIN_URL=postgres://migraloop:migraloop@127.0.0.1:5432/postgres
export MIGRALOOP_TEST_MONGO_HOST=127.0.0.1
export MIGRALOOP_TEST_MONGO_PORT=27017
```

預設 `cargo test -p migraloop-app` 會略過 `#[ignore]` 的 Lab Fixture／Lab Scenario／live Oracle 測試，以及僅供 `rqg-perf` 的 microbench—請維持如此。Lab Scenario `bulk-load` 維持**手動**；它不是 performance gate（`rqg-perf` 由 `ci/rqg/` 負責）。已出貨 Lab capability → 非 ignored CI twin 證據矩陣：`docs/rqg/CI_TWIN_COVERAGE.md`。

## 測試

Unit/crate 測試（以 workspace 方式執行時亦涵蓋於上方的 `rqg-unit`）：

```bash
cargo test -p migraloop-capture
cargo test -p migraloop-transform
cargo test -p migraloop-cli
```

`crates/app/tests` 下的整合測試通常需要可連線的 Postgres（以及常需要 MongoDB），變數見 [Release Quality Gate](#release-quality-gate)：

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

Lab Scenario Direct Pipeline、Rich Transform `project`/`filter`、多表 Transform Pipeline、concurrent Source workload、bulk-load、idempotent-redelivery、pause-resume、remove-pipeline、change-pipeline 、poison-quarantine、schema-change-pause 、source-alignment、drift-check 、bounded-backpressure、observability-surface 、platform-store-guardrails 與 backward-compatible-upgrades seams（預設 ignored；需要 Docker Lab Fixture + Instant Client）。這些是**手動 Lab** seams—不是 Release Quality Gate 證據，也不應接到 CI：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
```

Lab DB-level escape-hatch load 後接 product status/inspect（預設 ignored；需要 Docker Lab Fixture + Instant Client）：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_escape_hatch -- --ignored --nocapture
```

Operator 的 apply/sync/inspect 驗證步驟見 [Source System](source-system.md)。

## Handbook guard（文件 CI seam）

變更 Operator/Developer 可見行為或 handbook 頁面時，執行與 CI 相同的 entrypoint（這是與 Release Quality Gate jobs 並行的 Handbook guard check—不能取代 `rqg-unit`／`rqg-integration`／`rqg-perf`）：

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
