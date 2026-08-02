# Developer 本地设置

在此模块化 Rust monorepo 中 clone、build、启动 Platform Store，并运行测试。给 Operator 的产品用法在其他 handbook 章节—本页是 Developer 路径。

## 前置需求

- 符合 `rust-toolchain.toml` 的 Rust toolchain（stable）
- Docker / Docker Compose（Platform Store 与可选的集成测试依赖）
- Git
- **可选（live Oracle Source）：** 在运行 `migraloop` 的机器上安装 Oracle Instant Client Basic 或 Basic Light，并将 `LD_LIBRARY_PATH` 指向 Instant Client 目录。真实 host 的 Initial Load 与 LogMiner (OCI) 需要它；`host: contract` / `stub` 的 CI 切片不需要。
- **可选（contract/stub CI 切片）：** `MIGRALOOP_CONTRACT_SOURCE_CATALOG` 可指向 JSON 文件，merge/override 进程内 contract Source catalog 以供 schema discovery + Initial Load（见 [Source System](source-system.md)／[CLI 与 Config](cli-and-config.md)）。默认命名 fixtures 仍供场景可读性；这不是 production Source 机制。

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

可丢弃的 Oracle + MongoDB + Platform Store + app，供**手动** Sync→Delivery 验证（ADR-0025）。与 **Release Quality Gate**／CI contract-stub harness 不同：由 operator 选择 Lab Scenarios；**不要**把 Scenario catalog 当成 CI suite，也不要新增会跑完整 catalog 的 release-gate job。

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
# keep-on-finish 后 lab status 会标出 leftover Namespace；也可用 base / derived / target 查看。
# 重跑会先 wipe Namespace；或：lab scenario remove <id> / run --auto-remove
./target/debug/migraloop lab down
```

Bring-up 后默认：Platform Store `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Oracle `SYNC_USER` / `lab_oracle` @ `FREEPDB1`、MongoDB URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`。Lab bring-up 不会套用 sample Deployments/Pipelines。需要 Docker Compose；`lab up` 若缺少 binary 会构建 `target/debug/migraloop`，再由 `lab/Dockerfile` 打包（Ubuntu 24.04 base 以对齐 host glibc）。Lab Compose 使用 `network_mode: host`。第一次 Oracle 开机可能要数分钟。嵌套 Docker whiteout 解压失败时，请使用 dockerd `storage-driver: fuse-overlayfs` 或 `vfs`（并关闭 containerd snapshotter）。在 **Cursor Cloud** 上，environment 的 `install`/`start` 已配置 `fuse-overlayfs` 并预热 Lab images—session 就绪后直接跑 `migraloop lab up`。见 [CLI 与 Config](cli-and-config.md)（`lab`）与 [Deployment](deployment.md)。

### DB-level restore / load escape hatch

若要在 Scenario recipes **之外**加载数据（SQL／JS／dumps 进入 Lab Oracle 与／或 Lab Mongo），请用 `lab/escape-hatch/`，并搭配 `migraloop lab status` 的可丢弃 Fixture 连接细节。这不是 Lab Scenario（`recipe.yaml`／`lab scenario run`），也不是 Release Quality Gate。

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

Operator 面向细节见 [Deployment](deployment.md)。CLI-seam 覆盖：always-on package 检查，以及 `crates/app/tests/cli_lab_escape_hatch.rs` 中默认 ignored 的 Fixture flow。

### 编写 Lab Scenario（feature-time coverage）

已出货第一级 capability 的完整度阶梯（ADR-0025 + ADR-0028）：**capability → Lab Scenario → 非 ignored 的 contract-path CI twin**。在手动 Lab Scenario 与 Release Quality Gate twin 都齐之前，该 capability 视为未完成。开发 feature 时请走这条可重复路径：

1. 创建 `lab/scenarios/<id>/`，包含：
   - `recipe.yaml` — catalog metadata：`id`、`summary`、**Scenario Namespace**（`source_tables`、`target_collections`、`deployment`、`pipelines`）、`workload`（`concurrency`：`serial`|`parallel`、有序 `steps`）、`checks.correctness`、可选的等权 `thresholds`（`max_settle_ms`、`max_lag`、`max_duration_ms`、`min_rows_per_s`）
   - `deployment.yaml` — 真实 product Deployment config（与 Operator `apply` 相同格式），且只能绑定 Lab Fixture engines（`migraloop lab status` 所示的 `127.0.0.1` / `localhost` Oracle + Mongo endpoints）。Scenario `run` 会在 apply/sync 前拒绝非 Lab／生产环境 engine targets。
2. 在 `crates/cli/src/lab_scenario.rs` 实现 Namespace prepare/remove、Source workload、checks 与 thresholds，并向其他 runners 注册 Scenario id。
3. 确认 `migraloop lab scenario list` 显示新 id，且 **summary 来自 `recipe.yaml`**。Selectable catalog = 已注册 runner，且在 `--lab-dir` 下同时有 recipe + deployment 文件。
4. 在 Lab Fixture 上手动验证 `migraloop lab scenario run <id>`。list／控制面行为保持 always-on CLI-seam 测试；完整 Fixture run 保持 `#[ignore]` — 不是 Release Quality Gate 证据。
5. 在 `crates/app/tests` 新增**非 ignored** 的 contract-path CI twin（优先延伸既有 CLI／`migraloop-app` seams，走 contract/stub + Platform Store/Mongo）。更新 Lab↔CI 矩阵 `docs/rqg/CI_TWIN_COVERAGE.md`。**不要**为了「过 gate」而取消 ignore Lab Scenario／Fixture／live Oracle 测试，也**不要**新增会跑 Lab Scenario catalog 的 CI job。

Recipe 惯例与短清单亦见 `lab/scenarios/README.md`。已出货 capability 的 Lab gaps：`lab/scenarios/COVERAGE.md`（亦由 `lab scenario list` 摘要）。同一批 capability 的 CI twin 行：`docs/rqg/CI_TWIN_COVERAGE.md`。

## Release Quality Gate

每个 PR／push 都必须让四个并行 checks 全绿（ADR-0011、ADR-0028）。Handbook guard 保持独立 workflow；其余三个 jobs 在 `.github/workflows/release-quality-gate.yml`。自动化表面请称为 **Release Quality Gate**／**contract-path CI twin**—绝不要叫「Mock Lab」，也不要把 Local Sync Lab 当成 gate。

| Check | 跑什么 | 本地再现 |
| --- | --- | --- |
| **Handbook guard** | `cargo test -p handbook-guard` 加上 handbook check entrypoint | 见下方「Handbook guard」一节 |
| **rqg-unit** | workspace crate 测试，排除 `migraloop-app` 与 `handbook-guard`（不需要 Postgres/Mongo） | `cargo test --workspace --exclude migraloop-app --exclude handbook-guard` |
| **rqg-integration** | 非 ignored 的 `migraloop-app` 测试（正确性、contract、fault、capability CI twins） | 下方 CI 对齐 env，再 `cargo test -p migraloop-app` |
| **rqg-perf** | contract/stub 上固定 Direct Pipeline microbench，对照 committed baseline（约 20% regression；失败可重试一次） | 下方 CI 对齐 env，再 `bash ci/rqg/run_direct_pipeline_microbench.sh` |

`rqg-integration` 与 `rqg-perf` 使用与 CI 相同的 service 凭据。执行那些 cargo／bash 命令前请设置：

| 变量 | CI／本地对齐值 |
| --- | --- |
| `MIGRALOOP_TEST_ADMIN_URL` | `postgres://migraloop:migraloop@127.0.0.1:5432/postgres` |
| `MIGRALOOP_TEST_MONGO_HOST` | `127.0.0.1` |
| `MIGRALOOP_TEST_MONGO_PORT` | `27017` |

这些 jobs 的 MongoDB 预期 root 账密为 `deliver_user`／`mongo-secret-value`（`authSource=admin`）—与 app 集成测试硬编码的默认相同。本地服务示例：

```bash
docker compose up -d platform-store   # Postgres 16；admin URL 如上
docker run -d --name migraloop-rqg-mongo -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=deliver_user \
  -e MONGO_INITDB_ROOT_PASSWORD=mongo-secret-value \
  mongo:7
# 部分 schema／Delivery probes 还需要：pip install pymongo
export MIGRALOOP_TEST_ADMIN_URL=postgres://migraloop:migraloop@127.0.0.1:5432/postgres
export MIGRALOOP_TEST_MONGO_HOST=127.0.0.1
export MIGRALOOP_TEST_MONGO_PORT=27017
```

默认 `cargo test -p migraloop-app` 会跳过 `#[ignore]` 的 Lab Fixture／Lab Scenario／live Oracle 测试，以及仅供 `rqg-perf` 的 microbench—请保持如此。Lab Scenario `bulk-load` 保持**手动**；它不是 performance gate（`rqg-perf` 由 `ci/rqg/` 负责）。已出货 Lab capability → 非 ignored CI twin 证据矩阵：`docs/rqg/CI_TWIN_COVERAGE.md`。

## 测试

Unit/crate 测试（以 workspace 方式执行时也覆盖在上方的 `rqg-unit`）：

```bash
cargo test -p migraloop-capture
cargo test -p migraloop-transform
cargo test -p migraloop-cli
```

`crates/app/tests` 下的集成测试通常需要可连接的 Postgres（以及常需要 MongoDB），变量见 [Release Quality Gate](#release-quality-gate)：

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

Lab Scenario Direct Pipeline、Rich Transform `project`/`filter`、多表 Transform Pipeline、concurrent Source workload、bulk-load、idempotent-redelivery 与 pause-resume seams（默认 ignored；需要 Docker Lab Fixture + Instant Client）。这些是**手动 Lab** seams—不是 Release Quality Gate 证据，也不应接到 CI：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
```

Lab DB-level escape-hatch load 后接 product status/inspect（默认 ignored；需要 Docker Lab Fixture + Instant Client）：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_escape_hatch -- --ignored --nocapture
```

Operator 的 apply/sync/inspect 验证步骤见 [Source System](source-system.md)。

## Handbook guard（文档 CI seam）

变更 Operator/Developer 可见行为或 handbook 页面时，运行与 CI 相同的 entrypoint（这是与 Release Quality Gate jobs 并行的 Handbook guard check—不能取代 `rqg-unit`／`rqg-integration`／`rqg-perf`）：

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
