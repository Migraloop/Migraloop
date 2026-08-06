# Developer 本地设置

在此模块化 Rust monorepo 中 clone、build、启动 Platform Store，并运行测试。给 Operator 的产品用法在其他 handbook 章节—本页是 Developer 路径。

## 前置需求

- 符合 `rust-toolchain.toml` 的 Rust toolchain（stable）
- Docker / Docker Compose（Platform Store 与可选的集成测试依赖）
- Git
- **可选（live Oracle Source）：** 在运行 `migraloop` 的机器上安装 Oracle Instant Client Basic 或 Basic Light，并将 `LD_LIBRARY_PATH` 指向 Instant Client 目录。真实 host 的 Initial Load 与 LogMiner (OCI) 需要它；`host: contract` / `stub` 的 CI 切片不需要。若要用 Source TLS（TCPS），另挂载 Instant Client wallet 并设置 `spec.source.tls`（`enabled` + `walletLocation`）—见 [Security](security.md)。
- **可选（contract/stub CI 切片）：** 将 `MIGRALOOP_CONTRACT_SOURCE_CATALOG` 指向 harness catalog 表 JSON（schema discovery + Initial Load），需要时再将 `MIGRALOOP_INJECT_LOGMINER_CONTENTS` 指向 Incremental LogMiner contents（可选 `rs_id` / `ssn` ordering keys 供 same-SCN 多行流；见 [Source System](source-system.md)／[CLI 与 Config](cli-and-config.md)）。命名 scenario fixtures 应放在这些 inject 文件供测试使用—不是 shipped product path。

## Clone 与 build

```bash
git clone https://github.com/Migraloop/Migraloop.git
cd Migraloop
cargo build -p migraloop-app
```

Workspace members：`crates/app`（binary `migraloop`）、`cli`、`runtime`、`capture`、`platform-store`、`transform`、`delivery`、`types`，以及 `ci/handbook`（Handbook guard）。

## 以 compose 启动 Platform Store

```bash
docker compose up -d platform-store
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
# 本地 cleartext 即可。生产环境 store TLS：加上 ?sslmode=require&sslrootcert=/path/to/ca.pem
cargo run -p migraloop-app -- migrate
cargo run -p migraloop-app -- status
```

完整双 container stack（store + app `run`）：

```bash
docker compose up -d --build
```

Compose 默认凭证（`migraloop` / `migraloop`）仅供本地开发。

## Local Sync Lab Fixture

可丢弃的 Oracle + MongoDB + Platform Store + app，供**手动** Sync→Delivery 验证（ADR-0025）。与 **Release Quality Gate**／CI contract-stub harness 不同：由 operator 选择 Lab Scenarios；**不要**把 Scenario catalog 当成 CI suite，也不要新增会跑完整 catalog 的 release-gate job。Scenario 执行期间 Lab 会暂停 Fixture `app`（`migraloop run`），让 host `apply`/`sync` 成为唯一的 Incremental Capture 消费者，结束后再恢复 `app`——仍是真实 product CLI Sync／Delivery，不是 Lab stub。

```bash
cargo build -p migraloop-app
./target/debug/migraloop lab up
./target/debug/migraloop lab status   # Fixture ready + Scenario run active/leftover/(none)
./target/debug/migraloop lab scenario list
# Scenario apply/sync 需要 Instant Client：export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop lab scenario run direct-pipeline
./target/debug/migraloop lab scenario run rt-project
./target/debug/migraloop lab scenario run rt-filter
./target/debug/migraloop lab scenario run rt-field-ops
./target/debug/migraloop lab scenario run rt-equilookup
./target/debug/migraloop lab scenario run rt-union
./target/debug/migraloop lab scenario run rt-unwind
./target/debug/migraloop lab scenario run rt-distinct-addtoset
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
./target/debug/migraloop lab scenario run initial-load-throttled
./target/debug/migraloop lab scenario run backward-compatible-upgrades
# keep-on-finish 后 lab status 会标出 leftover Namespace；也可用 base / derived / target 查看。
# 重跑会先 wipe Namespace；或：lab scenario remove <id> / run --auto-remove
./target/debug/migraloop lab down
```

Bring-up 后默认：Platform Store `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Oracle `SYNC_USER` / `lab_oracle` @ `FREEPDB1`、MongoDB URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`。Lab Compose 也会把这些 Lab-only secrets 注入 Fixture `app` 供 continuous Sync。Lab bring-up 不会套用 sample Deployments/Pipelines。需要 Docker Compose；`lab up` 若缺少 binary 会构建 `target/debug/migraloop`，再由 `lab/Dockerfile` 打包（Ubuntu 24.04 base 以对齐 host glibc，并内建 Oracle Instant Client Basic Light，让 Fixture `migraloop run` 能打开 LogMiner OCI）。Host 上的 Scenario `apply`/`sync` 仍需本机 Instant Client（`LD_LIBRARY_PATH`）。Lab Compose 使用 `network_mode: host`。第一次 Oracle 开机可能要数分钟。嵌套 Docker whiteout 解压失败时，请使用 dockerd `storage-driver: fuse-overlayfs` 或 `vfs`（并关闭 containerd snapshotter）。在 **Cursor Cloud** 上，environment 的 `install`/`start` 已配置 `fuse-overlayfs` 并预热 Lab images—session 就绪后直接跑 `migraloop lab up`。见 [CLI 与 Config](cli-and-config.md)（`lab`）与 [Deployment](deployment.md)。

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
   - `recipe.yaml` — recipe-driven runner 接口（亦为 catalog metadata）：`id`、`summary`、**Scenario Namespace**（`source_tables`、`target_collections`、`deployment`、`pipelines`，以及含 table column DDL + `seed_sql`、可选 `mutate_sql` 的 `lifecycle`，供共用 Namespace runner）、`workload`（`concurrency`：`serial`|`parallel`、有序散文 `steps`、typed `product_path` 供共用 prepare→apply→mutate→sync→assert）、executable `checks.correctness`（Managed present/absent、Derived/Target inspect、status 文字、row/document counts）、可选的等权 `thresholds`（`max_settle_ms`、`max_lag`、`max_duration_ms`、`min_rows_per_s`）
   - `deployment.yaml` — 真实 product Deployment config（与 Operator `apply` 相同格式），且只能绑定 Lab Fixture engines（`migraloop lab status` 所示的 `127.0.0.1` / `localhost` Oracle + Mongo endpoints）。Scenario `run` 会在 apply/sync 前拒绝非 Lab／生产环境 engine targets。
2. 注册 Scenario id，并为 `workload.product_path`（catalog Scenarios 必填）只实现 rare escapes 的 thin hooks。共用 Namespace wipe／prepare（以及可选 `mutate_sql`）来自 `namespace.lifecycle`；isomorphic Managed／Derived／Target correctness 来自 executable `checks.correctness`——不要再复制另一份 prepare／remove 三元组或 fat inspect assert。Recipe 的 `workload`／`namespace.lifecycle`／executable `checks.correctness`／`thresholds` 是 recipe-driven runner 接口——不要把 threshold 数值、Namespace wipe／prepare SQL、isomorphic Managed／Derived／Target inspect assert，或共用 product-path 序列再复制成 Rust constants。
3. 确认 `migraloop lab scenario list` 显示新 id，且 **summary 来自 `recipe.yaml`**。Selectable catalog = 已注册 Scenario adapter，且在 `--lab-dir` 下同时有 recipe + deployment 文件。
4. 在 Lab Fixture 上手动验证 `migraloop lab scenario run <id>`。list／控制面行为保持 always-on CLI-seam 测试；完整 Fixture run 保持 `#[ignore]` — 不是 Release Quality Gate 证据。
5. 在 `crates/app/tests` 新增**非 ignored** 的 contract-path CI twin（优先延伸既有 CLI／`migraloop-app` seams，走 contract/stub + Platform Store/Mongo）。更新 Lab↔CI 矩阵 `docs/rqg/CI_TWIN_COVERAGE.md`。**不要**为了「过 gate」而取消 ignore Lab Scenario／Fixture／live Oracle 测试，也**不要**新增会跑 Lab Scenario catalog 的 CI job。

Recipe 惯例与短清单亦见 `lab/scenarios/README.md`。已出货 capability 的 Lab gaps：`lab/scenarios/COVERAGE.md`（亦由 `lab scenario list` 摘要）。同一批 capability 的 CI twin 行：`docs/rqg/CI_TWIN_COVERAGE.md`。

### 新增 Source 或 Target engine（Developer checklist）

新的 Source System 或 Target System kind 应接在稳定接口上。**不要**为了新 engine 重塑 Sync、Rich Transform、Delivery、Deployment runtime 或 Platform Store 概念——实现 seam、补齐 Operator prerequisites 文档，再完成 capability ladder（ADR-0024 / ADR-0025 / ADR-0028）。

1. **实现 engine interface**
   - Source：`crates/capture` 的 `SourceEngine` / `IncrementalCaptureSession`（schema discovery、Initial Load chunks、Incremental Capture resume、prerequisites check、alignment reads、schema-change classification inputs）。
   - Target：`crates/delivery` 的 `TargetEngine`（按 Output Identity upsert Managed fields、按 identity delete、Drift Check／inspect 所需的 list/read helpers）。
   - Source discovery 的 columns 在 `SourceEngine` seam 暴露 engine-agnostic `data_type`／shared `ColumnShape`。Oracle allow-list、size caps、与 type-brand 命名留在 adapter-private（ADR-0018）。NUMBER→Mongo classification 只放在 `migraloop-types` 的 `ColumnShape` 旁（ADR-0023）——不要在 capture／delivery 再留 twin helpers。
   - 通过 Deployment runtime factory helpers（`source_engine_from_connection` / `target_engine_from_deployment`）接线。这些 factories 返回 `SourceEngine`／`TargetEngine` interfaces（call site 不应出现具体 Oracle／Mongo types）。Runtime Sync／Delivery 必须继续依赖 interfaces。
   - Deployment runtime 的 public surface 仅限 Operator Deployment verbs（apply、Incremental Sync／supervise、Pipeline lifecycle、Source Alignment Check、Drift Check、status inventory、inspect）加上上述 factory entry points。不要从 `migraloop-runtime` 外部调用已 demote 的 internal helpers。Continuous Sync／supervise 偏好在 Operator edge 打开的 Platform Store session。
   - 默认 Operator CLI `apply`／`run`／`sync` 仍经由上述 factories 构建 v1 Oracle LogMiner 与 MongoDB adapters。kind 选择留在 factories；engine 经 factory 选出或注入后，apply／Incremental Sync orchestration 不再以 `kind == "oracle"` 重闸。完整 Incremental Sync 在 runtime interface 接受 typed `SyncOptions`（`run_incremental_sync`／`run_incremental_sync_with_engines`；continuous／supervise 保留真正的 poll 与 restart policy）。仅重新命名 OneShot verb 的 Sync entry aliases 已收敛（#208）。Apply／Initial Load 接受 typed ApplyOptions 与可选的 injected engines（`apply_with_options`／`apply_with_engines`）。RQG contract twins 与 Lab fault paths 以 typed SyncOptions（隐藏的 `migraloop sync` flags 或显式 structs）传入 Poison／delay／fail-after／queue-capacity knobs，并以 typed ApplyOptions（隐藏的 `migraloop apply` flags 或显式 structs／`apply_with_options`）传入 Initial Load chunk／rate／pause／store-delay knobs——不以 process env 作为主要 adapter。Continuous Sync idle poll 间隔 typed 于 SyncOptions（`poll_interval_ms`；`run` 上的 hidden `--sync-poll-interval-ms`）。Fake adapters 可走 production apply 与 Sync path 且不依赖 orchestration kind gates。Poison quarantine、Schema Change pause、与 bounded Backpressure 在 Incremental Sync 内仍是 distinct policies。既有 fault-injection env knobs（`MIGRALOOP_DELIVERY_POISON_IDENTITIES`、`MIGRALOOP_DELIVERY_DELAY_MS` 等）仅在未设置 typed overrides 时保留为薄的临时 compat shim（`SyncOptions::for_cli`／`from_env_compat`）。Legacy Lab-inject env（如 `MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS`／`MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS`）在 typed overrides 未设置时仍为薄 compat shim。Operator knobs 如 `MIGRALOOP_SYNC_QUEUE_CAPACITY`／`MIGRALOOP_POISON_MAX_ATTEMPTS`／`MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE`／`MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC`／`MIGRALOOP_SYNC_POLL_INTERVAL_MS` 仍可经由 env 生效。
   - Rich Transform／Affect Analysis 只读 platform-managed Base/Derived data——绝不要把新 engine 当成 transform compute。
2. **Prerequisites 与 handbook**
   - 在对应 Operator 章节（[Source System](source-system.md)／[Target System](target-system.md)）以**三个 locales**文档化 engine-specific Source Prerequisites／Required Privileges（或 Target Delivery grants）。
   - prerequisites 未满足时在 apply/sync **fail fast**；默认不要自动改动客户 Source/Target 设置。
3. **Lab Scenario**
   - 在 `lab/scenarios/<id>/` 新增可选择的 Lab Scenario，在真实 product path 上演练新 engine（同上 recipe-driven runner checklist）。以 Lab-only bindings 做 Namespace isolation。
4. **CI contract twin**
   - 在 `crates/app/tests` 新增**非 ignored** 的 contract-path twin，并更新 `docs/rqg/CI_TWIN_COVERAGE.md`。优先 contract/stub + Platform Store/Mongo（或实现相同 interface 的 in-memory fake）——不要把 Lab catalog 当 CI。
5. **Packaging guards**
   - 保持 modular monorepo + 单一 `migraloop` binary（ADR-0024）。不要引入第二个 Platform Store engine（ADR-0001）。

Seam 测试可用 in-memory `FakeSource`／`FakeTarget`（含 injected apply／Initial Load 与完整 Incremental Sync）；它们不是第二个 production engine。

## Release Quality Gate

每个 PR／push 都必须让四个并行 checks 全绿（ADR-0011、ADR-0028）。Handbook guard 保持独立 workflow；其余三个 jobs 在 `.github/workflows/release-quality-gate.yml`。自动化表面请称为 **Release Quality Gate**／**contract-path CI twin**—绝不要叫「Mock Lab」，也不要把 Local Sync Lab 当成 gate。

| Check | 跑什么 | 本地再现 |
| --- | --- | --- |
| **Handbook guard** | `cargo test -p handbook-guard` 加上 handbook check entrypoint | 见下方「Handbook guard」一节 |
| **rqg-unit** | workspace crate 测试，排除 `migraloop-app` 与 `handbook-guard`（不需要 Postgres/Mongo） | `cargo test --workspace --exclude migraloop-app --exclude handbook-guard` |
| **rqg-integration** | 非 ignored 的 `migraloop-app` 测试（正确性、contract、fault、capability CI twins） | 下方 CI 对齐 env，再 `cargo test -p migraloop-app` |
| **rqg-perf** | contract/stub 上固定 Direct Pipeline microbench，对照 committed baseline（`allowed_regression_pct` 约 55，应对 hosted runner 噪声；最多 3 次 attempts） | 下方 CI 对齐 env，再 `bash ci/rqg/run_direct_pipeline_microbench.sh` |

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

Lab Scenario Direct Pipeline、Rich Transform `project`/`filter`/`addFields`/`rename`/`remove`/`equiLookup`/`union`/`unwind`、多表 Transform Pipeline（`groupBy` sum/count/min/max/avg）、concurrent Source workload、bulk-load、idempotent-redelivery、pause-resume、remove-pipeline、change-pipeline 、poison-quarantine、schema-change-pause 、source-alignment、drift-check、bounded-backpressure、observability-surface 、platform-store-guardrails 、backward-compatible-upgrades 和 initial-load-throttled seams（默认 ignored；需要 Docker Lab Fixture + Instant Client）。这些是**手动 Lab** seams—不是 Release Quality Gate 证据，也不应接到 CI：

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
- Source／Target Operator contracts：[Source System](source-system.md)、[Target System](target-system.md)
