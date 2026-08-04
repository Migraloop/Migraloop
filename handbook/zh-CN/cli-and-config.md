# CLI 与 Config 参考

给 Operator 的命令、标志、环境变量与 Deployment 配置字段。

## Binary

```text
migraloop <subcommand> [flags]
```

由 `crates/app` 构建（`Dockerfile` release binary）。所有与 Platform Store 通信的 Operator 子命令都接受 `--platform-store-url` 或下方环境变量。

Operator 动词（`apply`、`sync`、`run`、`status`、inspect、pause/resume/remove、align、drift）是薄的 clap/config/env adapter，接在 Deployment **runtime** 之上——Sync／Affect Analysis／Delivery orchestration 不由 CLI 模块拥有。runtime 的 public surface 是这些 Operator verbs 加上 Source／Target factory entry points；Operator narrative formatting（例如 `status` labels）留在此 CLI adapter，continuous Sync／supervise 偏好于此打开的 Platform Store session。要扩展 Source 或 Target engine 的 Developers，请依 [Developer 本地设置](developer-local-setup.md#新增-source-或-target-enginedeveloper-checklist) 的 checklist。

## Operator CLI 子命令

`migraloop` Operator CLI 当前提供这些子命令：

### `migrate`

应用版本化 Platform Store schema migrations。

```bash
migraloop migrate --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `apply`

应用声明式 Deployment 配置（YAML 或 JSON）。验证 secrets-by-reference、Source/Target kinds、Pipeline specs、（当 Pipelines 引用表时）Source Prerequisites，视需要执行 schema discovery + Initial Load，并 upsert Deployment/Pipeline 状态。以语义 Pipeline 变更重新 apply（transform/binding 等）是 Operator 的 **Pipeline revision** 路径：暂停该 Pipeline 的旧 Delivery，按需要重建 Derived 并重新 Delivery，然后继续 incremental；Shared Bases 不重建。仅变更可选的 `description` 属 metadata-only，可跳过 rebuild。

在真实 Oracle Source host（非 `contract`/`stub`）上，apply 会通过 OCI 从 live Source 做 schema discovery 与 Initial Load（需要 Instant Client；见 [Source System](source-system.md)）。contract/stub host 仅使用**注入的**进程内 **contract Source catalog**（CI 切片；`MIGRALOOP_CONTRACT_SOURCE_CATALOG` 供 discovery/Initial Load；`MIGRALOOP_INJECT_LOGMINER_CONTENTS` 供 Incremental Capture）—不是随产品附带的业务表 catalog，也不是受支持的 production Source 机制。

Initial Load 以**有界 chunks**读取 Source（默认 `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE=1000`），会打印 `Initial Load progress`／structured `initial_load_progress` events，并可用 `MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC` 限速。Load 中途 pause（对引用该表的 Pipeline 执行 Operator `migraloop pause`，或 Lab inject `MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS`）会持久化进度（`status=initial_load_paused`）与 cutover low-watermark；再跑 `migraloop apply` 即可 resume，无需拆除 Deployment。在 Downstream／store 压力下，apply 会打印 `Initial Load backoff`，而不是让内存无界增长（见 [Operations](operations.md)）。

```bash
migraloop apply --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" -f deployment.yaml
```

| 标志 | 含义 |
| --- | --- |
| `-f`, `--file` | Deployment 配置路径 |

### `status`

报告 Platform Store 健康、Deployments、Pipelines、Base Datasets、Sync Health（`unknown` / `ok` / `lagging` / `failed`）、Source Alignment、Delivery Health、Quarantine 行、Schema Change impacts，与 Derived Datasets。Sync Health 与 Delivery Health 都暴露 `lag=`（从 capture resume position 起算的剩余 pending 工作）；labels 来自与 Prometheus 共用的同一个 runtime Observability assembly。Downstream 变慢时，lag 会在 backpressure 下上升，但不会因此 pause Pipeline；capture 一次仍只填满一个 bounded queue window（ADR-0020）。当 Poison Change quarantine 作用中时，Delivery Health 为 `unhealthy`，并把每个被 quarantine 的 Output Identity 标为 unhealthy / not aligned（ADR-0015）。当 blocking Schema Change pause 作用中时，Delivery Health 为 `paused`，且 `status` 会列出 Schema Change blocking 行（ADR-0009）—与 quarantine 不同。

```bash
migraloop status --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `base`

查看某个 Source 表的 Base Dataset 行。

```bash
migraloop base --table ORDERS [--deployment oracle-to-mongo]
```

| 标志 | 含义 |
| --- | --- |
| `--table` | Source 表名（必要） |
| `--deployment` | 多个 Bases 共用表名时消歧义 |

### `target`

查看某个 Pipeline collection 的 Target 文档。

```bash
migraloop target --collection orders [--deployment oracle-to-mongo]
```

| 标志 | 含义 |
| --- | --- |
| `--collection` | Target collection 名称（必要） |
| `--deployment` | 共用 collection 名称时消歧义 |

### `derived`

查看 Transform Pipeline 的 Derived Dataset 行。

```bash
migraloop derived --pipeline orders_by_customer [--deployment oracle-to-mongo]
```

| 标志 | 含义 |
| --- | --- |
| `--pipeline` | Pipeline 名称（必要） |
| `--deployment` | 共用 Pipeline 名称时消歧义 |

### `sync`

One-shot Incremental Capture 写入 Base Datasets、维护 Derived Datasets，然后 Delivery。用于 Lab scenarios 与 Operator catch-up；steady-state continuous Sync 请用 `migraloop run`（compose 默认）。

Oracle Incremental Capture 一律走 LogMiner：真实 host 使用 **LogMiner (OCI)**；`host: contract` / `stub` 使用进程内 contract harness。真实 host **不会** silent fallback 到 stub catalog。缺少 Instant Client 或 OCI 失败时会以 LogMiner/OCI 名称 fail fast。

已 pause 的 Pipelines 在 `sync` 期间会跳过 Delivery/processing；共用 Base Dataset 的 Incremental Capture 仍会继续，让其他 Pipelines 与之后的 resume catch-up 保持正确。

当单个 Output Identity 的 Delivery 反复失败时，`sync` 会重试最多 `MIGRALOOP_POISON_MAX_ATTEMPTS` 次（默认 `3`），然后 quarantine 该 identity、发出 Operator 可见的 **ALERT**、继续其他 changes，并在 `status` 上显示 quarantine（ADR-0015 / issue #22）。

当 Incremental Capture 看到会 **blocking** 某条 Pipeline 依赖的 Source DDL 时，`sync` 会发出 Operator 可见的 **WARN**、pause 受影响的 Pipeline(s)，并在 `status` 记录 Schema Change impact—不会 quarantine（ADR-0009 / issue #23）。Unaffecting 或 non-blocking 的 schema changes 会继续。

Platform Store 会序列化 Incremental Capture cycles，避免 one-shot `sync` 与 continuous `run` 同时 multi-write 同一个 Deployment。

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `align`

运行 **Source Alignment Check**（issue #24）：以非实时、**resource-gated** 方式验证 Base 是否匹配 Source；若不一致，用同一批 Source check reads 修复 Base。检查**从不写入 Source**。在把 Base 当作可靠 Drift baseline 之前需要此检查；单靠 Sync Health 不够。

默认 `--max-rows` 为 `1000`，方便 Operator 调度检查而不做全表 slam。更大 budget（或重复执行）可覆盖其余行；`status` 显示上次执行的 `Source Alignment: aligned|partial|unknown` 与 checked/mismatched 计数（`partial` 表示 budget 被截断）。

```bash
migraloop align --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" [--table CUSTOMERS] [--deployment oracle-to-mongo] [--max-rows 1000]
```

| 旗标 | 含义 |
| --- | --- |
| `--table` | Source 表 / Base Dataset（默认：所有 Bases） |
| `--deployment` | 多个 Bases 共用表名时用以消歧 |
| `--max-rows` | 每个 Base 最多读取的 Source 行数（resource gate；默认 `1000`） |

### `drift`

运行 **Drift Check**（issue #25）：以非实时、**resource-gated** 方式验证 Target 上的 Managed fields 是否匹配平台 expected dataset（Direct 用 Base，Transform 用 Derived）。默认会对检测到的 Managed drift 走与 Delivery 相同的 Managed-only upsert 路径做 **auto-repair**；**non-Managed Target fields 会被忽略**、不会被覆盖。对 Direct Pipelines，Base 必须已有 Source Alignment（`aligned` 或 `partial`）—先运行 `migraloop align`。Auto-repair 不会在 Alignment baseline 之外再增加 Source load。

默认 `--max-rows` 为 `1000`，方便 Operator 调度检查而不做全 collection slam。更大 budget（或重复执行）可覆盖其余 Output Identities；`status` 显示上次执行的 `Drift: ok|partial|unknown` 与 checked/mismatched 计数（`partial` 表示 budget 被截断）。

```bash
migraloop drift --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" [--pipeline customers] [--deployment oracle-to-mongo] [--max-rows 1000]
```

| 旗标 | 含义 |
| --- | --- |
| `--pipeline` | Pipeline 名称（默认：所有具 Target Binding 的 Pipelines） |
| `--deployment` | 多个 Deployments 共用同名 Pipeline 时用以消歧 |
| `--max-rows` | 每个 Pipeline 最多检查的 Output Identities（resource gate；默认 `1000`） |

### `pause`

暂停一条 Pipeline，且不重启 Deployment（ADR-0007）。停止该 Pipeline 后续的 Delivery/processing；耐久的 Base/checkpoint 状态会保留。其他 Pipelines 继续运行。

```bash
migraloop pause --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | 含义 |
| --- | --- |
| `--pipeline` | Pipeline 名称（必填） |
| `--deployment` | 多个 Deployments 共用同名 Pipeline 时用以消歧 |

### `resume`

恢复已 pause 的 Pipeline。清除耐久 pause 标志，并按当前 Platform Store 的 Base/Derived 状态做 catch-up Delivery（含 pause 期间消失的 identities 的 deletes），之后由 continuous `run`（或后续 one-shot `sync`）继续 Incremental Delivery。

```bash
migraloop resume --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | 含义 |
| --- | --- |
| `--pipeline` | Pipeline 名称（必填） |
| `--deployment` | 多个 Deployments 共用同名 Pipeline 时用以消歧 |

### `remove`

在不重启 Deployment 的前提下移除一条 Pipeline（ADR-0007）。会停止该 Pipeline 的 Delivery/processing。若其他 Pipelines 仍引用，Shared Base Datasets 会保留；不再被引用的 Bases 会被 prune。`status` 不再把该 Pipeline 列为 active。已 Deliver 的 Target documents 会保留（停止 Delivery，不是 wipe）。若要在之后的 `apply` 中持续省略它，也请从 declarative config 移除。

```bash
migraloop remove --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | 含义 |
| --- | --- |
| `--pipeline` | Pipeline 名称（必填） |
| `--deployment` | 多个 Deployments 共用同名 Pipeline 时用以消歧 |

### `run`

启动时 migrate，对已应用（未 pause）的 Pipelines 持续执行 Incremental Capture → Affect Analysis → Delivery，并在同一个 single active instance 上提供 Observability Surface Prometheus scrape endpoint，然后保持进程运行（compose 默认 command）。Steady-state Sync 不需要外部 sync scheduler。Caught up 或尚未应用 Deployment 时会 idle-poll（`MIGRALOOP_SYNC_POLL_INTERVAL_MS`）。Source/Target secret refs 必须存在于此进程环境。

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" \
  [--metrics-addr 0.0.0.0:9090]
```

| Flag / env | 含义 |
| --- | --- |
| `--metrics-addr` / `MIGRALOOP_METRICS_ADDR` | Prometheus `/metrics` listen address（默认 `0.0.0.0:9090`）。Compose 会 map host `9090`。见 [Observability](observability.md)。 |

### `lab`

Local Sync Lab Fixture 与 Lab Scenarios（ADR-0025）。布署可丢弃的真实堆栈—Oracle Source（Lab 已满足的 Source Prerequisites）、MongoDB Target、Platform Store 与 app。Bring-up **不会**套用 sample Deployment 或 Pipelines。Operator 接着可 list/run 可选的 **Lab Scenarios**，在 **Scenario Namespace** 内以真实 product path 套用 Deployment 并驱动 Sync/Delivery。需要 Docker Compose 与 repo 的 `lab/` 目录（或 `--lab-dir`）。Scenario 的 `apply`/`sync` 需要 host 上的 Oracle Instant Client（`LD_LIBRARY_PATH`）。若嵌套 Docker 在 overlay whiteout 解压失败，需改用 dockerd `fuse-overlayfs` 或 `vfs`（并关闭 containerd snapshotter）；`lab up` 遇 whiteout/`EPERM` 时会打印此提示。**Cursor Cloud** 会在 environment `install`/`start` 套用 `fuse-overlayfs`—见 [Developer local setup](developer-local-setup.md) 与 [Deployment](deployment.md)。

```bash
migraloop lab up [--lab-dir lab]
migraloop lab status [--lab-dir lab]
migraloop lab down [--lab-dir lab]
migraloop lab scenario list [--lab-dir lab]
migraloop lab scenario run <scenario-id> [--lab-dir lab] [--auto-remove]
migraloop lab scenario remove <scenario-id> [--lab-dir lab]
```

| Subcommand | 含义 |
| --- | --- |
| `up` | 启动可丢弃 Fixture；就绪时打印连接细节 |
| `status` | 报告 Fixture 就绪状态（engines + Oracle prerequisites + Platform Store），以及哪个 Scenario Namespace 为 **active**（run 进行中）或 **leftover**（run 结束后保留），或各自为 `(none)`。在你套用配置或运行 Lab Scenario 之前也会显示 `Deployment: (none)` / `Pipeline: (none)` — 请用 Scenario run / leftover 行判断，不必从那些行自行猜测 |
| `down` | 拆除 containers 与 volumes |
| `scenario list` | 按 `--lab-dir` 磁盘上的 recipe 列出可选 Lab Scenarios（`lab/scenarios/<id>/recipe.yaml` + `deployment.yaml`，且已注册 Scenario adapter）。summary 来自各 recipe—例如 `direct-pipeline`、`rt-project`、`rt-filter`、`rt-field-ops`、`rt-equilookup`、`rt-union`、`rt-unwind`、`rt-distinct-addtoset`、`transform-pipeline`、`concurrent-source-workload`、`bulk-load`、`idempotent-redelivery`、`pause-resume`、`remove-pipeline`、`change-pipeline`、`poison-quarantine`、`schema-change-pause`、`source-alignment`、`drift-check`、`bounded-backpressure`、`observability-surface`、`platform-store-guardrails`、`backward-compatible-upgrades`、`initial-load-throttled`。list 也会回报已出货 capability 覆盖（complete vs gaps；见 `lab/scenarios/COVERAGE.md`） |
| `scenario run` | 按 id 经 recipe-driven runner 运行一个 Lab Scenario（等权 `thresholds` 与 typed `product_path` 从 `recipe.yaml` 读取—不复制成 Rust constants；共用 product-path 步骤执行 prepare／apply／sync；thin hooks 提供 Namespace seeds／workload escapes／correctness）。若已有 Scenario 正在运行则拒绝。若 Source/Target 不是 Lab Fixture engines 也会拒绝（客户／生产数据库不在 Lab 范围—那些请用普通的 `apply`/`sync`）。重跑同一 Scenario 会先完整移除其 Namespace 再重建。回报 pass/fail 以及 `duration_ms`、rows/throughput、lag，以及 Scenario 定义的 thresholds（例如 settle time，或 bulk-load 的 lag／throughput／duration，若有）（correctness 与 operational metrics 等权）。`rt-project` / `rt-filter` / `rt-field-ops` / `rt-equilookup` / `rt-union` / `rt-unwind` / `rt-distinct-addtoset` 覆盖已出货 Rich Transform `project`、`filter`、`addFields`/`rename`/`remove`、`equiLookup`、`union`、`unwind`，以及 `distinct`/`addToSet` operators；`transform-pipeline` 覆盖多表 `groupBy` 的 `sum`/`count`/`min`/`max`/`avg`；`concurrent-source-workload` 在单一 Scenario 内跑并行 Source sessions；`bulk-load` 会 bulk-insert 约 100k Source rows，且 metric thresholds 可独立于 correctness 让 run 失败；`idempotent-redelivery` 会强制对同一批 Output Identities 做 duplicate-safe re-Delivery，并检查 Managed Target 结果仍正确；`pause-resume` 覆盖 `pause` / `resume` CLI 动词（一条 Pipeline 停止 Delivery、另一条继续；resume 自耐久 Base catch-up）。`remove-pipeline` 覆盖 `remove`（停止 Delivery；仍被引用的 Shared Base 保留；status 不再列出该 Pipeline）；`change-pipeline` 覆盖通过 `apply` 的 Pipeline revision（暂停旧 Delivery → 重建该 Pipeline 的 Derived／重新 Delivery；Shared Bases 不重建；仅 `description` 的 metadata-only 变更可跳过 rebuild）；`poison-quarantine` 在有界重试后 quarantine 单个 poison Output Identity 并 ALERT，Pipeline 继续，且 `status` 显示 unhealthy / not aligned；`schema-change-pause` 会在 blocking DDL 时 WARN 并 pause 受影响的 Pipeline（与 poison quarantine 不同）；`source-alignment` 会检测 Base≠Source、仅用 Source reads 修复 Base，并练习 resource-gated `--max-rows`；`drift-check` 覆盖 Drift Check（Managed Target drift 检测 + 默认 Managed auto-repair；保留 non-Managed；resource-gated `--max-rows`）；`bounded-backpressure` 以 Downstream Delivery delay 搭配极小 queue capacity，验证 Backpressure／lag 且不 pause，再 catch-up；`initial-load-throttled` 演练 chunked／rate-limited／pausable Initial Load，并在 store 压力下 backoff，同时保留 cutover low-watermark。第二个 Scenario run 仍会被拒绝。默认 keep-on-finish 保留 Namespace 供实时 `base`/`derived`/`target` 检查；成功后若要删除可传 `--auto-remove`；`observability-surface` 覆盖 structured JSON operator logs、Prometheus `/metrics` lag/failures，以及 `status` 上的 Sync/Delivery Health；`platform-store-guardrails` 演练 Platform Store Guardrails（拒绝过低设置；可用磁盘 WARN + `platform_store_disk_warn` 且 store 仍 healthy；Pipeline 不自动 pause）；`backward-compatible-upgrades` 在升级路径上 migrate Platform Store、保留既有 Deployments/Base，并以较旧 SemVer-compatible `apiVersion`（`migraloop.dev/v1.0.0`）重新 apply 而不做 Initial Load rebuild |
| `scenario remove` | 完整移除 Scenario Namespace（Source tables、Target collections、Platform Store Deployment），且不启动 run。若已有 Scenario 作用中则拒绝。已不存在时为 idempotent |

| Flag | 含义 |
| --- | --- |
| `--lab-dir` | 含 Lab `compose.yaml` 的目录（默认：`lab`） |
| `--auto-remove` | 仅用于 `scenario run`：成功结束后完整移除 Scenario Namespace（opt-in；失败时仍保留 Namespace 以便调试） |

Lab 是手动验证—不是 Release Quality Gate，也不是 contract/stub LogMiner harness。可选的 Scenario catalog 是 feature-time 完整度表面（ADR-0025），不是 CI suite：不要新增会跑完整 catalog 的 release-gate job。Scenario recipe 惯例、编写路径（recipe-driven runner 接口：`workload`／`product_path`／`checks`／`thresholds`；thin hooks 负责 Namespace prepare／workload／correctness），以及已出货 capability 覆盖 gaps 见 [Developer local setup](developer-local-setup.md)、`lab/scenarios/README.md` 与 `lab/scenarios/COVERAGE.md`。若要在 Scenario recipes **之外**做 DB-level restore/load（用 `lab status` 连接细节对 Lab Oracle/Mongo 跑 SQL/mongosh/dumps，再接普通 `apply`／`status`／inspect／`sync`），见 `lab/escape-hatch/` 与 [Deployment](deployment.md)—该 escape hatch 不是第二套 Scenario 模型，也不是 CI。

## 公开环境变量契约

| 变量 | 含义 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Operator CLI 与 compose `app` 使用的 Platform Store 连接 URL（`postgres://...`）。TLS 请用 `sslmode=require\|verify-ca\|verify-full`，可选 `sslrootcert=/path/to/ca.pem` |
| `MIGRALOOP_PLATFORM_STORE_DATA_DIR` | app filesystem 上用来观测 Platform Store 可用磁盘的路径（compose 会把 store data volume 以 read-only 挂在 `/var/lib/migraloop/platform-store-data`） |
| `MIGRALOOP_PLATFORM_STORE_FREE_DISK_BYTES` | 选用：当无法做 filesystem probe 时，由 Operator／orchestrator 提供的可用磁盘字节数（覆盖目录探测以供 warn threshold） |
| `MIGRALOOP_METRICS_ADDR` | `migraloop run` 的 Prometheus scrape listen address（默认 `0.0.0.0:9090`） |
| `MIGRALOOP_SYNC_POLL_INTERVAL_MS` | `migraloop run` 内 continuous Incremental Capture cycles 之间的 idle poll 间隔（默认 `1000`；必须 > 0） |
| 配置中 `fromEnv` 引用的密钥环境变量名 | 你在 `password.fromEnv` 写的任何名称（例如 `ORACLE_PASSWORD`、`MONGO_PASSWORD`）在 apply / one-shot `sync` / continuous `run` 时必须存在于进程环境 |
| `LD_LIBRARY_PATH` | 真实 Oracle host：Oracle Instant Client libraries 目录（apply/sync/`run` runtime 需要；`contract`/`stub` 不使用） |
| `MIGRALOOP_CONTRACT_SOURCE_CATALOG` | 仅 contract/stub host：harness catalog 表的 JSON 文件路径，供 schema discovery + Initial Load（CI／本地切片；未设置为空；不是 production Source 机制） |
| `MIGRALOOP_POISON_MAX_ATTEMPTS` | Poison Change quarantine 前的有界 Delivery 重试次数（默认 `3`；必须 > 0） |
| `MIGRALOOP_DELIVERY_POISON_IDENTITIES` | 已弃用、仅薄临时 compat shim：以逗号分隔、一律让 Delivery 失败的 Output Identity keys。请改用 typed SyncOptions（`migraloop sync` 的 `--sync-poison-identity` 等隐藏 Test/Lab flags，或 in-process `SyncOptions`；不是 production Operator 控制） |
| `MIGRALOOP_INJECT_SCHEMA_CHANGES` | 仅 Test/Lab injection：Schema Change events 的 JSON 文件路径（`scn`、`table`、`kind`、`columns` …），以便在没有 LogMiner DDL capture 时演练 blocking DDL warn+pause（不是 production Operator 控制） |
| `MIGRALOOP_SYNC_QUEUE_CAPACITY` | Bounded Incremental Capture / Delivery window 大小（默认 `256`；必须 > 0）。one-shot `sync` 或 continuous `run` 下各阶段一次 materialize 的 pending changes 都不超过此容量（ADR-0020）。Test/Lab 也可在 `migraloop sync` 上设 `--sync-queue-capacity` |
| `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE` | Bounded Initial Load Source read window（默认 `1000`；必须 > 0）。apply 的正常路径不会把 unbounded full-table slam 整表灌进内存 |
| `MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC` | 可选的 Initial Load throttle（rows/second；`0`／未设置 = 除 chunking 外不再人工限速）。在 progress 行／`initial_load_progress` 以 `rate_limit` 可见 |
| `MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS` | 仅供 Test/Lab inject：成功 N 个 chunks 后 pause Initial Load，以便演练 durable pause/resume（不是 production Operator 控制；Operators 请在 chunks 之间使用 `migraloop pause`） |
| `MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS` | 仅供 Test/Lab inject：Initial Load 期间人工延迟 Platform Store／Downstream，以便演练 backoff（不是 production Operator 控制） |
| `MIGRALOOP_DELIVERY_DELAY_MS` | 已弃用、仅薄临时 compat shim：人工 Downstream Delivery 延迟（毫秒）。请改用 typed SyncOptions（`migraloop sync` 的 `--sync-delivery-delay-ms`、`--sync-fail-after-changes` 等隐藏 Test/Lab flags，或 in-process `SyncOptions`；不是 production Operator 控制） |
| `MIGRALOOP_INJECT_LOGMINER_CONTENTS` | 仅 Test/Lab injection：contract LogMiner contents 的 JSON 文件路径（`contents: [{scn, operation, table_name, identity, after_image, rs_id?, ssn?}, …]`），供 `contract`/`stub` hosts 的 Incremental Capture。可选的 `rs_id` / `ssn` 是 LogMiner ordering keys，让同一 SCN 的多行在 dedupe 与 resume-safe catch-up 时保持可区分（未设置时 harness Incremental 流为空；不是 production Operator 控制） |
| Lab disposable defaults | `migraloop lab up` 之后：`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store URL `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Mongo URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`（仅本地 Lab） |

### Contract-harness Source Prerequisite probes（仅 host `stub` / `contract`）

进程内 LogMiner harness 的环境变量名称与默认见 [Source System](source-system.md)（`MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_REDO_RETENTION_HOURS`）。

## Deployment 配置契约

| 字段 | 必要 | 说明 |
| --- | --- | --- |
| `apiVersion` | 是 | `migraloop.dev/v1`（major 1 内 SemVer 较旧或相等：也接受 `migraloop.dev/v1.0` / `migraloop.dev/v1.0.0`；较新 minor/patch 与其他 major 会被拒绝） |
| `kind` | 是 | `Deployment` |
| `metadata.name` | 是 | 非空 Deployment 名称 |
| `spec.source` | 是 | 见下 |
| `spec.target` | 是 | 见下 |
| `spec.pipelines` | 否 | 默认 `[]`（只应用 Deployment） |

### `spec.source` / `spec.target`

| 字段 | Source | Target | 说明 |
| --- | --- | --- | --- |
| `kind` | `oracle` | `mongodb` | v1 固定配对 |
| `host` | 是 | 是 | Source `stub`/`contract` → LogMiner harness + contract-catalog Initial Load；其他 host → live OCI Initial Load + LogMiner |
| `port` | 是 | 是 | 有效 TCP port |
| `database` | 是 | 是 | |
| `username` | 是 | 是 | 省略 Pipeline `source.schema` 时，也作为默认 Oracle schema/owner |
| `password` | 是 | 是 | 恰好一个 `fromEnv`、`fromFile`、`fromDockerSecret` |
| `timezone` | 可选 | n/a | IANA 或 Oracle 风格 `±HH:MM`，供 naive 时间；两种形式均可在 `apply` 通过 |
| `tls` | 可选 | 可选 | 见下；省略／`enabled: false` 仍允许 cleartext |

#### `tls`（可选）

| 字段 | Source | Target | 说明 |
| --- | --- | --- | --- |
| `enabled` | 可选 | 可选 | 为 `true` 时以 TLS 连接；配置错误会明确失败（不静默回退 cleartext） |
| `caFile` | **无效**（请用 `walletLocation`） | 可选路径 | Mongo CA 文件；仅文件系统路径（不可 inline PEM） |
| `walletLocation` | 可选目录 | **无效** | Oracle Instant Client wallet 目录 |
| `insecureSkipVerify` | 可选 bool | 可选 bool | 仅供开发/Lab；默认 `false` |

Platform Store TLS 配置在 `MIGRALOOP_PLATFORM_STORE_URL`（`sslmode=require|verify-ca|verify-full`，可选 `sslrootcert=…`）—见 [Security](security.md)。

Docker secrets 从 `/run/secrets/<name>` 解析。

### Pipeline 项（`spec.pipelines[]`）

| 字段 | 说明 |
| --- | --- |
| `name` | 非空 |
| `mode` | `direct` 或 `transform` |
| `description` | 可选、Operator-facing 注释；仅 metadata—单独变更不会重建 Derived 或重新 Delivery |
| `source.table` | 必要；可选 `source.schema`（live Oracle owner；默认为 Source `username`） |
| `target.collection` | Target Binding；仅 Base-only 实验可省略 |
| `fields` | 字段 → `{ as: string \| omit }` 的映射 |
| `outputIdentity` | `transform` 必要 |
| `transform` | 声明式步骤；`transform` mode 必要；`direct` 禁止 |

最小 Direct 示例：

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
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: orders
      mode: direct
      source:
        table: ORDERS
      target:
        collection: orders
```

## 相关章节

- 短路径：[从这里开始](start-here.md)
- Secrets 与 TLS：[Security](security.md)
- 章节深读：[Deployment](deployment.md)、[Pipeline](pipeline.md)、[Source System](source-system.md)
