# CLI 與 Config 參考

給 Operator 的指令、旗標、環境變數與 Deployment 設定欄位。

## Binary

```text
migraloop <subcommand> [flags]
```

由 `crates/app` 建置（`Dockerfile` release binary）。所有與 Platform Store 通訊的 Operator 子指令都接受 `--platform-store-url` 或下方環境變數。

Operator 動詞（`apply`、`sync`、`run`、`status`、`capacity-estimate`、inspect、pause/resume/remove、align、drift）是薄的 clap/config/env adapter，接在 Deployment **runtime** 之上——Sync／Affect Analysis／Delivery orchestration 不由 CLI 模組擁有。runtime 的 public surface 是這些 Operator verbs 加上 Source／Target factory entry points；Operator narrative formatting（例如 `status` labels）留在此 CLI adapter，continuous Sync／supervise 偏好於此開啟的 Platform Store session。要擴充 Source 或 Target engine 的 Developers，請依 [Developer 本機設定](developer-local-setup.md#新增-source-或-target-enginedeveloper-checklist) 的 checklist。

## Operator CLI 子指令

`migraloop` Operator CLI 目前提供這些子指令：

### `migrate`

套用版本化 Platform Store schema migrations。

```bash
migraloop migrate --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `apply`

套用宣告式 Deployment 設定（YAML 或 JSON）。驗證 secrets-by-reference、Source/Target kinds、Pipeline specs、（當 Pipelines 參照資料表時）Source Prerequisites，視需要執行 schema discovery + Initial Load，並 upsert Deployment/Pipeline 狀態。以語意 Pipeline 變更重新 apply（transform/binding 等）是 Operator 的 **Pipeline revision** 路徑：暫停該 Pipeline 的舊 Delivery，依需要重建 Derived 並重新 Delivery，然後繼續 incremental；Shared Bases 不重建。僅變更選用的 `description` 屬 metadata-only，可跳過 rebuild。

在真實 Oracle Source host（非 `contract`/`stub`）上，apply 會透過 OCI 從 live Source 做 schema discovery 與 Initial Load（需要 Instant Client；見 [Source System](source-system.md)）。contract/stub host 僅使用**注入的**行程內 **contract Source catalog**（CI 切片；`MIGRALOOP_CONTRACT_SOURCE_CATALOG` 供 discovery/Initial Load；`MIGRALOOP_INJECT_LOGMINER_CONTENTS` 供 Incremental Capture）—不是隨產品附帶的業務資料表 catalog，也不是受支援的 production Source 機制。

Initial Load 以**有界 chunks**讀取 Source（預設 chunk size `1000`，Operator env `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE`），會印出 `Initial Load progress`／structured `initial_load_progress` events，並可用 `MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC` 限速。Load 中途 pause（對引用該表的 Pipeline 執行 Operator `migraloop pause`，或 Test/Lab typed ApplyOptions inject）會持久化進度（`status=initial_load_paused`）與 cutover low-watermark；再跑 `migraloop apply` 即可 resume，無需拆除 Deployment。在 Downstream／store 壓力下，apply 會印出 `Initial Load backoff`，而不是讓記憶體無界成長（見 [Operations](operations.md)）。Lab／RQG 優先透過 `apply` 上的 typed ApplyOptions（`--initial-load-*` hidden flags）傳入 knobs，而不是以 process env 作為主要 adapter。

```bash
migraloop apply --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" -f deployment.yaml
```

| 旗標 | 意義 |
| --- | --- |
| `-f`, `--file` | Deployment 設定路徑 |
| `--initial-load-chunk-size` | Hidden Test/Lab／override：typed ApplyOptions chunk size（Operator env 也適用） |
| `--initial-load-rows-per-sec` | Hidden Test/Lab／override：typed ApplyOptions throttle（Operator env 也適用） |
| `--initial-load-pause-after-chunks` | Hidden Test/Lab：typed ApplyOptions，成功 N 個 chunks 後 pause inject |
| `--initial-load-store-delay-ms` | Hidden Test/Lab：typed ApplyOptions store-pressure inject |

### `status`

回報 Platform Store 健康、Deployments、Pipelines、Base Datasets、Sync Health（`unknown` / `ok` / `lagging` / `failed`）、Source Alignment、Delivery Health、Quarantine 列、Schema Change impacts、Derived Datasets，以及 **component pressure**（`app` / `source` / `platform_store` / `target`）。Sync Health 與 Delivery Health 都暴露 `lag=`（從 capture resume position 起算的剩餘 pending 工作）；labels 來自與 Prometheus 共用的同一個 runtime Observability assembly。Downstream 變慢時，lag 會在 backpressure 下上升，但不會因此 pause Pipeline；capture 一次仍只填滿一個 bounded queue window（ADR-0020）。當 Poison Change quarantine 作用中時，Delivery Health 為 `unhealthy`，並把每個被 quarantine 的 Output Identity 標為 unhealthy / not aligned（ADR-0015）。當 blocking Schema Change pause 作用中時，Delivery Health 為 `paused`，且 `status` 會列出 Schema Change blocking 列（ADR-0009）—與 quarantine 不同。

```bash
migraloop status --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `capacity-estimate`

回報目前 Deployment 條件下的 **Capacity Estimate**（ADR-0031 / issue #249）：`limiting_component`、粗粒度 `max_e2e_qps`、`infra_saturated`，以及與 `status` / Lab 報告相同的 component pressure 摘要。僅供建議 — **永不**修改 Source System 或 Target System 的資料庫設定。當 `infra_saturated=yes` 時，應調整 infra（或 Lab Fixture）後重跑；該結果不算產品失敗。

```bash
migraloop capacity-estimate --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `base`

檢視某個 Source 資料表的 Base Dataset 列。

```bash
migraloop base --table ORDERS [--deployment oracle-to-mongo]
```

| 旗標 | 意義 |
| --- | --- |
| `--table` | Source 資料表名稱（必要） |
| `--deployment` | 多個 Bases 共用表名時消歧義 |

### `target`

檢視某個 Pipeline collection 的 Target 文件。

```bash
migraloop target --collection orders [--deployment oracle-to-mongo]
```

| 旗標 | 意義 |
| --- | --- |
| `--collection` | Target collection 名稱（必要） |
| `--deployment` | 共用 collection 名稱時消歧義 |

### `derived`

檢視 Transform Pipeline 的 Derived Dataset 列。

```bash
migraloop derived --pipeline orders_by_customer [--deployment oracle-to-mongo]
```

| 旗標 | 意義 |
| --- | --- |
| `--pipeline` | Pipeline 名稱（必要） |
| `--deployment` | 共用 Pipeline 名稱時消歧義 |

### `sync`

One-shot Incremental Capture 寫入 Base Datasets、維護 Derived Datasets，然後 Delivery。用於 Lab scenarios 與 Operator catch-up；steady-state continuous Sync 請用 `migraloop run`（compose 預設）。

Oracle Incremental Capture 一律走 LogMiner：真實 host 使用 **LogMiner (OCI)**；`host: contract` / `stub` 使用行程內 contract harness。真實 host **不會** silent fallback 到 stub catalog。缺少 Instant Client 或 OCI 失敗時會以 LogMiner/OCI 名稱 fail fast。

已 pause 的 Pipelines 在 `sync` 期間會略過 Delivery/processing；共用 Base Dataset 的 Incremental Capture 仍會繼續，讓其他 Pipelines 與之後的 resume catch-up 保持正確。

當單一 Output Identity 的 Delivery 反覆失敗時，`sync` 會重試最多 `MIGRALOOP_POISON_MAX_ATTEMPTS` 次（預設 `3`），然後 quarantine 該 identity、發出 Operator 可見的 **ALERT**、繼續其他 changes，並在 `status` 上顯示 quarantine（ADR-0015 / issue #22）。

當 Incremental Capture 看到會 **blocking** 某條 Pipeline 相依性的 Source DDL 時，`sync` 會發出 Operator 可見的 **WARN**、pause 受影響的 Pipeline(s)，並在 `status` 記錄 Schema Change impact—不會 quarantine（ADR-0009 / issue #23）。Unaffecting 或 non-blocking 的 schema changes 會繼續。

Platform Store 會序列化 Incremental Capture cycles，避免 one-shot `sync` 與 continuous `run` 同時 multi-write 同一個 Deployment。

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `align`

執行 **Source Alignment Check**（issue #24）：以非即時、**resource-gated** 方式驗證 Base 是否符合 Source；若不一致，用同一批 Source check reads 修復 Base。檢查**從不寫入 Source**。在把 Base 當作可靠 Drift baseline 之前需要此檢查；單靠 Sync Health 不夠。

預設 `--max-rows` 為 `1000`，方便 Operator 排程檢查而不做全表 slam。較大 budget（或重複執行）可覆蓋其餘列；`status` 顯示上次執行的 `Source Alignment: aligned|partial|unknown` 與 checked/mismatched 計數（`partial` 表示 budget 被截斷）。

```bash
migraloop align --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" [--table CUSTOMERS] [--deployment oracle-to-mongo] [--max-rows 1000]
```

| 旗標 | 意義 |
| --- | --- |
| `--table` | Source 資料表 / Base Dataset（預設：所有 Bases） |
| `--deployment` | 多個 Bases 共用資料表名稱時用以消歧 |
| `--max-rows` | 每個 Base 最多讀取的 Source 列數（resource gate；預設 `1000`） |

### `drift`

執行 **Drift Check**（issue #25）：以非即時、**resource-gated** 方式驗證 Target 上的 Managed fields 是否符合平台 expected dataset（Direct 用 Base，Transform 用 Derived）。預設會對偵測到的 Managed drift 走與 Delivery 相同的 Managed-only upsert 路徑做 **auto-repair**；**non-Managed Target fields 會被忽略**、不會被覆寫。對 Direct Pipelines，Base 必須已有 Source Alignment（`aligned` 或 `partial`）—先執行 `migraloop align`。Auto-repair 不會在 Alignment baseline 之外再增加 Source load。

預設 `--max-rows` 為 `1000`，方便 Operator 排程檢查而不做全 collection slam。較大 budget（或重複執行）可覆蓋其餘 Output Identities；`status` 顯示上次執行的 `Drift: ok|partial|unknown` 與 checked/mismatched 計數（`partial` 表示 budget 被截斷）。

```bash
migraloop drift --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" [--pipeline customers] [--deployment oracle-to-mongo] [--max-rows 1000]
```

| 旗標 | 意義 |
| --- | --- |
| `--pipeline` | Pipeline 名稱（預設：所有具 Target Binding 的 Pipelines） |
| `--deployment` | 多個 Deployments 共用同名 Pipeline 時用以消歧 |
| `--max-rows` | 每個 Pipeline 最多檢查的 Output Identities（resource gate；預設 `1000`） |

### `pause`

暫停一條 Pipeline，且不重啟 Deployment（ADR-0007）。停止該 Pipeline 後續的 Delivery/processing；耐久的 Base/checkpoint 狀態會保留。其他 Pipelines 繼續執行。

```bash
migraloop pause --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | 意義 |
| --- | --- |
| `--pipeline` | Pipeline 名稱（必填） |
| `--deployment` | 多個 Deployments 共用同名 Pipeline 時用以消歧 |

### `resume`

恢復已 pause 的 Pipeline。清除耐久 pause 旗標，並依目前 Platform Store 的 Base/Derived 狀態做 catch-up Delivery（含 pause 期間消失的 identities 的 deletes），之後由 continuous `run`（或後續 one-shot `sync`）繼續 Incremental Delivery。

```bash
migraloop resume --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | 意義 |
| --- | --- |
| `--pipeline` | Pipeline 名稱（必填） |
| `--deployment` | 多個 Deployments 共用同名 Pipeline 時用以消歧 |

### `remove`

在不重啟 Deployment 的前提下移除一條 Pipeline（ADR-0007）。會停止該 Pipeline 的 Delivery/processing。若其他 Pipelines 仍引用，Shared Base Datasets 會保留；不再被引用的 Bases 會被 prune。`status` 不再把該 Pipeline 列為 active。已 Deliver 的 Target documents 會保留（停止 Delivery，不是 wipe）。若要在之後的 `apply` 中持續省略它，也請從 declarative config 移除。

```bash
migraloop remove --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | 意義 |
| --- | --- |
| `--pipeline` | Pipeline 名稱（必填） |
| `--deployment` | 多個 Deployments 共用同名 Pipeline 時用以消歧 |

### `run`

啟動時 migrate，對已套用（未 pause）的 Pipelines 持續執行 Incremental Capture → Affect Analysis → Delivery，並在同一個 single active instance 上提供 Observability Surface Prometheus scrape endpoint，然後維持行程運作（compose 預設 command）。Steady-state Sync 不需要外部 sync scheduler。Caught up 或尚未套用 Deployment 時會 idle-poll（typed SyncOptions `poll_interval_ms`；Operator env `MIGRALOOP_SYNC_POLL_INTERVAL_MS` 或 hidden `--sync-poll-interval-ms`）。Source/Target secret refs 必須存在於此行程環境。

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" \
  [--metrics-addr 0.0.0.0:9090]
```

| Flag / env | 意義 |
| --- | --- |
| `--metrics-addr` / `MIGRALOOP_METRICS_ADDR` | Prometheus `/metrics` listen address（預設 `0.0.0.0:9090`）。Compose 會 map host `9090`。見 [Observability](observability.md)。 |
| `--sync-poll-interval-ms` | Hidden Test/Lab／override：typed SyncOptions idle poll 間隔（Operator env `MIGRALOOP_SYNC_POLL_INTERVAL_MS` 也適用） |

### `lab`

Local Sync Lab Fixture 與 Lab Scenarios（ADR-0025）。佈建可拋棄的真實堆疊—Oracle Source（Lab 已滿足的 Source Prerequisites）、MongoDB Target、Platform Store 與 app。Bring-up **不會**套用 sample Deployment 或 Pipelines。Operator 接著可 list/run 可選的 **Lab Scenarios**，在 **Scenario Namespace** 內以真實 product path 套用 Deployment 並驅動 Sync/Delivery。`lab scenario run` 進行時 Lab 會暫停 Fixture `app`（`migraloop run`），讓 host `apply`/`sync` 成為唯一的 Incremental Capture 消費者，結束後再恢復 `app`（仍是真實 product CLI Sync／Delivery，不是 Lab stub）。需要 Docker Compose 與 repo 的 `lab/` 目錄（或 `--lab-dir`）。Scenario 的 `apply`/`sync` 需要 host 上的 Oracle Instant Client（`LD_LIBRARY_PATH`）。若巢狀 Docker 在 overlay whiteout 解壓失敗，需改用 dockerd `fuse-overlayfs` 或 `vfs`（並關閉 containerd snapshotter）；`lab up` 遇 whiteout/`EPERM` 時會印出此提示。**Cursor Cloud** 會在 environment `install`/`start` 套用 `fuse-overlayfs`—見 [Developer local setup](developer-local-setup.md) 與 [Deployment](deployment.md)。

```bash
migraloop lab up [--lab-dir lab]
migraloop lab status [--lab-dir lab]
migraloop lab down [--lab-dir lab]
migraloop lab scenario list [--lab-dir lab]
migraloop lab scenario run <scenario-id> [--lab-dir lab] [--auto-remove]
migraloop lab scenario remove <scenario-id> [--lab-dir lab]
```

| Subcommand | 意義 |
| --- | --- |
| `up` | 啟動可拋棄 Fixture；就緒時印出連線細節 |
| `status` | 回報 Fixture 就緒狀態（engines + Oracle prerequisites + Platform Store），以及哪個 Scenario Namespace 為 **active**（run 進行中）或 **leftover**（run 結束後保留），或各自為 `(none)`。在你套用設定或執行 Lab Scenario 之前也會顯示 `Deployment: (none)` / `Pipeline: (none)` — 請用 Scenario run / leftover 列判斷，不必從那些行自行猜測 |
| `down` | 拆除 containers 與 volumes |
| `scenario list` | 依 `--lab-dir` 磁碟上的 recipe 列出可選 Lab Scenarios（`lab/scenarios/<id>/recipe.yaml` + `deployment.yaml`，且已註冊 Scenario adapter）。summary 來自各 recipe—例如 `direct-pipeline`、`rt-project`、`rt-filter`、`rt-field-ops`、`rt-equilookup`、`rt-union`、`rt-unwind`、`rt-distinct-addtoset`、`transform-pipeline`、`concurrent-source-workload`、`change-ordering`、`bulk-load`、`idempotent-redelivery`、`pause-resume`、`remove-pipeline`、`change-pipeline`、`poison-quarantine`、`schema-change-pause`、`source-alignment`、`drift-check`、`bounded-backpressure`、`observability-surface`、`platform-store-guardrails`、`backward-compatible-upgrades`、`initial-load-throttled`。list 也會回報已出貨 capability 覆蓋（complete vs gaps；見 `lab/scenarios/COVERAGE.md`） |
| `scenario run` | 依 id 經 recipe-driven runner 執行一個 Lab Scenario（等權 `thresholds` 與 typed `product_path` 從 `recipe.yaml` 讀取—不複製成 Rust constants；共用 product-path 步驟執行 Namespace lifecycle + prepare／apply／sync；`namespace.lifecycle` 提供 wipe／seed（可選 mutate SQL）；thin hooks 提供 rare escapes；`checks.correctness` 執行 inspect expectations）。若已有 Scenario 正在執行則拒絕。若 Source/Target 不是 Lab Fixture engines 也會拒絕（客戶／正式環境資料庫不在 Lab 範圍—那些請用一般的 `apply`/`sync`）。重跑同一 Scenario 會先完整移除其 Namespace 再重建。回報 pass/fail（或 `INFRA-SATURATED`）以及 `duration_ms`、rows/throughput、lag、Scenario 定義的 thresholds（例如 settle time，或 bulk-load 的 lag／throughput／duration，若有）（correctness 與 operational metrics 等權），以及 component pressure 摘要（`app` / `source` / `platform_store` / `target`）。當 Source／Platform Store／Target 飽和時，結果為 `INFRA-SATURATED` 並給出擴容指引 — 不是產品 FAIL（ADR-0031）。`rt-project` / `rt-filter` / `rt-field-ops` / `rt-equilookup` / `rt-union` / `rt-unwind` / `rt-distinct-addtoset` 覆蓋已出貨 Rich Transform `project`、`filter`、`addFields`/`rename`/`remove`、`equiLookup`、`union`、`unwind`，以及 `distinct`/`addToSet` operators；`transform-pipeline` 覆蓋多表 `groupBy` 的 `sum`/`count`/`min`/`max`/`avg`；`concurrent-source-workload` 在單一 Scenario 內跑平行 Source sessions；`change-ordering` 證明 Change Ordering / confluence（同 key A→B→C、跨 key interleave、min Base recompute）走正常 Incremental 路徑；`bulk-load` 會 bulk-insert 約 100k Source rows，且 metric thresholds 可獨立於 correctness 讓 run 失敗；`idempotent-redelivery` 會強制對同一批 Output Identities 做 duplicate-safe re-Delivery，並檢查 Managed Target 結果仍正確；`pause-resume` 覆蓋 `pause` / `resume` CLI 動詞（一條 Pipeline 停止 Delivery、另一條繼續；resume 自耐久 Base catch-up）。`remove-pipeline` 覆蓋 `remove`（停止 Delivery；仍被引用的 Shared Base 保留；status 不再列出該 Pipeline）；`change-pipeline` 覆蓋透過 `apply` 的 Pipeline revision（暫停舊 Delivery → 重建該 Pipeline 的 Derived／重新 Delivery；Shared Bases 不重建；僅 `description` 的 metadata-only 變更可跳過 rebuild）；`poison-quarantine` 在有界重試後 quarantine 單一 poison Output Identity 並 ALERT，Pipeline 繼續，且 `status` 顯示 unhealthy / not aligned；`schema-change-pause` 會在 blocking DDL 時 WARN 並 pause 受影響的 Pipeline（與 poison quarantine 不同）；`source-alignment` 會偵測 Base≠Source、僅用 Source reads 修復 Base，並練習 resource-gated `--max-rows`；`drift-check` 覆蓋 Drift Check（Managed Target drift 偵測 + 預設 Managed auto-repair；保留 non-Managed；resource-gated `--max-rows`）；`bounded-backpressure` 以 Downstream Delivery delay 搭配極小 queue capacity，驗證 Backpressure／lag 且不 pause，再 catch-up；`initial-load-throttled` 演練 chunked／rate-limited／pausable Initial Load，並在 store 壓力下 backoff，同時保留 cutover low-watermark。第二個 Scenario run 仍會被拒絕。預設 keep-on-finish 保留 Namespace 供即時 `base`/`derived`/`target` 檢查；成功後若要刪除可傳 `--auto-remove`；`observability-surface` 覆蓋 structured JSON operator logs、Prometheus `/metrics` lag/failures，以及 `status` 上的 Sync/Delivery Health；`platform-store-guardrails` 演練 Platform Store Guardrails（拒絕過低設定；可用磁碟 WARN + `platform_store_disk_warn` 且 store 仍 healthy；Pipeline 不自動 pause）；`backward-compatible-upgrades` 在升級路徑上 migrate Platform Store、保留既有 Deployments/Base，並以較舊 SemVer-compatible `apiVersion`（`migraloop.dev/v1.0.0`）重新 apply 而不做 Initial Load rebuild |
| `scenario remove` | 完整移除 Scenario Namespace（Source tables、Target collections、Platform Store Deployment），且不啟動 run。若已有 Scenario 作用中則拒絕。已不存在時為 idempotent |

| Flag | 意義 |
| --- | --- |
| `--lab-dir` | 含 Lab `compose.yaml` 的目錄（預設：`lab`） |
| `--auto-remove` | 僅用於 `scenario run`：成功結束後完整移除 Scenario Namespace（opt-in；失敗時仍保留 Namespace 以便除錯） |

Lab 是手動驗證—不是 Release Quality Gate，也不是 contract/stub LogMiner harness。可選的 Scenario catalog 是 feature-time 完整度表面（ADR-0025），不是 CI suite：不要新增會跑完整 catalog 的 release-gate job。Scenario recipe 慣例、撰寫路徑（recipe-driven runner 介面：`workload`／`product_path`／`namespace.lifecycle`／`checks`／`thresholds`；thin hooks 負責 rare escapes；`checks.correctness` 為可執行 inspect vocabulary），以及已出貨 capability 覆蓋 gaps 見 [Developer local setup](developer-local-setup.md)、`lab/scenarios/README.md` 與 `lab/scenarios/COVERAGE.md`。若要在 Scenario recipes **之外**做 DB-level restore/load（用 `lab status` 連線細節對 Lab Oracle/Mongo 跑 SQL/mongosh/dumps，再接一般 `apply`／`status`／inspect／`sync`），見 `lab/escape-hatch/` 與 [Deployment](deployment.md)—該 escape hatch 不是第二套 Scenario 模型，也不是 CI。

## 公開環境變數契約

| 變數 | 意義 |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Operator CLI 與 compose `app` 使用的 Platform Store 連線 URL（`postgres://...`）。TLS 請用 `sslmode=require\|verify-ca\|verify-full`，可選 `sslrootcert=/path/to/ca.pem` |
| `MIGRALOOP_PLATFORM_STORE_DATA_DIR` | app filesystem 上用來觀測 Platform Store 可用磁碟的路徑（compose 會把 store data volume 以 read-only 掛在 `/var/lib/migraloop/platform-store-data`） |
| `MIGRALOOP_PLATFORM_STORE_FREE_DISK_BYTES` | 選用：當無法做 filesystem probe 時，由 Operator／orchestrator 提供的可用磁碟位元組數（覆寫目錄探測以供 warn threshold） |
| `MIGRALOOP_METRICS_ADDR` | `migraloop run` 的 Prometheus scrape listen address（預設 `0.0.0.0:9090`） |
| `MIGRALOOP_SYNC_POLL_INTERVAL_MS` | `migraloop run` 內 continuous Incremental Capture cycles 之間的 idle poll 間隔（預設 `1000`；必須 > 0）。Typed 於 SyncOptions；Test/Lab 也可在 `migraloop run` 設定 `--sync-poll-interval-ms` |
| 設定中 `fromEnv` 參照的密鑰環境變數名 | 你在 `password.fromEnv` 寫的任何名稱（例如 `ORACLE_PASSWORD`、`MONGO_PASSWORD`）在 apply / one-shot `sync` / continuous `run` 時必須存在於行程環境 |
| `LD_LIBRARY_PATH` | 真實 Oracle host：Oracle Instant Client libraries 目錄（apply/sync/`run` runtime 需要；`contract`/`stub` 不使用） |
| `MIGRALOOP_CONTRACT_SOURCE_CATALOG` | 僅 contract/stub host：harness catalog 資料表 JSON 檔路徑，供 schema discovery + Initial Load（CI／本機切片；未設定為空；不是 production Source 機制） |
| `MIGRALOOP_POISON_MAX_ATTEMPTS` | Poison Change quarantine 前的有界 Delivery 重試次數（預設 `3`；必須 > 0） |
| `MIGRALOOP_DELIVERY_POISON_IDENTITIES` | 已棄用、僅薄暫時 compat shim：以逗號分隔、一律讓 Delivery 失敗的 Output Identity keys。請改用 typed SyncOptions（`migraloop sync` 的 `--sync-poison-identity` 等隱藏 Test/Lab flags，或 in-process `SyncOptions`；不是 production Operator 控制） |
| `MIGRALOOP_INJECT_SCHEMA_CHANGES` | 僅 Test/Lab injection：Schema Change events 的 JSON 檔路徑（`scn`、`table`、`kind`、`columns` …），以便在沒有 LogMiner DDL capture 時演練 blocking DDL warn+pause（不是 production Operator 控制） |
| `MIGRALOOP_SYNC_QUEUE_CAPACITY` | Bounded Incremental Capture / Delivery window 大小（預設 `256`；必須 > 0）。one-shot `sync` 或 continuous `run` 下各階段一次 materialize 的 pending changes 都不超過此容量（ADR-0020）。Test/Lab 也可在 `migraloop sync` 上設 `--sync-queue-capacity` |
| `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE` | Bounded Initial Load Source read window（預設 `1000`；必須 > 0）。apply 的正常路徑不會把 unbounded full-table slam 整表灌進記憶體。Test/Lab 也可在 `migraloop apply` 設定 `--initial-load-chunk-size` |
| `MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC` | 可選的 Initial Load throttle（rows/second；`0`／未設定 = 除 chunking 外不再人工限速）。在 progress 行／`initial_load_progress` 以 `rate_limit` 可見。Test/Lab 也可在 `migraloop apply` 設定 `--initial-load-rows-per-sec` |
| `MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS` | 僅供 deprecated 薄 compat shim：成功 N 個 chunks 後 pause Initial Load。請優先使用 `migraloop apply` 上的 typed ApplyOptions（`--initial-load-pause-after-chunks`，hidden Test/Lab flag）或 in-process `ApplyOptions`（不是 production Operator 控制；Operators 請在 chunks 之間使用 `migraloop pause`） |
| `MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS` | 僅供 deprecated 薄 compat shim：Initial Load 期間人工延遲 Platform Store／Downstream。請優先使用 `migraloop apply` 上的 typed ApplyOptions（`--initial-load-store-delay-ms`，hidden Test/Lab flag）或 in-process `ApplyOptions`（不是 production Operator 控制） |
| `MIGRALOOP_DELIVERY_DELAY_MS` | 已棄用、僅薄暫時 compat shim：人工 Downstream Delivery 延遲（毫秒）。請改用 typed SyncOptions（`migraloop sync` 的 `--sync-delivery-delay-ms`、`--sync-fail-after-changes` 等隱藏 Test/Lab flags，或 in-process `SyncOptions`；不是 production Operator 控制） |
| `MIGRALOOP_INJECT_LOGMINER_CONTENTS` | 僅 Test/Lab injection：contract LogMiner contents 的 JSON 檔路徑（`contents: [{scn, operation, table_name, identity, after_image, rs_id?, ssn?}, …]`），供 `contract`/`stub` hosts 的 Incremental Capture。可選的 `rs_id` / `ssn` 是 LogMiner ordering keys，讓同一 SCN 的多列在 dedupe 與 resume-safe catch-up 時保持可區分（未設定時 harness Incremental 串流為空；不是 production Operator 控制） |
| Lab disposable defaults | `migraloop lab up` 之後：`ORACLE_PASSWORD=lab_oracle`、`MONGO_PASSWORD=lab_mongo`、Platform Store URL `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`、Mongo URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`（僅本機 Lab） |

### Contract-harness Source Prerequisite probes（僅 host `stub` / `contract`）

行程內 LogMiner harness 的環境變數名稱與預設見 [Source System](source-system.md)（`MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING`、`MIGRALOOP_STUB_REDO_RETENTION_HOURS`）。

## Deployment 設定契約

| 欄位 | 必要 | 說明 |
| --- | --- | --- |
| `apiVersion` | 是 | `migraloop.dev/v1`（major 1 內 SemVer 較舊或相等：也接受 `migraloop.dev/v1.0` / `migraloop.dev/v1.0.0`；較新 minor/patch 與其他 major 會被拒絕） |
| `kind` | 是 | `Deployment` |
| `metadata.name` | 是 | 非空 Deployment 名稱 |
| `spec.source` | 是 | 見下 |
| `spec.target` | 是 | 見下 |
| `spec.pipelines` | 否 | 預設 `[]`（只套用 Deployment） |

### `spec.source` / `spec.target`

| 欄位 | Source | Target | 說明 |
| --- | --- | --- | --- |
| `kind` | `oracle` | `mongodb` | v1 固定配對 |
| `host` | 是 | 是 | Source `stub`/`contract` → LogMiner harness + contract-catalog Initial Load；其他 host → live OCI Initial Load + LogMiner |
| `port` | 是 | 是 | 有效 TCP port |
| `database` | 是 | 是 | |
| `username` | 是 | 是 | 省略 Pipeline `source.schema` 時，也作為預設 Oracle schema/owner |
| `password` | 是 | 是 | 恰好一個 `fromEnv`、`fromFile`、`fromDockerSecret` |
| `timezone` | 可選 | n/a | IANA 或 Oracle 風格 `±HH:MM`，供 naive 時間；兩種形式皆可在 `apply` 通過 |
| `tls` | 可選 | 可選 | 見下；省略／`enabled: false` 仍允許 cleartext |

#### `tls`（可選）

| 欄位 | Source | Target | 說明 |
| --- | --- | --- | --- |
| `enabled` | 可選 | 可選 | 為 `true` 時以 TLS 連線；設定錯誤會明確失敗（不靜默回退 cleartext） |
| `caFile` | **無效**（請用 `walletLocation`） | 可選路徑 | Mongo CA 檔；僅檔案系統路徑（不可 inline PEM） |
| `walletLocation` | 可選目錄 | **無效** | Oracle Instant Client wallet 目錄 |
| `insecureSkipVerify` | 可選 bool | 可選 bool | 僅供開發/Lab；預設 `false` |

Platform Store TLS 設定在 `MIGRALOOP_PLATFORM_STORE_URL`（`sslmode=require|verify-ca|verify-full`，可選 `sslrootcert=…`）—見 [Security](security.md)。

Docker secrets 從 `/run/secrets/<name>` 解析。

### Pipeline 項目（`spec.pipelines[]`）

| 欄位 | 說明 |
| --- | --- |
| `name` | 非空 |
| `mode` | `direct` 或 `transform` |
| `description` | 選用、Operator-facing 註解；僅 metadata—單獨變更不會重建 Derived 或重新 Delivery |
| `source.table` | 必要；可選 `source.schema`（live Oracle owner；預設為 Source `username`） |
| `target.collection` | Target Binding；僅 Base-only 實驗可省略 |
| `fields` | 欄位 → `{ as: string \| omit }` 的對應 |
| `outputIdentity` | `transform` 必要 |
| `transform` | 宣告式步驟（建議 Aggregation／SQL-like DX；classic 仍 Upgrade Compatible）；`transform` mode 必要；`direct` 禁止 |

最小 Direct 範例：

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

## 相關章節

- 短路徑：[從這裡開始](start-here.md)
- Secrets 與 TLS：[Security](security.md)
- 章節深讀：[Deployment](deployment.md)、[Pipeline](pipeline.md)、[Source System](source-system.md)
