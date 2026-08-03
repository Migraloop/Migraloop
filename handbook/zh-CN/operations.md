# Operations

Operator 在生产环境运行 Deployments 时应预期的 day-2 行为。下列若干控制项是 ADRs 与领域 glossary 记录的 **产品契约**；每个控制项落地时，请以 `migraloop status` / logs 核对你的 build 实际暴露什么。

## Schema Change Handling

Source DDL 会按每条 Pipeline 的依赖分类（ADR-0009）：

| 影响 | 预期平台行为 |
| --- | --- |
| 不影响该 Pipeline | 继续处理；schema 可稍后追上 |
| 影响 Pipeline 但 apply 仍安全 | 继续处理 |
| 阻挡安全 apply（重试无法前进） | **警告并 pause** 受影响的 Pipeline(s) |

此 pause 规则用于 **stream-wide blockers**，不是单行 poison data。当 Incremental Capture 遇到会阻塞的 Source DDL 时，`migraloop sync` 会发出 Operator 可见的 **WARN**、持久化 Schema Change impact，并以与 `migraloop pause` 相同的耐久 pause 旗标 pause 受影响的 Pipeline(s)—不会走 quarantine。Unaffecting 或 non-blocking 的 schema changes 会继续；`status` 会显示 `Delivery Health: paused`，以及作用中的 blocking Schema Change 行（与 Poison Change quarantine 不同）。Operator 也可主动用 `migraloop pause --pipeline <name>` / `migraloop resume --pipeline <name>` pause/resume 一条 Pipeline，或以 `migraloop remove --pipeline <name>` 移除一条（见 [Pipeline](pipeline.md) 与 [CLI 与 Config](cli-and-config.md)），且不必重启 Deployment。Resume 会清除该 Pipeline 作用中的 Schema Change impacts，并依耐久 Base/Derived 状态做 catch-up Delivery。

## Poison Change Handling

当单个 change 或 Output Identity 反复失败，但流其余部分仍可继续时（ADR-0015），预期路径是：

1. 有界重试
2. **Quarantine** 该 change/identity
3. **Alert** Operators
4. **让 Pipeline 继续跑**

被 quarantine 的 keys 在修复或重试前保持 unhealthy / not aligned—绝不默默跳过。不要预期单行坏数据就 pause 整条 Pipeline。有界 Delivery 重试后，`migraloop sync` 会持久化 quarantine、发出 Operator 可见的 **ALERT**，并继续处理其他 changes；`migraloop status` 会显示 `Delivery Health: unhealthy`，并把每个被 quarantine 的 Output Identity 标为 unhealthy / not aligned。


## Source Alignment Check

单靠 Sync Health 不能证明 Base 匹配 Source。Operator 在把 Base Dataset 当作 Drift baseline 之前，应运行可调度、resource-gated 的 **Source Alignment Check**：

```bash
migraloop align [--table CUSTOMERS] [--max-rows 1000]
```

检查最多读取 `--max-rows` 行 Source（默认 `1000`—不是全表 slam），按主键与 Base 比对，并在不一致时用这些 Source reads 修复 Base。**从不写入 Source**。`status` 显示上次执行的 `Source Alignment: aligned|partial|unknown` 与 checked/mismatched 计数（`partial` = budget 被截断）。见 [CLI 与 Config](cli-and-config.md) 与 [Observability](observability.md)。

## Drift Check

单靠 Delivery Health 不能证明 Target 上的 Managed fields 匹配平台 expected dataset。Operator 在 Direct Pipelines 完成 Source Alignment 后，应运行可调度、resource-gated 的 **Drift Check**，使 Base/Derived 成为可信 baseline：

```bash
migraloop drift [--pipeline customers] [--max-rows 1000]
```

检查最多读取 `--max-rows` 个 expected Output Identities（默认 `1000`—不是全表 slam），比对 Target 的 Managed fields，并默认以 Managed-only upsert **auto-repair** Managed drift。**non-Managed Target fields 会被忽略**且保持不动。不会在 Alignment baseline 之外再增加 Source load。`status` 显示 `Drift: ok|partial|unknown` 与 checked/mismatched 计数（`partial` = budget 被截断）。见 [CLI 与 Config](cli-and-config.md) 与 [Observability](observability.md)。

## Backpressure

当 Platform Store apply、Derived maintenance 或 Target Delivery 跟不上时（ADR-0020）：

- 各阶段使用 **bounded queues**（默认 Incremental window `MIGRALOOP_SYNC_QUEUE_CAPACITY`，256）并放慢 capture/apply
- Sync Health 与 Delivery Health 都暴露当前 window 剩余工作的 `lag=`；当 window 已满或 Downstream 延迟时，`sync` 会打印 `Backpressure: queue_depth=… capacity=…`
- 拒绝无界内存缓冲 / 把 OOM 当 backpressure
- 只因 Target 慢就 pause 整条 Pipeline **不是**默认行为

Operator 依可见 lag 行动（扩容 Target、降低负载、检查 Delivery 错误）—pause 留给真正的 blocker。Lab Scenario `bounded-backpressure` 可在可丢弃 Fixture 上演练此路径。

## Platform Store Guardrails

随附的 PostgreSQL Platform Store 带有安全默认与产品强制的下限（ADR-0010 / issue #28）。Operator 可以提高设置；app 会 **拒绝** 过低值，使 migrate / status / sync / apply / run 以 Guardrails 消息失败，而不是在资源不足下继续跑。

| 设置 | Compose 默认 | 产品下限（floor） |
| --- | --- | --- |
| `shared_buffers` | `128MB` | ≥ `64MB` |
| `work_mem` | `8MB` | ≥ `4MB` |
| `maintenance_work_mem` | `128MB` | ≥ `64MB` |
| `max_connections` | `100` | ≥ `20` |

根目录 `compose.yaml` 与 `lab/compose.yaml` 会在 `platform-store` service 带上这些默认。

**可用磁盘警告（warn-only）：** 当 Platform Store data volume 可用空间低于 **1 GiB** 时，产品会在 `migraloop status`（以及 sync / apply / run 路径）打印 `WARN: …`、发出 structured event `platform_store_disk_warn`，并暴露 Prometheus gauges `migraloop_platform_store_disk_free_bytes` 与 `migraloop_platform_store_disk_warn`。跨越门槛 **不会** 自动 pause Pipelines—除非另有问题，Platform Store 仍保持 healthy；如何响应警告是 Operator 的责任。

**如何观测可用磁盘：** compose 会把 store data volume 以 read-only 挂进 app 的 `/var/lib/migraloop/platform-store-data`，并设置 `MIGRALOOP_PLATFORM_STORE_DATA_DIR` 指向该路径。当无法做 filesystem probe 时，Operator／orchestrator 也可改供 `MIGRALOOP_PLATFORM_STORE_FREE_DISK_BYTES`。

Lab Scenario `platform-store-guardrails` 可在可丢弃 Fixture 上演练随附 Guardrail 下限、过低设置拒绝，以及 warn-only 可用磁盘行为。Postgres 备份仍是 Operator 的责任。

## Upgrades

升级必须 **backward compatible**（ADR-0014）：

- Platform Store schema 变更以启动时应用的版本化 migrations 出货（`migraloop run` / `migraloop migrate`）
- 较新的 app 必须能继续既有 Deployments 与可接受的旧配置，而不是 wipe-and-rebuild
- Deployment config `apiVersion` 在 major `1` 上为 SemVer 较旧或相等：正式写法 `migraloop.dev/v1`，较旧可接受形式如 `migraloop.dev/v1.0` / `migraloop.dev/v1.0.0` 仍可套用；较新 minor/patch 与不相容 major 会以清楚错误拒绝
- 单 instance 升级期间允许短暂 sync pause；不得丢失 checkpoint/数据
- v1 不要求支持 downgrade

建议升级循环：

1. `migraloop status` — 记下 checkpoints 与健康
2. 滚动新的 app image / binary
3. 确认 migrations（`status` 中的 `Schema version`）
4. 若需要，重新套用已接受的较旧 config（不得 Initial Load／从头 rebuild Base）
5. `migraloop sync` / 监视 Sync Health 与 Delivery Health

Lab Scenario `backward-compatible-upgrades` 可在可丢弃 Fixture 上演练升级时 migrate、较旧 SemVer-compatible config apply，以及不做 wipe-rebuild。

## 重启后 resume

持久的 capture 与 Delivery 进度存在 Platform Store。进程重启后，`migraloop sync` 会从存放的 checkpoint（exclusive）继续 Incremental Capture 并接续 Delivery—Operator 不应依赖仅存在本地的 recovery 文件。

## 相关章节

- 健康解读：[Observability](observability.md)
- 安装 / 单 instance 模型：[Deployment](deployment.md)
- CLI 动词：[CLI 与 Config 参考](cli-and-config.md)
