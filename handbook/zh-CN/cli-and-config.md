# CLI 与配置参考

命令、标志、环境变量，以及 Deployment 配置字段。

## Operator CLI 子命令

`migraloop` Operator CLI 当前提供这些子命令：

- `migrate` — 应用 Platform Store schema migrations
- `apply` — 应用声明式 Deployment 配置
- `status` — 报告 Platform Store 健康状态、Deployments、Pipelines 与 Base Datasets
- `base` — 查看某个 Source 表的 Base Dataset 行
- `target` — 查看某个 Pipeline collection 的 Target 文档
- `derived` — 查看 Transform Pipeline 的 Derived Dataset 行
- `sync` — 运行 Incremental Capture 写入 Base Datasets，并执行 Delivery
- `run` — 启动时 migrate，然后保持 app 进程运行

## 公开环境变量契约

- `MIGRALOOP_PLATFORM_STORE_URL` — Operator CLI 使用的 Platform Store 连接 URL

_章节 stub — 完整内容于后续 handbook 工单补齐。_
