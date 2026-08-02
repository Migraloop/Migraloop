# CLI 與設定參考

指令、旗標、環境變數，以及 Deployment 設定欄位。

## Operator CLI 子指令

`migraloop` Operator CLI 目前提供這些子指令：

- `migrate` — 套用 Platform Store schema migrations
- `apply` — 套用宣告式 Deployment 設定
- `status` — 回報 Platform Store 健康狀態、Deployments、Pipelines 與 Base Datasets
- `base` — 檢視某個 Source 資料表的 Base Dataset 列
- `target` — 檢視某個 Pipeline collection 的 Target 文件
- `derived` — 檢視 Transform Pipeline 的 Derived Dataset 列
- `sync` — 執行 Incremental Capture 寫入 Base Datasets，並進行 Delivery
- `run` — 啟動時 migrate，然後維持 app 行程運作

## 公開環境變數契約

- `MIGRALOOP_PLATFORM_STORE_URL` — Operator CLI 使用的 Platform Store 連線 URL

_章節 stub — 完整內容於後續 handbook 票據補齊。_
