# em-carts-amz-offers-task-sender

从 GCS cart analytics 种子文件读取 ASIN，通过 [em-spapi-celery](../em-spapi-celery) 发送带优先级的 SP-API offer 更新任务。

## 安装

```bash
cd /home/sky/src/em-carts-amz-offers-task-sender
uv sync
```

依赖本地 editable 的 `em-spapi-celery`（见 `pyproject.toml` 中 `[tool.uv.sources]`）。

## 配置

`em-spapi-celery` 需要 `config.ini`（product/offer ES 服务地址）。本地可复用：

```bash
export EM_SPAPI_CELERY_CONFIG=/path/to/em-spapi-celery/local_dev/config.ini
export BROKER_URL=redis://127.0.0.1:6379/0
```

## 运行

```bash
./scripts/amz_offers_update_task_sender.sh

# 或直接调用 CLI
amz_offers_update_task_sender \
  -s /path/to/gcs-service-account.json \
  -b redis://127.0.0.1:6379/0
```

默认使用最低优先级 `9`（bulk，写入 `SpapiItemOffersUpdate_{MP}:9`）。

### 常用参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `-s` | GCS service account JSON | 必填 |
| `-b` | Celery broker URL（或 `BROKER_URL` 环境变量） | 必填 |
| `-q` | 发送速率（msg/s） | 20 |
| `-t` | offer TTL 小时数（US 固定 24h） | 7 |
| `-p` | 任务优先级 0–9（0 最高，9 bulk 最低） | 9 |
| `-f` | 忽略队列深度上限强制发送 | false |

## 数据源

GCS 路径：`gs://em-bucket/em-analytics/carts/sources/AMZ_{MARKETPLACE}.txt`

每行格式：`{key}\t{json_product}`，从中提取 `source_product_id` 或 `asin`。

## 与 em-celery 的区别

- 独立项目，仅负责 cart 种子 → 入队
- 使用 `em-spapi-celery` 的 `dispatch_task()` 与 Redis 优先级子队列
- 队列深度统计与清空覆盖全部 priority 子队列（`:0` … `:9`）
