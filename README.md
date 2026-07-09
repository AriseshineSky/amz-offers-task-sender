# amz-offers-task-sender

从 **cart / ads / catalog** 三类数据源读取 Amazon ASIN，经 [em-spapi-celery](../em-spapi-celery) 按优先级写入 Celery 队列，触发 SP-API offer 更新任务。

## 功能概览

一次运行会依次处理 14 个 marketplace（US、CA、MX、AE、DE、IN、IT、JP、UK、BR、NL、BE、FR、PL）。每个 marketplace 内：

1. 从 GCS 下载 **cart**、**ads** 种子文件
2. 从 PostgreSQL **product_sources** 表流式读取 catalog ASIN
3. 查询 Elasticsearch 中已有 offer，跳过 TTL 内仍有效的 ASIN
4. 将需要更新的 ASIN 分批（每批最多 20 个）入队到 Redis 优先级子队列
5. 将运行统计写入 Elasticsearch metrics 索引

## 架构

```
┌─────────────────┐   ┌─────────────────┐   ┌──────────────────────┐
│  GCS cart seeds │   │  GCS ads seeds  │   │  PG product_sources  │
│  priority = 0   │   │  priority = 5   │   │  priority = 9        │
└────────┬────────┘   └────────┬────────┘   └──────────┬───────────┘
         │                     │                        │
         └─────────────────────┼────────────────────────┘
                               ▼
                  amz_offers_update_task_sender
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
         Offer ES 查询    去重 / TTL 过滤    dispatch_task()
                               │
                               ▼
              SpapiItemOffersUpdate_{MP}[:0..9]
                               │
                               ▼
                    em-spapi-celery workers
```

### 优先级 tier

优先级由数据源决定，无需手动指定 `-p`：

| Tier | 数据源 | 优先级 | 含义 | Redis 子队列示例（US） |
|------|--------|--------|------|------------------------|
| **cart** | GCS 购物车分析种子 | **0** (critical) | 最高，worker 优先消费 | `SpapiItemOffersUpdate_US` |
| **ads** | GCS 广告种子 | **5** (normal) | 中等 | `SpapiItemOffersUpdate_US:5` |
| **catalog** | PostgreSQL `product_sources` | **9** (bulk) | 最低，大批量补刷 | `SpapiItemOffersUpdate_US:9` |

处理顺序固定为 **cart → ads → catalog**。同一 ASIN 若在多个 tier 出现，先出现的 tier 入队，后续 tier 去重跳过。

定义见 `carts_amz_offers/priority_tiers.py`。

## 安装

```bash
git clone https://github.com/AriseshineSky/amz-offers-task-sender.git
cd amz-offers-task-sender
uv sync
```

本项目依赖本地 editable 的 `em-spapi-celery`（见 `pyproject.toml` 中 `[tool.uv.sources]`），需与 `em-spapi-celery` 仓库放在同级目录，或自行调整 path。

## 配置

### 环境变量

| 变量 | 说明 |
|------|------|
| `EM_SPAPI_CELERY_CONFIG` | em-spapi-celery 的 `config.ini` 路径（必填） |
| `BROKER_URL` | Celery Redis broker URL（脚本通过环境变量传给 CLI，避免 shell 转义问题） |
| `PG_DATABASE_URL` / `DATABASE_URL` | 可选，覆盖 config.ini 中的 PostgreSQL 连接 |

示例：

```bash
export EM_SPAPI_CELERY_CONFIG=/path/to/em-spapi-celery/local_dev/config.ini
export BROKER_URL='redis://127.0.0.1:6379/0'
```

含特殊字符的 Redis 密码请用**单引号**导出，或对密码做 URL 编码（如 `$` → `%24`）。脚本不会通过 `-b` 传 broker URL，而是 `export BROKER_URL` 后由 Python 从环境变量读取，避免 bash 二次解析密码中的 `$`、`"`、`` ` `` 等字符。

### config.ini 要求

`config.ini` 需包含以下 section：

- **product / offer ES 服务** — 供 offer 查询与 metrics 写入
- **`[pg_db]`** — catalog tier 读取 PostgreSQL，示例：

```ini
[pg_db]
host = localhost
port = 5432
user = myuser
password = mypass
name = mydb
product_sources_table = product_sources
```

也可使用 `url = postgresql://...` 代替 host/port/user/password/name。

### GCS

需提供 Google Cloud Storage service account JSON（`-s` 参数），用于下载种子文件。

## 运行

### 推荐：wrapper 脚本

```bash
./scripts/amz_offers_update_task_sender.sh
```

脚本默认读取以下环境变量（均可覆盖）：

| 环境变量 | 默认 | 说明 |
|----------|------|------|
| `GCS_SA` | `~/.em_celery/gcs-sa.json` | GCS service account 路径 |
| `BROKER_URL` | `redis://127.0.0.1:6379/0` | Redis broker |
| `QPS` | `20` | 发送速率（msg/s） |
| `TTL` | `24` | offer TTL 小时数 |

示例：

```bash
GCS_SA=~/.em_celery/gcs-sa.json \
BROKER_URL='redis://127.0.0.1:6379/0' \
QPS=10 TTL=24 \
./scripts/amz_offers_update_task_sender.sh
```

传参时直接转发给 CLI：

```bash
./scripts/amz_offers_update_task_sender.sh \
  -s ~/.em_celery/gcs-sa.json \
  -b 'redis://127.0.0.1:6379/0' \
  -q 20 -t 72
```

### 直接调用 CLI

```bash
uv run amz_offers_update_task_sender \
  -s /path/to/gcs-service-account.json \
  -b redis://127.0.0.1:6379/0
```

### CLI 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `-s`, `--gcs_service_account_path` | GCS service account JSON | **必填** |
| `-b`, `--broker_url` | Celery broker URL | `BROKER_URL` 环境变量 |
| `-q`, `--qps` | 发送速率（messages/s） | `20` |
| `-t`, `--ttl` | offer 过期小时数；TTL 内已有 offer 的 ASIN 跳过 | `7`（各 marketplace 默认 24h） |
| `-f`, `--force` | 忽略队列深度上限，强制入队 | `false` |

## 数据源

### Cart 种子（GCS）

```
gs://em-bucket/em-analytics/carts/sources/AMZ_{MARKETPLACE}.txt
```

本地缓存：`tmp/gcs/carts/amz_{marketplace}.txt`

### Ads 种子（GCS）

```
gs://em-bucket/em-analytics/sources/AMZ_{MARKETPLACE}.txt
```

本地缓存：`tmp/gcs/ads/amz_{marketplace}.txt`

### Catalog（PostgreSQL）

```sql
SELECT source, source_product_id
FROM product_sources
WHERE source = 'AMZ_{MARKETPLACE}'
```

### 种子文件格式

Tab 分隔，每行：

```
{key}\t{json_product}
```

从 JSON 中提取 `source_product_id` 或 `asin` 作为 ASIN。无效 ASIN 自动跳过。

若 cart 或 ads 种子文件不存在，对应 tier 会跳过并记录 warning，不影响其他 tier 继续运行。

## 入队逻辑

1. **队列深度检查**：若 `SpapiItemOffersUpdate_{MP}` 全部 priority 子队列合计深度 > 5000 且未加 `-f`，本次 marketplace 跳过入队
2. **Offer 过滤**（默认）：查询 ES `lowest_offer_listings`，跳过 TTL 内仍有效的 ASIN；`-f` 时跳过此检查，全部入队
3. **分批发送**：每批最多 20 个 ASIN，按 `-q` 限速
4. **Celery 任务**：`spapi_update_item_offers(marketplace, asins, condition="new")`

## 运行统计

每个 marketplace 运行结束后写入 ES 索引：

```
amz_offers_update_metrics_{marketplace}
```

主要字段：

| 字段 | 说明 |
|------|------|
| `seed_cnt` | 读取到的有效 ASIN 总数 |
| `queued_cnt` | 实际入队数 |
| `fresh_cnt` | 因 offer 仍有效而跳过数 |
| `queue_cnt_before` | 运行前队列深度 |
| `queue_full` | 是否因队列过满而跳过 |
| `tier_stats` | 各 tier 独立统计（seed / queued / fresh / dedup） |
| `status` | `finished` / `skipped` / `failed` |

日志文件：`amz_offers_update_task_sender.log`

## 开发

```bash
uv sync --group dev
uv run pytest
```

## 与 em-spapi-celery 的关系

- 本项目是**独立的 seed → 入队**工具，不包含 worker 逻辑
- 使用 `em-spapi-celery` 的 `dispatch_task()` 与 Redis 优先级子队列（`:0` … `:9`）
- 队列深度统计与清空覆盖全部 priority 子队列
- Worker 按 priority 0 → 9 顺序消费，cart 任务始终优先于 ads 和 catalog
