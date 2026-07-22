# amz-offers-task-sender

从 **cart / ads / catalog** 三类数据源（或本地 Amazon 链接文件）读取 ASIN，经 [em-spapi-celery](../em-spapi-celery) 的 `dispatch_task` 写入 Celery 队列。Worker 消费在 `em-spapi-celery`；本仓库只做 **producer（入队）**。

## 功能概览

一次运行按 **tier 全局阶段** 处理 15 个 marketplace（US、CA、MX、AE、DE、IN、IT、JP、UK、BR、NL、BE、FR、PL、TR）：

1. **cart 阶段**：对所有卖场从 GCS 下载 cart 种子并入队
2. **ads 阶段**：对所有卖场从 GCS 下载 ads 种子并入队
3. **catalog 阶段**：对所有卖场从 PostgreSQL `product_sources` 流式读取并入队
4. 各阶段内查询 Elasticsearch 已有 offer，跳过 TTL 内仍有效的 ASIN；每批最多 20 个入队
5. 全部阶段结束后，按卖场将运行统计写入 Elasticsearch metrics 索引

## 架构

```
Phase 1 (cart, all MPs)  →  Phase 2 (ads, all MPs)  →  Phase 3 (catalog/PG, all MPs)
         │                            │                            │
         └────────────────────────────┼────────────────────────────┘
                                      ▼
                         amz_offers_update_task_sender
                                      │
                     ┌────────────────┼────────────────┐
                     ▼                ▼                ▼
                Offer ES 查询    Redis 去重 / TTL 过滤    dispatch_task()
                                      │
                                      ▼
                     SpapiItemOffersUpdate_{MP}[:0..9]
```

### 优先级 tier

优先级由数据源决定，无需手动指定 `-p`：

| Tier | 数据源 | 优先级 | 含义 | Redis 子队列示例（US） |
|------|--------|--------|------|------------------------|
| **cart** | GCS 购物车分析种子 | **3** | 高于 ads，次于 critical/high | `SpapiItemOffersUpdate_US:3` |
| **ads** | GCS 广告种子 | **5** (normal) | 中等 | `SpapiItemOffersUpdate_US:5` |
| **catalog** | PostgreSQL `product_sources` | **8** | 全量补刷（低于 normal，高于 bulk=9） | `SpapiItemOffersUpdate_US:8` |

处理顺序固定为全局 **cart（全卖场）→ ads（全卖场）→ catalog/PG（全卖场）**。同一卖场内若 ASIN 在多个 tier 出现，通过 Redis SET（`amz_offers_update:seen:{mp}`）去重：高优先级 tier 先 claim，后续 tier 跳过。每次运行开始清空队列时一并删除该 SET。

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

含特殊字符的 Redis 密码请用**单引号**导出，或对密码做 URL 编码（如 `$` → `%24`、`^` → `%5E`、`*` → `%2A`）。脚本不会通过 `-b` 传 broker URL，而是 `export BROKER_URL` 后由 Python 从环境变量读取，避免 bash 二次解析密码中的 `$`、`"`、`` ` `` 等字符。

> 注意：`redis://pw@host/1` 会被解析成 **username=`pw`**（无密码），正确写法是 `redis://:pw@host/1`。

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

示例：

```bash
GCS_SA=~/.em_celery/gcs-sa.json \
BROKER_URL='redis://127.0.0.1:6379/0' \
QPS=10 \
./scripts/amz_offers_update_task_sender.sh
```

传参时直接转发给 CLI：

```bash
./scripts/amz_offers_update_task_sender.sh \
  -s ~/.em_celery/gcs-sa.json \
  -q 20 \
  -m US
```

### 紧急入队（Amazon URL / ASIN 文件 → priority 0）

从本地文件读取 Amazon 链接或裸 ASIN，解析卖场后写入 **priority 0（critical）** 队列——这是取价队列里**最高优先级**。**不会清空**现有队列（与上面的 cart/ads/catalog sender 不同）。

#### 步骤

1. 准备文件（每行一个 URL 或裸 ASIN）：

```bash
cat > /tmp/amz_urgent_links.txt <<'EOF'
https://www.amazon.com/dp/B00WW3LSUO
https://www.amazon.com/dp/B0CV63L8RS
B012345678
EOF
```

2. 配置环境并发送：

```bash
cd /path/to/amz-offers-task-sender
export EM_SPAPI_CELERY_CONFIG=~/.em_celery/config.ini
# 生产 worker 消费 Redis DB 1；BROKER_URL 与线上一致（见下方说明）
export BROKER_URL='redis://:URL_ENCODED_PASSWORD@34.133.1.247:6379/1'

./scripts/send_urgent_item_offers.sh /tmp/amz_urgent_links.txt
```

或直接 CLI：

```bash
uv run amz_offers_urgent_task_sender -p 0 -q 20 /tmp/amz_urgent_links.txt
```

成功时日志类似：

```text
[UrgentOfferSender] marketplace=us asins=4 priority=0 queue=SpapiItemOffersUpdate_US
[UrgentOfferSender] queued marketplace=us asins=['B00WW3LSUO', ...] priority=0
[UrgentOfferSender] done total_asins=4 priority=0
```

| 环境变量 / 参数 | 默认 | 说明 |
|-----------------|------|------|
| `BROKER_URL` / `-b` | `redis://127.0.0.1:6379/0` | Redis broker（生产须用 **`/1`**） |
| `QPS` / `-q` | `20` | 发送速率 |
| `PRIORITY` / `-p` | `0` | Celery 优先级（0 最高） |
| `-m` | `us` | 裸 ASIN 的默认卖场（URL 从主机名解析） |

#### 注意

- **卖场**：`amazon.com` → US，`amazon.co.uk` → UK 等；裸 ASIN 才用 `-m`（默认 `us`）。
- **Broker DB**：线上 offer worker 连 Redis **`/1`**。本地 `~/.em_celery/config.ini` 的 `[broker_url] amz` 可能仍是 `/0`；紧急入队请用与线上一致的 `BROKER_URL`（可参考 `~/.em_celery/amz_offers_sender.env` 或生产机 `/home/Admin/.em_celery/amz_offers_sender.env`）。
- **密码**：含 `$`、`^`、`*` 等字符时用单引号，或 URL 编码（`$`→`%24`，`^`→`%5E`，`*`→`%2A`）；格式为 `redis://:password@host:6379/1`（注意密码前的冒号）。
- **与定时 sender**：本命令不 clear 队列；但定时 `amz_offers_update_task_sender` 启动时会清空该卖场全部 priority 子队列，若紧接着跑 cron，刚入的 priority 0 任务可能被清掉。

### 直接调用 CLI

```bash
uv run amz_offers_update_task_sender \
  -s /path/to/gcs-service-account.json \
  -b 'redis://127.0.0.1:6379/0'
```

### CLI 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `-s`, `--gcs_service_account_path` | GCS service account JSON | **必填** |
| `-b`, `--broker_url` | Celery broker URL | `BROKER_URL` 环境变量 |
| `-q`, `--qps` | 发送速率（messages/s） | `20` |
| `-f`, `--force` | 忽略队列深度上限，强制入队 | `false` |
| `-m`, `--marketplace` | 指定卖场（可重复，或逗号分隔，如 `-m US -m CA` / `-m US,CA`） | 全部 15 个卖场 |

只跑指定卖场示例：

```bash
./scripts/amz_offers_update_task_sender.sh -m US
./scripts/amz_offers_update_task_sender.sh -m US -m CA -m DE
uv run amz_offers_update_task_sender -s ~/.em_celery/gcs-sa.json -m US,UK
```

### TTL（按卖场 × tier）

Offer 是否仍有效由 ES 中 offer 时间与 TTL（小时）比较决定。**只**从 `EM_SPAPI_CELERY_CONFIG`（如 `~/.em_celery/config.ini`）的 `[amz.offer.filter.{mp}]` 读取，缺 section 或缺任一键则启动失败：

| 配置键 | 对应 tier |
|--------|-----------|
| `cart_expire_hour` | **cart**（priority 3） |
| `ads_expire_hour` | **ads**（normal / 5） |
| `expire_hour` | **catalog**（priority 8） |

示例：

```ini
[amz.offer.filter.ae]
rating = 0
feedback = 0
domestic = True
shipping_time = 7
expire_hour = 120
cart_expire_hour = 24
ads_expire_hour = 48
```

每个 `MARKETPLACES` 中的卖场都必须有完整的上述三项配置。

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

1. **清空队列与去重 SET**：每次运行开始时，先清空各卖场 `SpapiItemOffersUpdate_{MP}` 全部 priority 子队列，并删除 `amz_offers_update:seen:{mp}`，避免与上次残留任务/ASIN 重复
2. **跨 tier Redis 去重**：按 cart → ads → catalog 顺序用 Redis `SADD` claim ASIN；已在高优先级（如 cart）出现的 ASIN 不会再入 ads / catalog 队列
3. **队列深度检查**：若入队前深度仍 > 5000 且未加 `-f`，本次 marketplace 跳过入队（清空后通常为 0）
4. **Offer 过滤**（默认）：查询 ES `lowest_offer_listings`，跳过 TTL 内仍有效的 ASIN；`-f` 时跳过此检查，全部入队
5. **分批发送**：每批最多 20 个 ASIN，按 `-q` 限速
6. **Celery 任务**：`spapi_update_item_offers(marketplace, asins, condition="new")`

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
| `queue_cnt_before` | 清空前队列深度 |
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
- Worker 按 priority 0 → 9 顺序消费，cart(3) 优先于 ads(5) 和 catalog(8)
