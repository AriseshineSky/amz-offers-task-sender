# -*- coding: utf-8 -*-

import datetime
import random
import time

import dateutil.parser
import redis
from dropshipping.utils.utils import is_asin_valid

from em_celery import logger, get_offer_service
from em_celery.scheduling.priority import iter_redis_priority_queue_keys, redis_priority_queue_depth
from em_celery.scheduling.send import dispatch_task
from em_celery.tasks.spapi_update_item_offers_task import spapi_update_item_offers
from em_celery.tools._sender_common import broker_connection

from carts_amz_offers.offers_update_run_stats import OffersUpdateRunStats, TierRunStats


def clear_marketplace_offer_queue(broker_url, marketplace):
    """Delete all priority sub-queues for SpapiItemOffersUpdate_{MP}.

    Returns the queue depth before clearing.
    """
    queue = "SpapiItemOffersUpdate_{}".format(marketplace.upper())
    client = redis.Redis.from_url(broker_url, decode_responses=True)
    depth_before = 0
    try:
        depth_before = redis_priority_queue_depth(client, queue)
        for key in iter_redis_priority_queue_keys(queue):
            client.delete(key)
        logger.info(
            "[TasksProcessing] Cleared queue %s (depth_before=%s)",
            queue,
            depth_before,
        )
    except Exception as e:
        logger.exception(e)
    return depth_before


class CartAmzOffersUpdateTaskSender:
    """Read ASINs from tiered data sources and enqueue SP-API offer update tasks."""

    def __init__(
        self,
        tiers,
        broker_url,
        qps,
        marketplace,
        condition,
        ttl,
        force=False,
    ):
        self.tiers = tiers
        self.offer_service = get_offer_service()
        self.broker_url = broker_url
        self.marketplace = marketplace.lower()
        self.qps = qps
        self.condition = condition
        self.ttl = ttl
        self.force = force
        self.connection = broker_connection(broker_url)
        self.queue = "SpapiItemOffersUpdate_{}".format(marketplace.upper())
        self.offer_type = "lowest_offer_listings"
        self.last_send_time = None
        self.redis = self._redis_client(broker_url)
        self.max_tasks_cnt = 5000
        self.stats = OffersUpdateRunStats()

    def _redis_client(self, broker_url):
        # from_url correctly decodes percent-encoded passwords (urlparse does not).
        return redis.Redis.from_url(broker_url, decode_responses=True)

    def run(self, seen_asins=None):
        """Enqueue configured tiers.

        Args:
            seen_asins: Optional shared set for cross-tier / cross-phase ASIN
                dedup within one marketplace. Mutated in place when provided.
        """
        cnt = self.tasks_cnt()
        self.stats.queue_cnt_before = cnt
        if cnt > self.max_tasks_cnt and not self.force:
            logger.info("[TasksProcessing] queue depth %s exceeds limit %s", cnt, self.max_tasks_cnt)
            self.stats.queue_full = True
            return self.stats

        if seen_asins is None:
            seen_asins = set()
        for tier_name, data_source, priority in self.tiers:
            tier_stats = TierRunStats()
            if data_source is None:
                tier_stats.skipped_missing_file = True
                self.stats.tier_stats[tier_name] = tier_stats
                continue

            asins_buf = []
            batch_size = 1000
            for sp in data_source.get_amz_products(self.marketplace):
                asin = sp.get("source_product_id") or sp.get("asin")
                if not asin or not is_asin_valid(asin):
                    continue

                if asin in seen_asins:
                    tier_stats.dedup_cnt += 1
                    continue

                seen_asins.add(asin)
                tier_stats.seed_cnt += 1
                asins_buf.append(asin)
                if len(asins_buf) < batch_size:
                    continue

                queued_cnt = self.process_products(asins_buf, priority)
                tier_stats.queued_cnt += queued_cnt
                tier_stats.fresh_cnt += len(asins_buf) - queued_cnt
                asins_buf = []

            if asins_buf:
                queued_cnt = self.process_products(asins_buf, priority)
                tier_stats.queued_cnt += queued_cnt
                tier_stats.fresh_cnt += len(asins_buf) - queued_cnt

            self.stats.tier_stats[tier_name] = tier_stats
            self.stats.seed_cnt += tier_stats.seed_cnt
            self.stats.queued_cnt += tier_stats.queued_cnt
            self.stats.fresh_cnt += tier_stats.fresh_cnt

        return self.stats

    def process_products(self, asins, priority):
        if self.force:
            asins_without_offer = list(asins)
        else:
            now = datetime.datetime.utcnow()
            offer_expire_time = now - datetime.timedelta(hours=self.ttl)

            offers = {}
            result = self.offer_service.search_offers(
                self.offer_type, asins, self.marketplace, self.condition
            )
            while isinstance(result, dict) and "hits" in result:
                result = result["hits"]
            if isinstance(result, list):
                for offer in result:
                    if not offer:
                        continue
                    offers[offer["_source"]["asin"]] = offer["_source"]
            else:
                offers = result or {}

            asins_without_offer = {}
            for asin in asins:
                if asin not in offers or not offers[asin]:
                    asins_without_offer[asin] = None
                    continue

                offer = offers[asin]
                offer_time_s = offer.get("time")
                if not offer_time_s:
                    asins_without_offer[asin] = None
                    continue

                try:
                    offer_time = dateutil.parser.parse(offer_time_s)
                    if offer_time < offer_expire_time:
                        asins_without_offer[asin] = None
                        continue
                except Exception:
                    asins_without_offer[asin] = None
                    continue

                logger.debug("[ASINHasOffer] %s", asin)

            asins_without_offer = list(asins_without_offer.keys())

        random.shuffle(asins_without_offer)
        chunks = [
            asins_without_offer[i : i + 20]
            for i in range(0, len(asins_without_offer), 20)
        ]
        for chunk in chunks:
            if self.qps and self.last_send_time:
                wait_time = 1 / self.qps - (time.time() - self.last_send_time)
                if wait_time > 0:
                    logger.debug("Waiting %.3fs to send next message", wait_time)
                    time.sleep(wait_time)

            self.last_send_time = time.time()
            dispatch_task(
                spapi_update_item_offers,
                args=(self.marketplace, chunk, self.condition),
                queue=self.queue,
                connection=self.connection,
                priority=priority,
            )
            logger.debug(
                "Added spapi_update_item_offers(%s, %s, %s, priority=%s)",
                self.marketplace,
                chunk,
                self.condition,
                priority,
            )

        return len(asins_without_offer)

    def clear_tasks(self):
        try:
            for key in iter_redis_priority_queue_keys(self.queue):
                self.redis.delete(key)
        except Exception as e:
            logger.exception(e)

    def tasks_cnt(self):
        try:
            return redis_priority_queue_depth(self.redis, self.queue)
        except Exception:
            return 0
