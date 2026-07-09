# -*- coding: utf-8 -*-

import datetime
import uuid

from em_celery import logger, get_product_service

from carts_amz_offers.offers_update_run_stats import OffersUpdateRunStats


def _normalize_stats(stats):
    if stats is None:
        return {}
    if isinstance(stats, OffersUpdateRunStats):
        return stats.to_dict()
    if isinstance(stats, dict):
        return stats
    if hasattr(stats, "to_dict"):
        return stats.to_dict()
    return {}


def save_offers_update_metrics(
    platform,
    marketplace,
    stats,
    start_time,
    end_time,
    ttl=None,
    error=None,
    source=None,
    index_name=None,
):
    """Persist a cart offer-update task sender run to Elasticsearch."""
    try:
        service = get_product_service()
        marketplace = marketplace.lower()
        platform = platform.lower()
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        started_at = (
            start_time.replace(tzinfo=datetime.timezone.utc)
            if start_time.tzinfo is None
            else start_time.astimezone(datetime.timezone.utc)
        )
        finished_at = (
            end_time.replace(tzinfo=datetime.timezone.utc)
            if end_time.tzinfo is None
            else end_time.astimezone(datetime.timezone.utc)
        )
        duration_ms = int((finished_at - started_at).total_seconds() * 1000)

        stats = _normalize_stats(stats)
        seed_cnt = int(stats.get("seed_cnt", 0) or 0)
        queued_cnt = int(stats.get("queued_cnt", stats.get("expired_cnt", 0)) or 0)
        fresh_cnt = int(stats.get("fresh_cnt", stats.get("alive_cnt", 0)) or 0)
        missing_cnt = int(stats.get("missing_cnt", 0) or 0)
        queue_full = bool(stats.get("queue_full", False))
        queue_cnt_before = int(stats.get("queue_cnt_before", 0) or 0)
        skipped_missing_file = bool(stats.get("skipped_missing_file", False))

        if error:
            status = "failed"
        elif queue_full:
            status = "skipped"
        elif skipped_missing_file:
            status = "skipped"
        else:
            status = "finished"

        source = source or "carts_{}_offers_update".format(platform)
        metric_doc = {
            "_id": "{}_{}_{}_{}".format(
                source,
                marketplace,
                now_utc.strftime("%Y%m%d%H%M%S"),
                uuid.uuid4().hex[:8],
            ),
            "source": source,
            "task_name": source,
            "platform": platform,
            "marketplace": marketplace,
            "timestamp": now_utc.isoformat(),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": duration_ms,
            "status": status,
            "seed_cnt": seed_cnt,
            "queued_cnt": queued_cnt,
            "fresh_cnt": fresh_cnt,
            "missing_cnt": missing_cnt,
            "queue_full": queue_full,
            "queue_cnt_before": queue_cnt_before,
            "skipped_missing_file": skipped_missing_file,
            "ttl_days": ttl,
            "error": error,
            "queue_rate_pct": round(queued_cnt * 100.0 / seed_cnt, 2) if seed_cnt else 0.0,
        }

        index_name = index_name or "carts_{}_offers_update_metrics_{}".format(
            platform, marketplace
        )
        service.save_products(index_name, [metric_doc])
    except Exception as e:
        logger.exception(e)
