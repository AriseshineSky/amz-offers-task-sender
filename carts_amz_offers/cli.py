# -*- coding: utf-8 -*-

import datetime
from collections import defaultdict
from pathlib import Path

import click

from em_celery import get_config, logger
from em_celery.runtime import setup_cli_logging
from em_celery.tools._sender_common import broker_option, normalize_broker

from carts_amz_offers.data_source import (
    ProductSourcesPgDataSource,
    SeedFileDataSource,
)
from carts_amz_offers.gcs_helper import GCSHelper
from carts_amz_offers.offers_update_metrics import save_offers_update_metrics
from carts_amz_offers.offers_update_run_stats import OffersUpdateRunStats, TierRunStats
from carts_amz_offers.priority_tiers import (
    PRIORITY_BY_TIER,
    TIER_ADS,
    TIER_CART,
    TIER_CATALOG,
)
from carts_amz_offers.sender import CartAmzOffersUpdateTaskSender

BUCKET_NAME = "em-bucket"
CART_GCS_PREFIX = "em-analytics/carts"
ADS_GCS_PREFIX = "em-analytics"
CART_SEED_BLOB_TEMPLATE = "em-analytics/carts/sources/AMZ_{}.txt"
ADS_SEED_BLOB_TEMPLATE = "em-analytics/sources/AMZ_{}.txt"
LOCAL_CART_SEED_TEMPLATE = "tmp/gcs/carts/amz_{}.txt"
LOCAL_ADS_SEED_TEMPLATE = "tmp/gcs/ads/amz_{}.txt"
METRICS_SOURCE = "amz_offers_update"
METRICS_INDEX_TEMPLATE = "amz_offers_update_metrics_{}"

MARKETPLACE_TTL_HOURS = defaultdict(lambda: 24)

MARKETPLACES = [
    "US",
    "CA",
    "MX",
    "AE",
    "DE",
    "IN",
    "IT",
    "JP",
    "UK",
    "BR",
    "NL",
    "BE",
    "FR",
    "PL",
]

# Global phase order: all marketplaces finish cart before any ads, then catalog (PG).
TIER_PHASES = (TIER_CART, TIER_ADS, TIER_CATALOG)


def _download_seed(gcs_helper, blob_name, local_path):
    local_path = Path(local_path)
    gcs_helper.download_file(blob_name, local_path)
    if local_path.is_file():
        return SeedFileDataSource(local_path)
    return None


def _load_tier_source(tier_name, marketplace, cart_gcs, ads_gcs, catalog_source):
    marketplace_key = marketplace.lower()
    if tier_name == TIER_CART:
        blob = CART_SEED_BLOB_TEMPLATE.format(marketplace.upper())
        local = Path(LOCAL_CART_SEED_TEMPLATE.format(marketplace_key))
        return _download_seed(cart_gcs, blob, local)
    if tier_name == TIER_ADS:
        blob = ADS_SEED_BLOB_TEMPLATE.format(marketplace.upper())
        local = Path(LOCAL_ADS_SEED_TEMPLATE.format(marketplace_key))
        return _download_seed(ads_gcs, blob, local)
    if tier_name == TIER_CATALOG:
        return catalog_source
    raise ValueError("Unknown tier: {}".format(tier_name))


def _run_marketplace_tier(
    marketplace,
    tier_name,
    data_source,
    broker_url,
    qps,
    ttl,
    force,
    seen_asins,
    stats,
):
    if data_source is None:
        logger.warning(
            "[AmzOffersUpdate] Missing seed file for %s tier=%s",
            marketplace,
            tier_name,
        )
        tier_stats = TierRunStats(skipped_missing_file=True)
        stats.tier_stats[tier_name] = tier_stats
        stats.skipped_missing_file = True
        return

    sender = CartAmzOffersUpdateTaskSender(
        [(tier_name, data_source, PRIORITY_BY_TIER[tier_name])],
        broker_url,
        qps,
        marketplace,
        condition="new",
        ttl=ttl,
        force=force,
    )
    phase_stats = sender.run(seen_asins=seen_asins) or OffersUpdateRunStats()
    stats.merge(phase_stats)


@click.command("Send Amazon offers update tasks from cart, ads, and catalog sources")
@broker_option()
@click.option(
    "-s",
    "--gcs_service_account_path",
    type=str,
    required=True,
    help="Google Cloud Storage service account JSON path.",
)
@click.option(
    "-q",
    "--qps",
    type=float,
    default=20,
    help="Task send rate (messages per second).",
)
@click.option(
    "-t",
    "--ttl",
    type=int,
    default=7,
    help="Offer alive hours before re-queue (overridden per marketplace defaults).",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Force enqueue even when queue depth exceeds the limit.",
)
def feed_seeds(
    gcs_service_account_path,
    broker_url,
    qps,
    ttl=7,
    force=False,
):
    setup_cli_logging("carts_amz_offers.cli", "amz_offers_update_task_sender.log")
    broker_url = normalize_broker(broker_url)
    config = get_config()
    pg_config = config.get("pg_db")
    if not pg_config:
        raise click.ClickException(
            "Missing [pg_db] section in em-spapi-celery config (EM_SPAPI_CELERY_CONFIG)."
        )

    cart_gcs = GCSHelper(gcs_service_account_path, BUCKET_NAME, CART_GCS_PREFIX)
    ads_gcs = GCSHelper(gcs_service_account_path, BUCKET_NAME, ADS_GCS_PREFIX)
    catalog_source = ProductSourcesPgDataSource(pg_config)

    # Per-marketplace state survives across tier phases for ASIN dedup + metrics.
    mp_state = {
        mp: {
            "seen_asins": set(),
            "stats": OffersUpdateRunStats(),
            "start_time": None,
            "error": None,
            "ttl": MARKETPLACE_TTL_HOURS.get(mp, ttl),
        }
        for mp in MARKETPLACES
    }

    for tier_name in TIER_PHASES:
        logger.info("[AmzOffersUpdate] Starting tier phase: %s", tier_name)
        for marketplace in MARKETPLACES:
            state = mp_state[marketplace]
            if state["start_time"] is None:
                state["start_time"] = datetime.datetime.now()
            try:
                data_source = _load_tier_source(
                    tier_name, marketplace, cart_gcs, ads_gcs, catalog_source
                )
                _run_marketplace_tier(
                    marketplace,
                    tier_name,
                    data_source,
                    broker_url,
                    qps,
                    state["ttl"],
                    force,
                    state["seen_asins"],
                    state["stats"],
                )
            except Exception as e:
                logger.exception(e)
                state["error"] = str(e)
        logger.info("[AmzOffersUpdate] Finished tier phase: %s", tier_name)

    end_time = datetime.datetime.now()
    for marketplace in MARKETPLACES:
        state = mp_state[marketplace]
        save_offers_update_metrics(
            platform="amz",
            marketplace=marketplace,
            stats=state["stats"],
            start_time=state["start_time"] or end_time,
            end_time=end_time,
            ttl=state["ttl"],
            error=state["error"],
            source=METRICS_SOURCE,
            index_name=METRICS_INDEX_TEMPLATE.format(marketplace.lower()),
        )


if __name__ == "__main__":
    feed_seeds()
