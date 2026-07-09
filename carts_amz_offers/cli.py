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
from carts_amz_offers.offers_update_run_stats import OffersUpdateRunStats
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


def _download_seed(gcs_helper, blob_name, local_path):
    local_path = Path(local_path)
    gcs_helper.download_file(blob_name, local_path)
    if local_path.is_file():
        return SeedFileDataSource(local_path)
    return None


def _build_tiers(marketplace, cart_gcs, ads_gcs, pg_config):
    marketplace_key = marketplace.lower()
    cart_blob = CART_SEED_BLOB_TEMPLATE.format(marketplace.upper())
    ads_blob = ADS_SEED_BLOB_TEMPLATE.format(marketplace.upper())
    cart_local = Path(LOCAL_CART_SEED_TEMPLATE.format(marketplace_key))
    ads_local = Path(LOCAL_ADS_SEED_TEMPLATE.format(marketplace_key))

    cart_source = _download_seed(cart_gcs, cart_blob, cart_local)
    ads_source = _download_seed(ads_gcs, ads_blob, ads_local)
    catalog_source = ProductSourcesPgDataSource(pg_config)

    return [
        (TIER_CART, cart_source, PRIORITY_BY_TIER[TIER_CART]),
        (TIER_ADS, ads_source, PRIORITY_BY_TIER[TIER_ADS]),
        (TIER_CATALOG, catalog_source, PRIORITY_BY_TIER[TIER_CATALOG]),
    ]


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
    setup_cli_logging("carts_amz_offers.cli", "carts_amz_offers_update_task_sender.log")
    broker_url = normalize_broker(broker_url)
    config = get_config()
    pg_config = config.get("pg_db")
    if not pg_config:
        raise click.ClickException(
            "Missing [pg_db] section in em-spapi-celery config (EM_SPAPI_CELERY_CONFIG)."
        )

    cart_gcs = GCSHelper(gcs_service_account_path, BUCKET_NAME, CART_GCS_PREFIX)
    ads_gcs = GCSHelper(gcs_service_account_path, BUCKET_NAME, ADS_GCS_PREFIX)

    for marketplace in MARKETPLACES:
        start_time = datetime.datetime.now()
        stats = OffersUpdateRunStats()
        error = None
        marketplace_ttl = MARKETPLACE_TTL_HOURS.get(marketplace, ttl)

        try:
            tiers = _build_tiers(marketplace, cart_gcs, ads_gcs, pg_config)
            missing_tiers = [
                tier_name
                for tier_name, data_source, _priority in tiers
                if data_source is None
            ]
            if missing_tiers:
                logger.warning(
                    "[AmzOffersUpdate] Missing seed files for %s tiers: %s",
                    marketplace,
                    ", ".join(missing_tiers),
                )

            sender = CartAmzOffersUpdateTaskSender(
                tiers,
                broker_url,
                qps,
                marketplace,
                condition="new",
                ttl=marketplace_ttl,
                force=force,
            )
            stats = sender.run() or stats
            stats.skipped_missing_file = bool(missing_tiers)
        except Exception as e:
            logger.exception(e)
            error = str(e)
        finally:
            end_time = datetime.datetime.now()
            save_offers_update_metrics(
                platform="amz",
                marketplace=marketplace,
                stats=stats,
                start_time=start_time,
                end_time=end_time,
                ttl=marketplace_ttl,
                error=error,
                source=METRICS_SOURCE,
                index_name=METRICS_INDEX_TEMPLATE.format(marketplace.lower()),
            )


if __name__ == "__main__":
    feed_seeds()
