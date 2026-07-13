# -*- coding: utf-8 -*-

import datetime
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
    MissingOfferTtlConfigError,
    load_marketplace_tier_ttls,
)
from carts_amz_offers.sender import (
    CartAmzOffersUpdateTaskSender,
    clear_marketplace_offer_queue,
)

BUCKET_NAME = "em-bucket"
CART_GCS_PREFIX = "em-analytics/carts"
ADS_GCS_PREFIX = "em-analytics"
CART_SEED_BLOB_TEMPLATE = "em-analytics/carts/sources/AMZ_{}.txt"
ADS_SEED_BLOB_TEMPLATE = "em-analytics/sources/AMZ_{}.txt"
LOCAL_CART_SEED_TEMPLATE = "tmp/gcs/carts/amz_{}.txt"
LOCAL_ADS_SEED_TEMPLATE = "tmp/gcs/ads/amz_{}.txt"
METRICS_SOURCE = "amz_offers_update"
METRICS_INDEX_TEMPLATE = "amz_offers_update_metrics_{}"

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

_MARKETPLACE_BY_KEY = {mp.upper(): mp for mp in MARKETPLACES}


def resolve_marketplaces(selected):
    """Normalize marketplace CLI args; default to all when empty.

    Accepts repeated flags and/or comma/space-separated values, e.g.
    ``("US", "ca")``, ``("US,CA",)``, ``("us ca",)``.
    """
    if not selected:
        return list(MARKETPLACES)

    resolved = []
    seen = set()
    for raw in selected:
        for part in str(raw).replace(",", " ").split():
            key = part.strip().upper()
            if key not in _MARKETPLACE_BY_KEY:
                raise click.ClickException(
                    "Unknown marketplace {!r}. Choose from: {}".format(
                        part, ", ".join(MARKETPLACES)
                    )
                )
            if key not in seen:
                seen.add(key)
                resolved.append(_MARKETPLACE_BY_KEY[key])
    if not resolved:
        raise click.ClickException(
            "No marketplaces selected. Choose from: {}".format(", ".join(MARKETPLACES))
        )
    return resolved


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


def _sender_log_basename(marketplaces):
    """One marketplace → dedicated log; otherwise shared sender log."""
    if len(marketplaces) == 1:
        return "amz_offers_update_task_sender_{}.log".format(
            marketplaces[0].lower()
        )
    return "amz_offers_update_task_sender.log"


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
    "-f",
    "--force",
    is_flag=True,
    help="Force enqueue even when queue depth exceeds the limit.",
)
@click.option(
    "-m",
    "--marketplace",
    "marketplaces",
    multiple=True,
    help=(
        "Marketplace(s) to process (e.g. -m US -m CA or -m US,CA). "
        "Default: all ({}).".format(", ".join(MARKETPLACES))
    ),
)
def feed_seeds(
    gcs_service_account_path,
    broker_url,
    qps,
    force=False,
    marketplaces=(),
):
    broker_url = normalize_broker(broker_url)
    marketplaces = resolve_marketplaces(marketplaces)
    setup_cli_logging(
        "carts_amz_offers.cli",
        _sender_log_basename(marketplaces),
    )
    logger.info(
        "[AmzOffersUpdate] Marketplaces: %s",
        ", ".join(marketplaces),
    )
    config = get_config()
    pg_config = config.get("pg_db")
    if not pg_config:
        raise click.ClickException(
            "Missing [pg_db] section in em-spapi-celery config (EM_SPAPI_CELERY_CONFIG)."
        )

    ttl_by_marketplace = {}
    ttl_errors = []
    for mp in marketplaces:
        try:
            ttl_by_marketplace[mp] = load_marketplace_tier_ttls(config, mp)
        except MissingOfferTtlConfigError as exc:
            ttl_errors.append(str(exc))
    if ttl_errors:
        raise click.ClickException(
            "Offer TTL config incomplete:\n  - {}".format("\n  - ".join(ttl_errors))
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
            "ttl_by_tier": ttl_by_marketplace[mp],
        }
        for mp in marketplaces
    }

    # Drop leftover tasks from prior runs so this run does not stack duplicates.
    for marketplace in marketplaces:
        depth_before = clear_marketplace_offer_queue(broker_url, marketplace)
        mp_state[marketplace]["stats"].queue_cnt_before = depth_before

    for tier_name in TIER_PHASES:
        logger.info("[AmzOffersUpdate] Starting tier phase: %s", tier_name)
        for marketplace in marketplaces:
            state = mp_state[marketplace]
            if state["start_time"] is None:
                state["start_time"] = datetime.datetime.now()
            try:
                data_source = _load_tier_source(
                    tier_name, marketplace, cart_gcs, ads_gcs, catalog_source
                )
                tier_ttl = state["ttl_by_tier"][tier_name]
                logger.info(
                    "[AmzOffersUpdate] %s tier=%s ttl=%sh",
                    marketplace,
                    tier_name,
                    tier_ttl,
                )
                _run_marketplace_tier(
                    marketplace,
                    tier_name,
                    data_source,
                    broker_url,
                    qps,
                    tier_ttl,
                    force,
                    state["seen_asins"],
                    state["stats"],
                )
            except Exception as e:
                logger.exception(e)
                state["error"] = str(e)
        logger.info("[AmzOffersUpdate] Finished tier phase: %s", tier_name)

    end_time = datetime.datetime.now()
    for marketplace in marketplaces:
        state = mp_state[marketplace]
        save_offers_update_metrics(
            platform="amz",
            marketplace=marketplace,
            stats=state["stats"],
            start_time=state["start_time"] or end_time,
            end_time=end_time,
            ttl=state["ttl_by_tier"],
            error=state["error"],
            source=METRICS_SOURCE,
            index_name=METRICS_INDEX_TEMPLATE.format(marketplace.lower()),
        )


if __name__ == "__main__":
    feed_seeds()
