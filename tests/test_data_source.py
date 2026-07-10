# -*- coding: utf-8 -*-

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from carts_amz_offers.data_source import (
    CartFileDataSource,
    ProductSourcesPgDataSource,
    SeedFileDataSource,
    build_pg_dsn,
)
from carts_amz_offers.offers_update_run_stats import OffersUpdateRunStats, TierRunStats
from carts_amz_offers.priority_tiers import (
    PRIORITY_BY_TIER,
    TIER_ADS,
    TIER_CART,
    TIER_CATALOG,
    MissingOfferTtlConfigError,
    load_marketplace_tier_ttls,
)
from carts_amz_offers.sender import CartAmzOffersUpdateTaskSender
from em_celery.scheduling.priority import PRIORITY_BULK, PRIORITY_CRITICAL, PRIORITY_NORMAL


def test_seed_file_data_source_reads_json_products(tmp_path):
    seed_file = tmp_path / "amz_us.txt"
    product = {"source_product_id": "B012345678", "title": "Test"}
    seed_file.write_text("key1\t{}\n".format(json.dumps(product)), encoding="utf-8")

    data_source = SeedFileDataSource(seed_file)
    products = list(data_source.get_amz_products("us"))

    assert len(products) == 1
    assert products[0]["source_product_id"] == "B012345678"


def test_cart_file_data_source_alias(tmp_path):
    seed_file = tmp_path / "amz_us.txt"
    product = {"source_product_id": "B012345678"}
    seed_file.write_text("key1\t{}\n".format(json.dumps(product)), encoding="utf-8")

    assert CartFileDataSource(seed_file) is not None


def test_build_pg_dsn_from_config():
    dsn = build_pg_dsn(
        {
            "host": "34.23.69.165",
            "port": "5432",
            "user": "product_sourcing",
            "password": "secret",
            "name": "em-catalog",
        }
    )
    assert "host=34.23.69.165" in dsn
    assert "dbname=em-catalog" in dsn


def test_product_sources_pg_data_source_yields_rows():
    rows = [("AMZ_US", "B012345678"), ("AMZ_US", "B087654321")]
    cursor = MagicMock()
    cursor.__iter__ = MagicMock(return_value=iter(rows))
    cursor_cm = MagicMock()
    cursor_cm.__enter__ = MagicMock(return_value=cursor)
    cursor_cm.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor_cm
    conn_cm = MagicMock()
    conn_cm.__enter__ = MagicMock(return_value=conn)
    conn_cm.__exit__ = MagicMock(return_value=False)

    data_source = ProductSourcesPgDataSource(
        {
            "host": "localhost",
            "user": "u",
            "password": "p",
            "name": "db",
            "product_sources_table": "product_sources",
        }
    )

    with patch("carts_amz_offers.data_source.psycopg.connect", return_value=conn_cm):
        products = list(data_source.get_amz_products("us"))

    assert products == [
        {"source": "AMZ_US", "source_product_id": "B012345678"},
        {"source": "AMZ_US", "source_product_id": "B087654321"},
    ]
    cursor.execute.assert_called_once()
    assert cursor.execute.call_args.args[1] == ("AMZ_US",)


def test_priority_tiers_order():
    assert PRIORITY_BY_TIER[TIER_CART] == PRIORITY_CRITICAL
    assert PRIORITY_BY_TIER[TIER_ADS] == PRIORITY_NORMAL
    assert PRIORITY_BY_TIER[TIER_CATALOG] == PRIORITY_BULK
    assert PRIORITY_BY_TIER[TIER_CART] < PRIORITY_BY_TIER[TIER_ADS] < PRIORITY_BY_TIER[TIER_CATALOG]


def test_load_marketplace_tier_ttls_from_offer_filter():
    config = {
        "amz.offer.filter.ae": {
            "expire_hour": "120",
            "cart_expire_hour": "6",
            "ads_expire_hour": "12",
        }
    }
    ttls = load_marketplace_tier_ttls(config, "AE")
    assert ttls == {TIER_CART: 6, TIER_ADS: 12, TIER_CATALOG: 120}


def test_load_marketplace_tier_ttls_requires_cart_ads_keys():
    config = {"amz.offer.filter.us": {"expire_hour": "180"}}
    try:
        load_marketplace_tier_ttls(config, "us")
        assert False, "expected MissingOfferTtlConfigError"
    except MissingOfferTtlConfigError as exc:
        assert "cart_expire_hour" in str(exc)


def test_load_marketplace_tier_ttls_requires_section():
    try:
        load_marketplace_tier_ttls({}, "FR")
        assert False, "expected MissingOfferTtlConfigError"
    except MissingOfferTtlConfigError as exc:
        assert "amz.offer.filter.fr" in str(exc)


def test_clear_marketplace_offer_queue_deletes_priority_keys():
    from carts_amz_offers.sender import clear_marketplace_offer_queue

    client = MagicMock()
    client.llen.side_effect = [3, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    with patch("carts_amz_offers.sender.redis.Redis.from_url", return_value=client):
        depth = clear_marketplace_offer_queue("redis://127.0.0.1:6379/0", "us")

    assert depth == 3
    deleted = {call.args[0] for call in client.delete.call_args_list}
    assert "SpapiItemOffersUpdate_US" in deleted
    assert "SpapiItemOffersUpdate_US:5" in deleted
    assert "SpapiItemOffersUpdate_US:9" in deleted


def test_sender_deduplicates_lower_priority_tiers():
    cart_source = MagicMock()
    cart_source.get_amz_products.return_value = [
        {"source_product_id": "B012345678"},
    ]
    ads_source = MagicMock()
    ads_source.get_amz_products.return_value = [
        {"source_product_id": "B012345678"},
        {"source_product_id": "B087654321"},
    ]
    catalog_source = MagicMock()
    catalog_source.get_amz_products.return_value = [
        {"source_product_id": "B087654321"},
        {"source_product_id": "B099999999"},
    ]

    tiers = [
        ("cart", cart_source, PRIORITY_CRITICAL),
        ("ads", ads_source, PRIORITY_NORMAL),
        ("catalog", catalog_source, PRIORITY_BULK),
    ]

    sender = CartAmzOffersUpdateTaskSender(
        tiers,
        broker_url="redis://127.0.0.1:6379/0",
        qps=0,
        marketplace="us",
        condition="new",
        ttl=24,
        force=True,
    )
    sender.process_products = MagicMock(side_effect=lambda asins, priority: len(asins))
    sender.tasks_cnt = MagicMock(return_value=0)

    stats = sender.run()

    assert stats.tier_stats["cart"].seed_cnt == 1
    assert stats.tier_stats["ads"].seed_cnt == 1
    assert stats.tier_stats["ads"].dedup_cnt == 1
    assert stats.tier_stats["catalog"].seed_cnt == 1
    assert stats.tier_stats["catalog"].dedup_cnt == 1
    assert sender.process_products.call_args_list[0].args[1] == PRIORITY_CRITICAL
    assert sender.process_products.call_args_list[1].args[1] == PRIORITY_NORMAL
    assert sender.process_products.call_args_list[2].args[1] == PRIORITY_BULK


def test_sender_deduplicates_across_phased_runs():
    """CLI runs cart → ads → catalog as separate phases with a shared seen set."""
    seen_asins = set()
    total = OffersUpdateRunStats()

    cart_source = MagicMock()
    cart_source.get_amz_products.return_value = [
        {"source_product_id": "B012345678"},
    ]
    ads_source = MagicMock()
    ads_source.get_amz_products.return_value = [
        {"source_product_id": "B012345678"},
        {"source_product_id": "B087654321"},
    ]

    for tier_name, source, priority in [
        ("cart", cart_source, PRIORITY_CRITICAL),
        ("ads", ads_source, PRIORITY_NORMAL),
    ]:
        sender = CartAmzOffersUpdateTaskSender(
            [(tier_name, source, priority)],
            broker_url="redis://127.0.0.1:6379/0",
            qps=0,
            marketplace="us",
            condition="new",
            ttl=24,
            force=True,
        )
        sender.process_products = MagicMock(side_effect=lambda asins, p: len(asins))
        sender.tasks_cnt = MagicMock(return_value=0)
        total.merge(sender.run(seen_asins=seen_asins))

    assert total.tier_stats["cart"].seed_cnt == 1
    assert total.tier_stats["ads"].seed_cnt == 1
    assert total.tier_stats["ads"].dedup_cnt == 1
    assert seen_asins == {"B012345678", "B087654321"}


def test_offers_update_run_stats_serializes_tier_stats():
    stats = OffersUpdateRunStats(
        tier_stats={
            "cart": TierRunStats(seed_cnt=10, queued_cnt=3),
        }
    )
    payload = stats.to_dict()
    assert payload["tier_stats"]["cart"]["seed_cnt"] == 10


def test_tier_phases_order():
    from carts_amz_offers.cli import TIER_PHASES

    assert TIER_PHASES == (TIER_CART, TIER_ADS, TIER_CATALOG)
