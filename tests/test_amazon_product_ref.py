# -*- coding: utf-8 -*-

from carts_amz_offers.amazon_product_ref import (
    asin_from_text,
    marketplace_from_host,
    parse_product_ref,
)


def test_marketplace_from_host():
    assert marketplace_from_host("www.amazon.com") == "us"
    assert marketplace_from_host("amazon.co.uk") == "uk"
    assert marketplace_from_host("www.amazon.com.mx") == "mx"
    assert marketplace_from_host("smile.amazon.de") == "de"
    assert marketplace_from_host("example.com") is None


def test_asin_from_url_and_bare():
    assert asin_from_text("https://www.amazon.com/dp/B00WW3LSUO") == "B00WW3LSUO"
    assert (
        asin_from_text("https://www.amazon.com/Some-Title/dp/B0CV63L8RS/ref=sr_1_1")
        == "B0CV63L8RS"
    )
    assert asin_from_text("B00WW3LSUO") == "B00WW3LSUO"
    assert asin_from_text("not-an-asin") is None


def test_parse_product_ref_url_and_bare():
    assert parse_product_ref("https://www.amazon.com/dp/B00WW3LSUO") == (
        "us",
        "B00WW3LSUO",
    )
    assert parse_product_ref("https://www.amazon.co.uk/dp/B0CV63L8RS") == (
        "uk",
        "B0CV63L8RS",
    )
    assert parse_product_ref("B00WW3LSUO", default_marketplace="us") == (
        "us",
        "B00WW3LSUO",
    )
    assert parse_product_ref("B00WW3LSUO") is None
    assert parse_product_ref("# comment") is None
