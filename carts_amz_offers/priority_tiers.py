# -*- coding: utf-8 -*-

from em_celery.scheduling.priority import (
    PRIORITY_BULK,
    PRIORITY_CRITICAL,
    PRIORITY_NORMAL,
)

TIER_CART = "cart"
TIER_ADS = "ads"
TIER_CATALOG = "catalog"

PRIORITY_BY_TIER = {
    TIER_CART: PRIORITY_CRITICAL,
    TIER_ADS: PRIORITY_NORMAL,
    TIER_CATALOG: PRIORITY_BULK,
}
