# -*- coding: utf-8 -*-

from em_celery.scheduling.priority import PRIORITY_NORMAL

TIER_CART = "cart"
TIER_ADS = "ads"
TIER_CATALOG = "catalog"

ALL_TIERS = (TIER_CART, TIER_ADS, TIER_CATALOG)

# Redis Celery priority: lower number = higher priority (0 critical … 9 lowest).
# cart=3 (between high=2 and normal=5), ads=5, catalog=8 (above bulk=9).
PRIORITY_BY_TIER = {
    TIER_CART: 3,
    TIER_ADS: PRIORITY_NORMAL,
    TIER_CATALOG: 8,
}

# Required keys under [amz.offer.filter.{mp}].
CONFIG_KEY_EXPIRE_HOUR = "expire_hour"
CONFIG_KEY_CART_EXPIRE_HOUR = "cart_expire_hour"
CONFIG_KEY_ADS_EXPIRE_HOUR = "ads_expire_hour"

_TIER_CONFIG_KEYS = (
    (TIER_CART, CONFIG_KEY_CART_EXPIRE_HOUR),
    (TIER_ADS, CONFIG_KEY_ADS_EXPIRE_HOUR),
    (TIER_CATALOG, CONFIG_KEY_EXPIRE_HOUR),
)


class MissingOfferTtlConfigError(ValueError):
    """Raised when [amz.offer.filter.{mp}] is missing required TTL keys."""


def _offer_filter_section(config, marketplace):
    mp = (marketplace or "").lower()
    if not config:
        return None
    return config.get("amz.offer.filter.{}".format(mp)) or config.get(
        "amz.offer.filter"
    )


def _require_int(section, key, section_name):
    if section is None or key not in section or section.get(key) in (None, ""):
        raise MissingOfferTtlConfigError(
            "Missing required key '{}' in [{}]".format(key, section_name)
        )
    try:
        return int(section.get(key))
    except (TypeError, ValueError) as exc:
        raise MissingOfferTtlConfigError(
            "Invalid integer for '{}' in [{}]: {!r}".format(
                key, section_name, section.get(key)
            )
        ) from exc


def load_marketplace_tier_ttls(config, marketplace):
    """Load per-tier TTL hours from [amz.offer.filter.{mp}].

    Required keys (no program defaults):
      cart_expire_hour  → cart
      ads_expire_hour   → ads
      expire_hour       → catalog

    Raises MissingOfferTtlConfigError when the section or any key is missing.
    """
    mp = (marketplace or "").lower()
    section_name = "amz.offer.filter.{}".format(mp)
    section = _offer_filter_section(config, marketplace)
    if not section:
        raise MissingOfferTtlConfigError(
            "Missing config section [{}] (or [amz.offer.filter])".format(section_name)
        )

    # Prefer marketplace-specific section name in error messages.
    if config.get(section_name):
        reported_section = section_name
    else:
        reported_section = "amz.offer.filter"

    return {
        tier: _require_int(section, key, reported_section)
        for tier, key in _TIER_CONFIG_KEYS
    }
