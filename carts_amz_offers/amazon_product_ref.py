# -*- coding: utf-8 -*-
"""Parse Amazon product URLs / bare ASINs into (marketplace, asin)."""

from __future__ import annotations

import re
from urllib.parse import urlparse

from dropshipping.utils.utils import is_asin_valid

# Longest domains first so amazon.com.mx matches before amazon.com.
_HOST_TO_MARKETPLACE = (
    ("amazon.com.mx", "mx"),
    ("amazon.com.br", "br"),
    ("amazon.com.be", "be"),
    ("amazon.com.tr", "tr"),
    ("amazon.co.jp", "jp"),
    ("amazon.co.uk", "uk"),
    ("amazon.com", "us"),
    ("amazon.ca", "ca"),
    ("amazon.ae", "ae"),
    ("amazon.de", "de"),
    ("amazon.in", "in"),
    ("amazon.it", "it"),
    ("amazon.nl", "nl"),
    ("amazon.fr", "fr"),
    ("amazon.pl", "pl"),
    ("amazon.es", "es"),
    ("amazon.com.au", "au"),
    ("amazon.sg", "sg"),
    ("amazon.se", "se"),
)

_ASIN_IN_PATH_RE = re.compile(
    r"/(?:dp|gp/product|gp/aw/d|product|asin)/([A-Za-z0-9]{10})(?:[/?]|$)",
    re.IGNORECASE,
)
_ASIN_QUERY_RE = re.compile(r"[?&]asin=([A-Za-z0-9]{10})(?:&|$)", re.IGNORECASE)


def marketplace_from_host(host: str) -> str | None:
    host = (host or "").lower().removeprefix("www.")
    if not host:
        return None
    for domain, marketplace in _HOST_TO_MARKETPLACE:
        if host == domain or host.endswith("." + domain):
            return marketplace
    return None


def asin_from_text(text: str) -> str | None:
    text = (text or "").strip()
    if not text:
        return None
    match = _ASIN_IN_PATH_RE.search(text)
    if match:
        asin = match.group(1).upper()
        return asin if is_asin_valid(asin) else None
    match = _ASIN_QUERY_RE.search(text)
    if match:
        asin = match.group(1).upper()
        return asin if is_asin_valid(asin) else None
    asin = text.upper()
    return asin if is_asin_valid(asin) else None


def parse_product_ref(line: str, default_marketplace: str | None = None):
    """Return (marketplace, asin) or None when the line cannot be parsed.

    Accepts a full Amazon product URL or a bare ASIN. For bare ASINs,
    ``default_marketplace`` is required.
    """
    line = (line or "").strip()
    if not line or line.startswith("#"):
        return None

    if "://" in line or line.lower().startswith("www.") or "amazon." in line.lower():
        url = line if "://" in line else "https://" + line
        parsed = urlparse(url)
        marketplace = marketplace_from_host(parsed.netloc)
        asin = asin_from_text(
            parsed.path + (("?" + parsed.query) if parsed.query else "")
        )
        if not asin:
            asin = asin_from_text(line)
        if marketplace and asin:
            return marketplace, asin
        return None

    asin = asin_from_text(line)
    if not asin:
        return None
    marketplace = (default_marketplace or "").strip().lower() or None
    if not marketplace:
        return None
    return marketplace, asin


def load_product_refs(path: str, default_marketplace: str | None = None):
    """Load unique (marketplace, asin) pairs from a text file (URL or ASIN per line)."""
    refs = []
    seen = set()
    with open(path, encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            parsed = parse_product_ref(line, default_marketplace=default_marketplace)
            if not parsed:
                continue
            if parsed in seen:
                continue
            seen.add(parsed)
            refs.append(parsed)
    return refs
