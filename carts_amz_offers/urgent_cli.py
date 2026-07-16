# -*- coding: utf-8 -*-
"""Enqueue urgent offer updates from Amazon URLs / ASINs at priority 0."""

import time
from collections import defaultdict
from pathlib import Path

import click

from em_celery import logger
from em_celery.runtime import setup_cli_logging
from em_celery.scheduling.send import PRIORITY_CRITICAL, dispatch_task
from em_celery.tasks.spapi_update_item_offers_task import spapi_update_item_offers
from em_celery.tools._sender_common import broker_connection, broker_option, normalize_broker

from carts_amz_offers.amazon_product_ref import load_product_refs


@click.command("Send urgent Amazon offer update tasks (priority 0) from a links file")
@broker_option()
@click.option(
    "-m",
    "--marketplace",
    type=str,
    default="us",
    show_default=True,
    help="Default marketplace for bare ASINs (ignored when line is an Amazon URL).",
)
@click.option("-c", "--condition", type=str, default="new", show_default=True)
@click.option(
    "-q",
    "--qps",
    type=float,
    default=20,
    show_default=True,
    help="Task send rate (messages per second).",
)
@click.option(
    "-p",
    "--priority",
    type=int,
    default=PRIORITY_CRITICAL,
    show_default=True,
    help="Celery Redis priority (0=highest/critical … 9=bulk).",
)
@click.argument("links_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def send_urgent_offers(
    links_path,
    broker_url,
    marketplace="us",
    condition="new",
    qps=20,
    priority=PRIORITY_CRITICAL,
):
    """Enqueue offer updates without clearing queues or reading GCS/PG seeds.

    Each line in LINKS_PATH may be an Amazon product URL or a bare ASIN.
    URLs determine marketplace from the host; bare ASINs use ``-m``.
    Does not clear existing Redis queues (unlike the cart/ads/catalog sender).
    """
    setup_cli_logging(
        "carts_amz_offers.urgent_cli",
        "amz_offers_urgent_task_sender.log",
    )
    broker_url = normalize_broker(broker_url)
    refs = load_product_refs(str(links_path), default_marketplace=marketplace)
    if not refs:
        raise click.ClickException(
            "No valid Amazon URLs/ASINs in {} "
            "(URLs need host+ASIN; bare ASINs need -m)".format(links_path)
        )

    by_marketplace = defaultdict(list)
    for mp, asin in refs:
        by_marketplace[mp].append(asin)

    connection = broker_connection(broker_url)
    last_send_time = None
    total = 0

    for mp in sorted(by_marketplace):
        asins = by_marketplace[mp]
        queue = "SpapiItemOffersUpdate_{}".format(mp.upper())
        logger.info(
            "[UrgentOfferSender] marketplace=%s asins=%s priority=%s queue=%s",
            mp,
            len(asins),
            priority,
            queue,
        )
        for i in range(0, len(asins), 20):
            chunk = asins[i : i + 20]
            if qps and last_send_time:
                wait_time = 1 / qps - (time.time() - last_send_time)
                if wait_time > 0:
                    time.sleep(wait_time)
            last_send_time = time.time()
            dispatch_task(
                spapi_update_item_offers,
                args=(mp, chunk, condition),
                queue=queue,
                connection=connection,
                priority=priority,
            )
            total += len(chunk)
            logger.info(
                "[UrgentOfferSender] queued marketplace=%s asins=%s priority=%s",
                mp,
                chunk,
                priority,
            )

    logger.info("[UrgentOfferSender] done total_asins=%s priority=%s", total, priority)


if __name__ == "__main__":
    send_urgent_offers()
