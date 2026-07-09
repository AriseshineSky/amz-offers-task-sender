# -*- coding: utf-8 -*-

import json
import os
import re
from pathlib import Path

import psycopg

_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SeedFileDataSource:
    """Read Amazon product JSON records from tab-separated GCS seed files."""

    def __init__(self, file_path):
        self.file_path = Path(file_path)

    def get_amz_products(self, marketplace):
        if not self.file_path.is_file():
            return

        with open(self.file_path, encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                if not line.strip():
                    continue

                _, payload = line.strip().split("\t", 1)
                if not payload.strip():
                    continue

                try:
                    yield json.loads(payload.strip())
                except Exception:
                    continue


# Backward-compatible alias for cart seeds.
CartFileDataSource = SeedFileDataSource


def build_pg_dsn(pg_config):
    env_url = os.getenv("PG_DATABASE_URL") or os.getenv("DATABASE_URL")
    if env_url:
        return env_url

    if pg_config.get("url"):
        return pg_config["url"]

    host = pg_config.get("host", "localhost")
    port = pg_config.get("port", "5432")
    user = pg_config["user"]
    password = pg_config["password"]
    name = pg_config["name"]
    return "host={} port={} user={} password={} dbname={}".format(
        host, port, user, password, name
    )


class ProductSourcesPgDataSource:
    """Stream Amazon ASINs from PostgreSQL product_sources."""

    def __init__(self, pg_config, fetch_size=5000):
        self.pg_config = pg_config
        self.fetch_size = fetch_size
        table_name = pg_config.get("product_sources_table", "product_sources")
        if not _TABLE_NAME_RE.match(table_name):
            raise ValueError("Invalid product_sources_table: {}".format(table_name))
        self.table_name = table_name

    def get_amz_products(self, marketplace):
        source = "AMZ_{}".format(marketplace.upper())
        query = (
            "SELECT source, source_product_id "
            "FROM {} "
            "WHERE source = %s"
        ).format(self.table_name)

        with psycopg.connect(build_pg_dsn(self.pg_config)) as conn:
            with conn.cursor(name="product_sources_{}".format(source)) as cur:
                cur.itersize = self.fetch_size
                cur.execute(query, (source,))
                for row in cur:
                    yield {
                        "source": row[0],
                        "source_product_id": row[1],
                    }
