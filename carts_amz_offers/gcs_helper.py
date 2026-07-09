# -*- coding: utf-8 -*-

import os
import time

from google.cloud import storage
from google.api_core.retry import Retry

from em_celery import logger


class GCSHelper:
    def __init__(self, service_account_path, bucket_name, prefix):
        if not service_account_path:
            raise ValueError("service_account_path is required")

        service_account_path = os.path.abspath(os.path.expanduser(service_account_path))
        if not os.path.isfile(service_account_path):
            raise ValueError(
                "service_account_path {} can not be found".format(service_account_path)
            )

        self.client = storage.Client.from_service_account_json(service_account_path)
        self.bucket = self.client.bucket(bucket_name)
        self.retry = Retry(initial=1.0, maximum=10.0, multiplier=2.0, deadline=30.0)
        self.prefix = prefix

    def download_file(self, blob_name, local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        retries = 3
        for attempt in range(retries):
            try:
                blob = self.bucket.blob(blob_name)
                blob.download_to_filename(
                    local_path,
                    retry=self.retry,
                    timeout=60,
                )
                blob.reload()
                if int(blob.size) != os.path.getsize(local_path):
                    raise ValueError("size mismatch")

                logger.debug("DOWNLOAD OK: %s", blob_name)
                return True
            except Exception as e:
                if attempt == retries - 1:
                    logger.debug("DOWNLOAD FAILED: %s - %s", blob_name, e)
                    return False

                time.sleep(2 ** attempt)

        return False
