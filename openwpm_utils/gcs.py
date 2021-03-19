from typing import List, Optional, Union

import gcsfs
from google.api_core.exceptions import NotFound
import jsbeautifier
import pyarrow.parquet as pq
import pyspark.sql.functions as F
from google.cloud import storage
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

from openwpm_utils.crawlhistory import get_worst_status_per_visit_id


class GCSDataset(object):
    def __init__(self, base_dir: str, bucket:Optional[str]="openwpm-data", **kwargs) -> None:
        """Helper class to load OpenWPM datasets from GCS using pandas

        This dataset wrapper is safe to use by spark worker processes, as it
        does not require the spark context.

        Parameters
        ----------
        base_dir
            Directory within the GCS bucket in which the dataset is saved.
        bucket
            The bucket name on GCS.
        **kwargs
            Passed on to GCSFS so you can customize it to your needs
        """
        self._kwargs = kwargs
        self._bucket = bucket
        self._base_dir = base_dir
        self._table_location_format_string = f"{bucket}/{base_dir}/visits/%s"
        self._content_key = f"{base_dir}/content/%s.gz"
        self._gcsfs = gcsfs.GCSFileSystem(**kwargs)

    def read_table(self, table_name, columns=None):
        """Read `table_name` from OpenWPM dataset into a pyspark dataframe.

        Parameters
        ----------
        table_name : string
            OpenWPM table to read
        columns : list of strings
            The set of columns to filter the parquet dataset by
        """
        return (
            pq.ParquetDataset(
                self._table_location_format_string % table_name,
                filesystem= self._gcsfs,
                metadata_nthreads=4,
            )
            .read(use_pandas_metadata=True, columns=columns)
            .to_pandas()
        )

    def collect_content(self, content_hash: str, beautify: bool=False) -> Optional[Union[bytes, str]]:
        """Collect content by directly connecting to GCS via google.cloud.storage"""
        storage_client = storage.Client()
        bucket = storage_client.bucket(self._bucket)


        blob = bucket.blob(self._content_key % content_hash)
        content: Union[bytes, str] = blob.download_as_bytes()

        if beautify:
            try:
                content = jsbeautifier.beautify(content)
            except IndexError:
                pass
        return content

class PySparkGCSDataset(GCSDataset):
    def __init__(self, spark_context: SparkContext, base_dir: str, bucket:str="openwpm-data", **kwargs) -> None:
        """Helper class to load OpenWPM datasets from GCS using PySpark

        Parameters
        ----------
        spark_context
            Spark context. In databricks, this is available via the `sc`
            variable.
        base_dir : string
            Directory within the bucket in which the dataset is saved.
        bucket : string, optional
            The bucket name on GCS. Defaults to `openwpm-data`.
        """
        super().__init__(base_dir, bucket, **kwargs)
        self._spark_context = spark_context
        self._sql_context = SQLContext(spark_context)
        self._table_location_format_string = f"gcs://{self._table_location_format_string}"
        self._incomplete_visit_ids = self.read_table(
            "incomplete_visits", mode="all"
        ).select("visit_id")
        crawl_history = self.read_table("crawl_history", mode="all")
        self._failed_visit_ids = (
            get_worst_status_per_visit_id(crawl_history)
            .where(F.col("worst_status") != "ok")
            .select("visit_id")
        )

    def read_table(
        self, table_name: str, columns: List[str] = None, mode: str = "successful"
    ):
        """Read `table_name` from OpenWPM dataset into a pyspark dataframe.

        Parameters
        ----------
        table_name : string
            OpenWPM table to read
        columns : list of strings
            The set of columns to filter the parquet dataset by
        mode : string
            The valid values are "successful", "failed", "all"
            Success is determined per visit_id. A visit_id is failed
            if one of it's commands failed or if it's in the interrupted table
        """
        table = self._sql_context.read.parquet(self._table_location_format_string % table_name)
        if columns is not None:
            table = table.select(columns)
        if mode == "all":
            return table
        if mode == "failed":
            return table.join(self._failed_visit_ids, "visit_id", how="inner").union(
                table.join(self._incomplete_visit_ids, "visit_id", how="inner")
            )
        if mode == "successful":
            return table.join(self._failed_visit_ids, "visit_id", how="leftanti").join(
                self._incomplete_visit_ids, "visit_id", how="leftanti"
            )
        else:
            raise AssertionError(
                f"Mode was ${mode},"
                "allowed modes are 'all', 'failed' and 'successful'"
            )
        return table
