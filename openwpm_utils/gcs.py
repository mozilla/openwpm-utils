from typing import Any, Dict, List, Optional, Union

import gcsfs
import jsbeautifier
import pyarrow.parquet as pq
import pyspark.sql.functions as F
from google.cloud import storage
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from openwpm_utils.dataquality import TableFilter


class GCSDataset(object):
    def __init__(
        self,
        base_dir: str,
        bucket: Optional[str] = "openwpm-data",
        **kwargs: Dict[Any, Any],
    ) -> None:
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

    def read_table(self, table_name: str, columns: List[str] = None) -> PandasDataFrame:
        """Read `table_name` from OpenWPM dataset into a pandas dataframe.

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
                filesystem=self._gcsfs,
                metadata_nthreads=4,
            )
            .read(use_pandas_metadata=True, columns=columns)
            .to_pandas()
        )

    def collect_content(
        self, content_hash: str, beautify: bool = False
    ) -> Optional[Union[bytes, str]]:
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
    def __init__(
        self,
        spark_session: SparkSession,
        base_dir: str,
        bucket: str = "openwpm-data",
        **kwargs: Dict[Any, Any],
    ) -> None:
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
        self._spark_session = spark_session
        self._table_location_format_string = (
            f"gs://{self._table_location_format_string}"
        )
        incomplete_visits = self.read_table("incomplete_visits", mode="all")
        crawl_history = self.read_table("crawl_history", mode="all")
        self._filter = TableFilter(incomplete_visits, crawl_history)

    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        mode: str = "successful",
    ) -> DataFrame:
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
        table = self._spark_session.read.parquet(
            self._table_location_format_string % table_name
        )

        if mode == "all":
            table = table
        if mode == "failed":
            table = self._filter.dirty_table(table)
        if mode == "successful":
            table = self._filter.clean_table(table)
        else:
            raise AssertionError(
                f"Mode was ${mode},"
                "allowed modes are 'all', 'failed' and 'successful'"
            )

        if columns is not None:
            table = table.select(columns)

        return table
