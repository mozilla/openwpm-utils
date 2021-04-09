import gzip
from typing import List

import boto3
import jsbeautifier
import pyarrow.parquet as pq
import pyspark.sql.functions as F
import s3fs
from botocore.exceptions import ClientError
from pyarrow.filesystem import S3FSWrapper  # noqa
from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext

from openwpm_utils.crawlhistory import get_worst_status_per_visit_id
from openwpm_utils.dataquality import TableFilter


class S3Dataset:
    def __init__(self, s3_directory: str, s3_bucket: str = "openwpm-crawls"):
        """Helper class to load OpenWPM datasets from S3 using pandas

        This dataset wrapper is safe to use by spark worker processes, as it
        does not require the spark context.

        Parameters
        ----------
        s3_directory : string
            Directory within the S3 bucket in which the dataset is saved.
        s3_bucket : string, optional
            The bucket name on S3. Defaults to `openwpm-crawls`.
        """
        self._s3_bucket = s3_bucket
        self._s3_directory = s3_directory
        self._s3_table_loc = "%s/%s/visits/%%s" % (s3_bucket, s3_directory)
        self._content_key = "%s/content/%%s.gz" % s3_directory
        self._s3fs = s3fs.S3FileSystem()

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
                self._s3_table_loc % table_name,
                filesystem=self._s3fs,
                metadata_nthreads=4,
            )
            .read(use_pandas_metadata=True, columns=columns)
            .to_pandas()
        )

    def collect_content(self, content_hash, beautify=False):
        """Collect content by directly connecting to S3 via boto3"""
        s3 = boto3.client("s3")
        try:
            obj = s3.get_object(
                Bucket=self._s3_bucket, Key=self._content_key % content_hash
            )
            body = obj["Body"]
            compressed_content = body.read()
            body.close()
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise
            else:
                return None

        with gzip.GzipFile(fileobj=compressed_content, mode="r") as f:
            content = f.read()

        if content is None or content == "":
            return ""

        if beautify:
            try:
                content = jsbeautifier.beautify(content)
            except IndexError:
                pass
        return content


class PySparkS3Dataset(S3Dataset):
    def __init__(
        self,
        spark_context: SparkContext,
        s3_directory: str,
        s3_bucket: str = "openwpm-crawls",
    ) -> None:
        """Helper class to load OpenWPM datasets from S3 using PySpark

        Parameters
        ----------
        spark_context
            Spark context. In databricks, this is available via the `sc`
            variable.
        s3_directory : string
            Directory within the S3 bucket in which the dataset is saved.
        s3_bucket : string, optional
            The bucket name on S3. Defaults to `openwpm-crawls`.
        """
        super().__init__(s3_directory, s3_bucket)
        self._spark_context = spark_context
        self._sql_context = SQLContext(spark_context)
        self._s3_table_loc = f"s3a://{self._s3_table_loc}"
        incomplete_visits = self.read_table("incomplete_visits", mode="all")
        crawl_history = self.read_table("crawl_history", mode="all")
        self._filter = TableFilter(incomplete_visits, crawl_history)

    def read_table(
        self, table_name: str, columns: List[str] = None, mode: str = "successful"
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
        table = self._sql_context.read.parquet(self._s3_table_loc % table_name)
        if mode == "all":
            table = table
        elif mode == "failed":
            table = self._filter.dirty_table(table)
        elif mode == "successful":
            table = self._filter.clean_table(table)
        else:
            raise AssertionError(
                f"Mode was ${mode},"
                "allowed modes are 'all', 'failed' and 'successful'"
            )

        if columns is not None:
            table = table.select(columns)

        return table
