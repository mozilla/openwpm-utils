import gzip

import boto3
import jsbeautifier
import pyarrow.parquet as pq
import s3fs
import six
from botocore.exceptions import ClientError
from pyarrow.filesystem import S3FSWrapper  # noqa
from pyspark.sql import SQLContext
import pyspark.sql.functions as F


from openwpm_utils.crawlhistory import get_worst_status_per_visit_id


class PySparkS3Dataset(object):
    def __init__(self, spark_context, s3_directory, s3_bucket="openwpm-crawls"):
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
        self._s3_bucket = s3_bucket
        self._s3_directory = s3_directory
        self._spark_context = spark_context
        self._sql_context = SQLContext(spark_context)
        self._s3_table_loc = "s3a://%s/%s/visits/%%s/" % (s3_bucket, s3_directory)
        self._s3_content_loc = "s3a://%s/%s/content/%%s.gz" % (s3_bucket, s3_directory)
        self._incomplete_visit_ids = self.read_table(
            "incomplete_visits", mode="all"
        ).select("visit_id")
        crawl_history = self.read_table("crawl_history", mode="all")
        self._failed_visit_ids = (
            get_worst_status_per_visit_id(crawl_history)
            .where(F.col("worst_status") != "ok")
            .select("visit_id")
        )


    def read_table(self, table_name, columns=None, mode = "successful"):
        """Read `table_name` from OpenWPM dataset into a pyspark dataframe.

        Parameters
        ----------
        table_name : string
            OpenWPM table to read
        columns : list of strings
            The set of columns to filter the parquet dataset by
        """
        table = self._sql_context.read.parquet(self._s3_table_loc % table_name)
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

    def read_content(self, content_hash):
        """Read the content corresponding to `content_hash`.

        NOTE: This can only be run in the driver process since it requires
              access to the spark context
        """
        return self._spark_context.textFile(self._s3_content_loc % content_hash)

    def collect_content(self, content_hash, beautify=False):
        """Collect content for `content_hash` to driver

        NOTE: This can only be run in the driver process since it requires
              access to the spark context
        """
        content = "".join(self.read_content(content_hash).collect())
        if beautify:
            return jsbeautifier.beautify(content)
        return content


class S3Dataset(object):
    def __init__(self, s3_directory, s3_bucket="openwpm-crawls"):
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
        mode : string
            The valid values are "successful", "failed", "all"
            Success is determined per visit_id. A visit_id is failed
            if one of it's commands failed or if it's in the interrupted table
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
            compressed_content = six.BytesIO(body.read())
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
