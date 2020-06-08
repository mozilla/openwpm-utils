import gzip

import boto3
import jsbeautifier
import pyarrow.parquet as pq
import s3fs
from botocore.exceptions import ClientError
from pyarrow.filesystem import S3FSWrapper  # noqa
from pyspark.sql import SQLContext


class PySparkS3Dataset(object):
    def __init__(self, spark_context, s3_directory,
                 s3_bucket='openwpm-crawls'):
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
        self._s3_table_loc = "s3a://%s/%s/visits/%%s/" % (
            s3_bucket, s3_directory)
        self._s3_content_loc = "s3a://%s/%s/content/%%s.gz" % (
            s3_bucket, s3_directory)

    def read_table(self, table_name, columns=None):
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
            return table.select(columns)
        return table

    def read_content(self, content_hash):
        """Read the content corresponding to `content_hash`.

        NOTE: This can only be run in the driver process since it requires
              access to the spark context
        """
        return self._spark_context.textFile(
            self._s3_content_loc % content_hash)

    def collect_content(self, content_hash, beautify=False):
        """Collect content for `content_hash` to driver

        NOTE: This can only be run in the driver process since it requires
              access to the spark context
        """
        content = ''.join(self.read_content(content_hash).collect())
        if beautify:
            return jsbeautifier.beautify(content)
        return content


class S3Dataset(object):
    def __init__(self, s3_directory, s3_bucket='openwpm-crawls'):
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
        return pq.ParquetDataset(
            self._s3_table_loc % table_name,
            filesystem=self._s3fs,
            metadata_nthreads=4
        ).read(use_pandas_metadata=True, columns=columns).to_pandas()

    def collect_content(self, content_hash, beautify=False):
        """Collect content by directly connecting to S3 via boto3"""
        s3 = boto3.client('s3')
        try:
            obj = s3.get_object(
                Bucket=self._s3_bucket,
                Key=self._content_key % content_hash
            )
            body = obj["Body"]
            compressed_content = body.read()
            body.close()
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchKey':
                raise
            else:
                return None

        with gzip.GzipFile(fileobj=compressed_content, mode='r') as f:
            content = f.read()

        if content is None or content == "":
            return ""

        if beautify:
            try:
                content = jsbeautifier.beautify(content)
            except IndexError:
                pass
        return content
