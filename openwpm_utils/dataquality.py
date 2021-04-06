import pyspark.sql.functions as F
from pyspark.mllib.stat import Statistics
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, countDistinct, isnan, lit, sum, when

from openwpm_utils.crawlhistory import get_worst_status_per_visit_id


def count_not_null(c, nan_as_null=False):
    """Use conversion between boolean and integer
    - False -> 0
    - True ->  1
    TODO: add `blank_as_null`
    """
    pred = col(c).isNotNull() & (~isnan(c) if nan_as_null else lit(True))
    return sum(pred.cast("integer")).alias(c)


def count_null(c, nan_as_null=False):
    """Use conversion between boolean and integer
    - False -> 0
    - True ->  1
    TODO: add `blank_as_null`
    """
    pred = col(c).isNull() | (isnan(c) if nan_as_null else lit(False))
    return sum(pred.cast("integer")).alias(c)


def print_distinct_counts(df, col_name):
    print(
        "Number of distinct %s %d"
        % (col_name, df.agg(countDistinct(col(col_name))).collect()[0][0])
    )


def print_total_counts(df, col_name):
    print("Total number of %s %d" % (col_name, df.select(col(col_name)).count()))


def check_df(df, skip_null_check=True):
    """A set of generic checks to run on each table"""
    print("Total number of records: %d" % df.count())

    for item in ["visit_id", "instance_id"]:
        print_distinct_counts(df, item)

    # Count of nulls
    if not skip_null_check:
        print("\nColumns with > 0 number of nulls / NaN values:")
        for c in df.columns:
            count = df.agg(count_null(c)).collect()[0][0]
            if count > 0:
                print("* %-20s | %10d" % (c, count))

    # Count of bad visit ids (default when not available in extension)
    print(
        "\nNumber of records with visit_id == -1: %d"
        % df.where(df.visit_id == -1).count()
    )


class TableFilter:
    def __init__(self, incomplete_visits, crawl_history):
        self._incomplete_visit_ids = incomplete_visits.select("visit_id")
        self._failed_visit_ids = (
            get_worst_status_per_visit_id(crawl_history)
            .where(F.col("worst_status") != "ok")
            .select("visit_id")
        )

    def clean_table(self, table: DataFrame) -> DataFrame:
        return table.join(self._failed_visit_ids, "visit_id", how="leftanti").join(
            self._incomplete_visit_ids, "visit_id", how="leftanti"
        )

    def dirty_table(self, table: DataFrame) -> DataFrame:
        return table.join(self._failed_visit_ids, "visit_id", how="inner").union(
            table.join(self._incomplete_visit_ids, "visit_id", how="inner")
        )
