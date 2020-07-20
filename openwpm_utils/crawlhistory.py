import pyspark.sql.functions as F
from pyspark.sql.types import StringType

reduce_to_worst_command_status = (
    F.when(F.array_contains("command_status", "critical"), "critical")
    .when(F.array_contains("command_status", "error"), "error")
    .when(F.array_contains("command_status", "neterror"), "neterror")
    .when(F.array_contains("command_status", "timeout"), "timeout")
    .otherwise("ok")
    .alias("worst_status")
)


reduce_to_best_command_status = (
    F.when(F.array_contains("command_status", "critical"), "critical")
    .when(F.array_contains("command_status", "ok"), "ok")
    .when(F.array_contains("command_status", "timeout"), "timeout")
    .when(F.array_contains("command_status", "neterror"), "neterror")
    .otherwise("critical")
    .alias("worst_status")
)


def get_worst_status_per_visit_id(crawl_history):
    """Adds column `worst_status`"""
    return (crawl_history.groupBy("visit_id")
            .agg(F.collect_list("command_status").alias("command_status"))
            .withColumn("worst_status",reduce_to_worst_command_status))
