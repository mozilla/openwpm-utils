import pyspark.sql.functions as F
from pyspark.sql.types import StringType


def reduce_to_worst_command_status(statuses):
    """Takes a list of command_statuses and returns the worst of them
    signature: List[str] -> str
    """
    if "critical" in statuses:
        return "critical"
    if "error" in statuses:
        return "error"
    if "neterror" in statuses:
        return "neterror"
    if "timeout" in statuses:
        return "timeout"
    return "ok"


udf_reduce_to_worst_command_status = F.udf(reduce_to_worst_command_status, StringType())


def reduce_to_best_command_status(statuses):
    """Takes a list of command_statuses and returns the worst of them
    signature: List[str] -> str
    """
    if "ok" in statuses:
        return "ok"
    if "timeout" in statuses:
        return "timeout"
    if "neterror" in statuses:
        return "neterror"
    if "error" in statuses:
        return "error"
    return "critical"


udf_reduce_to_best_command_status = F.udf(reduce_to_worst_command_status, StringType())


def get_worst_status_per_visit_id(crawl_history):
    return crawl_history.groupBy("visit_id").agg(
        udf_reduce_to_worst_command_status(F.collect_list("command_status")).alias(
            "worst_status"
        )
    )


