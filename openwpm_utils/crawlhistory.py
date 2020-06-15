import json

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
    F.when(F.array_contains("command_status", "ok"), "ok")
    .when(F.array_contains("command_status", "timeout"), "timeout")
    .when(F.array_contains("command_status", "neterror"), "neterror")
    .when(F.array_contains("command_status", "error"), "error")
    .otherwise("critical")
    .alias("best_status")
)


def get_worst_status_per_visit_id(crawl_history):
    """Adds column `worst_status`"""
    return (crawl_history.groupBy("visit_id")
            .agg(F.collect_list("command_status").alias("command_status"))
            .withColumn("worst_status",reduce_to_worst_command_status))


def display_crawl_history_per_command_sequence(crawl_history, interrupted_visits):
    """
        Analyze crawl_history and interrupted_visits to display general
        success statistics
        This function should be given the all entries in the crawl_history and
        interrupted_visits tableq
    """
    crawl_history.groupBy("command").count().show()

    # Analyzing status per command_sequence
    total_num_command_sequences = crawl_history.groupBy("visit_id").count().count()
    visit_id_and_worst_status = get_worst_status_per_visit_id(crawl_history)
    print(
        "Percentage of command_sequence that didn't complete successfully %0.2f%%"
        % (
            visit_id_and_worst_status.where(F.col("worst_status") != "ok").count()
            / float(total_num_command_sequences)
            * 100
        )
    )
    net_error_count = visit_id_and_worst_status.where(
        F.col("worst_status") == "neterror"
    ).count()
    print(
        "There were a total of %d neterrors(%0.2f%% of the all command_sequences)"
        % (net_error_count, net_error_count / float(total_num_command_sequences) * 100)
    )
    timeout_count = visit_id_and_worst_status.where(
        F.col("worst_status") == "timeout"
    ).count()
    print(
        "There were a total of %d timeouts(%0.2f%% of the all command_sequences)"
        % (timeout_count, timeout_count / float(total_num_command_sequences) * 100)
    )

    error_count = visit_id_and_worst_status.where(
        F.col("worst_status") == "error"
    ).count()
    print(
        "There were a total of %d errors(%0.2f%% of the all command_sequences)"
        % (error_count, error_count / float(total_num_command_sequences) * 100)
    )

    print(
        f"A total of {interrupted_visits.count()} command_sequences were interrupted."
        f"This represents {interrupted_visits.count()/ float(total_num_command_sequences)* 100:.2f} % of the entire crawl"
    )


def display_crawl_history_per_website(crawl_history, interrupted_visits):
    # Analyzing status per website
    visit_id_and_worst_status = get_worst_status_per_visit_id(crawl_history)

    def extract_website_from_arguments(arguments):
        """Given the arguments of a get_command this function returns which website was visited"""
        return json.loads(arguments)["url"]

    udf_extract_website_from_arguments = F.udf(
        extract_website_from_arguments, StringType()
    )

    visit_id_to_website = crawl_history.where(
        F.col("command") == "GetCommand"
    ).withColumn("website", udf_extract_website_from_arguments("arguments"))

    visit_id_to_website = visit_id_to_website[["visit_id", "website"]]

    visit_id_website_status = visit_id_and_worst_status.join(
        visit_id_to_website, "visit_id"
    )
    best_status_per_website = visit_id_website_status.groupBy("website").agg(
        udf_reduce_to_best_command_status(F.collect_list("worst_status")).alias(
            "best_status"
        )
    )
    total_number_websites = best_status_per_website.count()
    print(f"There was an attempt to visit a total of {total_number_websites} websites")

    print(
        "Percentage of websites that didn't complete successfully %0.2f%%"
        % (
            best_status_per_website.where(F.col("best_status") != "ok").count()
            / float(total_number_websites)
            * 100
        )
    )
    net_error_count = best_status_per_website.where(
        F.col("best_status") == "neterror"
    ).count()
    print(
        "There were a total of %d neterrors (%0.2f%% of the all websites)"
        % (net_error_count, net_error_count / float(total_number_websites) * 100)
    )
    timeout_count = best_status_per_website.where(
        F.col("best_status") == "timeout"
    ).count()
    print(
        "There were a total of %d timeouts (%0.2f%% of the all websites)"
        % (timeout_count, timeout_count / float(total_number_websites) * 100)
    )

    error_count = best_status_per_website.where(F.col("best_status") == "error").count()

    print(
        "There were a total of %d errors (%0.2f%% of the websites)"
        % (error_count, error_count / float(total_number_websites) * 100)
    )

    multiple_successes = (
        visit_id_website_status.where(F.col("worst_status") == "ok")
        .join(interrupted_visits, "visit_id", how="leftanti")
        .groupBy("website")
        .count()
        .filter("count > 1")
        .orderBy(F.desc("count"))
    )

    print(
        f"There were {multiple_successes.count()} websites that were successfully visited multiple times"
    )
    multiple_successes.groupBy(
        F.col("count").alias("Number of successes")
    ).count().show()
    multiple_successes.filter("count > 2").show()
