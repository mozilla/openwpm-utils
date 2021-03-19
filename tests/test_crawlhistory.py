from openwpm_utils.crawlhistory import get_worst_status_per_visit_id, reduce_to_worst_command_status

from collections import namedtuple
import pyspark as spark

srow = namedtuple('simple_row', 'visit_id command_status'.split())
data = [
    srow('1', "critical"),
    srow('1', "ok"),
    srow('2', "ok"),
    srow('3', "neterror"),
    srow('3', "timeout")
]
data2 = [
    srow('1', ["ok", "critical"]),
    srow('2', ["ok"]),
    srow('3', ["timeout", "neterror"]),
]

test_df = spark.createDataFrame(data)
test_df.printSchema()
test_df2 = spark.createDataFrame(data2)
test_df2.printSchema()


display(get_worst_status_per_visit_id(test_df))