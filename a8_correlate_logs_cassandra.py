#!/usr/bin/env pySpark
import sys
import re
from datetime import datetime
from uuid import uuid4

import lit
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType


def parse_log(line):
    # parse log according to format and get the request data with (host, datetime, path, bytes)
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = line_re.match(line)

    if match:
        host = match.group(1)
        timestamp = datetime.strptime(match.group(2), '%d/%b/%Y:%H:%M:%S')
        path = match.group(3)
        bytes = int(match.group(4))
        id = str(uuid4())
        return host, timestamp, path, bytes, id
    else:
        return None


def main(inputs, user_keyspace,user_table):
    # main logic starts here
   log = spark.read.format("org.apache.spark.sql.cassandra").options(table=user_table, keyspace=user_table).load()

   log2 = log.select(
        lit(1).alias("one"),
        col("host"),
        col("bytes")
    )

    log3 = log2.groupBy("host").select((sum("one").alias("count"),sum("bytes").alias("bytes_host"))
    corr_factor = log3.corr("count", "bytes_host")
    print("Python function corr calculation result:", corr_factor)
    # new dataframe for 1,x,y,x^2,y^2,xy

    req_data2 = log3.select(
        lit(1).alias("one"),
        col("count"),
        col("bytes_host").alias("bytes"),
        (col("count") ** 2).alias("count_squared"),
        (col("bytes_host") ** 2).alias("bytes_squared"),
        (col("count") * col("bytes")).alias("count_times_bytes")
    )

    # get sum of each column of req_data2
    sum_data = req_data2.select(
        sum(col("one")).alias("n"),
        sum(col("count")).alias("count_sum"),
        sum(col("bytes")).alias("bytes_sum"),
        sum(col("count_squared")).alias("count_squared_sum"),
        sum(col("bytes_squared")).alias("bytes_squared_sum"),
        sum(col("count_times_bytes")).alias("count_times_bytes_sum")
    )

    # show the sum result of 1,x,y,x^2,y^2,xy
    sum_data.show()
    # use a tuple to get the sum of 1,x,y,x^2,y^2,xy
    sum_row = sum_data.first()
    cal_corr = tuple(sum_row)
    n = cal_corr[0]
    count_sum = cal_corr[1]
    bytes_sum = cal_corr[2]
    count_squared_sum = cal_corr[3]
    bytes_squared_sum = cal_corr[4]
    count_times_bytes_sum = cal_corr[5]

    # calculate r and r ^ 2
    corr_new = (n * count_times_bytes_sum - count_sum * bytes_sum) / (
        math.sqrt(n * count_squared_sum - count_sum * count_sum)) / (
                   math.sqrt(n * bytes_squared_sum - bytes_sum * bytes_sum))
    corr_new_squared = corr_new * corr_new

    print("This program calculate r  :", corr_new)
    print("This program calculate r^2:", corr_new_squared)



if __name__ == '__main__':
    inputs = sys.argv[1]
    user_keyspace = sys.argv[2]
    user_table = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra example') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,user_keyspace, user_table)
