
from __future__ import print_function

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# To run example
# spark-submit --conf spark.hadoop.fs.defaultFS=hdfs://192.168.122.60:54310  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyorder-processing.py

# New way to run it with checkpointing: 1000040000
# spark-submit --conf spark.sql.streaming.checkpointLocation=/tmp/check --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0    pyorder-processing.py
# Note: You can use --conf spark.sql.streaming.checkpointLocation=/tmp/check inside of the writeStream.option('checkpointLocation','/tmp/check')


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("streamingOrders")\
        .getOrCreate()
    kafkaURI=os.environ.get('KAFKA_URI',"192.168.0.10:9092")
    kafkaTopic=os.environ.get('KAFKA_TOPIC',"devnet")
    print("KAFKA CONNECTION: "+ kafkaURI)
    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafkaURI)\
        .option("subscribe", kafkaTopic)\
        .load()\
        .selectExpr("CAST(value AS STRING)")


    df=lines.select(get_json_object(lines.value, '$.event').alias("event"),\
    get_json_object(lines.value, '$.order.id').alias("id"),\
    get_json_object(lines.value, '$.order.created').alias("created"),\
    get_json_object(lines.value, '$.order.customerId').alias("customerId"),\
    get_json_object(lines.value, '$.order.productId').alias("productId"),\
    get_json_object(lines.value, '$.order.productQuantity').alias("productQuantity"))


    # query = df\
    #     .writeStream\
    #     .format('console')\
    #     .start()
    query = df\
        .writeStream\
        .option('path','hdfs://192.168.122.60:54310/orders/newyork/warehouse')\
        .option('checkpointLocation','hdfs://192.168.122.60:54310/orders/newyork/check')\
        .format('parquet')\
        .start()

    query.awaitTermination()
