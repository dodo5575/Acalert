import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 pyspark-shell'

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col, from_json, udf, struct, window
from pyspark.sql.types import *

#    json parsing
import json, math, datetime

#sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
#sc.setLogLevel("WARN")
#
#ssc = StreamingContext(sc, 8)
#ssc.checkpoint("hdfs://ec2-34-208-235-111.us-west-2.compute.amazonaws.com:9000/user/checkpoint")
#
#kafkaStream = KafkaUtils.createDirectStream(ssc, ['test_data_json_2'], {"metadata.broker.list": "localhost:9092", "auto.offset.reset": "smallest"})
#
#parsed = kafkaStream.map(lambda v: json.loads(v[1]))
#
#parsed.count().map(lambda x:'Records in this batch: %s' % x).union(parsed).pprint()
#
##parsed.pprint()
#
#users_dstream = parsed.map(lambda x: x['userid'])
#
##users_dstream.countByValue().pprint()
#
#rdd0 = parsed.map(lambda x: (x['userid'], x['xacc']))
#
##rdd0.pprint()
#
#rdd0.reduceByKeyAndWindow(lambda a, b: int(a)+int(b), None, 8,8)\
#    .map(lambda x:"ID: %s\tSUM %s" % (x[0],x[1]))\
#    .pprint()
#
#rdd0.window(8,8)\
#    .combineByKey(lambda value: (value, 1),
#                  lambda x, value: (int(x[0]) + int(value), int(x[1]) + 1),
#                  lambda x, y: (int(x[0]) + int(y[0]), int(x[1]) + int(y[1])))\
#    .map(lambda x:"ID: %s\t(SUM, COUNT):%s\tAVE %s" % (x[0],x[1], x[1][0] / x[1][1]))\
#    .pprint()
#
#
#def getSquared(tuples):
#
#    key = tuples[0]
#    val = int(tuples[1])
#
#    return (key, (val, val*val, 1))
#
#def getSTD(item):
#    num = item[1][0]
#    num2 = item[1][1]
#    n = item[1][2]
#    std = math.sqrt( (num2/n) - ((num / n) ** 2) )
#    #avg = num / n
#    return (item[0], (n, num, num2, std))
#
#
#rdd0.window(8,8)\
#    .map(getSquared)\
#    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))\
#    .map(getSTD)\
#    .map(lambda x:"ID: %s\t(COUNT, SUM, SUM2, STD):%s" % (x[0],x[1]))\
#    .pprint()


# Subscribe to 1 topic
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

df0 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ec2-34-208-235-111.us-west-2.compute.amazonaws.com:9092,ec2-52-42-210-151.us-west-2.compute.amazonaws.com:9092,ec2-34-214-141-162.us-west-2.compute.amazonaws.com:9092,ec2-35-165-131-139.us-west-2.compute.amazonaws.com:9092") \
    .option("subscribe", "test_data_json_2") \
    .load()
#df1 = df0.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df1 = df0.selectExpr("CAST(value AS STRING)")

#df.isStreaming() 
#df0.printSchema()

def parse_json(df):
    partition = str(json.loads(df[0])['partition'])
    userid    = str(json.loads(df[0])['userid'])
    time      = datetime.datetime.strptime(str(json.loads(df[0])['time']), "%Y-%m-%d %H:%M:%S")
    xacc      = str(json.loads(df[0])['xacc'])
    return [partition, userid, time, xacc]


schema = StructType().add("partition", StringType()) \
                     .add("userid"   , StringType())\
                     .add("time"     , TimestampType())\
                     .add("xacc"     , StringType())
                     #.add("time"     , TimestampType())\
                     #.add("xacc", IntegerType())

udf_parse_json = udf(parse_json, schema)

df2 = df1.withColumn("parsed_field", udf_parse_json(struct([df1[x] for x in df1.columns]))) \
                   .where(col("parsed_field").isNotNull()) \
                   .withColumn("partition", col("parsed_field.partition")) \
                   .withColumn("userid", col("parsed_field.userid")) \
                   .withColumn("time", col("parsed_field.time")) \
                   .withColumn("xacc", col("parsed_field.xacc"))




#df2 = df1.select( from_json(df1.value, schema).alias("json") )
#df3 = df2.withColumn("partition", df2.json[0]) \
#         .withColumn("userid", df2.json[1])\
#         .withColumn("time", df2.json[2])\
#         .withColumn("xacc", df2.json[3])

#df1.printSchema()
#df2.printSchema()
#print(df.isStreaming)
#print(df)

#query = df.groupby("userid").count()
#df = df.select('value')
#type(query)

#q0 = df2.writeStream.outputMode("Append").format("console").start()
##print(query.lastProgress)
##print(query.status)
#q0.awaitTermination()

df3 = df2.withWatermark("time", "10 seconds")\
         .groupBy(
         "userid",
         window(df2.time, "10 seconds", "10 seconds"))\
         .count()
q1 = df3.writeStream.outputMode("Complete").format("console").start()
q1.awaitTermination()


#ssc.start()
##ssc.awaitTermination(timeout=180)
#ssc.awaitTermination(timeout=32)



