import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json, math

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 8)
ssc.checkpoint("hdfs://ec2-34-208-235-111.us-west-2.compute.amazonaws.com:9000/user/checkpoint")

kafkaStream = KafkaUtils.createDirectStream(ssc, ['test_data_json_2'], {"metadata.broker.list": "localhost:9092", "auto.offset.reset": "smallest"})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.count().map(lambda x:'Records in this batch: %s' % x).union(parsed).pprint()

#parsed.pprint()

users_dstream = parsed.map(lambda x: x['userid'])

#users_dstream.countByValue().pprint()

rdd0 = parsed.map(lambda x: (x['userid'], x['xacc']))

#rdd0.pprint()

rdd0.reduceByKeyAndWindow(lambda a, b: int(a)+int(b), None, 8,8)\
    .map(lambda x:"ID: %s\tSUM %s" % (x[0],x[1]))\
    .pprint()

rdd0.window(8,8)\
    .combineByKey(lambda value: (value, 1),
                  lambda x, value: (int(x[0]) + int(value), int(x[1]) + 1),
                  lambda x, y: (int(x[0]) + int(y[0]), int(x[1]) + int(y[1])))\
    .map(lambda x:"ID: %s\t(SUM, COUNT):%s\tAVE %s" % (x[0],x[1], x[1][0] / x[1][1]))\
    .pprint()


def getSquared(tuples):

    key = tuples[0]
    val = int(tuples[1])

    return (key, (val, val*val, 1))

def getSTD(item):
    num = item[1][0]
    num2 = item[1][1]
    n = item[1][2]
    std = math.sqrt( (num2/n) - ((num / n) ** 2) )
    #avg = num / n
    return (item[0], (n, num, num2, std))


rdd0.window(8,8)\
    .map(getSquared)\
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))\
    .map(getSTD)\
    .map(lambda x:"ID: %s\t(COUNT, SUM, SUM2, STD):%s" % (x[0],x[1]))\
    .pprint()

#users_dstream = parsed.map(lambda x: x[1])

#parsed.\
#    flatMap(lambda tweet:tweet['text'].split(" "))\
#    .countByValue()\
#    .transform\
#      (lambda rdd:rdd.sortBy(lambda x:-x[1]))\
#    .pprint()


ssc.start()
#ssc.awaitTermination(timeout=180)
ssc.awaitTermination()



