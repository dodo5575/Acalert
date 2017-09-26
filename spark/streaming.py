import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'

import numpy as np
#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
#    json parsing
import json, math, datetime

import rethinkdb as r

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel


def getSquared(tuples):

    key = tuples[0]
    val = float(tuples[1][0])

    return (key, (val, val*val, 1))


def getAvgStd(tuples):
    num = tuples[1][0]
    num2 = tuples[1][1]
    n = tuples[1][2]
    std = math.sqrt( (num2/n) - ((num / n) ** 2) )
    avg = num / n
    return (tuples[0], (avg, std))


def anomaly(tuples):
    key = int(tuples[0])
    val = float(tuples[1][0][0])
    time = datetime.datetime.strptime(tuples[1][0][1], "%Y-%m-%d %H:%M:%S")
    avg = float(tuples[1][1][0])
    std = float(tuples[1][1][1])
    if np.abs(val - avg) > 3 * np.abs(std):
        return (key, time, val, avg, std, 'danger')
    else:
        return (key, time, val, avg, std, 'safe')


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def sendRethink(rdd):
    print("Inside sendRethink")
    #try:
    # Get the singleton instance of SparkSession
    spark = getSparkSessionInstance(rdd.context.getConf())

    # Convert RDD[String] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda w: Row(userid=w[0], time=w[1], acc=w[2], mean=w[3], std=w[4], status=w[5]))
    d0 = spark.createDataFrame(rowRdd)
    d0 = d0.orderBy(d0.time.desc())
    d0.show()
    d1 = d0.filter(d0.status == 'danger')
    d1.show()
    complete_list = [row.userid for row in d0.select('userid').distinct().collect()]
    danger_list = [row.userid for row in d1.select('userid').distinct().collect()]
    print(complete_list)
    print(danger_list)
   
    status_list = [(ID, 'danger') if ID in danger_list else (ID, 'safe') for ID in complete_list]
    print(status_list)

    
    conn = r.connect(host='ec2-54-70-66-115.us-west-2.compute.amazonaws.com', port=28015, db='test')

    for record in status_list:

        # if the record is not exist, insert, else, update
        if r.table('status').filter(r.row['userid'] == record[0]).count().run(conn) == 0:
            r.table("status").insert([{"userid": record[0], "status": record[1]}]).run(conn)

        else:
            r.table('status').filter(r.row['userid'] == record[0])\
             .update({"status": record[1]}).run(conn)

    conn.close()


def sendCassandra(iter):
    cluster = Cluster(['ec2-35-162-98-222.us-west-2.compute.amazonaws.com'])
    session = cluster.connect('playground')

    insert_statement = session.prepare("INSERT INTO data (userid, time, acc, mean, std, status) VALUES (?, ?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)


    for record in iter:
        print(record[0], record[1], record[2], record[3], record[4], record[5])
        batch.add(insert_statement, (int(record[0]), record[1], float(record[2]), float(record[3]), float(record[4]), record[5]))


    session.execute(batch)
    session.shutdown()





sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 8)
ssc.checkpoint("hdfs://ec2-34-208-235-111.us-west-2.compute.amazonaws.com:9000/user/checkpoint")

kafkaStream = KafkaUtils.createDirectStream(ssc, ['data'], {"metadata.broker.list": "ec2-34-214-188-4.us-west-2.compute.amazonaws.com:9092,ec2-52-42-208-185.us-west-2.compute.amazonaws.com:9092,ec2-35-163-245-197.us-west-2.compute.amazonaws.com:9092"})
#kafkaStream = KafkaUtils.createDirectStream(ssc, ['test_data_json_2'], {"metadata.broker.list": "localhost:9092", "auto.offset.reset": "smallest"})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.count().map(lambda x:'Records in this batch: %s' % x).union(parsed).pprint()

#parsed.pprint()

rdd0 = parsed.map(lambda x: (x['userid'], (x['acc'], x['time'])))
#rdd0.pprint()


rdd1 = rdd0.window(8,8)\
       .map(getSquared)\
       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]))\
       .map(getAvgStd)


joined = rdd0.join(rdd1)
#joined.pprint()

alarm = joined.map(anomaly)
#alarm.pprint()

alarm.foreachRDD(sendRethink)
alarm.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))

ssc.start()
#ssc.awaitTermination(timeout=180)
ssc.awaitTermination()



