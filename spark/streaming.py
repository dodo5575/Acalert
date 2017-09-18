import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)
ssc.checkpoint("hdfs://ec2-34-208-235-111.us-west-2.compute.amazonaws.com:9000/user/checkpoint")

kafkaStream = KafkaUtils.createDirectStream(ssc, ['tweet'], {"metadata.broker.list": "localhost:9092", "auto.offset.reset": "smallest"})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()


## windowed
count_windowed = parsed.countByWindow(60,60).map(lambda x:('Tweets total (One minute rolling count): %s' % x))

# Get authors
authors_dstream = parsed.map(lambda tweet: tweet['user']['screen_name'])
# Count each value and number of occurences in the batch windowed
count_values_windowed = authors_dstream.countByValueAndWindow(60,60)\
                                .transform(lambda rdd:rdd\
                                  .sortBy(lambda x:-x[1]))\
                            .map(lambda x:"Author counts (One minute rolling):\tValue %s\tCount %s" % (x[0],x[1]))

count_windowed.union(count_values_windowed).pprint()


parsed.\
    flatMap(lambda tweet:tweet['text'].split(" "))\
    .countByValue()\
    .transform\
      (lambda rdd:rdd.sortBy(lambda x:-x[1]))\
    .pprint()


ssc.start()
ssc.awaitTermination(timeout=180)



