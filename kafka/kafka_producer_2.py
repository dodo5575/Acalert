import random
import sys
import six
import datetime
from kafka import KafkaProducer



args = sys.argv
partition_key = str(args[1])

producer = KafkaProducer(bootstrap_servers='localhost:9092')

price_field = random.randint(800,1400)
msg_cnt = 0
for userid in range(3):
    for record in range(10):
        time= datetime.datetime.now() + datetime.timedelta(minutes = record)
        time_field = time.strftime("%Y-%m-%d %H:%M:%S")

        xacc_field = random.randint(-5, 5)
        str_fmt = "{};{};{};{}"
        message_info = str_fmt.format(partition_key,
                                      userid,
                                      time_field,
                                      xacc_field)
        print(message_info)
        producer.send('test_data_1', message_info.encode('utf-8'))

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
