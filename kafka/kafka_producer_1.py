import random
import sys
import six
from datetime import datetime
from kafka import KafkaProducer



args = sys.argv
partition_key = str(args[1])

producer = KafkaProducer(bootstrap_servers='localhost:9092')

price_field = random.randint(800,1400)
msg_cnt = 0
while msg_cnt < 999:
    time_field = datetime.now().strftime("%Y%m%d %H%M%S")
    price_field += random.randint(-10, 10)/10.0
    volume_field = random.randint(1, 1000)
    str_fmt = "{};{};{};{}"
    message_info = str_fmt.format(partition_key,
                                  time_field,
                                  price_field,
                                  volume_field)
    print(message_info)
    producer.send('price_data_part4', message_info.encode('utf-8'))
    msg_cnt += 1

