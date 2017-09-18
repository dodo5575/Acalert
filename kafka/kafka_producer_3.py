import random
import sys
import six
import datetime
from kafka import KafkaProducer
from time import sleep


args = sys.argv
partition_key = str(args[1])

producer = KafkaProducer(bootstrap_servers='localhost:9092')

msg_cnt = 0
#userid = 0
#for record in range(1000):
while True:
    for userid in range(3):
        time= datetime.datetime.now() 
    #+ datetime.timedelta(minutes = record)
        time_field = time.strftime("%Y-%m-%d %H:%M:%S")
    
        xacc_field = random.randint(-5, 5)
        #str_fmt = "{};{};{};{}"
    
        message_info = '{"partition": "%s", "userid": "%s", "time": "%s", "xacc": "%s"}' % (partition_key, userid, time_field, xacc_field)
    
        print(message_info)
        producer.send('test_data_json_2', message_info.encode('utf-8'))
        sleep(1)

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
