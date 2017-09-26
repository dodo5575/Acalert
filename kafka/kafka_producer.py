import random
import sys
import datetime
import numpy as np
from kafka import KafkaProducer
from time import sleep


userNum = sys.argv[1]

producer = KafkaProducer(bootstrap_servers='localhost:9092')

count = 0
while True:
    for userid_field in range(int(userNum)):
        time= datetime.datetime.now() 
        time_field = time.strftime("%Y-%m-%d %H:%M:%S")
    
        acc_field = np.random.randn()
        if count % 100 == 0:
            acc_field += 10  # Add anomaly

        message_info = '{"userid": "%s", "time": "%s", "acc": "%s"}' % (userid_field, time_field, acc_field)
    
        print(message_info)
        producer.send('data', message_info.encode('utf-8'))
        sleep(0.1)
        count += 1

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
