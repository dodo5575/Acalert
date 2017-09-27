import random
import sys
import datetime
import numpy as np
from kafka import KafkaProducer
from time import sleep


NUM_SPAWNS = int(sys.argv[1])
ID = int(sys.argv[2])

nUsers = 200
users_array = np.array(range(nUsers))
subUser_array = np.array_split(users_array, NUM_SPAWNS)[ID]

producer = KafkaProducer(bootstrap_servers='ec2-34-214-188-4.us-west-2.compute.amazonaws.com:9092,ec2-52-42-208-185.us-west-2.compute.amazonaws.com:9092,ec2-35-163-245-197.us-west-2.compute.amazonaws.com:9092')

count = 0
while True:

    for userid_field in subUser_array:
        time= datetime.datetime.now() 
        time_field = time.strftime("%Y-%m-%d %H:%M:%S")
    
        acc_field = np.random.randn()
        if count % 333 == 0:
            acc_field += 10  # Add anomaly

        message_info = '{"userid": "%s", "time": "%s", "acc": "%s"}' % (userid_field, time_field, acc_field)
    
        #print(message_info)
        producer.send('data', message_info.encode('utf-8'))
        #sleep(0.01)
        count += 1

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
