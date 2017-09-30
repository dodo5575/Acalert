from cassandra.cluster import Cluster
import datetime

cluster = Cluster(['ec2-35-162-98-222.us-west-2.compute.amazonaws.com'])
session = cluster.connect('playground')

#rows = session.execute('SELECT * FROM email')
#for user_row in rows:
#    print(user_row.id, user_row.date, user_row.fname)

session.execute('DROP TABLE IF EXISTS data;')
session.execute('CREATE TABLE data (userid int, time timestamp, acc float, mean float, std float, status text, PRIMARY KEY (userid, time) ) WITH CLUSTERING ORDER BY (time DESC);')



#rows = session.execute('SELECT * FROM test')
#for user_row in rows:
#    print(user_row)
#    print(user_row.id, user_row.time, user_row.acc)


