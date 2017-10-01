#######################################################
# This script is for creating a table in cassandra    #
#######################################################

from cassandra.cluster import Cluster
import config

cluster = Cluster(config.CASSANDRA_SERVER)
session = cluster.connect('playground')

session.execute('DROP TABLE IF EXISTS data;')
session.execute('CREATE TABLE data (userid int, time timestamp, acc float, mean float, std float, status text, PRIMARY KEY (userid, time) ) WITH CLUSTERING ORDER BY (time DESC);')

