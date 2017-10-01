#######################################################
# This script is for creating a table in rethinkDB    #
#######################################################

import rethinkdb as r

conn = r.connect(host='localhost', port=28015, db='test')

r.db('test').table_drop('status').run(conn)
r.db('test').table_create('status', primary_key='userid').run(conn)


