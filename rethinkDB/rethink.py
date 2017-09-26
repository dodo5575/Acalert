import rethinkdb as r

conn = r.connect(host='localhost', port=28015, db='test')

r.db('test').table_create('status').run(conn)

for change in r.table('status').changes().run(conn):
    print(change)

