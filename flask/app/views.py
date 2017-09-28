from flask import jsonify, request
from flask import render_template
from flask_socketio import SocketIO, emit
import rethinkdb as r

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# setting up connections to cassandra
cluster = Cluster(['ec2-35-162-98-222.us-west-2.compute.amazonaws.com','ec2-54-148-202-236.us-west-2.compute.amazonaws.com', 'ec2-34-209-99-28.us-west-2.compute.amazonaws.com'])
session = cluster.connect('playground')


# setting up to listen to rethinkDB
socketio = SocketIO(app)

def bg_rethink():
    conn = r.connect(host="localhost", port=28015, db="test")
    ccCursor = r.table("status").changes().run(conn)
    for cc in ccCursor:
        socketio.emit('components', {'data': cc}, json=True)
        socketio.sleep(0.1)

thread = None
@socketio.on('connect')
def connected():
    print('connected')
    global thread
    if thread is None:
        thread = socketio.start_background_task(target=bg_rethink)

@socketio.on('disconnect')
def disconnected():
    print('disconnected')    
    
@app.route('/')
def hello():
    #return render_template("movingTrace.html")
    return render_template("index.html")



@app.route('/_query')
def add_numbers():
    """get the user id and number of points for query"""
    a = request.args.get('a', 0, type=int)
    b = request.args.get('b', 0, type=int)
    stmt = "SELECT userid, time, acc, mean, std, status FROM data WHERE userid=%s limit %s"
    response = session.execute(stmt, parameters=[a, b])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"userid": x.userid, "time": x.time.strftime("%Y-%m-%d %H:%M:%S %f"), "acc": x.acc, "mean": x.mean, "std": x.std, "status": x.status} for x in response_list]
    return jsonify(result=jsonresponse)



