from flask import jsonify, request, redirect, render_template
from flask_socketio import SocketIO, emit
import rethinkdb as r

import sys
sys.path.append('/home/ubuntu/flask/app')
import config

from app import app

from cassandra.cluster import Cluster

# setting up connections to cassandra
cluster = Cluster(config.CASSANDRA_SERVERS)
session = cluster.connect(config.CASSANDRA_NAMESPACE)


# setting up to listen to rethinkDB
socketio = SocketIO(app)

def bg_rethink():
    conn = r.connect(host=config.RETHINKDB_SERVER, \
                     port=28015, \
                       db=config.RETHINKDB_DB)

    ccCursor = r.table(config.RETHINKDB_TABLE).changes().run(conn)
    for cc in ccCursor:
        socketio.emit('components', {'data': cc}, json=True)
        socketio.sleep(0.001)


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
    jsonresponse = [{"userid": x.userid, 
                       "time": x.time.strftime("%Y-%m-%d %H:%M:%S %f"), 
                        "acc": x.acc, 
                       "mean": x.mean, 
                        "std": x.std, 
                     "status": x.status} for x in response_list]
    return jsonify(result=jsonresponse)


@app.route('/github')
def github():
    return redirect("https://github.com/dodo5575/Acalert")

@app.route('/slides')
def slides():
    return redirect("https://drive.google.com/open?id=1GSehAzTXAU0JdmulQR1Vnmd3FLVfnhPH6Bj1TcgOkJQ")




