# jsonify creates a json representation of the response
from flask import jsonify, request
from flask import render_template
from flask_socketio import SocketIO, emit
import rethinkdb as r

from app import app

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

socketio = SocketIO(app)

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-35-162-98-222.us-west-2.compute.amazonaws.com','ec2-54-148-202-236.us-west-2.compute.amazonaws.com', 'ec2-34-209-99-28.us-west-2.compute.amazonaws.com'])

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('playground')

#@app.route('/')
#@app.route('/index')
#def index():
#  user = { 'nickname': 'Miguel' } # fake user
#  mylist = [1,2,3,4]
#  return render_template("index.html", title = 'Home', user = user, mylist = mylist)
#
#@app.route('/api/<email>/<date>')
#def get_email(email, date):
#       stmt = "SELECT * FROM email WHERE id=%s and date=%s"
#       response = session.execute(stmt, parameters=[email, date])
#       response_list = []
#       for val in response:
#            response_list.append(val)
#       jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
#       return jsonify(emails=jsonresponse)

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


@app.route('/api/<id>')
def get_data(id):
    stmt = "SELECT userid, time, acc, mean, std FROM data WHERE userid=%s limit 25"
    response = session.execute(stmt, parameters=[int(id)])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"userid": x.userid, "time": x.time, "acc": x.acc, "mean": x.mean, "std": x.std} for x in response_list]
    return render_template("index.html", data = jsonresponse) 

@app.route('/_add_numbers')
def add_numbers():
    """Add two numbers server side, ridiculous but well..."""
    a = request.args.get('a', 0, type=int)
    b = request.args.get('b', 0, type=int)
    stmt = "SELECT userid, time, acc, mean, std FROM data WHERE userid=%s limit %s"
    response = session.execute(stmt, parameters=[a, b])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"userid": x.userid, "time": x.time, "acc": x.acc, "mean": x.mean, "std": x.std} for x in response_list]
    return jsonify(result=jsonresponse)


@app.route('/email')
def email():
 return render_template("email.html")

@app.route("/email", methods=['POST'])
def email_post():
    emailid = request.form["emailid"]
    date = request.form["date"]
   
    #email entered is in emailid and date selected in dropdown is in date variable respectively
   
    stmt = "SELECT * FROM email WHERE id=%s and date=%s"
    response = session.execute(stmt, parameters=[emailid, date])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"fname": x.fname, "lname": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list]
    return render_template("emailop.html", output=jsonresponse)

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")

#@app.route('/movingTrace')
#def movingTrace():
#    return render_template("movingTrace.html")

