from flask import Flask, render_template
from cassandra.cluster import Cluster
import json


app = Flask(__name__)

@app.route('/', methods=["GET","POST"])
def main():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_no2_by_month = session.execute("SELECT * FROM no2_month")
    rows_so2_by_month = session.execute("SELECT * FROM so2_month")
    rows_co_by_month = session.execute("SELECT * FROM co_month")
    rows_o3_by_month = session.execute("SELECT * FROM o3_month")

    rows_no2_by_year = session.execute("SELECT * FROM no2_year")
    rows_so2_by_year = session.execute("SELECT * FROM so2_year")
    rows_co_by_year = session.execute("SELECT * FROM co_year")
    rows_o3_by_year = session.execute("SELECT * FROM o3_year")
   
    return render_template('index.html', data_no2_month = rows_no2_by_month, data_so2_month = rows_so2_by_month, data_co_month = rows_co_by_month, data_o3_month = rows_o3_by_month,
    data_no2_year = rows_no2_by_year, data_so2_year = rows_so2_by_year, data_co_year = rows_co_by_year, data_o3_year = rows_o3_by_year)


#------------------------------ Update tables by month --------------------------------------
@app.route('/data-month-no2' , methods=["GET", "POST"])
def datano2():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_no2_by_month = session.execute("SELECT * FROM no2_month")
    data = rows_no2_by_month.all()
   
    return json.dumps(data)

@app.route('/data-month-so2' , methods=["GET", "POST"])
def dataso2():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_so2_by_month = session.execute("SELECT * FROM so2_month")
    data = rows_so2_by_month.all()
   
    return json.dumps(data)

@app.route('/data-month-co' , methods=["GET", "POST"])
def dataco():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_co_by_month = session.execute("SELECT * FROM co_month")
    data = rows_co_by_month.all()
   
    return json.dumps(data)

@app.route('/data-month-o3' , methods=["GET", "POST"])
def datao3():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_o3_by_month = session.execute("SELECT * FROM o3_month")
    data = rows_o3_by_month.all()
   
    return json.dumps(data)

#--------------------------------- Update tables by year ----------------------------------------
@app.route('/data-year-no2' , methods=["GET", "POST"])
def datayearno2():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_no2_by_year = session.execute("SELECT * FROM no2_year")
    data = rows_no2_by_year.all()
   
    return json.dumps(data)

@app.route('/data-year-so2' , methods=["GET", "POST"])
def datayearso2():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_so2_by_year = session.execute("SELECT * FROM so2_year")
    data = rows_so2_by_year.all()
   
    return json.dumps(data)

@app.route('/data-year-co' , methods=["GET", "POST"])
def datayearco():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_co_by_year = session.execute("SELECT * FROM co_year")
    data = rows_co_by_year.all()
   
    return json.dumps(data)

@app.route('/data-year-o3' , methods=["GET", "POST"])
def datayearo3():

    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_o3_by_year = session.execute("SELECT * FROM o3_year")
    data = rows_o3_by_year.all()
   
    return json.dumps(data)

if __name__ == "__main__":
    app.run()

