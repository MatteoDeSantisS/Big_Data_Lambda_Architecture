from flask import Flask,render_template
from cassandra.cluster import Cluster 
import time

app = Flask(__name__)
headings=["Year", "State", "City","No2 Mean","No2 Aqi"]

if __name__ == "__main__":
    app.run()

@app.route("/")
def show_table_most_polluted_by_year():
    
    cluster=Cluster()
    session = cluster.connect("stuff")

    rows = session.execute("SELECT * FROM year_most_polluted WHERE year=2010")
   
    return render_template('index.html', headings=headings, data=rows)

