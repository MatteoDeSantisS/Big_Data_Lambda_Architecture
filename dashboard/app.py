from flask import Flask,render_template
from cassandra.cluster import Cluster 
import time

app = Flask(__name__)
hyear = ["Year", "State", "City", "No2 Aqi", "No2 Mean",]
hmonth = ["Year", "Month", "State", "City", "No2 Aqi", "No2 Mean",]

if __name__ == "__main__":
    app.run()

@app.route("/")
def show_tables():
    
    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_by_year = session.execute("SELECT * FROM year_most_polluted WHERE year=2010")

    rows_by_month = session.execute("SELECT * FROM month_most_polluted WHERE year=2010")
   
    return render_template('index.html', headers_year = hyear, headers_month = hmonth, data_year = rows_by_year, data_month = rows_by_month)




