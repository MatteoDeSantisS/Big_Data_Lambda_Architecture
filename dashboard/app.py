from flask import Flask, render_template
from cassandra.cluster import Cluster 

app = Flask(__name__)

def show_tables():
    
    cluster = Cluster()
    session = cluster.connect("stuff")

    rows_no2_by_month = session.execute("SELECT * FROM no2_month WHERE year=2010")
    rows_so2_by_month = session.execute("SELECT * FROM so2_month WHERE year=2010")
    rows_co_by_month = session.execute("SELECT * FROM co_month WHERE year=2010")
    rows_o3_by_month = session.execute("SELECT * FROM o3_month WHERE year=2010")
   
    return render_template('index.html', 
                            headers_no2month = ["Year", "Month", "State", "City", "NO2 Aqi", "NO2 Mean"],
                            headers_so2month = ["Year", "Month", "State", "City", "SO2 Aqi", "SO2 Mean"],
                            headers_comonth = ["Year", "Month", "State", "City", "CO Aqi", "CO Mean"],
                            headers_o3month = ["Year", "Month", "State", "City", "O3 Aqi", "O3 Mean"], 
                            data_no2_month = rows_no2_by_month,
                            data_so2_month = rows_so2_by_month,
                            data_co_month = rows_co_by_month,
                            data_o3_month = rows_o3_by_month,)

@app.route("/")
def get_index():
    return show_tables()

if __name__ == "__main__":
    app.run()

