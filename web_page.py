from flask import Flask,render_template
from cassandra.cluster import Cluster 


app = Flask(__name__)
headings=["city","datelocal","no2mean","o3mean","so2mean","comean","address","county","state","statecode"]



@app.route("/")
def main():
    cluster=Cluster()
    session = cluster.connect("stuff")

    rows = session.execute('SELECT * FROM pollution')
   
    return render_template('index.html',headings=headings, data=rows)









if __name__ == "__main__":
    app.run()
