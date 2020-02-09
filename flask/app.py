from flask import (Flask, url_for, flash, render_template, request, jsonify)
import database
import folium

app = Flask(__name__)


#change this later
db= 'bikeshare'

@app.route('/', methods=["GET", "POST"])
def index():
    conn = database.getConn(db)
    folium_map = folium.Map(location=[40.738, -73.98],
                        width='50%',
                        zoom_start=13,
                        tiles="CartoDB dark_matter")
    if request.method == 'POST':
       print("in the post")
       input = request.form["location"].lower()
       #prevent sql injection
       errorWords =['select', 'delete', 'update','insert', 'create', 'drop table'] 
       for word in errorWords:
          if word in input:
             flash("Improper Location")
       #function to find it
    else:
        all = database.slots(conn, '2013-07-01 10:00:00')
        print(all[0][0], len(all))
        for points in all:
           folium.CircleMarker(location=points[0],fill=True).add_to(folium_map)
    folium_map.save('templates/map.html')

    return render_template('index.html',  base=all)

if __name__ == '__main__':
    app.debug = True
    app.run('0.0.0.0',5000)
