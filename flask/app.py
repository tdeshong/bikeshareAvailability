from flask import (Flask, url_for, flash, render_template, request, jsonify)
import database
import folium

app = Flask(__name__)

#change this
db= 'bikeshare'

@app.route('/', methods=["GET", "POST"])
def index():
    if request.method == 'POST':
        print(request.form["location"], type(request.form["location"]))
    conn = database.getConn(db)
    all = database.sample(conn)
    #now = servertime.now()
    folium_map = folium.Map(location=[40.738, -73.98],
                        width='50%',
                        zoom_start=13,
                        tiles="CartoDB dark_matter")
    folium.CircleMarker(location=[40.738, -73.98],fill=True).add_to(folium_map)
    folium_map.save('templates/map.html')
#    return folium_map._repr_html_()
    return render_template('index.html',  base=all)

#def neighbors()


if __name__ == '__main__':
    app.debug = True
    app.run('0.0.0.0',5000)
