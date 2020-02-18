from flask import (Flask, flash, render_template, request)
import database
import folium
from geopy.geocoders import Nominatim
from datetime import datetime

app = Flask(__name__)
app.secret_key = "notverysecret"



db= 'bikeshare'

@app.route('/bikeshare', methods=["GET", "POST"])
def index():
    conn = database.getConn(db)
    date =str(datetime.now())
    folium_map = folium.Map(location=[40.738, -73.98],
                        width='50%',
                        zoom_start=13,
                        tiles="CartoDB dark_matter")
    selective = folium.Map(location=[40.738, -73.98],
                        width='90%',height ='80%',
                        zoom_start=13,
                        tiles="CartoDB dark_matter")
    all = database.slots(conn)

    for points in all:
       folium.CircleMarker(location=points[0],fill=True).add_to(folium_map)
    folium_map.save('templates/map.html')
    
    if request.method == 'POST':
       print("in the post")
       input = request.form["location"].lower()
       #prevent sql injection
       errorWords =['select', 'delete', 'update','insert', 'create', 'drop table'] 
       for word in errorWords:
          if word in input:
             flash("Improper Location")
             break
      
       else:
            #put the input location on map with red circle
            street = geolocator.geocode(input)
            loc = [street.latitude, street.longitude]
            folium.CircleMarker(location = loc, fill=True, color ='red').add_to(selective)
            
            #finds the neighboring available docks and put them in circle
            all = database.neighbors(conn,loc)
            if len(all)==1:
               flash("There are no available docks near you")

            for points in all:
               folium.CircleMarker(location=points[0],fill=True).add_to(selective)
            selective.save('templates/map.html')

    return render_template('index.html',  base=all)

if __name__ == '__main__':
    app.debug = True
    app.run('0.0.0.0',80)
