from flask import (Flask, url_for, flash, render_template, request, jsonify)
import database

app = Flask(__name__)


#change this later
db= 'bikeshare'

@app.route('/')
def index():
    conn = database.getConn(db)
    all = database.sample(conn)
    #now = servertime.now()
    return render_template('index.html',  base=all)

if __name__ == '__main__':
    app.debug = True
    app.run('0.0.0.0',5000)
