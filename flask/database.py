import sys
import psycopg2



#pass everything as one long string

#db should fill in with dbname instead of test2
def getConn(db):
    conn = psycopg2.connect("host= ec2-18-210-209-145.compute-1.amazonaws.com dbname=bikeshare user=ubuntu password=123")
#    conn.autocommit(True)
    return conn

def sample(conn):
    # curs = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
    curs = conn.cursor()
    curs.execute('select * from location limit 10')
    return curs.fetchall()

def findNeighbors(conn, location):
    #columns will be returned as a dictionary
    curs = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
    #write the query for the
    curs.execute('select')
    return curs.fetchall()

def slots(conn):
    curs = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
    #write the query for the
    curs.execute('select')
    return curs.fetchall()

def test(conn):
    curs = conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
    #write the query for the
    curs.execute('select * from test2')
    return curs.fetchall()
