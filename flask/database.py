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

def findNeighbors(conn, time, location):
    #columns will be returned as a dictionary
    curs = conn.cursor()
    latUpper = location[0]+0.007
    latLower = location[0]-0.007
    longUpper = location[1]+0.007
    longLower = location[1]-0.007
    #write the query for the
    curs.execute('''Select distinct(c1.loc) from 
                (select * from records a1 inner join 
                (select bike_id, max(time) as max from records group by bike_id having max(time)<'{}')b1 
                 on a1.bike_id = b1.bike_id and a1.time=b1.max)c1 where loc[0] between latUpper and latLower
                 and loc[1] between longUpper and longLower'''.format(time))
    return curs.fetchall()

def slots(conn, time):
    curs = conn.cursor()
    curs.execute('''Select distinct(c1.loc) from 
                (select * from records a1 inner join 
                (select bike_id, max(time) as max from records group by bike_id having max(time)< '{}' )b1 
                 on a1.bike_id = b1.bike_id and a1.time=b1.max)c1 where status = True'''.format(time))
    return curs.fetchall()


