import sys
import psycopg2


'''
Database api that queries bike location from postgres database
'''
def getConn(db):
    conn = psycopg2.connect("host= ec2-18-210-209-145.compute-1.amazonaws.com dbname=bikeshare user=ubuntu password=123")
    return conn


def findNeighbors(conn, time, location):
    '''
    parameters: time --> current time
                location --> [latitude, longtitude]
    return list of dock locations in the radius of 0.007 degrees
           in latitude and longtitude
    '''
    curs = conn.cursor()
    latUpper = location[0]+0.007
    latLower = location[0]-0.007
    longUpper = location[1]+0.007
    longLower = location[1]-0.007

    curs.execute('''Select distinct(c1.loc) from 
                (select * from records a1 inner join 
                (select bike_id, max(time) as max from records group by bike_id having max(time)<'{}')b1 
                 on a1.bike_id = b1.bike_id and a1.time=b1.max)c1 where loc[0] between latUpper and latLower
                 and loc[1] between longUpper and longLower'''.format(time))
    return curs.fetchall()

def slots(conn, time):
    '''
    paramter: time --> current time
    return list of locations of all the available bike docks
    '''
    curs = conn.cursor()
    curs.execute('''Select distinct(c1.loc) from 
                (select * from records a1 inner join 
                (select bike_id, max(time) as max from records group by bike_id having max(time)< '{}' )b1 
                 on a1.bike_id = b1.bike_id and a1.time=b1.max)c1 where status = True'''.format(time))
    return curs.fetchall()


