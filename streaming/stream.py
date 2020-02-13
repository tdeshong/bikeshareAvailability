import sys
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


class Streamer(object):
    '''
    Gets messages from the Kafka Brokers and splits message into two entries
    to simulate the information coming in as near real time entries and inserts
    the split messages into tables into postgres database
    '''
    def __init__(self, topic, broker, postgresaddress, database):
        self.sc = SparkContext()
        self.ssc = StreamingContext(self.sc,2)
        self.sc.setLogLevel("ERROR")
        self.spark = SparkSession.builder.master('local').getOrCreate()
        self.url = 'jdbc:postgresql://{}:5432/{}'.format(postgresaddress, database)
        self.properties = {'driver': 'org.postgresql.Driver',
                      'user': 'ubuntu',
                      'password': '123'}
        #schema for the dataframe the message would go into 
        self.original = StructType([StructField("tripduration", StringType(), False),\
                             StructField("Starttime", StringType(), True), \
                             StructField("Stoptime", StringType(), True),\
                             StructField("locationStart", StringType(),True),\
                             StructField("startLat", StringType(),True),\
                             StructField("startLong", StringType(),True),\
                             StructField("locationEnd", StringType(),True),\
                             StructField("endLat", StringType(),True),\
                             StructField("endLong", StringType(),True),\
                             StructField("bike_id", StringType(), True)])
        
        self.stream= KafkaUtils.createDirectStream(self.ssc, ["kiosk"],{"metadata.broker.list":",".join(broker)})
    
    def insert(self, time, anRDD):
       '''
       call function that creates dataframe from the rdd in the direct stream
       call functions that will insert the information from the dataframe
       into the appropriate tables in the database
       '''
       df = self.createDataframe(time, anRDD)
       self.readyLoc(df)
       self.readyRecords(df)

    def createDataframe(self, time, anRDD):
        '''
        creates dataframe with the information from the incoming stream
        Adds a column which contains an array of the latitude and londitude
        coordinates of the start location and another column which contains
        an array of the latitude and londitude coordinates of the end location

        returns dataframe
        '''
        org = self.spark.createDataFrame(anRDD, self.original)

        #casting columns to the appropriate type
        org = org.withColumn("tripduration", org["tripduration"].cast(IntegerType()))
        org = org.withColumn("bike_id", org["bike_id"].cast(IntegerType()))
        org = org.withColumn("startLat", org["startLat"].cast(DoubleType()))
        org = org.withColumn("startLong", org["startLong"].cast(DoubleType()))
        org = org.withColumn("endLat", org["endLat"].cast(DoubleType()))
        org = org.withColumn("endLong", org["endLong"].cast(DoubleType()))

        #create extra columns with the lat and long combined in an array
        #f is an alias for pyspark.sql.function
        org = org.withColumn("latLongStart", f.array("startLat","startLong"))
        org = org.withColumn("latLongEnd", f.array("endLat", "endLong"))

        return org

    def readyLoc(self, df):
        '''
        Parameter: dataframe with schema as the one returned by createDataFrame()
        
        creates dataframe with 2 columns: street name and an array with latitude and longtitude
        inserts the dataframe into the location table in the database
        '''
        try:
            #creating new dataframe from the start and end locations
            df1 =df.select("locationStart", "latLongStart")
            df1 = df1.withColumnRenamed("locationStart", "street").withColumnRenamed("latLongStart","coordinates")

            df2 =df.select("locationEnd", "latLongEnd")
            df2 = df2.withColumnRenamed("locationStart", "street").withColumnRenamed("latLongStart","coordinates")
            both = df1.union(df2)
            
            both.write.jdbc(url=self.url, table='location', mode='append', properties=self.properties).save()
        except AttributeError:
            pass

     def readyRecords(self, df):
        '''
        Parameter: dataframe with schema as the one returned by createDataFrame()
        
        creates dataframe with 4 columns: bike_id, timestamp, location in latitude and
                                          longtitude, status of the bike as boolean
        inserts the dataframe into the location table in the database
        '''
        try:
           df1 = df.select("bike_id","Starttime", "latLongStart")
           df1 = df1.withColumnRenamed("Starttime", "time").withColumnRenamed("latLongStart","loc")
           df1 =df1.withColumn("status", f.lit(False))

           df2 = df.select("bike_id","Stoptime", "latLongEnd")
           df2 = df2.withColumnRenamed("Stoptime", "time").withColumnRenamed("latLongEnd","coordinates")
           df2 =df2.withColumn("status", f.lit(True))
           both = df1.union(df2)
           both.write.jdbc(url=self.url, table='records', mode='append', properties=self.properties).save()
        except AttributeError:
            pass



    def processStream(self):
       '''
       Encodes the stream from the broker and
       sends to insert() to insert the applicable information to the applicable
       tables in the database
       '''
       #no need to encode because python3 handles it
       convert = self.stream.map(lambda x: json.loads(x[1]))
       convert.pprint()
       convert.foreachRDD(self.insert)


    def run(self):
        self.processStream()
        self.ssc.start()
        print("started spark context")
        self.ssc.awaitTermination()



if __name__ == "__main__":
    args = sys.argv
    print ("Streams args: ", args)
    topic = "kiosk"
    broker = "have to make secret"
    bikes = Streamer(topic, broker)
    bikes.run()
