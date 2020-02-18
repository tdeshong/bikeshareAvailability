import sys
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import Broker, KafkaUtils, OffsetRange, TopicAndPartition
from pyspark.sql.types import *
import json


class Streamer(object):
    '''
    Gets messages from the Kafka Brokers and splits message into two entries
    to simulate the information coming in as near real time entries and inserts
    the split messages into tables into postgres database
    '''
    def __init__(self, topic, broker, postgresaddress, database):
        self.sc = SparkContext()
        self.ssc = StreamingContext(self.sc,5)
        self.sc.setLogLevel("ERROR")
        self.spark = SparkSession.builder.master('spark://ec2-52-6-100-31.compute-1.amazonaws.com:7077').getOrCreate()
        self.url = 'jdbc:postgresql://{}:5432/{}'.format(postgresaddress, database)
        self.properties = {'driver': 'org.postgresql.Driver',
                      'user': 'ubuntu',
                      'password': '123'}
        #schema for the dataframe the message would go into 
        self.original = StructType([StructField("bike_id", StringType(), True), 
                                    StructField("time", StringType(), True), \
                                    StructField("location", StringType(),True),\
                                    StructField("latitude", StringType(),True),\
                                    StructField("longitude", StringType(),True),\
                                    StructField("status", BooleanType(), True)])
        
        self.stream= KafkaUtils.createDirectStream(self.ssc, [topic],{"metadata.broker.list":",".join(broker)})
    
    def insert(self, time, anRDD):
       '''
       call function that creates dataframe from the rdd in the direct stream
       call functions that will insert the information from the dataframe into location and records table
       inserts dataframe from query that counts the number of bikes that leave
       a station every 5 seconds into frequency table in postgres
       '''
       df = self.createDataframe(time, anRDD)
       self.readyLoc(df)
       self.readyRecords(df)
       
 
       try: 
           self.sqlContext.registerDataFrameAsTable(df, "temp")
           # query for selecting bikes that are starting a ride
           freq = self.sqlContext.sql("select time, count(time), status from records group by time, status having status = False")
           freq.write.jdbc(url=self.url, table='frequency', mode='append', properties=self.properties).save()
       except AttributeError:
            pass
       
       

    def createDataframe(self, time, anRDD):
        '''
        creates dataframe with the information from the incoming stream
        Adds a column which contains an array of the latitude and londitude
        coordinates 
        inserts dataframe into the database
        returns dataframe
        '''
 
        org = self.spark.createDataFrame(anRDD, self.original)

        #casting columns to the appropriate type
        org = org.withColumn("bike_id", org["bike_id"].cast(IntegerType()))
        org = org.withColumn("latitude", org["latitude"].cast(DoubleType()))
        org = org.withColumn("longitude", org["longitude"].cast(DoubleType()))

        #create extra columns with the lat and long combined in an array
        org = org.withColumn("latLong", func.array("latitude","longitude"))

        return org

    def readyLoc(self, df):
        '''
        Parameter: dataframe with schema as the one returned by createDataFrame()
        
        creates dataframe with 2 columns: street name and an array with latitude and longtitude
        inserts the dataframe into the location table in the database
        '''
        try:
            #creating new dataframe from the start and end locations
            df =df.select("location", "latLong")
            df = df.withColumnRenamed("location", "street").withColumnRenamed("latLong","coordinates")
            df.write.jdbc(url=self.url, table='location', mode='append', properties=self.properties).save()
        except AttributeError:
            pass

    def readyRecords(self, df)
        '''
        Parameter: dataframe with schema as the one returned by createDataFrame()
        creates dataframe with 4 columns: bike id, latLong, time, status
        inserts the dataframe into the records table in the database
        '''
        try: 
            df = df.select("bike_id", "time", "latLong", "status")
            df.write.jdbc(url=self.url, table='records', mode='append', properties=self.properties).save()
        except AttributeError:
            pass
        
    def processStream(self):
       '''
       Encodes the stream from the broker and
       sends to insert() to insert the applicable information to the applicable
       tables in the database
       '''
       convert = self.stream.map(lambda x: json.loads(x[1]))
       convert.foreachRDD(self.insert)


    def run(self):
        self.processStream()
        self.ssc.start()
        self.ssc.awaitTermination()



if __name__ == "__main__":
    args = sys.argv
    print ("Streams args: ", args)
    topic = "kiosk"
    broker = "have to make secret"
    bikes = Streamer(topic, broker)
    bikes.run()
