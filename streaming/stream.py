import sys
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json


class Streamer(object):
    '''
    Gets messages from the Kafka Brokers and splits message into two entries
    to simulate the information coming in as near real time entries and inserts
    the split messages into tables into postgres database
    '''
    def __init__(self, topic, broker, postgresaddress):
        self.sc = SparkContext()
        self.ssc = StreamingContext(self.sc,2)
        self.sc.setLogLevel("ERROR")
        #made spark bc i need it to create a dataframe
        self.spark = SparkSession.builder.master('local').getOrCreate()
        self.url = 'jdbc:postgresql://{}:5432/bikeshare'.format(postgresaddress)
        self.properties = {'driver': 'org.postgresql.Driver',
                      'user': 'ubuntu',
                      'password': '123'}
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
        #this does not work but in many examples
        #broker = self._kafkaTestUtils.brokerAddress()
        self.stream= KafkaUtils.createDirectStream(self.ssc, ["kiosk"],{"metadata.broker.list":",".join(broker)})
    
    def ready (self, anRDD, something):
       x = self.readyForDB(anRDD, something)
       self.readyLoc(x)
       self.readyRecords(x)
    
    def readyForDB(self, anRDD, something):
            org = self.spark.createDataFrame(something, self.original)

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
       #'''
        #the location database contains many duplication of location bc the producer
        #is duplicating the records .
       #'''

        try:
            #creating new dataframe from the start and end locations 
            df1 =df.select("locationStart", "latLongStart")
            df1 = df1.withColumnRenamed("locationStart", "street").withColumnRenamed("latLongStart","coordinates")

            df2 =df.select("locationEnd", "latLongEnd")
            df2 = df2.withColumnRenamed("locationStart", "street").withColumnRenamed("latLongStart","coordinates")
            both = df1.union(df2)
            #tried ignore for mode  and it did nothing to the database
            both.write.jdbc(url=self.url, table='location', mode='append', properties=self.properties).save()
        except AttributeError:
            pass
        
     def readyRecords(self, df): 
        try:
           df1 = df.select("bike_id","Starttime", "latLongStart")
#           print(df1.collect())
           df1 = df1.withColumnRenamed("Starttime", "time").withColumnRenamed("latLongStart","loc")
#           print(df1.collect())
           df1 =df1.withColumn("status", f.lit(False))
           print(df1.collect())

           df2 = df.select("bike_id","Stoptime", "latLongEnd")
           df2 = df2.withColumnRenamed("Stoptime", "time").withColumnRenamed("latLongEnd","coordinates")
           df2 =df2.withColumn("status", f.lit(True))
           both = df1.union(df2)
           print(both.collect())
           both.write.jdbc(url=self.url, table='records', mode='append', properties=self.properties).save()
        except AttributeError:
            pass
    
     def write(self, table, timing, item):
        ''' 
        function not in use
        returns boolean for if you can write to the database or not
        '''
        locExist = False
        if table =='location':
           #if throws an error put () outside the string
           if locExist:
               query = '(select * from location where "{}" = "{}") as foo'.format(timing,item)
               loc =self.spark.read.jdbc(url=self.url, table = query, properties = self.properties)
               loc.pprint()
               print(loc)
           #some conditional for if it does not have it
           #make the data

 #wrote to database it says
        #return df

    # more processing will happen at a later date
    def process_stream(self):
       #no need to encode because python3 handles it
       convert = self.stream.map(lambda x: json.loads(x[1]))
       convert.pprint()
       convert.foreachRDD(self.ready)


    def run(self):
        self.process_stream()
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
