import sys
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import json


class Streamer(object):
    #figure out how to use session with kafkautils
    #takes in a list of brokers
    def __init__(self, topic, broker):
        self.sc = SparkContext()
        self.ssc = StreamingContext(self.sc,2)
        self.sc.setLogLevel("ERROR")
        #made spark bc i need it to create a dataframe
        self.spark = SparkSession.builder.master('local').getOrCreate()
        self.url = 'jdbc:postgresql://ec2-18-210-209-145.compute-1.amazonaws.com:5432/bikeshare'
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
            
            #x = org.collect()[0][10]
            #self.write('location','latLongStart', x)
            #print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            #print(org.collect()[0][11])
            #print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            #if self.write('location','latLongStart'
            #read the databse to see if these things are already in there
            #what is item bc several things need to be checked
            # if self.write('location',item):
               # df =org.select("locationStart", "latLongStart")
               #change name of the columns
               # df=df.selectExpr("locationStart as street", "latLongStart as coordinates")
               # df.write.jdbc(url=self.url, table='location', mode='append', properties=self.properties).save()


            org.write.jdbc(url=self.url, table='test3', mode='append', properties=self.properties).save()
        except AttributeError:
            pass
    
     def write(self, table, timing, item):
        ''' returns boolean for if you can write to the database or not
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
    broker = ["ec2-34-226-21-253.compute-1.amazonaws.com:9092","ec2-54-86-226-3.compute-1.amazonaws.com:9092"]
    #broker =["ip-10-0-0-9:9092","ip-10-0-0-11:9092", "ip-10-0-0-4:9092"]
    # bikes = Streamer(topic)
    bikes = Streamer(topic, broker)
    bikes.run()
