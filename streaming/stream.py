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

    
     # more processing will happen at a later date
    def process_stream(self):
           #no need to encode because python3 handles it
       convert = self.stream.map(lambda x: json.loads(x[1]))
       convert.pprint()
       convert.foreachRDD(self.readyForDB)

#        convert.write.jdbc(url=self.url, table='cititest', mode='append', properties=self.properties)
        # df.write.format('jdbc')\
        #        .option('url', self.url)\
        #        .option('dbtable', 'citi')\
        #        .option('user', 'ubuntu')\
        #        .option('password','123')\
        #        .option('driver','org.postgresql.Driver')\
        #        .mode('append').save()

    def printstream(self, stream):
        print(stream)


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
