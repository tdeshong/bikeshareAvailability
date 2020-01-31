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
        self.url = 'jdbc:postgresql://localhost:5432/'
        self.properties = {'driver': 'org.postgresql.Driver',
                      'user': 'postgres',
                      'password': ''}
        #this does not work but in many examples
        #broker = self._kafkaTestUtils.brokerAddress()
        self.stream= KafkaUtils.createDirectStream(self.ssc, ["kiosk"],{"metadata.broker.list":",".join(broker)})
    
    def readyForDB(self, anRDD, something):
        schema1 = StructType([StructField("bike_id", IntegerType(), False),\
                             StructField("locationStart", StringType(), True), \
                             StructField("locationEnd", StringType(), True),\
                             StructField("Starttime", TimestampType(),True),\
                             StructField("Stoptime", TimestampType(),True),\
                             StructField("tripduration", IntegerType(), True)])

        schema = StructType([StructField("bike_id", IntegerType(), True),\
                             StructField("location", StringType(), True), \
                             StructField("time", TimestampType(),True),\
                             StructField("out", BooleanType(), True)])

        df = self.spark.createDataFrame(something, schema1)
        print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
        df.write.jdbc(url=self.url, table='test', mode='append', properties=self.properties)
        #wrote to database it says
        return df
    
    # more processing will happen at a later date
    def process_stream(self):
        #self.initialize_stream()
        convert = self.stream.map(lambda x: json.loads(x[1]))
        convert.pprint()
        #convert.foreachRDD(print)
        convert.foreachRDD(self.readyForDB)
        #self.sc.parallelize(convert).toDF().foreachPartition(printstream)
        #convert.foreachRDD(lambda rdd: self.readyForDB(rdd))
        # print("convert type: ", type(convert))
        # print("one convert type: ", convert[0])
        #convert should be a dictionary so the information should be associated with the 
        # key in each  one
        #split the dictionary
        #col = ["id", "bike_id", "location", "out"]
        #rdd  =sc.parallelize(convert)
        #test_output.pprint()
        # convert = self.stream.map(lambda x: json.loads(x[1]))
        # convert.pprint()
        # splitInfo = convert.map(lambda i: (i["bikeid"], i["start station"], i["starttime"], True)\
#                                         (i["bikeid"], i["end station"], i["stoptime"], False))
        # print("splitInfo: ", splitInfo)
        # print("**************************************")
        #splitInfo=[]
        #for i in convert:
        #    splitInfo.append(i["bikeid"], i["start station"], i["starttime"], True)
         #   splitInfo.append(i["bikeid"], i["end station"], i["stoptime"], False)

        # rdd =self.sc.parallelize(splitInfo)
        # df = self.readyForDB(rdd)
        # df.write.jdbc(url=url, table='bikes.data', mode='append', properties=properties)

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
