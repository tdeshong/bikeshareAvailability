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
        #this does not work but in many examples
        #broker = self._kafkaTestUtils.brokerAddress()
        #print("brokers list: ", broke)
        self.stream= KafkaUtils.createDirectStream(self.ssc, ["kiosk"],{"metadata.broker.list":",".join(broker)})

    # more processing will happen at a later date
    def process_stream(self):
        #self.stream.pprint()
        #self.stream.foreachRDD(lambda rdd:rdd.foreachPartition(self.printstream))
        convert = self.stream.map(lambda x: json.loads(x[1]))
        convert.pprint()
        #test_output.pprint()

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
