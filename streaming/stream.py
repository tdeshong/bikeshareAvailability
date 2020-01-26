import pyspark_cassandra
import pyspark_cassandra.streaming

from pyspark_cassandra import CassandraSparkContext
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
        self.sc = SparkContent().getOrCreate()
        self.ssc = StreamingContext(self.sc,2)
        self.sc.setLogLevel("ERROR")

        self.stream= KafkaUtils.createDirectStream(self.ssc, "kiosk",\
                                 {“metadata.broker.list”: broker})

    # more processing will happen at a later date
    def process_stream(self):
        self.initialize_stream()
        convert = self.stream.map(lambda x: x[1])
        convert = self.stream.map(lambda x: loads(x[1]))
        test_output.pprint()


    def run(self):
        self.process_stream()
        self.ssc.start()
        self.ssc.awaitTermination(timeout=180)


if __name__ == "__main__":
    args = sys.argv
    print ("args: ", args)
    # bike = Streamer("kiosk", broker)
    # bike.run()
