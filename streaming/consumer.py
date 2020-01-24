from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

import json

class Streamer(object):
    #takes in a list of brokers
    def __init__(self, topic, broker):
        self.sc = SparkContent().getOrCreate()
        self.ssc = StreamingContext(self.sc,2)

        self.stream= KafkaUtils.createDirectStream(self.ssc, [topic],\
                                 {“metadata.broker.list”: broker})

    # more processing will happen at a later date
    def process_stream(self):
        self.initialize_stream()
        self.stream =(self.stream.map(lambda x:loads(x[1])))

    def run(self):
        self.process_stream()
        self.ssc.start()
        self.ssc.awaitTermination(timeout=180)


if __name__ == "__main__:
    args = sys.argv
