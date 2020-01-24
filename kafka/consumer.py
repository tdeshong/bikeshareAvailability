import pyspark_cassandra
import pyspark_cassandra.streaming

from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

import json



def Consumer(object):
    def __init__(self, name,topic, broker):
        sc = CassandraSparkContext(conf=conf)
        sql = SQLContext(sc)
        ssc = StreamingContext(sc, 1)
        # sc = SparkContent(appName = name)
        # ssc = StreamingContext(sc,2)
        kafkaStream= KafkaUtils.createDirectStream(ssc, [topic],\
                                 {“metadata.broker.list”: brokers})


    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    #after this line is where you do your processing
    #variable name for this is processing is summed
    #then the thing summed you can save to Cassandra
    #but figure out when/where to create keyspace and make table
    summed.saveToCassandra(KEYSPACE, TABLENAME)


# need this to start the streams
ssc.start()
ssc.awaitTermination(timeout=180)

if __name__ == "__main__:
    args = sys.argv
