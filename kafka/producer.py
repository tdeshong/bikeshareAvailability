import boto3
import sys
from time import sleep
from json import dumps
from kafka import KafkaProducer

class Producer(object):
    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers =[addr], \
                                  value_serializer = lambda x: dumps(x).encode('utf-8'))

        self.schema = {
            "DELIMITER":  ",",
            "FIELDS":
            {
                "tripduration": {"index": 0, "type": "str"},
                "starttime":   {"index": 1, "type": "str"},
                "stoptime": {"index": 2, "type": "str"},
                "start station":  {"index": 4, "type": "str"},
                "end station":   {"index": 8, "type": "str"},
                "bikeid":   {"index": 11, "type": "str"}
            }
        }

    def map_schema(line, schema):
    """
    cleans the message msg, leaving only the fields given by schema, and casts the appropriate types
    returns None if unable to parse
    :type line  : str       message to parse
    :type schema: dict      schema that contains the fields to filter
    :rtype      : dict      message in the format {"field": value}
    """
        try:
            msg = line.split(schema["DELIMITER"])
            msg = {key:eval("%s(\"%s\")" % (schema["FIELDS"][key]["type"],
                                    msg[schema["FIELDS"][key]["index"]]))
                        for key in schema["FIELDS"].keys()}
        except:
            return
        return msg

    def producer_msgs(self):
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket="citibikes-data-bucket", Key= ""data/201502-citibike-tripdata.csv"")
        text =  obj['Body'].read().decode('utf-8')
        #starts after the headers in the csv
        text = text.split("\n")[1:]

        for line in text:
           message = line.strip()
           msg = map_schema(message, self.schema)
           self.producer.send("kiosk", value =dumps(msg)) #, key=self.get_key(msg))
           sleep(5)

if __name__ == "__main__":
    args = sys.argv
    print("sys arg values: ", sys.argv)
    ip_addr = str(args[1])
    # prod =Producer(ip_addr)
    prod = Producer("localhost:9092")
    prod.producer_msgs()

