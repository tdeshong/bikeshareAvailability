import boto3
import sys
from time import sleep
from json import dumps
from kafka import KafkaProducer

class Producer(object):
    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers =[addr], \
                                  value_serializer = lambda x: dumps(x).encode('utf-8'))

        self.schema = {"tripduration"
        
        }

        
    def producer_msgs(self):
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket="citibikes-data-bucket", Key= "test/kiosk.csv")
        print ("the object in the s3: " , obj)
        print ("***************************************************")
        # read csv into bytes?
        # obj.get()['Body'].read().decode('utf-8')
        text =  obj['Body'].read().decode('utf-8')

        for i in range(10):
           print("line test: ", text[i])
           self.producer.send("kiosk", value=text[i])
           sleep(5)

        # for line in text:
             # message = line.strip().split(",")
             #  self.producer.send("kiosk", value =line )
             
if __name__ == "__main__":
    args = sys.argv
    print("sys arg values: ", sys.argv)
    ip_addr = str(args[1])
    # prod =Producer(ip_addr)
    prod = Producer("localhost:9092")
    prod.producer_msgs()

