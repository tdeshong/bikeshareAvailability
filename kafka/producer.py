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
                "bikeid":   {"index": 11, "type": "str"}}}

    def map_schema(self,line, schema):
        try:
            print("in the try")
            msg = line.split(schema["DELIMITER"])
            data ={}
            for key in schema["FIELDS"]:
               datatype = schema["FIELDS"][key]["type"]
               dataIndex = schema["FIELDS"][key]["index"]
               #noticed that every string came with extra quotes eg. '"X"'
               data[key]= msg[dataIndex].strip('"')

        except:
            print("whale explains the nonetype")
            return
        return data


    def producer_msgs(self):
        s3 = boto3.client('s3')
        bucket = "citibikes-data-bucket"
        prefix ="data"
        response = s3.list_objects(Bucket = bucket, Prefix = prefix)


        #get iterate through all the files
        for file in response['Contents']:
            name = file['Key'].rsplit('/', 1)
            obj = s3.get_object(Bucket=bucket, Key= prefix+'/'+name[1])
            text =  obj['Body'].read().decode('utf-8')
            text = text.split("\n")[1:]

            # get the info in the files
            for line in text:
               message = line.strip()
               msg = self.map_schema(message, self.schema)
               #convert this to bytes
               msgKey = name[1].encode('utf-8')
               # topic set up already in command line
               self.producer.send("kiosk", value =dumps(msg), key = msgKey)
               sleep(5)

if __name__ == "__main__":
    args = sys.argv
    print("sys arg values: ", sys.argv)
    ip_addr = str(args[1])
    # throught an error below
    # ValueError: invalid literal for int() with base 10: '9092,ec2-54-86-226-3.compute-1.amazonaws.com'
    # addr = ["ec2-34-226-21-253.compute-1.amazonaws.com:9092","ec2-54-86-226-3.compute-1.amazonaws.com:9092"]
    # prod =Producer(",".join(addr))
    prod = Producer("localhost:9092")
    prod.producer_msgs()
