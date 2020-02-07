import boto3
import sys
from time import sleep
from json import dumps
from kafka import KafkaProducer

class Producer(object):
    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers =[addr], \
                                  value_serializer = lambda x: dumps(x).encode('utf-8'))

        self.fields = ['bike_id', 'starttime', 'stoptime', 'start station',
                       'startLat', 'startLong', 'end station','endLat', 'endLong', 'tripduration']
        self.schema = {
            "DELIMITER":  ",",
            "FIELDS":
            {
                "tripduration":{"index":0, "type": "str"},
                "starttime":   {"index": 1, "type": "str"},
                "stoptime": {"index": 2, "type": "str"},
                "start station":  {"index": 4, "type": "str"},
                "startLat" :{"index":5, "type": "str"},
                "startLong": {"index":6,  "type": "str"},
                "endLat": {"index":9,  "type": "str"},
                "endLong":{"index":10, "type": "str"},
                "end station":   {"index": 8, "type": "str"},
                "bike_id":   {"index": 11, "type": "int"}}}

    def map_schema(self,line, schema):
        try:
            msg = line.split(self.schema["DELIMITER"])
            print ("msg: ", msg)
            data =[]
            for key in self.fields:
               print("key: ", key)
               datatype = self.schema["FIELDS"][key]["type"]
               dataIndex = self.schema["FIELDS"][key]["index"]
               #noticed that every string came with extra quotes eg. '"X"'
               print("the info: ", msg[dataIndex])
               info = msg[dataIndex].strip('"')
               print("stripped info: ", info)
               data.append(info)

                #msg = {key:eval("%s(\"%s\")" % (schema["FIELDS"][key]["type"],
                 #                       msg[schema["FIELDS"][key]["index"]]))
                  #          for key in schema["FIELDS"].keys()}
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
    addr = "put in secrets the 1 broker"
    prod =Producer(addr)
    prod.producer_msgs()
