import boto3
import sys
from json import dumps
from kafka import KafkaProducer

class Producer(object):
    '''
    Creates a Kafka Producer object and sends rows of information from csv
    files in s3 bucket to Kafka consumer
    '''
    def __init__(self, addr, schema, fields):
        '''
        addr --> list of public ip addresses
        schema --> dictionary with schema
                     { "DELIMITER": delimiter as str
                       "FIELDS"   : {"column name1": {"INDEX": number as int}
                                     "column name2": {"INDEX": number as int}
                                     ...
                                    }
                     }
        fields --> list of field keys in the desired order that the information
                  should be sent to the consumer
        '''
        self.producer = KafkaProducer(bootstrap_servers =[addr], \
                                  value_serializer = lambda x: dumps(x).encode('utf-8'))
        
        self.schema = schema
        self.fields = fields
        

    def map_schema(self,line):
        '''
        Parameters: line   --> row of csv as a string
        Returns: list of desired columns from row
        '''
        try:
            msg = line.split(self.schema["DELIMITER"])
            data =[]
            for key in self.fields:
               datatype = self.schema["FIELDS"][key]["type"]
               dataIndex = self.schema["FIELDS"][key]["index"]
               #strip items of extra quotations
               info = msg[dataIndex].strip('"')
               data.append(info)
        except:
            return
        return data


    def producer_msgs(self, bucket, prefix, topic):
        '''
        Gets all the files from bucket/prefix and sends each row as a messages
        with the desired schema to the consumer
        '''

        s3 = boto3.client('s3')
        response = s3.list_objects(Bucket = bucket, Prefix = prefix)


        #get iterate through all the files
        for file in response['Contents']:
            name = file['Key'].rsplit('/', 1)
            obj = s3.get_object(Bucket=bucket, Key= prefix+'/'+name[1])
            text =  obj['Body'].read().decode('utf-8')
            #skips row with header
            text = text.split("\n")[1:]

            # get the info in the files
            for line in text:
               message = line.strip()
               msg = self.map_schema(message, self.schema,self.fields)
               # split the row into 2 different messages
               msgStart = [msg[0], msg[1], msg[3], msg[4], msg[5]]
               msgEnd = [msg[0], msg[2], msg[6], msg[7], msg[8]]
               msgKey = name[1].encode('utf-8')
               
               self.producer.send(topic, value=dumps(msgStart), key =msgKey)
               self.producer.send(topic, value=dumps(msgEnd), key =msgKey)


if __name__ == "__main__":
    args = sys.argv
    fields = ['bike_id', 'starttime', 'stoptime', 'start station','startLat','startLong', 'end station','endLat', 'endLong']
    schema = {
            "DELIMITER":  ",",
            "FIELDS":
            {
                "starttime":   {"index": 1, "type": "str"},
                "stoptime": {"index": 2, "type": "str"},
                "start station":  {"index": 4, "type": "str"},
                "startLat" :{"index":5, "type": "str"},
                "startLong": {"index":6,  "type": "str"},
                "endLat": {"index":9,  "type": "str"},
                "endLong":{"index":10, "type": "str"},
                "end station":   {"index": 8, "type": "str"},
                "bike_id":   {"index": 11, "type": "int"}}}
    
    addr = "put in secrets the 1 broker"
    prod =Producer(addr)
    prod.producer_msgs()
