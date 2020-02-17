import boto3
import sys
from json import dumps
from kafka import KafkaProducer

class Producer(object):
    '''
    Creates a Kafka Producer object and sends rows of information from csv
    files in s3 bucket to Kafka consumer
    '''
    def __init__(self, addr):
        '''
        addr --> list of public ip addresses
        '''
        self.producer = KafkaProducer(bootstrap_servers =[addr], \
                                  value_serializer = lambda x: dumps(x).encode('utf-8'))
        
        #debating if this should be arguments for the Prducer object or just extra information provided
        #later for map_schema
        self.fields = ['bike_id', 'starttime', 'stoptime', 'start station','startLat',
                       'startLong', 'end station','endLat', 'endLong']
        self.schema = {
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

    def map_schema(self,line, schema, fields):
        '''
        Parameters: line   --> row of csv as a string
                    schema --> see __init__ for details
                    fields --> see __init__ for details

        Returns: list of desired columns from row

        schema --> dictionary with schema
                     { "DELIMITER": delimiter as str
                       "FIELDS"   : {"column name1": {"INDEX": number as int}
                                     "column name2": {"INDEX": number as int}
                                     ...
                                    }
                     }
        field --> list of field keys in the desired order that the information
                  should be sent to the consumer
        '''
        try:
            msg = line.split(schema["DELIMITER"])
            data =[]
            for key in fields:
               datatype = schema["FIELDS"][key]["type"]
               dataIndex = schema["FIELDS"][key]["index"]
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
               msgKey = name[1].encode('utf-8')
               self.producer.send(topic, value=dumps(msg), key =msgKey)
               sleep(5)

if __name__ == "__main__":
    args = sys.argv
    print("sys arg values: ", sys.argv)
    ip_addr = str(args[1])
    addr = "put in secrets the 1 broker"
    prod =Producer(addr)
    prod.producer_msgs()
