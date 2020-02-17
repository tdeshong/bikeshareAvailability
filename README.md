# Bikeshare Availability

The goal of this project is for bikeshare customer to have near real time information on the availability status of bikeshare docks.

This ETL uses Kafka, Spark Streaming and Postgres. The data consists of records of when bikes leave and enter docks which are in csv files stored in an S3 bucket.


## Table of Contents
1. [Repo Directory Structure](README.md#Repo-Directory-Structure)
2. [Pipeline](README.md#Pipeline)
3. [Approach](README.md#Approach)
4. [Evironment SetUp](README.md#Evironment-Setup)
5. [Demo](README.md#Demo)
6. [Further Extention](README.md#Further-Extention)




## Repo Directory Structure

    ├── README.md
    ├── flask
    │   └── app.py
    │   └── database.py
    │   ├── templates
    │         └── index.html
    ├── kaka
    │   └── producer.py
    │   └── startProducer.sh
    │  
    └── streaming
        └── stream.py
    
## Pipeline

![alt text](pic/pipeline.png)

## Approach

### Dataset

## Environment Setup
python 3.5
### Cluster Setup
8 AWS EC2 instances:

- (3 nodes) Kafka Cluster
- (3 nodes) Spark Streaming Cluster
- Postgres Node
- Flask Node

### Security Groups
The Kafka Cluster and the Spark Streaming Cluster use the computing security group which has traffic from jumphost, itself, ssh and postgres database. The computing security group also has ports open for the Spark Web UI and to the flask security group. The Postgres Node is has ports open to the postgresql, itself, jumphost, computing and ssh. The flask node is open to port 5000, 80 for http and 443 for https and ssh.

### Kafka Setup
pip install `boto3` and `kafka-python`

Create Kafka topics using this command

`kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor <rep-factor> --partitions <num-partitions> --topic <topic-name>`

Check that Kafka topic and partitions are as expected using this command 

`kafka-topics.sh --describe --zookeeper localhost:2181 --topic <topic-name>`

Used this command lne to test if Kafka consumer was receiving the messages from the Kafka producer before connecting Spark Streams to Kafka producer

`kafka-console-consumer.sh --zookeeper localhost:2181 --topic <topic-name>`

run the bash script `startProducer.sh` to kick off kafka producer

### Spark Streaming Setup
Add JDBC driver to spark class path

`bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar`

### PostgreSQL Setup
Change listening address in `postgresql.conf` from local host to the IP addresses that you would like it to listen to

Changed the hosts postgresql is allowed to connect to in `pg_hba.conf` by adding this line to the file

` host    <database>      <user>       0.0.0.0/0        md5`

### Flask Setup
pip install `psycopg2` to access postgres database

pip install `folium` for map in the html

pip install `geopy.geocoders` to convert addresses in number street name form to latitude and longditude

## Demo
[Demo](https://www.youtube.com/watch?v=QS-lSPjHsqQ)

When the customer opens the app they will see blue circles on the New York City map that represent the open docks at that time. They also have the option of searching a location, which will display as a red circle on the map, and blue circles, that represent open docks, in a 3 block radius of the input location would show up on the map. Also there is a flash message if the format of the location is not in the expected format. 

## Further Extention
Further extensions for this project can be replacing Spark Streaming with Structured Streaming, preprocessing the [data](README.md#Dataset) in spark and putting it in s3 before sending it throught the [pipeline](README.md#Pipeline) as well as choas testing.
