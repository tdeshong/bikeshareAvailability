# Bikeshare Availability

The goal of this project is for bikeshare customer to have near real time information on the availability status of bikeshare docks.

This ETL uses Kafka, Spark Streaming and Postgres. The data consists of records of when bikes leave and enter docks which are in csv files stored in an S3 bucket.

The dataset is all available [citibike historical bike data](https://www.citibikenyc.com/system-data)

## Table of Contents
1. [Repo Directory Structure](README.md#Repo-Directory-Structure)
2. [Pipeline](README.md#Pipeline)
3. [Approach](README.md#Approach)
4. [Evironment SetUp](README.md#Evironment-Setup)
5. [Demo](README.md#Demo)
6. [Further Extention](README.md#Further-Extention)

    
## Pipeline

![alt text](pic/pipeline.png)

##### Kafka
producer: 
The producer ingest the unzipped citibike csv files from my s3 bucket row by row and based on the provided schema, extracts the desired information from the row. It simulates near real time information of the bikes by splitting the information in row into starting information and ending information for the bikes and sending this as two records to the broker.

##### Spark Streaming
Spark Streaming consumes 5 seconds worth messages from the broker as a dstream and distributes them amongst the workers. Each rdd in the dstream gets put into a dataframe and the information is casted based on a schema provided. There are 3 different types of dataframes that get written to 3 corresponding tables in postgres.

##### Postgres
I created a normalized database that contains a records table with columns for time, bike id and location in latitude and longitude, a location table with columns for street name and latitude and longitude and a frequency table which contains columns for the time and the number of bikes that were taken out at that time. 

##### Flask
The app.py queries the database, through the API (database.py), to see what the current status of the bike docks are and displays it on the map. If a user enters a correctly formatted location in the text bar, the app queries the bike docks available in a 3 block radius of that location.


## Environment Setup
Python version 3.5
### AWS Setup
8 AWS EC2 instances (nodes):

- (3 nodes) Kafka Cluster

         - 3 partitions and 2 replication factors
         
- (3 nodes) Spark Streaming Cluster

         - 1 master and 2 workers
         
- Postgres Node

         - Change listening address in `postgresql.conf` from local host to the IP addresses of the spark streaming nodes and changed the host postgresql is allowed to connect to in `pg_hba.conf` 

- Flask Node

        - runs the application, public facing open to the internet, different SG/rules than other clusters

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

When the customer opens the app they will see blue circles on the New York City map that represent the open bikeshare docks at that time. They also have the option of searching a location, which will display as a red circle on the map, and the available bikeshare docks in a 3 block radius of that location will appear on the map as blue dots. A flash message will appear on the screen if the customer inputs a location that is not in the format of building number street name such as 1 Park Avenue.

## Further Extention
Further extensions for this project can be replacing Spark Streaming with Structured Streaming, preprocessing the [data](README.md#Dataset) in spark and storing it in s3 before sending it through the [pipeline above](README.md#Pipeline) as well as chaos testing.
