# Bikeshare Availability

The goal of this project is for bikeshare customer to have near real time information on the availability status of bikeshare docks. 

This ETL uses Kafka, Spark Streaming and Postgres. The data consists of records of when bikes leave and enter docks which are in csv files stored in an S3 bucket.


## Environment Setup

#### Cluster Setup
5 AWS EC2 instances:

- (3 nodes) Kafka Cluster and Spark Streaming
- Postgres Node
- Flask Node

### Kafka
##### Set up
pip install
boto3; kafka-python

Create Kafka topics in the command lines
`kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor <rep-factor> --partitions <num-partitions> --topic <topic-name>`

Check the topic and the topic's paritions
`kafka-topics.sh --describe --zookeeper localhost:2181 --topic <topic-name>`


Test to make sure that consumer is getting information from the brokers which the producer sent to it
`kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic <topic-name>`

run the bash script `spawn_kafka_streams.sh` to kick off kafka producer

### Spark Streaming
Splits messages and puts them in the appropriate tables in postgres
use of jdbc to insert dataframes into postgres

#### PostgreSQL setup
installations
The PostgreSQL database sits on its own ec2 instance. After downloading PostgresSQL database, change the configurations in postgresql.conf and pg_hba.conf to allow the other ec2 instances to access the database

### Flask
installations psycopg2 for the database api
import folium
from geopy.geocoders
what is happening in that file


