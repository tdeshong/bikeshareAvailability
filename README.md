# Bikeshare Availability

The goal of this project is for bikeshare customer to have near real time information on the availability status of bikeshare docks. 

This ETL uses Kafka, Spark Streaming and Postgres. The data consists of records of when bikes leave and enter docks which are in csv files stored in an S3 bucket.

## Repo directory structure

    ├── README.md
    ├── flask
    │   └── app.py
    │   └── database.py
    │   ├── templates
    │         └── index.html
    ├── kaka
    │   └── producer.py
    │   └── spawn_kafka_streams.sh
    │  
    └── streaming
        └── stream.py

## Environment Setup
python 3.5
### Cluster Setup
5 AWS EC2 instances:

- (3 nodes) Kafka Cluster and Spark Streaming
- Postgres Node
- Flask Node

### Kafka Setup
pip install `boto3` and `kafka-python`

Create Kafka topics using this command line

`kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor <rep-factor> --partitions <num-partitions> --topic <topic-name>`

And use this command line to check that Kafka topic and partitions are as expected

`kafka-topics.sh --describe --zookeeper localhost:2181 --topic <topic-name>`

Used this command lne to test if Kafka consumer was receiving the messages from the Kafka producer before connecting Spark Streams to Kafka producer

`kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic <topic-name>`

run the bash script `spawn_kafka_streams.sh` to kick off kafka producer

### Spark Streaming Setup
Splits messages and puts them in the appropriate tables in postgres
use of jdbc to insert dataframes into postgres

### PostgreSQL Setup
Change listening address in `postgresql.conf` from local host to the IP addresses that you would like it to listen to

Changed the hosts postgresql is allowed to connect to in `pg_hba.conf` by adding this line to the file

` host    <database>      <user>       0.0.0.0/0        md5`

### Flask Setup
pip install `psycopg2` for the database api to query from postgres

pip install `folium` for map in the html

pip install `geopy.geocoders` to convert addresses in number street name form to latitude and longditude

