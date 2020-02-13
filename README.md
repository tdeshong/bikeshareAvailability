# Bikeshare Availability

The goal of this project is for bikeshare customer to have near real time information on the availability status of bikeshare docks. 

This ETL uses Kafka, Spark Streaming and Postgres. The data consists of records of when bikes leave and enter docks which are in csv files stored in an S3 bucket.


### Environment Setup

#### Cluster Setup
5 AWS EC2 instances:

- (3 nodes) Kafka Cluster and Spark Streaming
- Postgres Node
- Flask Node

### Kafka
`kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor <rep-factor> --partitions <num-partitions> --topic <topic-name>`

`kafka-topics.sh --describe --zookeeper localhost:2181 --topic <topic-name>`

`kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic <topic-name>`

### Spark Streaming
installations
what is happening in spark streaming

#### PostgreSQL setup
installations
The PostgreSQL database sits on its own ec2 instance. After downloading PostgresSQL database, change the configurations in postgresql.conf and pg_hba.conf to allow the other ec2 instances to access the database

### Flask
installations
what is happening in that file


