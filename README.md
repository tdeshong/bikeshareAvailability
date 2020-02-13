# Bikeshare Availability \n

### DataSource

### Environment Setup

#### Cluster Setup
5 AWS EC2 instances:

- (3 nodes) Kafka Cluster and Spark Streaming
- Postgres Node
- Flask Node

#### PostgreSQL setup
The PostgreSQL database sits on its own ec2 instance. After downloading PostgresSQL database, change the configurations in postgresql.conf and pg_hba.conf to allow the other ec2 instances to access the database

### Flask
