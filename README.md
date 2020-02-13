# Bikeshare Availability
-----------------
This project aims to provide bikeshare customers the status of bikeshare docks in near real time.

When a bike enters and exits a dock that is the information that my pipeline collects in real time.

Pipeline
-----------------


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

## Repo directory structure

The directory structure for the repo should look like this:

    ├── README.md
    ├── frontend
    │   └── frontend.py
    ├── ingestion
    │   └── ingest.sh
    ├── postgres
    │   └── brand_product.csv
    └── spark
        ├── built.sbt
        ├── src
        │   └── main
        │       └── scala
        │           └── etl.scala
        └── target
            └── scala-2.11
                ├── etl_2.11-1.0.jar
                └── postgresql-42.2.9.jar
