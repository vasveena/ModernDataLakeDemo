# ModernDataLakeDemo
Demo using AWS MSK + EMR Spark Streaming

Create an EC2 instance and run below commands:

sudo yum install java-1.8.0
wget https://archive.apache.org/dist/kafka/2.2.1/kafka_2.12-2.2.1.tgz
tar -xzf kafka_2.12-2.2.1.tgz

Install python3 and pip. Pip install following packages as root user. Make sure pip points to python3.

sudo pip3 install protobuf
sudo pip3 install requests
sudo pip3 install kafka-python
sudo pip3 install --upgrade gtfs-realtime-bindings
sudo pip3 install underground
sudo pip3 install pathlib

Create Kafka cluster:
https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html
For simplicity, disable TLS client encryption and accept both TLS encrypted and plaintext traffic
You can enable in-cluster encryption - both at-rest and in-transit

Create Kafka topic:
https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html

Following are the commands to create Kafka topic and obtain bootstrap brokers to write Producer

aws kafka describe-cluster --region us-east-1 --cluster-arn arn:aws:kafka:us-east-1:620614497509:cluster/test/2dbb304e-79fe-4beb-a02d-35e0fea4524b-2 -> copy the "ZookeeperConnectString" from returned JSON

bin/kafka-topics.sh --create --zookeeper "<zookeeper string>" --replication-factor 3 --partitions 1 --topic trip_update_topic
bin/kafka-topics.sh --create --zookeeper "<zookeeper string>" --replication-factor 3 --partitions 1 --topic vehicle_topic

aws kafka get-bootstrap-brokers --cluster-arn "arn:aws:kafka:us-east-1:620614497509:cluster/test/2dbb304e-79fe-4beb-a02d-35e0fea4524b-2" -> copy the bootstrap servers string

Copy the train_arrival_producer.py into an EC2 instance and modify the bootstrap servers

Get an MTA API key by creating an account here - https://api.mta.info/#/landing
Export this key in the EC2 instance

export MTA_API_KEY=xxxx

On the same terminal, run train_arrival_producer.py -> python3 train_arrival_producer.py

From a different terminal, ssh into the same EC2 instance and run the console Kafka consumer to verify the data is getting ingested.

bin/kafka-console-consumer.sh --bootstrap-server "<bootstrap string>" --topic trip_update_topic
bin/kafka-console-consumer.sh --bootstrap-server "<bootstrap string>" --topic vehicle_topic

Create an EMR cluster (stop the kafka ingestion if needed before running steps below)
SSH into EMR cluster, navigate to "/usr/lib/spark/jars/" and get the dependencies from maven repo

cd /usr/lib/spark/jars/
sudo wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.0.1/spark-streaming-kafka-0-10_2.12-3.0.1.jar
sudo wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.1/spark-sql-kafka-0-10_2.12-3.0.1.jar
sudo wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.2.1/kafka-clients-2.2.1.jar
sudo wget https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.0.0-preview2/spark-streaming-kafka-0-10-assembly_2.12-3.0.0-preview2.jar
sudo wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

Invoke Spark shell:

spark-shell

Copy and paste the train_arrival_consumer.scala contents directly into spark-shell

Once Spark streaming job runs, make sure the files are getting deposited into S3 data lake

After this, create a Redshift cluster and connect to it using SQL Workbench or any other means with Redshift JDBC drivers.

Create Redshift spectrum table on top of this S3 data and run some explorative queries. Examples are in the redshift_spectrum_queries folder

Create a data source in Quicksight with this Redshift spectrum table and build some visualizations and live dashboards
