# kafka-consumer

This is a simple java consumer for a kafka cluster that is running locally. It takes in 
the argument through command line to consume messages. 
Build it using mvn clean package at the root level. Example to pull data is 
java -jar kafka-consumer-1.0.jar "<required_groupId>" "<required_topic_name>" "<optional_partition>"


# Kafka Cluster Instruction
kafka_cluster.yaml file contains a zoo keeper and a kafka cluster image that can be started 
through docker.  

# Bring up Kafka cluster 
docker-compose -f kafka_cluster.yaml up -d 
docker ps
# Bring down Kafka cluster
docker-compose -f kafka_cluster.yaml down

# Create a topic
docker exec -it kafka /bin/sh
cd /opt/kafka/bin
# Create a topic by mentioning. Replace my_first_topic with the topic name and partitions can be changed too. Other properties can't be changed
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic my_first_topic
# List all the topics
kafka-topics.sh --list --bootstrap-server localhost:9092