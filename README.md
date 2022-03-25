# Introduction

A maybe little known fact is that in Apache Kafka, you cannot just alter replication factor once you've created a topic. If you want to bump up the replication factor, you have to create a JSON with a new reassignment plan and use kafka-reassign-partitions to run the plan and, in effect, modify the replication factor. Sometimes this is a little cumbersome especially if you use racks and you have lots of machines spread over the racks.

This utility calculates and executes a rack alternating broker assignment while keeping the existing leaders where they are.

# Build

```
> mvn clean install -DskipTests=true
``` 

# Run
> java -jar target/kafka-alter-rf-0.0.1-SNAPSHOT.jar -b localhost:9092 -t testTopic -r 2 -c <client config file>

If you have no security on your cluster you can just run it with the bootstrap server, topic, and new replication factor.


