# Introduction

A maybe little known fact is that in Apache Kafka, you cannot just alter replication factor once you've created a topic. If you want to bump up the replication factor, you have to create a JSON with a new reassignment plan and use kafka-reassign-partitions to run the plan and, in effect, modify the replication factor. Sometimes this is a little cumbersome especially if you use racks and you have lots of machines spread over the racks.

This utility calculates and executes a rack alternating broker assignment while keeping the existing leaders where they are.

# Build

```
> mvn clean install -DskipTests=true
``` 

# Usage

```
Usage: kafka-alter-rf [-ehV] [-b=<bootstrapServers>] [-c=<commandConfigFile>]
                      [-f=<file>] -r=<replicationFactor> -t=<topic>
A simply utility to alter the replication factor of a topic
  -b, --bootstrap-server=<bootstrapServers>
                        List of Kafka Bootstrap servers
                          Default: localhost:9092
  -c, --command-config=<commandConfigFile>
                        Config file containing properties like security
                          credentials, etc
                          Default:
  -e, --execute         Execute the plan
  -f, --file=<file>     File to export reassignment json to
  -fr, --force      Force reassignment even if the replication factor is met
  -h, --help            Show this help message and exit.
  -pr, --preferred-rack=<preferredRack>
                        Preferred rack for leaders
  -r, --replication-factor=<replicationFactor>
                        New replication factor
  -t, --topic=<topic>   Topic to alter replication factor on
  -V, --version         Print version information and exit.
```


# Run
> ./bin/kafka-alter-rf -b localhost:9092 -t testTopic -r 2 -c <client config file>

If you have no security on your cluster you can just run it with the bootstrap server, topic, and new replication factor. If it is a secure cluster, create a client.properties file like the example below and modify the properties based on your security mechanisms (this will support mTLS too if you provide all the ssl.* properties):

```
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
bootstrap.servers=kafka1:9091
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" \
  password="admin-secret";
```
# Assigning all the leaders to one rack

If there is a situation where all the leaders need to be assigned on one rack and the followers to the other racks, this can be done with the addition --preferred-rack (or -pr for short)
(this works for both MRC topics as well as regular topics without a replication factor set)

> ./bin/kafka-alter-rf -b localhost:9092 -t testTopic  -c <client config file> --preferred-rack us-west-1a



