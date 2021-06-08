# Springboot 1.5: Spring Batch Remote + Local Partitioning with Kafka
A cloud ready app for extreme batch processing using N servers and N threads for each server. I have achieved Remote + Local partitioning using Kafka.
<br>eg. you can run this app on 4 servers and each server can have 8 threads. So total 32 threads are processing the batches in parallel.

### How to run
1. From kafka folder run following commands
    <br>a) .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
    <br>b) .\bin\windows\kafka-server-start.bat .\config\server.properties
    <br>c) .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic migration-topics
2. Since it's using h2 db. You can start the application and go to localhost:8080/h2-console and add following details, then click on connect.
<br>Driver class: org.h2.Driver
<br>jdbcURL: jdbc:h2:./data/sbremote
<br>username: sa
<br>pwd: blank
3. Run the SQL script provided in data.sql file.
4. Using intelliJ you can run the application parallely by providing different ports in the variables.
5. just open http://localhost:8080 to start the job.

### Misc
1. Added Custom KafkaPartitioner to evenly distribute batch partitioning messages to all available partitions.

### Pending Tasks
1. It is working with polling mechanism, need to try with aggregating from following example.
https://github.com/spring-projects/spring-batch/tree/master/spring-batch-samples/src/main/java/org/springframework/batch/sample/remotepartitioning/aggregating