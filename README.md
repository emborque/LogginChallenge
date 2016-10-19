# LogginChallenge


In order to develop my solution I have used 3 main technologies:

##Apache Kafka:

As suggested in the statement Apache Kafka is the messaging system used. 

The messages are consumed in Spark creating a stream with all the messages in the "CustomerLogins” topic. I assume that in the data stream there are 		succesful and wrong login tries.

At the final step of the process, the stream of messages only contains those which are third consecutive wrong logging. This messages, with their timestamp, 		will be send to a new Kafka topic by a Kafka Producer.

##Spark Streaming:

I have used Spark as engine for data processing. Being a stream of data Spark Streaming was the needed library. I used it because I have previous experience 		working with that framework, but Flink could also been a proper option providing a better CEP features.
I choose Spark 1.6 version because the Kafka libraries have no compatibility with later versions (2.0).  As a coding language I used Scala , in 2.11 version 		which is needed by the Redis client. 

The micro-batch is defined to 1 second, time in which the user only can make one login attempt. 

##Redis:

For the storage of the login counter Redis is the technology selected. I have chosen that technology for 3 different reasons:
1. Provides high availability and scalability.
2. It is an in-memory database providing a great performance in the request, something crucial in streaming environments.
3. The key-value structure fits perfectly for storing and Id and his counter.

##Other considerations:

Al the code is parameterized for a Local Standalone execution but it could be modified for a cluster execution. In that case some parameter should be 		provided at the time of the submission of the job.

Running Kafka with various partitions in the topic will need to configure Spark with a consumer group. Doing it the same id messages would always be consumed 		by the same machine.

In case of fails, both Kafka and Redis can persist the information.
Kafka provides data loss protection with the replica of the partitions. Redis can be also configured with a master/slave replication system.
