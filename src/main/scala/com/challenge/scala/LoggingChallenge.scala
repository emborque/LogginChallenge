package com.challenge.scala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import net.liftweb.json._
import com.redis.RedisClient
import java.util.HashMap


object LoggingChallenge {

  //Suppress Spark output
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  implicit val formats = DefaultFormats
  case class inputmessaje(CustomerId: String, BrandName:String, Timestamp:String, IsSuccessful:Boolean)

  // Split the fields of the JSON message
  def JSON_parser(json: String): (String,Boolean, String) = {
    val parsedJson = parse(json)
    val m = parsedJson.extract[inputmessaje]
    (m.CustomerId, m.IsSuccessful, m.Timestamp)
  }

  // Compose output JSON message
  def JSON_message_generator (customerId:String, timestamp:String): String = {
    val json = "{\"CustomerId\":\""+customerId+"\",\"EventName\":\"Failed3ConsecutiveLogins\",\"Timestamp\":\""+timestamp+"\"}"
    json
  }

  // Extract the value of an Option field as an int
  def redistoInt (num:Any):Int = num match {
    case Some(x:String) => x.toInt
    case _ => 0
  }

  // Get the value of the ID stored in Redis.
  def checkRedis (redis:RedisClient, value:(String,Boolean,String)):(String, Boolean,String,Int)={
    val iD = value._1
    val isSuccesful = value._2
    val timestamp = value._3

    val times = redis.get(iD) // Get redis value
    (iD, isSuccesful, timestamp, redistoInt(times) + 1) // Add one occurence
  }

  // Update the value of the ID occurrences in Redis after the new message
  def updateRedis (redis: RedisClient, value: (String, Boolean, String, Int)):(String,String,Boolean)={
    val iD = value._1
    val isSuccesful = value._2
    val timestamp = value._3
    val times = value._4

    isSuccesful match {
      case true =>
        redis.set(iD,0) // Reset the counter to 0
        (iD, timestamp, false)

      case false =>
        times match {
          case 0 => (iD, timestamp, false)
          case 1 | 2 =>
            redis.set(iD,times) // Increase the counter
            (iD, timestamp, false)

          case _ =>
            redis.set(iD,0) // Reset the counter to 0
            (iD, timestamp, true)
        }
    }
  }


  def main(args: Array[String]) {
    println("Logging congrol!!!")

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("LoggingChallenge")

    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaBrokers = "localhost:9092"
    val zkQuorum="localhost:2181"
    val group = "my-consumer-group"
    val topic = {"CustomerLogins"}
    val topicMap = topic.split(",").map((_, 1)).toMap
    val kafkastream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val id = kafkastream
      .map(JSON_parser)
      .mapPartitions{partition =>
        val redis_client = new RedisClient("localhost", 6379)

        val loggins_set = partition.map(loggin => checkRedis (redis_client,loggin)) // Check the stored counter
                                   .map(loggin => updateRedis(redis_client,loggin))// Update that counter

        loggins_set
      }
      .filter (_._3==true)  // Only "true" tuples contains wrong 3rd loggins that have to be sent
      .map(message => JSON_message_generator(message._1, message._2))

      .mapPartitions( partition => { // Kafka producer
        val kafkaOpTopic = "ConsumerTopic”"
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)
        val new_messages = partition.map( record => {
            //println("New Message")
            //println (record)

            val data = record.toString
            val message = new ProducerRecord[String, String](kafkaOpTopic, null, data)
            producer.send(message)
        } )
        producer.close()

        new_messages
      })

    id.print ()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}