package day15

import java.util.{Collections, Properties}

//import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecords, KafkaConsumer}

import scala.xml.Properties

object KafkaOperator {
  def main(args: Array[String]): Unit = {
    /*
    *        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>1.1.0</version>
        </dependency>
            val props = new Properties()

    props.put("bootstrap.servers", "itcast02:9092,itcast03:9092")
    props.put("group.id", "CountryCounter")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)

    val consumerConfig = new ConsumerConfig(props)
    //val consumer = Consumer.Create(consumerConfig)

    consumer.subscribe(Collections.singletonList("customerCountries"))

    try
        while ( {
          true
        }) { //1)
          val records = consumer.poll(100) //2)
          import scala.collection.JavaConversions._
          for (record <- records) { //3)
            println(record.value)
          }
        }
    finally consumer.close() //4

     */
    /**
      * val topicCountMap = Map(topic -> 1)
      * val consumerMap = consumer.createMessageStreams(topicCountMap)
      * val streams = consumerMap.get(topic).get
      * for (stream <- streams) {
      * val it = stream.iterator()
      *
      * while (it.hasNext()) {
      * val messageAndMetadata = it.next()
      *
      * val message = s"Topic:${messageAndMetadata.topic}, GroupID:$groupid, Consumer ID:$consumerid, PartitionID:${messageAndMetadata.partition}, " +
      * s"Offset:${messageAndMetadata.offset}, Message Key:${new String(messageAndMetadata.key())}, Message Payload: ${new String(messageAndMetadata.message())}"
      *
      *         System.out.println(message);
      *          }
      *              }
      */
  }
}
