package com.astro.sparkTest

import org.apache.kafka.common.TopicPartition
import kafka.message.MessageAndMetadata
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OffsetApp {

  def main(args: Array[String]): Unit = {

    val sc = new SparkConf().setMaster("local[2]").setAppName("OffsetApp")
    val ssc = new StreamingContext(sc,Seconds(5))

    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
              "key.deserializer" -> classOf[StringDeserializer],
              "value.deserializer" -> classOf[StringDeserializer],
              "group.id" -> "test-consumer-group",
//              "request.required.acks" -> "1",
              "auto.offset.reset" -> "latest"
    )
    val topics = Set("test")


    val fromOffsets = Map[TopicPartition,Long]()
    val MessagerHandler = (mm:MessageAndMetadata[String,String])
    => (mm.key(),mm.message())
    // TODO ...  业务逻辑

    if(fromOffsets.size == 0){
      KafkaUtils.createDirectStream[String,String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    }
    else{
      KafkaUtils.createDirectStream[String,String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams,fromOffsets)
      )
    }




    ssc.start()
    ssc.awaitTermination()
  }
}
