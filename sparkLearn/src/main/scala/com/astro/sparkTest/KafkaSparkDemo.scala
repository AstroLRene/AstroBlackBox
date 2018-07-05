package com.astro.sparkTest

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}




object KafkaSparkDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaSparkDemo")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topics = Set("maxwell")
    val kafkaParam = Map[String,Object]("bootstrap.servers" -> "hostname:9092",
                                                "key.deserializer" -> classOf[StringDeserializer],
                                                "value.deserializer" -> classOf[StringDeserializer],
                                                "group.id" -> "test-consumer-group")
    val stream:InputDStream[ConsumerRecord[String,String]] = createStream(ssc,kafkaParam,topics)
    stream.map(record=>getJson(record.value().toString).get("data")).print

    ssc.start()
    ssc.awaitTermination()
  }

  def createStream(ssc: StreamingContext,kafkaParam: Map[String,Object],topics: Set[String]) = {
      KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,String](topics, kafkaParam))
  }

  def getJson(json:String) = {
    JSON.parseObject(json)
  }

}
