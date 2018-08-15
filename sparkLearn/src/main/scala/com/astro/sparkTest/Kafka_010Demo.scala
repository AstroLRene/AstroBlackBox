package com.astro.sparkTest

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object OffsetInZookeeperKafka_010Demo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("Kafka_010Demo")
    val ssc = new StreamingContext(sc,Seconds(5))
    val topics = Array("astrotest")
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicDirs = new ZKGroupTopicDirs("test-consumer-group", topics(0))  //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"          //获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name

    val zkClient = new ZkClient("localhost:2181")          //zookeeper 的host 和 ip，创建一个 client
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    var fromOffsets: Map[TopicPartition, Long] = Map()

    val stream =  if(children > 0){
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = new TopicPartition(topics(0), i)
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      KafkaUtils.createDirectStream[String,String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams, fromOffsets)
      )
    }else {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    }

    var offsetRanges = Array[OffsetRange]()
    stream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for(o <- offsetRanges){
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }
    }

        //    stream.map(record => (record.key,record.value)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
