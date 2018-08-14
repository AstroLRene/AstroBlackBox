package com.astro.sparkTest

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OffsetInZookeeperApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("OffsetInZookeeperApp")
    val ssc = new StreamingContext(sc,Seconds(5))

    val topic : String = "topic_name"  //消费的 topic 名字
    val topics : Set[String] = Set(topic)                    //创建 stream 时使用的 topic 名字集合
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "est-consumer-group",
      "request.required.acks" -> "1",
      "auto.offset.reset" -> "latest"
    )

    val topicDirs = new ZKGroupTopicDirs("test_spark_streaming_group", topic)  //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"          //获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name

    val zkClient = new ZkClient("10.4.232.77:2181")          //zookeeper 的host 和 ip，创建一个 client
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

    var kafkaStream : InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map()  //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    if (children > 0) {  //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for (i <- 0 untilchildren) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
        logInfo("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
      }

      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())  //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    else {
       KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      ) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
    }

    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(msg => Utils.msgDecode(msg)).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
        logInfo(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }

      rdd.foreachPartition(
        message => {
          while(message.hasNext) {
            logInfo(s"@^_^@   [" + message.next() + "] @^_^@")
          }
        }
      )
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
