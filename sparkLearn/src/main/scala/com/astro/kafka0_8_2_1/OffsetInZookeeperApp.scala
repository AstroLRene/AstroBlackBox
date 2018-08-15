package com.astro.kafka0_8_2_1

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OffsetInZookeeperApp {
  @transient lazy val log = LogManager.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("OffsetInZookeeperApp")
    val ssc = new StreamingContext(sc,Seconds(5))

    val topic : String = "astrotest"                          //消费的 topic 名字
    val topics : Set[String] = Set(topic)                    //创建 stream 时使用的 topic 名字集合
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "localhost:9092",
              "group.id" -> "test-consumer-group",
              "auto.offset.reset" -> "smallest"
    )

    val topicDirs = new ZKGroupTopicDirs("test-consumer-group", topic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val zkClient = new ZkClient("localhost:2181")
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    val stream = if(children > 0) {  //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = new TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
        log.info("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
      }
      val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(),mm.message())
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)

    }
    else{
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    }

    var offsetRanges = Array[OffsetRange]()
    stream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      for(o <- offsetRanges){
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
        log.info(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
