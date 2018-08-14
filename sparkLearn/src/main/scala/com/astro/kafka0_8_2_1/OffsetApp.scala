package com.astro.kafka0_8_2_1

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc._

object OffsetApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("OffsetApp")
    val ssc = new StreamingContext(sc,Seconds(10))


    val kafkaParams = Map(
      "metadata.broker.list" -> "localhost:9092",
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "smallest"
    )
    val topics = "astrotest".split(",").toSet

    DBs.setup()
    val fromOffsets = DB.readOnly(implicit session => {
      SQL("select * from astro_offset").map(rs => {
        (TopicAndPartition(rs.string("topic"),rs.int("partitions")),rs.long("offset"))
      }).list().apply()
    }).toMap


    val stream = if(fromOffsets.size == 0) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }
    else{
      val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(),mm.message())
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)
    }

    // 保存 offset
    var offsetRanges = Array[OffsetRange]()
    stream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD(rdd => {
      for(o <- offsetRanges){
        //        println("topic:"+o.topic + " partition:"+o.partition + " offset:" + o.fromOffset)
        val offset = o.untilOffset
        val topic = o.topic
        val partition = o.partition
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        if(o.fromOffset < o.untilOffset) {
          DB.autoCommit(implicit session => {
            SQL("update astro_offset set offset = ? where topic = ? and partitions = ? and groupid = ?").bind(offset, topic, partition, kafkaParams("group.id")).update().apply()
          })
        }
      }
    })

    // 业务逻辑处理
    stream.foreachRDD(rdd =>{
      println(rdd.count())
    })



    ssc.start()
    ssc.awaitTermination()

  }

}
