package com.astro.sparkTest

import com.alibaba.fastjson.JSON
import com.astro.utils.DataUtil.transformat
import com.astro.utils.HBaseUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaSparkHbase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaSparkHbase")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topics = Set("test")
    val kafkaParam = Map[String, Object]("bootstrap.servers" -> "hostname:9092",
                                                 "key.deserializer" -> classOf[StringDeserializer],
                                                 "value.deserializer" -> classOf[StringDeserializer],
                                                 "group.id" -> "test-consumer-group")

    val stream: InputDStream[ConsumerRecord[String, String]] = createStream(ssc, kafkaParam, topics)
    val lines: DStream[String] = stream.map(_.value())
    lines.transform(rdd => {
      rdd.filter(line => {
        //过滤掉没用的日志
       
      })
    }).map(word => {
      // 获取hbase链接
      val conn = HBaseUtil.getHbaseConn()
      val table = conn.getTable(TableName.valueOf("tableName"))
      var rowKey = ""
      // 处理数据的逻辑
      val put = new Put(Bytes.toBytes(rowKey))
      // 存入hbase 参数依次是  列簇、列名和列值
      put.addColumn("args".getBytes, "dateTime".getBytes, Bytes.toBytes(dateTime))
      
      table.put(put)
       
      table.close()
      conn.close()
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }


  def createStream(ssc: StreamingContext,kafkaParam: Map[String,Object],topics: Set[String]) = {
    KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParam))
  }
}
