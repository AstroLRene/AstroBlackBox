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
    val kafkaParam = Map[String, Object]("bootstrap.servers" -> "192.168.199.142:9092",
                                                 "key.deserializer" -> classOf[StringDeserializer],
                                                 "value.deserializer" -> classOf[StringDeserializer],
                                                 "group.id" -> "test-consumer-group")

    val stream: InputDStream[ConsumerRecord[String, String]] = createStream(ssc, kafkaParam, topics)
    val lines: DStream[String] = stream.map(_.value())
    lines.transform(rdd => {
      rdd.filter(line => {
        //过滤掉没用的日志
       (line.contains("DeString")||line.contains("responseToapp")||line.contains("requestToMucfc")||line.contains("respFromMucfc"))&&(!line.contains("facePhoto"))&&(!line.contains("backPhoto"))&&(line.contains("flowNo"))

      })
    }).map(word => {
      // 获取hbase链接
      val conn = HBaseUtil.getHbaseConn()
      val table = conn.getTable(TableName.valueOf("diary"))
      var rowKey = ""
      if(word.contains("DeString")) {
        val arr = word.split("=")
        val str = arr(1)
        rowKey = arr(3)
        //      println(str)
        val json = JSON.parseObject(str.substring(1, str.length - 4))
        val args = json.getJSONObject("args")
        // 接口
        val connector = json.getString("call")
        // 时间
        val time = json.getString("timestamp")
        //      println(time)
        //      val date = transformat(time,"yyyyMMddHHmm")
        var dateTime = ""
        try {
          dateTime = transformat(time, "yyyy-MM-dd HH:mm:ss")
        }catch{
          case e:NumberFormatException =>{
            println(word)
          }
        }
        val deviceInfo = args.getJSONObject("deviceInfo")
        // uuid
        var uuid = deviceInfo.getString("uuid")
        //      println(uuid)
        val body = args.getJSONObject("body")
        // 手机号
        var mobile = body.getString("mobile")
        println(rowKey)
        //请求参数全部放在app列簇
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn("app".getBytes, "req.dateTime".getBytes, Bytes.toBytes(dateTime))
        put.addColumn("app".getBytes, "req.connector".getBytes, Bytes.toBytes(connector))
        import scala.collection.JavaConversions._
        for (key <- deviceInfo.keySet()) {
          put.addColumn("app".getBytes, Bytes.toBytes("req."+key), Bytes.toBytes(deviceInfo.getString(key)))
        }
        for (key <- body.keySet()) {
          put.addColumn("app".getBytes, Bytes.toBytes("req."+key), Bytes.toBytes(body.getString(key)))
        }
        table.put(put)
      }
      if(word.contains("responseToapp")){
        rowKey = word.substring(word.length-36)
        println(rowKey)
        val put = new Put(Bytes.toBytes(rowKey))
        val index = word.indexOf("responseToapp")
        val reps = JSON.parseObject(word.substring(index+14,word.length-44))
        val response = reps.getJSONObject("response")
        // 客户端返回存在app列簇里，在列名前加rep.
        put.addColumn("app".getBytes, "rep.cost".getBytes, Bytes.toBytes(reps.getString("cost")))
        put.addColumn("app".getBytes, "rep.returnCode".getBytes, Bytes.toBytes(reps.getString("returnCode")))
        put.addColumn("app".getBytes, "rep.returnMsg".getBytes, Bytes.toBytes(reps.getString("returnMsg")))
        if(response != null) {
          import scala.collection.JavaConversions._
          val encryptedData = response.getString("encryptedData")
          if(encryptedData != null){
            response.remove("encryptedData")
          }
          val idCardInfo = response.getJSONObject("idCardInfo")
          if(idCardInfo != null){
            response.remove("idCardInfo")
            for(key <- idCardInfo.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(idCardInfo.getString(key)))
            }
          }
          val basicInfo = response.getJSONObject("basicInfo")
          if(basicInfo != null){
            response.remove("basicInfo")
            for(key <- basicInfo.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(basicInfo.getString(key)))
            }
          }
          val emergRelativesInfo = response.getJSONObject("emergRelativesInfo")
          if(emergRelativesInfo != null){
            response.remove("emergRelativesInfo")
            for(key <- emergRelativesInfo.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(emergRelativesInfo.getString(key)))
            }
          }
          val permanentAddress = response.getJSONObject("permanentAddress")
          if(permanentAddress != null){
            response.remove("permanentAddress")
            for(key <- permanentAddress.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(permanentAddress.getString(key)))
            }
          }
          for (key <- response.keySet()) {
            put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(response.getString(key)))
          }
        }
        table.put(put)
      }
      if(word.contains("requestToMucfc")){
        rowKey = word.substring(word.length-36)
        println(rowKey)
        val put = new Put(Bytes.toBytes(rowKey))
        val index = word.indexOf("requestToMucfc")
        val requestToMucfc = JSON.parseObject(word.substring(index+15,word.length-44))
        val reqcontent = requestToMucfc.getJSONObject("bizContent")
        if(reqcontent != null){
          requestToMucfc.remove("bizContent")
          import scala.collection.JavaConversions._
          for (key <- reqcontent.keySet()) {
            put.addColumn("partner".getBytes, Bytes.toBytes("rep." + key), Bytes.toBytes(reqcontent.getString(key)))
          }
        }
        import scala.collection.JavaConversions._
        for(key <- requestToMucfc.keySet()){
          put.addColumn("partner".getBytes, Bytes.toBytes("rep." + key), Bytes.toBytes(requestToMucfc.getString(key)))
        }
        table.put(put)
      }
      if(word.contains("respFromMucfc")){
        rowKey = word.substring(word.length-36)
        println(rowKey)
        val put = new Put(Bytes.toBytes(rowKey))
        val index = word.indexOf("respFromMucfc")
        val requestFromMucfc = JSON.parseObject(word.substring(index+14,word.length-44))
        requestFromMucfc.remove("encryptKey")
        requestFromMucfc.remove("sign")
        import scala.collection.JavaConversions._
        for(key <- requestFromMucfc.keySet()){
          put.addColumn("partner".getBytes, Bytes.toBytes("rep." + key), Bytes.toBytes(requestFromMucfc.getString(key)))
        }
        table.put(put)
      }
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
