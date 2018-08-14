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
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}




object LogHandleDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogHandleDemo")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topics = Set("test")
    val kafkaParam = Map[String,Object]("bootstrap.servers" -> "192.168.199.142:9092",
                                                "key.deserializer" -> classOf[StringDeserializer],
                                                "value.deserializer" -> classOf[StringDeserializer],
                                                "group.id" -> "test-consumer-group")

    val stream:InputDStream[ConsumerRecord[String,String]] = createStream(ssc,kafkaParam,topics)
    val lines:DStream[String] = stream.map(record=>record.value())
    lines.transform(rdd => {
      rdd.filter(line => {
        //过滤掉没用的日志
        (line.contains("DeString")||line.contains("responseToapp")||line.contains("requestToMucfc")||line.contains("respFromMucfc"))&&(!line.contains("facePhoto"))&&(!line.contains("backPhoto"))&&(line.contains("flowNo"))

      })
    }).map(word => {
      // 获取hbase链接
      val conn = HBaseUtil.getHbaseConn()
      val table = conn.getTable(TableName.valueOf("test"))
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
        println(time)
        val date = transformat(time,"yyyyMMddHHmm")
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
        println("app.req.dateTime："+ dateTime)
        println("app.req.connector："+ connector)
        import scala.collection.JavaConversions._
        for (key <- deviceInfo.keySet()) {
          put.addColumn("app".getBytes, Bytes.toBytes("req."+key), Bytes.toBytes(deviceInfo.getString(key)))
          println("app.req."+key + ":"+deviceInfo.getString(key))
        }
        for (key <- body.keySet()) {
          put.addColumn("app".getBytes, Bytes.toBytes("req."+key), Bytes.toBytes(body.getString(key)))
          println("app.req."+key + ":"+body.getString(key))
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
        println("app.rep.cost:"+reps.getString("cost"))
        println("app.rep.returnCode:"+reps.getString("coreturnCodest"))
        println("app.rep.returnMsg:"+reps.getString("returnMsg"))
        if(response != null) {
//          import scala.collection.JavaConversions._
          val encryptedData = response.getString("encryptedData")
          if(encryptedData != null){
            response.remove("encryptedData")
          }
          val idCardInfo = response.getJSONObject("idCardInfo")
          if(idCardInfo != null){
            response.remove("idCardInfo")
            import scala.collection.JavaConversions._
            for(key <- idCardInfo.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(idCardInfo.getString(key)))
              println("app.rep."+key+":"+idCardInfo.getString(key))
            }
          }
          val basicInfo = response.getJSONObject("basicInfo")
          if(basicInfo != null){
            response.remove("basicInfo")
            import scala.collection.JavaConversions._
            for(key <- basicInfo.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(basicInfo.getString(key)))
              println("app.rep."+key+":"+basicInfo.getString(key))
            }
          }
          val emergRelativesInfo = response.getJSONObject("emergRelativesInfo")
          if(emergRelativesInfo != null){
            response.remove("emergRelativesInfo")
            import scala.collection.JavaConversions._
            for(key <- emergRelativesInfo.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(emergRelativesInfo.getString(key)))
              println("app.rep."+key+":"+emergRelativesInfo.getString(key))
            }
          }
          val permanentAddress = response.getJSONObject("permanentAddress")
          if(permanentAddress != null){
            response.remove("permanentAddress")
            import scala.collection.JavaConversions._
            for(key <- permanentAddress.keySet()){
              put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(permanentAddress.getString(key)))
              println("app.rep."+key+":"+permanentAddress.getString(key))
            }
          }
          import scala.collection.JavaConversions._
          for (key <- response.keySet()) {
            put.addColumn("app".getBytes, Bytes.toBytes("rep."+key), Bytes.toBytes(response.getString(key)))
            println("app.rep."+key+":"+response.getString(key))
          }
        }
        table.put(put)
      }
      if(word.contains("requestToMucfc")){
        rowKey = word.substring(word.length-36)
        if(rowKey.contains("=")){
          println(rowKey)
        }else {
          println(rowKey)
          val put = new Put(Bytes.toBytes(rowKey))
          val index = word.indexOf("requestToMucfc")
          val requestToMucfc = JSON.parseObject(word.substring(index + 15, word.length - 44))
          val reqcontent = requestToMucfc.getJSONObject("bizContent")
//          println(reqcontent)
          if (reqcontent != null) {
            requestToMucfc.remove("bizContent")
            import scala.collection.JavaConversions._
            for (key <- reqcontent.keySet()) {
              put.addColumn("partner".getBytes, Bytes.toBytes("req." + key), Bytes.toBytes(reqcontent.getString(key)))
              println("partner.req." + key + ":" + reqcontent.getString(key))
            }
          }
          import scala.collection.JavaConversions._
          for (key <- requestToMucfc.keySet()) {
            put.addColumn("partner".getBytes, Bytes.toBytes("req." + key), Bytes.toBytes(requestToMucfc.getString(key)))
            println("partner.req." + key + ":" + requestToMucfc.getString(key))
          }
          table.put(put)
        }
      }
      if(word.contains("respFromMucfc")) {
        rowKey = word.substring(word.length - 36)
        println(rowKey)
        if (rowKey.contains("=")) {
          println(rowKey)
        } else {
          val put = new Put(Bytes.toBytes(rowKey))
          val index = word.indexOf("respFromMucfc")
          val requestFromMucfc = JSON.parseObject(word.substring(index + 14, word.length - 44))
          requestFromMucfc.remove("encryptKey")
          requestFromMucfc.remove("sign")
          import scala.collection.JavaConversions._
          for (key <- requestFromMucfc.keySet()) {
            put.addColumn("partner".getBytes, Bytes.toBytes("rep." + key), Bytes.toBytes(requestFromMucfc.getString(key)))
            println("partner.rep." + key + ":" + requestFromMucfc.getString(key))
          }
          table.put(put)
        }
      }
      table.close()
      conn.close()
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createStream(ssc: StreamingContext,kafkaParam: Map[String,Object],topics: Set[String]) = {
    KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParam))
  }

}
