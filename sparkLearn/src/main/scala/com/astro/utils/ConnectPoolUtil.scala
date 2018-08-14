package com.astro.utils

import java.sql.{Connection, DriverManager}

import com.alibaba.fastjson.{JSON, JSONObject}



object ConnectPoolUtil {

  val url = "jdbc:mysql://192.168.199.106:3306/diary?useUnicode=true&characterEncoding=UTF-8"
  val username = "root"
  val password = "root"

  Class.forName("com.mysql.jdbc.Driver")

  def getConnection():Connection = {
    DriverManager.getConnection(url,username,password)
  }
  def close(conn: Connection): Unit = {
    try{
      if(!conn.isClosed() || conn != null){
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def insert(json:JSONObject,tabName:String): Unit ={
    val keys = json.keySet()
    var columns = "("
    var values = " values("

    val iter = keys.iterator()
    while(iter.hasNext){
      val field = iter.next()
      columns = columns + field + ","
      values = values + "'" + json.getString(field) + "',"
    }
    val len = columns.length
    val valen = values.length
    columns = columns.substring(0,len-1) + ") "
    values = values.substring(0,valen-1) + ")"
    val sql = "insert into " + tabName + columns + values
    println(sql)
    val conn = getConnection()
    conn.prepareStatement(sql).executeUpdate()
    close(conn)
  }



//  def main(args: Array[String]): Unit = {
//    val str = "{\"name\":\"elena\",\"age\":18,\"body\":\"beauty\"}"
//    val json:JSONObject = JSON.parseObject(str)
//
//    println(json)
//    json.put("weight","123")
//    print(json)
//    insert(json,"people")
//  }
}
