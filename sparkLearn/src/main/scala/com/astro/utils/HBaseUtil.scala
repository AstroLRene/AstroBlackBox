package com.astro.utils

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.Try

object HBaseUtil extends Serializable {

  def getHbaseConn()={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.199.142")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }

//  def main(args: Array[String]): Unit = {
//    val conn = getHbaseConn()
//    val table = conn.getTable(TableName.valueOf("hello"))
//    val put = new Put(Bytes.toBytes("10001"))
//    put.addColumn("info".getBytes(),"name".getBytes(),"astro".getBytes())
//
//      Try(table.put(put)).getOrElse(table.close())
//
//    table.close()
//    conn.close()
//
//
//  }

}

