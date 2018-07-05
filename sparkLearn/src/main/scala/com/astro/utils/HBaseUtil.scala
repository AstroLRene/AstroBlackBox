package com.astro.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

object HBaseUtil extends Serializable {

  def getHbaseConn()={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hostname")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }

}

