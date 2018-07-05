package com.maimob.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HbaseUntil {

    public static Configuration config;
    public static Connection conn;

    public static Table getTable(String tableName) throws IOException {
        init();
        Table table = conn.getTable(TableName.valueOf(tableName));
        return table;
    }

    public static void init() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hostName");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(){
        try{
            if(conn != null){
                conn.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
