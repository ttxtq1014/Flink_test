package com.example.flink.datagen2mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * description: TODO
 * author L
 * date 2024/5/11 10:22
 */
public class DataGen2Mysql {
    public static void main(String[] args) {
        //1.创建流处理环境i
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //2.创建数据源，随机生成数据
        tEnv.executeSql(
                "create table dataGenSourceTable(\n" +
                        "order_number bigint,\n" +
                        "price decimal(10,2),\n" +
                        "buyer string,\n" +
                        "order_time timestamp(3)\n" +
                        ") with (\n" +
                        "'connector'='datagen',\n" +
                        "'rows-per-second'='10',\n" +
                        "'number-of-rows'='50'\n" +
                        ");"
        );

        //3.创建Mysql Sink
        tEnv.executeSql(
                "CREATE TABLE mysql_sink (\" +\n" +
                        "                \"  id BIGINT,\" +\n" +
                        "                \"  value STRING,\" +\n" +
                        "                \"  ts TIMESTAMP\" +\n" +
                        "                \") WITH (\" +\n" +
                        "                \"  'connector' = 'jdbc',\" +\n" +
                        "                \"  'url' = 'jdbc:mysql://localhost:3306/test',\" +\n" +
                        "                \"  'table-name' = 'test_table',\" +\n" +
                        "                \"  'driver' = 'com.mysql.jdbc.Driver',\" +\n" +
                        "                \"  'username' = 'root',\" +\n" +
                        "                \"  'password' = 'password'\" +\n" +
                        "                \")"
        );
    }
}
