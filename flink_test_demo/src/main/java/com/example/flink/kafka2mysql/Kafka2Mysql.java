package com.example.flink.kafka2mysql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * description: TODO
 * author L
 * date 2024/5/10 15:57
 */
public class Kafka2Mysql {

    public static void main(String[] args) {
        //try {
            //创建flink table 环境
            EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();//选择流环境
            TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);//创建表环境

            //创建Kafka表，用于读取交易数据
            tableEnvironment.executeSql(
                    "create table transactions \n" +
                            "(\n" +
                            "account_id bigint,\n" +
                            "amount bigint,\n" +
                            "transaction_time timestamp(3),\n" +
                            "watermark for transaction_time as transaction_time - interval '5' second\n" +
                            ") with \n" +
                            "(\n" +
                            "'connector'='kafka',\n" +
                            "'topic'='transactions',\n" +
                            "'properties.bootstrap.servers'='hadoop102:9092',\n" +
                            "'scan.startup.mode'='earliest-offset',\n" +
                            "'format'='json'\n" +
                            ");"
            );

            //创建JDBC表，用于存储报告数据到MYSQL
            tableEnvironment.executeSql(
                    "create table spend_report \n" +
                            "(\n" +
                            "account_id bigint,\n" +
                            "log_ts timestamp(3),\n" +
                            "amount bigint,\n" +
                            "primary key (account_id,log_ts) not enforced\n" +
                            ") with \n" +
                            "(\n" +
                            "'connector'='jdbc',\n" +
                            "'url'='jdbc:mysql://mysql:3306/flink_test',\n" +
                            "'table-name'='spend_report',\n" +
                            "'driver'='com.mysql.jdbc.Driver',\n" +
                            "'username'='root',\n" +
                            "'password'='000000'\n" +
                            ");"
            );

            //从kafka中读取数据
            Table transactions = tableEnvironment.from("transactions");
            //report(transactions).printSchema();
            Table t = report(transactions);
            t.executeInsert("spend_report");
            //调用report方法处理数据，并将结果插入到MYSQL表中
            // 使用 SQL 对交易数据按 amount 字段升序排序
//            Table sortedTransactions = tableEnvironment.sqlQuery(
//                    "SELECT * FROM transactions ORDER BY transaction_time ASC"
//            );
//
//            tableEnvironment.executeSql("insert into spend_report select * from " + sortedTransactions);


    }

    public static Table report(Table transactions) {
        return transactions.select(
                        $("account_id"),
                        $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
                        $("amount"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts"),
                        $("amount").sum().as("amount"));
    }
}
