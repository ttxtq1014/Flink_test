package com.example.flink.project;

import com.example.flink.project.util.WordSplitter;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description: TODO
 * author L
 * date 2024/5/10 11:19
 */
/**
 * 读取Socket数据进行实时WordCount统计
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取Socket数据
        DataStreamSource<String> ds = env.socketTextStream("hadoop103", 9999);
        //3.准备K,V格式数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = ds.flatMap(new WordSplitter())
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        //4.聚合打印结果
        tupleDS.keyBy(0).sum(1).print();
        //5.execute触发执行
        env.execute();
    }
}
