package com.example.flink.project.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

/**
 * description: TODO
 * author L
 * date 2024/5/10 11:32
 */
public class WordSplitter implements FlatMapFunction<String , Tuple2<String, Integer>>  {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = line.split(",");
        for (String word : words) {
            out.collect(new Tuple2<>(word,1));
        }
    }
}
