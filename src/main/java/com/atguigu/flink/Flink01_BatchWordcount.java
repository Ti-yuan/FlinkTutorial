package com.atguigu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @AUTHOR: Maynard
 * @DATE: 2022/04/03 20:46
 **/

public class Flink01_BatchWordcount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> wordDS = env.readTextFile("words.txt");
        FlatMapOperator<String, String> wordFMO = wordDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(word);
                }
            }
        });

        MapOperator<String, Tuple2<String, Long>> wordTuple = wordFMO.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> wordUG = wordTuple.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> wordAgg = wordUG.sum(1);
        wordAgg.print();
    }
}
