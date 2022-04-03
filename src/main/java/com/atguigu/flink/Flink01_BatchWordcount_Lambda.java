package com.atguigu.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @AUTHOR: Maynard
 * @DATE: 2022/04/03 21:21
 **/

public class Flink01_BatchWordcount_Lambda {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> wordDS = env.readTextFile("words.txt");
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = wordDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = line.split(" ");
                    for (String word : split) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> result = wordAndOneUG.sum(1);
        result.print();
    }
}
