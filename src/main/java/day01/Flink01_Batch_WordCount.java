package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @Author: Gm
 * @Date: 2021/8/6 11:42
 */

public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建批执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.获取数据
        DataSource<String> dataSource = env.readTextFile("input\\word.txt");

        //3.flatMap-> 将每一行的数据按照空格切出每一个单词
        FlatMapOperator<String, String> word = dataSource.flatMap(new MyFlatMap());

        //4.map -> 将每一个单词组成Tuple元组
        MapOperator<String, Tuple2<String, Long>> wordToOne = word.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                return Tuple2.of(s, 1L);
//                return new Tuple2<>(s,1L);
            }
        });

        //5.reduceByKey -> 先将相同的key的数据聚合到一起 -> 再做聚合操作
        UnsortedGrouping<Tuple2<String, Long>> groupBy = wordToOne.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> wordToSum = groupBy.sum(1);

        //打印
        wordToSum.print();

    }

    public static class MyFlatMap implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        }
    }
}
