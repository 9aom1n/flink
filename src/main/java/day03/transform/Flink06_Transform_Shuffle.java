package day03.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @Author: Gm
 * @Date: 2021/8/9 12:56
 */

public class Flink06_Transform_Shuffle {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/word.txt");
        //按照空格切分
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //TODO shuffle
        DataStream<Tuple2<String, Long>> shuffle = wordToOneDStream.shuffle();

        wordToOneDStream.print("原始数据");

        keyedStream.print("keyBY后的数据");

        shuffle.print("shuffle之后的数据");
        env.execute();
    }
}
