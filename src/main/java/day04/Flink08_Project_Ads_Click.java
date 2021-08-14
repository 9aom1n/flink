package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/10 18:29
 */

public class Flink08_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //通过文件获取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input\\UserBehavior.csv");

        //将数据转换成KV -> tuple2元组，key -> province adId value -> 1L
        SingleOutputStreamOperator<Tuple2<String, Long>> adsToOneDStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple2.of(split[2] + "-" + split[1], 1L);
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = adsToOneDStream.keyBy(0);

        keyedStream.sum(1).print();

        env.execute();
    }
}
