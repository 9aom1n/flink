package day04;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author: Gm
 * @Date: 2021/8/10 16:57
 */

public class Flink05_Project_UV_2 {
    public static void main(String[] args) throws Exception {
        //创建留的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件获取数据

        DataStreamSource<String> dataStreamSource = env.readTextFile("input\\UserBehavior.csv");

        SingleOutputStreamOperator<Tuple2<String, Long>> uv = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
                if ("pv".equals(userBehavior.getBehavior())) {
                    collector.collect(Tuple2.of("uv", userBehavior.getUserId()));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = uv.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Long>> uv_count = keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Long>>() {
            HashSet<Long> uids = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                uids.add(value.f1);
                out.collect(Tuple2.of("uv", (long) uids.size()));
            }
        });

        uv_count.print();
        env.execute();
    }
}
