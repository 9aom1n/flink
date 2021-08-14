package day04;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
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
 * @Date: 2021/8/10 16:34
 */

public class Flink04_Project_UV {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
        env.setParallelism(1);

        //从文件获取数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input\\UserBehavior.csv");

        //将数据转换为JavaBean
//        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
//            @Override
//            public UserBehavior map(String s) throws Exception {
//                String[] split = s.split(",");
//                return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
//            }
//        });
        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream = dataStreamSource.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        });

        //过滤出pv的数据并将数据转换为tuple
        SingleOutputStreamOperator<UserBehavior> pvToOneDStream = userBehaviorDStream.flatMap(new FlatMapFunction<UserBehavior, UserBehavior>() {
            @Override
            public void flatMap(UserBehavior userBehavior, Collector<UserBehavior> collector) throws Exception {
                if ("pv".equals(userBehavior.getBehavior())) {
                    collector.collect(userBehavior);
                }
            }
        });

        //按照pv分组
        KeyedStream<UserBehavior, Tuple> keyByuserId = pvToOneDStream.keyBy("userId");

        SingleOutputStreamOperator<Tuple2<String, Long>> uvCount = keyByuserId.process(new KeyedProcessFunction<Tuple, UserBehavior, Tuple2<String, Long>>() {
            HashSet<Object> hashSet = new HashSet<>();

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                hashSet.add(value.getUserId());
                out.collect(Tuple2.of("uv", (long) hashSet.size()));
            }
        });

        uvCount.print();
        env.execute();
    }
}
