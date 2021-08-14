package day04;

import bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/10 11:48
 */

public class Flink03_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<Long> pvCount = dataStreamSource.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        })
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                .keyBy(userBehavior -> userBehavior.getBehavior())
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    private Long count = 0L;

                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        count++;
                        out.collect(count);
                    }
                });

        SingleOutputStreamOperator<Tuple2> pv_count = pvCount.map(num -> {
           return new Tuple2("pv", num);
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        pv_count.print();
        env.execute();
    }
}
