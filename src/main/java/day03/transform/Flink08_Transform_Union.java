package day03.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/9 18:19
 */

public class Flink08_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> integerDataStreamSource1 = env.fromElements(2, 3, 4, 5, 6, 7);

        //Union将两条流连在一起
        DataStream<Integer> union = integerDataStreamSource.union(integerDataStreamSource1);
        SingleOutputStreamOperator<Integer> process = union.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value + 100);
            }
        });

        process.print();
        env.execute();
    }
}
