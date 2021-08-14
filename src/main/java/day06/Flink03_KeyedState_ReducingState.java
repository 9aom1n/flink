package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/13 18:51
 */

public class Flink03_KeyedState_ReducingState {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("localhost", 9999);

        //将数据转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] s = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(s[0], Long.valueOf(s[1]), Integer.valueOf(s[2]));
                return waterSensor;
            }
        });


        //按照相同的id进行聚合
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(r -> r.getId());

        //计算每个传感器的水位
        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private ReducingState<Integer> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("vcSum", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                }, Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //使用状态
                //先将数据存入状态做累加操作
                reducingState.add(value.getVc());

                //在从状态中取出累加的过后结构
                Integer vcSum = reducingState.get();
                out.collect(value.getId() + ":" + vcSum);
            }
        });
        process.print();

        environment.execute();

    }
}
