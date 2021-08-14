package day05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: Gm
 * @Date: 2021/8/12 10:36
 */

public class Flink09_Output_Exec {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("localhost", 9999);

        //将数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(r -> r.getId());
        //采集监控传感器水位值，将水位值高于5cm的值输出到sideOutput
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                out.collect(value);
                if (value.getVc() > 5) {
                    ctx.output(new OutputTag<WaterSensor>("Warning:") {
                    }, value);
                }
            }
        });

        process.print("main:");
        process.getSideOutput(new OutputTag<WaterSensor>("Warning:"){}).print("Warning:");
        environment.execute();
    }
}
