package day03.transform;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: Gm
 * @Date: 2021/8/9 18:53
 */

public class Flink11_Transform_Process {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置流的并行度
        env.setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        //使用process进行一个过滤的功能实现
        SingleOutputStreamOperator<WaterSensor> processWithFilter = map.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if ("s1".equals(value.getId())) {
                    out.collect(value);
                }
            }
        });

        // todo 使用process进行累加的操作
        SingleOutputStreamOperator<WaterSensor> processWithSum = processWithFilter.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            Integer lastNum = 0;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                lastNum += value.getVc();
                out.collect(new WaterSensor(value.getId(), value.getTs(), lastNum));
            }
        });

        processWithSum.print();
        env.execute();
    }
}
