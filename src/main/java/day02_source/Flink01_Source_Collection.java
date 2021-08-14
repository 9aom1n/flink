package day02_source;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Gm
 * @Date: 2021/8/7 16:21
 */

public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42)
        );

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(1);
        //获取数据
        DataStreamSource<WaterSensor> dataStreamSource = env.fromCollection(waterSensors);

        dataStreamSource.print();
        env.execute();
    }
}
