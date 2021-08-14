package day03.sink;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Gm
 * @Date: 2021/8/9 21:03
 */

public class Flink03_Sink_ES {
    public static void main(String[] args) {
       // 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        //对数据进行处理
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] words = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
                return waterSensor;
            }
        });

    }
}
