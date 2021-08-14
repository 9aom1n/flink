package day03.transform;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/9 18:31
 */

public class Flink09_Transform_MaxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从端口获得数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorMap = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] elements = s.split(" ");
                return new WaterSensor(elements[0], Long.parseLong(elements[1]), Integer.parseInt(elements[2]));
            }
        });

        //对id相同的数据进行分组
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorMap.keyBy("id");

        SingleOutputStreamOperator<WaterSensor> result = keyedStream.maxBy("vc", false);

        result.print();
        env.execute();
    }
}
