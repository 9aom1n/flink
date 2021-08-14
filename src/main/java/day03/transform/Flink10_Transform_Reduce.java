package day03.transform;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Gm
 * @Date: 2021/8/9 18:45
 */

public class Flink10_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorMap = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] elements = s.split(" ");
                return new WaterSensor(elements[0], Long.parseLong(elements[1]), Integer.parseInt(elements[2]));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyByIDStream = waterSensorMap.keyBy("id");

        //todo reduce 聚合操作value1 代表的是上一次的聚合结构，value2代表的是当前的数据 流式处理一个分组的第一条数据不会进入到reduce方法中
        SingleOutputStreamOperator<WaterSensor> reduce = keyByIDStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
            }
        });

        reduce.print();

        env.execute();
    }
}
