package day07;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/8/14 11:44
 */

public class Flink04_CEP_Condition {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取文件中的数据
        DataStreamSource<String> dataStreamSource = environment.readTextFile("input\\sensor.txt");

        //将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDStream.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //todo 定义模式
        Pattern<WaterSensor, WaterSensor> begin = Pattern.<WaterSensor>begin("begin")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() > 30;
                    }
                }).or(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return waterSensor.getTs() > 2000;
                    }
                });

        // todo 将流作用于模式上
        PatternStream<WaterSensor> pattern = CEP.pattern(waterSensorSingleOutputStreamOperator, begin);
        //todo 查询出模式匹配搭配到的数据
        SingleOutputStreamOperator<String> select = pattern.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        });

        select.print();
        environment.execute();
    }
}
