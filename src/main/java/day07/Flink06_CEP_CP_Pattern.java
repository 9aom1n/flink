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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author: Gm
 * @Date: 2021/8/14 11:28
 */

public class Flink06_CEP_CP_Pattern {
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

        //定义模式
        Pattern<WaterSensor, WaterSensor> patternDStream = Pattern.<WaterSensor>begin("begin").where(new IterativeCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                return "sensor_1".equals(waterSensor.getId());
            }
        })
//                .next("next")   //严格连续 : 期望所有匹配的时间严格的一个接着一个的出现，中间没有任何不匹配的事件
                .followedBy("followBy1") //松散连续 : 忽略匹配的事件中间的不匹配事件
//                .followedByAny("followByAny") //非确定的松散连续 : 更进一步的松散连续，允许忽略一些匹配事件的附加匹配
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_2".equals(waterSensor.getId());
                    }
                })
                .followedBy("followBy2")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_2".equals(waterSensor.getId());
                    }
                });
        //TODO 将流作用于模式上
        PatternStream<WaterSensor> pattern = CEP.pattern(waterSensorSingleOutputStreamOperator, patternDStream);

        //TODO 查询出模式匹配搭配到的数据
        pattern.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();

        environment.execute();
    }
}
