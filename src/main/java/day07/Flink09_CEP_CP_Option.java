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
 * @Date: 2021/8/14 11:28
 */

public class Flink09_CEP_CP_Option {
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
        Pattern<WaterSensor, WaterSensor> begin = Pattern.<WaterSensor>begin("begin").where(new IterativeCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                return "sensor_1".equals(waterSensor.getId());
            }
        })
                .times(1,3)      //固定的次数  默认是松散连续
//                .optional() //可选 ：可以使用pattern.optional()方法让所有的模式变成可选的，不管是不是循环模式
                .next("next")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor, Context<WaterSensor> context) throws Exception {
                        return "sensor_2".equals(waterSensor.getId());
                    }
                })
                ;
        //todo 将流作用于模式上
        PatternStream<WaterSensor> pattern = CEP.pattern(waterSensorSingleOutputStreamOperator, begin);

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
