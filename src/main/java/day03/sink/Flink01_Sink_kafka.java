package day03.sink;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author: Gm
 * @Date: 2021/8/9 19:36
 */

public class Flink01_Sink_kafka {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 19999);

        //对数据做处理
        SingleOutputStreamOperator<String> map = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] elements = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(elements[0], Long.parseLong(elements[1]), Integer.parseInt(elements[2]));
                String jsonString = JSON.toJSONString(waterSensor);
                return jsonString;
            }
        });

        //todo 将数据发送至kafka
        //配置kafka连接地址
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        //指定kafka中的topic以及序列化类型
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("topic_sensor", new SimpleStringSchema(), properties);

        map.addSink(kafkaProducer);
        env.execute();
    }
}
