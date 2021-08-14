package day02_source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: Gm
 * @Date: 2021/8/7 16:30
 */

public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink01_Source_kafka");
        properties.setProperty("auto.offset.reset", "latest");

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties)).print("kafka_source");

        env.execute();
    }
}
