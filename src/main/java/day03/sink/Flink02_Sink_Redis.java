package day03.sink;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author: Gm
 * @Date: 2021/8/9 20:50
 */

public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 19999);
        //对数据做处理
        SingleOutputStreamOperator<String> jsonString = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] split = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return JSON.toJSONString(waterSensor);
            }
        });

        //将数据发送给Redis
        FlinkJedisPoolConfig hadoop102 = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        jsonString.addSink(new RedisSink<>(hadoop102, new RedisMapper<String>() {
            //编写redis的命令
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "0325");
            }

            //这是redis默认为rediskey 如果是Hash类型的话，则是数据中的小Key
            @Override
            public String getKeyFromData(String s) {
                WaterSensor waterSensor = JSON.parseObject(s, WaterSensor.class);
                return waterSensor.getId();
            }

            //设置具体的value
            @Override
            public String getValueFromData(String s) {
                return s;
            }
        }));

        env.execute();
    }
}
