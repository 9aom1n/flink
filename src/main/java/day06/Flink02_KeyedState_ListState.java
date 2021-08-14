package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author: Gm
 * @Date: 2021/8/13 11:54
 */

public class Flink02_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        //创建流的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口获取数据
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("localhost", 9999);
        //将数据转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] splits = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(splits[0], Long.parseLong(splits[1]), Integer.parseInt(splits[2]));
                return waterSensor;
            }
        });

        //将相同的id进行聚合
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(r -> r.getId());


        //针对每个传感器输出最高的三个水位槽
        SingleOutputStreamOperator<String> listStateProcess = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //先将当前的值保存到状态中
                listState.add(value.getVc());

                //取出状态中的值
                ArrayList<Integer> vcState = new ArrayList<>();
                for (Integer vc : listState.get()) {
                    vcState.add(vc);
                }

                //对新的集合中的数据做排序(降序排列)
                vcState.sort((o1, o2) -> (o2 - o1));

                //删除集合中的最后一条数据
                if (vcState.size() > 3) {
                    vcState.remove(3);
                }

                //将新的集合重新保存到状态中
                listState.update(vcState);

                out.collect(listState.get().toString());
            }
        });

        listStateProcess.print();

        environment.execute();
    }
}
