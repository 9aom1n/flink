package day04;

import bean.OrderEvent;
import bean.TxEvent;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author: Gm
 * @Date: 2021/8/10 18:39
 */

public class Flink09_Project_Order {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //分别从不同的数据源获取不同的数据 ，并转换为JavBean
        //获取订单相关的数据
        DataStreamSource<String> orderStreamSource = env.readTextFile("input\\OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderMapDStream = orderStreamSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        });
        //获取交易相关的数据
        DataStreamSource<String> receiptStreamSource = env.readTextFile("input\\ReceiptLog.csv");
        SingleOutputStreamOperator<TxEvent> receiptMapDStream = receiptStreamSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String s) throws Exception {
                String[] split = s.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        //将两个流关联起来
        ConnectedStreams<OrderEvent, TxEvent> connect = orderMapDStream.connect(receiptMapDStream);

        ConnectedStreams<OrderEvent, TxEvent> keyBy_txId = connect.keyBy("txId", "txId");

        //通过缓存的方法将两个流关联起来
        SingleOutputStreamOperator<String> orderWithTxDStream = keyBy_txId.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            //存放订单数据的缓存区
            HashMap<String, OrderEvent> orderMap = new HashMap<>();
            //存放交易数据的缓存区
            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                //缓存区中有数据则关联输出然后移除缓存区
                if (txMap.containsKey(value.getTxId())) {
                    out.collect("订单" + value.getOrderId() + "成功对账");
                    txMap.remove(value.getTxId());
                } else {
                    //缓存区中没有则将订单信息写到订单缓存区
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if (orderMap.containsKey(value.getTxId())) {
                    out.collect("订单" + orderMap.get(value.getTxId()).getOrderId() + "成功对账");
                    txMap.remove(value.getTxId());
                } else {
                    txMap.put(value.getTxId(), value);
                }

            }
        });

        orderWithTxDStream.print();
        env.execute();

    }
}
