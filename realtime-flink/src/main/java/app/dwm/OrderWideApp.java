package app.dwm;

import bean.OrderDetail;
import bean.OrderInfo;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.MyKafkaUtil;

import java.text.SimpleDateFormat;

/**
 * @author yycstart
 * @create 2021-02-15 18:51
 *      第4章 DWM层 订单宽表
        4.1 需求分析与思路
        订单是统计分析的重要的对象，围绕订单有很多的维度统计需求，比如用户、地区、商品、品类、品牌等等。
     为了之后统计计算更加方便，减少大表之间的关联，所以在实时计算过程中将围绕订单的相关数据整合成为一张订单的宽表。
     那究竟哪些数据需要和订单整合在一起。

        用户表 -- \                                              / --   品牌表
                 |  ---  （订单表 ） --- （订单明细） --- 商品表   --|  --   分类表
        地区表 --/                                              \  --    SPU表

       如上图，由于在之前的操作我们已经把数据分拆成了事实数据和维度数据，事实数据（带括号）进入Kafka数据流中，
     维度数据（不带括号）进入HBase中长期保存。
        那么我们在DWM层中要把实时和维度数据进行整合关联在一起，形成宽表。
        那么这里就要处理有两种关联，事实数据和事实数据关联、事实数据和维度数据关联。事实数据和事实数据关联，
        其实就是流与流之间的关联。事实数据与维度数据关联，其实就是流计算中查询外部数据源。接下来咱们分别实现：
 */
public class OrderWideApp  {
    public static void main(String[] args) {
        //1. 获取执行环境，指定事件时间，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        //2. 声明Topic信息
        String groupId = "order_wide_group";

        String orderInfoSourceTopic = "DWD_ORDER_INFO";
        String  orderDetailSourceTopic = "DWD_ORDER_DETAIL";

        String orderWideSinkTopic = "DWM_ORDER_WIDE";

        //3. 从Kafka中获取Topic数据
        FlinkKafkaConsumer<String> sourceOrderInfo = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> sourceOrderDetail = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);

        DataStreamSource<String> orderInfoJSONDStream = env.addSource(sourceOrderInfo);
        DataStreamSource<String> orderDetailJSONDStream = env.addSource(sourceOrderDetail);

        SingleOutputStreamOperator<OrderInfo> OrderInfoDStream = orderInfoJSONDStream.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public OrderInfo map(String s) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());

                return orderInfo;
            }
        });

        DataStream<OrderDetail> orderDetailDStream   = orderDetailJSONDStream.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat simpleDateFormat=null;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
            }
            @Override
            public OrderDetail map(String jsonString) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonString, OrderDetail.class);
                orderDetail.setCreate_ts (simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        //打印
        orderDetailDStream.print();

        //执行
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
