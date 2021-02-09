package app.dwd;


import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.istack.Nullable;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import process.DimSink;
import utils.MyKafkaUtil;
import process.TableProcessFunction;

/**
 * @author yycstart
 * @create 2021-02-09 15:24
 */

/**
 * RichMapFunction, ProcessFunction, RichSinkFunction。 那这几个我们什么时候会用到呢？如何选择？
 *
 * Function	        可转换结构	可过滤数据	侧输出	open方法	可以使用状态	输出至
 *
 * MapFunction	        Yes	        No	      No	No	        No	    下游算子
 * FilterFunction	    No	        Yes	      No	No	        No	    下游算子
 * RichMapFunction	    Yes 	    No	      No	Yes	        Yes	    下游算子
 * RichFilterFunction	No	        Yes	      No	Yes	        Yes	    下游算子
 * ProcessFunction	    Yes 	    Yes	     Yes	Yes	        Yes	    下游算子
 * SinkFunction	        Yes	        Yes	      No	 No	        No	     外部
 * RichSinkFunction	    Yes	        Yes	     No	     Yes	    Yes	     外部
 */
public class Dwd_Base_DB {
    public static void main(String[] args) {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2. 定义消费者的主题和消费者组
        String groupId = "ods_order_detail_group";
        String topic = "ODS_BASE_DB_M";

        /**
         * 功能一：接收Kafka数据，过滤空值数据
         */
        //3. 读取Kafka主题数据并转换成JSONObject
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonDStream = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjectDStream = jsonDStream.map(JSON::parseObject);

        //4. 过滤掉空值数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjectDStream.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getString("table") != null &&
                        value.getString("data") != null &&
                        value.getString("data").length() > 3;
            }
        });

        /**
         * 功能二：根据MySQL的动态配置表，进行分流。
         */

        //5.定义输出到HBase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {};

        //6.使用ProcessFunction实现数据分流操作,事实表放入主流,维度表根据维度信息放入侧输出流
        SingleOutputStreamOperator<JSONObject> kafkaDStream = filterDS.process(new TableProcessFunction(hbaseTag));

        //7.获取侧输出流数据,即将来写往HBase(Phoenix)的数据
        DataStream<JSONObject> hbaseDStream = kafkaDStream.getSideOutput(hbaseTag);

        //8. 将侧输出流写入到HBase中
        hbaseDStream.addSink(new DimSink());

        //9.将主流数据写入Kafka
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("启动Kafka Sink");
            }
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(topic, jsonObject.toJSONString().getBytes());
            }
        });

        kafkaDStream.addSink(kafkaSink);


    }
}
