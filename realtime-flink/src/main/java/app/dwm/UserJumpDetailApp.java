package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctc.wstx.sw.EncodingXmlWriter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.MyKafkaUtil;

import java.sql.Time;

/**
 * @author yycstart
 * @create 2021-02-13 18:25
 */
public class UserJumpDetailApp {
    public static void main(String[] args) {

        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //2.定义消费者参数
        String sourceTopic = "DWD_PAGE_LOG";
        String sinkTopic = "DWM_USER_JUMP_DETAIL";
        String groupId = "UserJumpDetailApp";

        //3. 读取Kafka主题数据创建流
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        SingleOutputStreamOperator<JSONObject> map = env.addSource(kafkaSource).map(JSON::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //11.任务启动
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}