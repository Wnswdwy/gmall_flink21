package app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctc.wstx.sw.EncodingXmlWriter;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sun.awt.SunHints;
import utils.MyKafkaUtil;

import java.util.List;
import java.util.Map;

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

        //4. 按照Mid进行分组
        KeyedStream<JSONObject, String> keyedStream = map.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //5. 定义模式匹配序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() == 0;

            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                return pageId != null && pageId.length() > 0;
            }
        }).within(Time.seconds(10));

        //6. 使用模式匹配在流上进行筛选数据
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //7. 定义测输出流标签
        OutputTag<String> timeOutTag = new OutputTag<>("timeout");

        //8. 提取事件，超时事件到测输出流是我们需要跳转的数据
        SingleOutputStreamOperator<String> filterStream = patternStream.flatSelect(timeOutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        List<JSONObject> objectList = map.get("start");
                        for (JSONObject jsonObject : objectList) {
                            System.out.println("timeOut:" + jsonObject);
                            collector.collect(jsonObject.toJSONString());
                            System.out.println("timeOut:" + jsonObject.toJSONString());
                        }
                    }
                }
                , new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {
                        List<JSONObject> objectList = map.get("next");
                        for (JSONObject jsonObject : objectList) {
                            System.out.println("inTime:" + jsonObject.toJSONString());
                            collector.collect(jsonObject.toJSONString());
                            System.out.println("inTime:" + jsonObject.toJSONString());
                        }
                    }
                });
        //9. 获取测输出流数据
        DataStream<String> jumpDStream = filterStream.getSideOutput(timeOutTag);
        filterStream.print("in time ==> ");
        jumpDStream.print("timeOut");

        //10. 使用自定义元素数据源测试
        DataStreamSource<String> dataStream = env.fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"detail\"},\"ts\":30000} "
        );

        //11. 将数据写入对应的Kafka主题
        jumpDStream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //11.任务启动
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}