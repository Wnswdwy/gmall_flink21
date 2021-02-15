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
 *
 *               第3章 DWM层 跳出明细计算
 *      3.1 需求分析与思路
 *      首先要了解什么是跳出？跳出就是用户成功访问了一个页面后退出不再继续访问，即仅阅读了一个页面就离开网站。
 *      而跳出率就是用跳出次数除以访问次数。关注跳出率，可以看出引流过来的访客是否能很快的被吸引，渠道引流过来的用户之间
 *    的质量对比，对于应用优化前后跳出率的对比也能看出优化改进的成果。
 *      计算跳出率的思路：首先要把识别哪些是跳出行为，要把这些跳出的访客最后一个访问的页面识别出来。那么要抓住几个特征：
 *      1）该页面是用户近期访问的第一个页面。这个可以通过该页面是否有上一个页面（last_page_id）来判断，
 *   如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
 *      2）首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
 *       这第一个特征的识别很简单，保留last_page_id为空的就可以了，但是第二个访问的判断，其实有点麻烦，
 *    首先这不是用一条数据就能得出结论的，需要组合判断，要用一条存在的数据和不存在的数据进行组合判断。而且
 *    要通过一个不存在的数据求得一条存在的数据。更麻烦的他并不是永远不存在，而是在一定时间范围内不存在。
 *    那么如何识别有一定失效的组合行为呢？
 *       最简单的办法就是Flink自带的CEP技术。这个CEP非常适合通过多条数据组合来识别某个事件。用户跳出事件，
 *   本质上就是一个条件事件加一个超时事件的组合。
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