package app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yycstart
 * @create 2021-02-09 12:52
 */
public class Dwd_Base_Log {

    //定义用户行为主题信息
    private static final String TOPIC_START = "DWD_START_LOG";
    private static final String TOPIC_PAGE = "DWD_PAGE_LOG";
    private static final String TOPIC_DISPLAY = "DWD_DISPLAY_LOG";

    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 设置CK相关参数
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoiny"));

        //3. 指定消费者配置信息
        String groupId = "ods_dwd_base_log_app";
        String topic = "ODS_BASE_LOG";

        //4. 从指定的Kafka主题中读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> baseLogDS = env.addSource(kafkaSource);

        //5. 转换成Json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = baseLogDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        /**
         * 功能二：增加识别新老访客
         *      保存每个mid的首次访问日期，每条进入该算子的访问记录，
         *   都会把mid对应的首次访问时间读取出来，跟当前日期进行比较，
         *   只有首次访问时间不为空，且首次访问时间早于当日的，则认为该访客是老访客，否则是新访客。
         *  同时如果是新访客且没有访问记录的话，会写入首次访问时间。
         */


        //6. 按照Mid进行分组
        KeyedStream<JSONObject, String> midKeyedStream = jsonObjectDS.keyBy(data ->
                data.getJSONObject("common").getString("mid"));

        //7. 对数据做处理，校验数据中Mid是否为今天第一次访问
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDStream = midKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //声明第一次访问日期的状态
            private ValueState<String> firstVisitDataState;
            //声明日期数据格式化对象
            private SimpleDateFormat simpleDataFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDataState", String.class));
                simpleDataFormat = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //打印数据
                System.out.println(jsonObject);

                //获取数据中携带的是否为第一次访问标记（'0'代表不是第一次，'1'表示为第一次）
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                //获取数据中的时间戳
                Long ts = jsonObject.getLong("ts");

                //判断标记，如果为'1'，则继续校验
                if ("1".equals(isNew)) {
                    String newMidValue = firstVisitDataState.value();
                    String tsDate = simpleDataFormat.format(new Date(ts));

                    //如果新用户状态不为空，则将标记置为0
                    if (firstVisitDataState != null && newMidValue.length() == 0 && !newMidValue.equals(tsDate)) {
                        isNew = "0";
                        jsonObject.getJSONObject("common").put("isNew", isNew);
                    }

                    //更新状态
                    firstVisitDataState.update(tsDate);
                }

                //返回添加了新标记的数据
                return jsonObject;

            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        /**
         * 功能三：利用侧输出流实现数据拆分
         */

        //8. 定义启动和曝光数据的测输出流标签
        final OutputTag<String> startTag = new OutputTag<String>("start") {};
        final OutputTag<String> displayTag = new OutputTag<String>("display") {};

        //9. 根据数据内容将数据分为3类，页面日志输出到主流，
        // 启动日志输出到启动测输出流，曝光日志输出到曝光日志测输出流

        SingleOutputStreamOperator<String> pageDStream = midWithNewFlagDStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                //获取数据中相关的启动相关字段
                JSONObject startJsonObj = jsonObject.getJSONObject("start");
                //将数据转换成字符串，准备最后的输出
                String dataString = jsonObject.toString();

                //启动数据不为空，输出到测输出流
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    context.output(startTag, dataString);
                } else {
                    //非启动日志，则为页面日志或者曝光日志（携带页面信息）
                    System.out.println("PageString" + dataString);

                    //将数据输出到主流，即是页面日志流
                    collector.collect(dataString);

                    //获取数据的曝光数据，如果不为空，则将每条曝光数据写入到曝光测输出流
                    JSONArray display = jsonObject.getJSONArray("display");
                    if (display != null && display.size() > 0) {
                        for (int i = 0; i < display.size(); i++) {
                            JSONObject displayJSONObject = display.getJSONObject(i);
                            String pageId = displayJSONObject.getJSONObject("page").getString("page_id");
                            displayJSONObject.put("page_id", pageId);
                            context.output(displayTag, displayJSONObject.toString());
                        }
                    }
                }
            }
        });

        //10. 获取测输出流数据
        DataStream<String> startDStream = pageDStream.getSideOutput(startTag);
        DataStream<String> displayDStream = pageDStream.getSideOutput(displayTag);

        //11. 将各个流的数据写入到队形的主题中
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);

        pageDStream.addSink(pageSink);
        startDStream.addSink(startSink);
        displayDStream.addSink(displaySink);

        //打印测试
        jsonObjectDS.print();
        env.execute("Dwd_Base_Log Job");

    }
}
