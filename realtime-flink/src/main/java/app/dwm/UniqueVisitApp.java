package app.dwm;

import com.alibaba.fastjson.JSON;

import utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;


/**
 * @author yycstart
 * @create 2021-02-13 17:50
 */
public class UniqueVisitApp {
    public static void main(String[] args) {

        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //2. 定义消费者参数
        String groupId = "unique_visit_app";
        String sourceTopic = "DWD_PAGE_LOG";
        String sinkTopic = "DWM_UNIQUE_VISIT";

        //3. 读取Kafka主题数据，创建流
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> streamSource = env.addSource(kafkaSource);

        //4.将数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjStream = streamSource.map(JSON::parseObject);

        /**

         1)	 首先用keyBy按照mid进行分组，因为之后我们要使用keyedState.
         2)	 因为我们及需要使用状态，又要进行过滤所以要使用RichFilterFunction
         3)	重写open 方法用来初始化状态
         4)	重写filter方法进行过滤
             a)	可以直接筛掉last_page_id不为空的字段，因为只要有上一页，说明这条不是这个用户进入的首个页面。
             b)	 状态用来记录用户的进入时间，只要这个lastVisitDate是今天，就说明用户今天已经访问过了所以筛除掉。
           如果为空或者不是今天，说明今天还没访问过，则保留。
             c)	因为状态值主要用于筛选是否今天来过，所以这个记录过了今天基本上没有用了，这里enableTimeToLive 设定
           了1天的过期时间，避免状态过大。


         */
        //5. 按照Mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //6. 过滤数据，如果有上一次访问的页面则过滤掉，如果上一次访问日期不为空也过滤掉
        SingleOutputStreamOperator<JSONObject> filter = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //定义上一次访问日期状态
            private ValueState<String> lastVisitState;
            //定义日期格式化对象
            private SimpleDateFormat simpleDateFormat;

            /**
             * 声明周期方法，做属性的初始化
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visit-state", String.class));
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                Long ts = value.getLong("ts");
                String startDate = simpleDateFormat.format(ts);
                String lastVisitDate = lastVisitState.value();
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                System.out.println("起始访问");
                if (lastVisitDate != null && lastVisitDate.length() > 0 && startDate.equals(lastVisitDate)) {
                    System.out.println("已访问：lastVisit:" + lastVisitDate + "|| startDate:" + startDate);
                } else {
                    System.out.println("未访问：lastVisit:" + lastVisitDate + "|| startDate:" + startDate);
                    lastVisitState.update(startDate);
                }
                return true;
            }
        });

        //7. 将数据转换成字符串
        SingleOutputStreamOperator<String> jsonStrStream = filter.map(JSON::toString);

        //8. 数据输出到对应的主题中
        jsonStrStream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //9. 启动任务
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
