package utils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;


/**
 * @author yycstart
 * @create 2021-02-09 12:27
 */


import java.util.Properties;

public class MyKafkaUtil {

    private static Properties properties = new Properties();
    private static final String DEFAULT_TOPIC = "DEFAULT_DATA";

    static {
        String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
    }


    //创建从Kafka读取数据的Source方法
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }

    //创建往Kafka写出数据的Sink方法
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), properties);
    }

    /**
     *          除了缺省情况下回采用DEFAULT_TOPIC，
     *      一般情况下可以根据不同的业务数据在KafkaSerializationSchema中通过方法实现
     * @param serializationSchema
     * @param <T>
     * @return
     */
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        return new FlinkKafkaProducer<>(DEFAULT_TOPIC, serializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


}


