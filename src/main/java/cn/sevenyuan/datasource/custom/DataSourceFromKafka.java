package cn.sevenyuan.datasource.custom;

import cn.sevenyuan.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 官方库 kafka 数据源（准确来说是 kafka 连接器 connector）
 * @author JingQ at 2019-09-22
 */
public class DataSourceFromKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaUtils.BROKER_LIST);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", KafkaUtils.TOPIC_STUDENT);
        props.put("key.deserializer", KafkaUtils.KEY_SERIALIZER);
        props.put("value.deserializer", KafkaUtils.VALUE_SERIALIZER);
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>(
                KafkaUtils.TOPIC_STUDENT,
                new SimpleStringSchema(),
                props
        )).setParallelism(1);

        dataStreamSource.print();

        env.execute("test custom kafka datasource");
    }

}
