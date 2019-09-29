package cn.sevenyuan.datasource.custom;

import cn.sevenyuan.domain.Student;
import cn.sevenyuan.sink.SinkToMySQL;
import cn.sevenyuan.util.KafkaUtils;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
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


        // 数据下沉
        addMySQLSink(dataStreamSource);
        env.execute("test custom kafka datasource");
    }

    private static void addMySQLSink(DataStreamSource<String> dataStreamSource) {
        // 从 kafka 读数据，然后进行 map 映射转换
        DataStream<Student> dataStream = dataStreamSource.map(value -> JSONObject.parseObject(value, Student.class));
        // 不需要 keyBy 分类，所以使用 windowAll，每 10s 统计接收到的数据，批量插入到数据库中
        dataStream
                .timeWindowAll(Time.seconds(10))
                .apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out) throws Exception {
                        List<Student> students = Lists.newArrayList(values);
                        if (students.size() > 0) {
                            System.out.println("最近 10 秒汇集到 " + students.size() + " 条数据");
                            out.collect(students);
                        }
                    }
                })
                .addSink(new SinkToMySQL());
        // 输出结果如下：
        // 最近 10 秒汇集到 3 条数据
        // success insert number : 3
    }
}
