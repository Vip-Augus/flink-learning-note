package cn.sevenyuan.util;

import cn.sevenyuan.domain.Student;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

/**
 * kafka 工具类
 * @author JingQ at 2019-09-22
 */
public class KafkaUtils {

    public static final String BROKER_LIST = "localhost:9092";

    public static final String TOPIC_STUDENT = "student";

    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void writeToKafka() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 制造传递的对象
        int randomInt = RandomUtils.nextInt(1, 100);
        Student stu = new Student(randomInt, "name" + randomInt, randomInt, "=-=");
        stu.setCheckInTime(new Date());
        // 发送数据
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_STUDENT, null, null, JSON.toJSONString(stu));
        producer.send(record);
        System.out.println("kafka 已发送消息 : " + JSON.toJSONString(stu));
        producer.flush();
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            Thread.sleep(3000);
            writeToKafka();
        }
    }
}
