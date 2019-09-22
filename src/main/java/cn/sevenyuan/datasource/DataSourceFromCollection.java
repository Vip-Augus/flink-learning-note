package cn.sevenyuan.datasource;

import cn.sevenyuan.domain.Student;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 来源于集合
 * @author JingQ at 2019-09-22
 */
public class DataSourceFromCollection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Student> source1 = collection1(env);
//        SingleOutputStreamOperator<Student> operator1 = source1.map(new MapFunction<Student, Student>() {
//            @Override
//            public Student map(Student student) throws Exception {
//                Student result = new Student();
//                result.setId(student.getId());
//                result.setName(student.getName());
//                result.setAge(student.getAge());
//                result.setAddress("加密地址");
//                return result;
//            }
//        });
//        operator1.print();


        DataStreamSource<Long> source2 = collection2(env);
        SingleOutputStreamOperator<Student> operator2 = source2.flatMap(new FlatMapFunction<Long, Student>() {
            @Override
            public void flatMap(Long aLong, Collector<Student> collector) throws Exception {
                if (aLong % 2 == 0) {
                    collector.collect(new Student(aLong.intValue(), "name" + aLong, aLong.intValue(), "加密地址"));
                }
            }
        });
        operator2.print();


        env.execute("test collection source");
    }

    private static DataStreamSource<Student> collection1(StreamExecutionEnvironment env) {
        List<Student> studentList = Lists.newArrayList(
                new Student(1, "name1", 23, "address1"),
                new Student(2, "name2", 23, "address2"),
                new Student(3, "name3", 23, "address3")
        );
        return env.fromCollection(studentList);
    }

    /**
     * 生成 一段序列
     * @param env   运行环境
     * @return      序列输入流
     */
    private static DataStreamSource<Long> collection2(StreamExecutionEnvironment env) {
        return env.generateSequence(1, 20);
    }
}