package cn.sevenyuan.transformation;

import cn.sevenyuan.domain.Student;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

import java.util.stream.Stream;

/**
 * 算子操作
 *
 * @author JingQ at 2019-09-23
 */
public class OperatorTransformation {

    /**
     * 文件路径要写具体的
     */
    public static final String FILE_PATH = "/Users/jingqi/Deploy/Project/IdeaProject/flink-quick-start/src/main/resources/datasource/student.txt";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> fileSource = env.readTextFile(FILE_PATH);

        // Transformation map
//        map(env, fileSource);
        // Transformation flatMap
//        flatMap(env, fileSource);
        // Transformation filter
//        filter(env, fileSource);
        // Transformation keyBy
//        keyBy(env, fileSource);
        // Transformation keyByAndReduce
        keyByAndReduce(env, fileSource);
        // Transformation aggregations
//        keyByAndAggregations(env, fileSource);



    }

    private static void map(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        SingleOutputStreamOperator<Student> operator = source
                .map((MapFunction<String, Student>)
                        value -> parseTokens2Object(parseString2Tokens(value)));
        operator.print();
        env.execute("test Transformation : map ");
    }

    private static void flatMap(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        SingleOutputStreamOperator<String> operator = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] tokens = parseString2Tokens(value);
                if (tokens == null) {
                    return;
                }
                for (String token : tokens) {
                    out.collect(token);
                }
            }
        });
        operator.print();
        env.execute("test Transformation : flatMap");
    }

    private static void filter(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        SingleOutputStreamOperator operator = source.filter((FilterFunction<String>) value -> {
            Student stu = parseTokens2Object(parseString2Tokens(value));
            return stu != null && stu.getId() % 2 == 0;
        });
        operator.print();
        env.execute("test Transformation : filter");
    }

    private static void keyBy(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        KeyedStream<String, Integer> keyedStream = source.keyBy((KeySelector<String, Integer>) value -> {
            String[] tokens = parseString2Tokens(value);
            return tokens == null ? 0 : Integer.valueOf(tokens[0]);
        });
        keyedStream.print();
        env.execute("test Transformation : keyBy");
    }

    private static void keyByAndReduce(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        SingleOutputStreamOperator operator = source
                .map((MapFunction<String, Student>) value -> parseTokens2Object(parseString2Tokens(value)))
                .keyBy((KeySelector<Student, Integer>) value ->  value == null ? 0 : value.getId())
                .reduce((ReduceFunction<Student>) (value1, value2) -> {
                    Student student = new Student();
                    student.setId(value1.getId() + value2.getId());
                    student.setName(value1.getName() + " || " + value2.getName());
                    student.setAge(value1.getAge() + value2.getAge());
                    return student;
                });
        operator.print();
        env.execute("test Transformation : keyByAndReduce");
    }

    private static void keyByAndAggregations(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        SingleOutputStreamOperator operator = source
                .map((MapFunction<String, Student>) value -> {
                    Student student = parseTokens2Object(parseString2Tokens(value));
                    System.out.println("当前处理时间是：" + System.currentTimeMillis() + "  对象信息 : " + student.getName());
                    return  student; }
                )
                .keyBy(5)
                .minBy("age");
        operator.print();
        env.execute("test Transformation : keyByAndReduce");
    }


    private static Student parseTokens2Object(String[] tokens) {
        if (tokens == null) {
            return null;
        }
        Student stu = new Student();
        stu.setId(Integer.valueOf(tokens[0]));
        stu.setName(tokens[1]);
        stu.setAge(Integer.valueOf(tokens[2]));
        return stu;
    }

    private static String[] parseString2Tokens(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return s.split("\\W+");
    }
}
