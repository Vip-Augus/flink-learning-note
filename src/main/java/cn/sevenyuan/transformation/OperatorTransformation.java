package cn.sevenyuan.transformation;

import cn.sevenyuan.domain.Student;
import cn.sevenyuan.wordcount.SocketWindowWordCount;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * 算子操作
 *
 * @author JingQ at 2019-09-23
 */
public class OperatorTransformation {

    /**
     * 文件路径
     */
    public static final String FILE_PATH = OperatorTransformation.class.getClassLoader().getResource("datasource/student.txt").getPath();

    public static final String MIN_BY_FILE_PATH = OperatorTransformation.class.getClassLoader().getResource("datasource/student_min&minBy.txt").getPath();


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
//        keyByAndReduce(env, fileSource);

        // Transformation aggregations
//        keyByAndAggregations(env, fileSource);

        // Transformation fold
//        fold(env, fileSource);

        // Transformation timeWindow
//        window();

        // Transformation windowAll
//        windowAllAndApply(env, fileSource);

        // Transformation union
//        union(env, fileSource);

        // Transformation split
//        splitAndSelect(env, fileSource);

        // Transformation cogroup
        cogroup(env, fileSource);

        // Transformation connectAndCogroupMap
//        connectAndCogroupMap(env, fileSource);

        // Transformation project
//        project(env);
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
        source = env.readTextFile(MIN_BY_FILE_PATH);
        SingleOutputStreamOperator operator = source
                .map((MapFunction<String, Student>) value -> {
                    Student student = parseTokens2Object(parseString2Tokens(value));
                    System.out.println("当前处理时间是：" + System.currentTimeMillis() + "  对象信息 : " + student.getName());
                    return  student; }
                )
                .keyBy("id")
                .minBy("age")
                ;
        operator.print();
        env.execute("test Transformation : keyByAndReduce");
    }

    private static void fold(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        SingleOutputStreamOperator operator = source
                .map((MapFunction<String, Student>) value -> parseTokens2Object(parseString2Tokens(value)))
                .keyBy("id")
                .fold("strat", new FoldFunction<Student, String>() {
                    @Override
                    public String fold(String accumulator, Student value) throws Exception {
                        return accumulator + " || " + value;
                    }
                });
        operator.print();
        env.execute("test Transformation : fold");
    }

    private static void window() throws Exception {
        // 读取文件速度太快了，这里参考前面写过的 socket 输入流，栗子中的窗口是时间驱动 timeWindow
        SocketWindowWordCount.main(new String[] {"--port", "9000"});
    }

    private static void windowAllAndApply(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        // 在普通流上进行窗口操作，会将所有分区的流都汇集到单个的 Task 中，性能会降低
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Student> operator = source
                .map((MapFunction<String, Student>) value -> parseTokens2Object(parseString2Tokens(value)))
                .assignTimestampsAndWatermarks(new MyTimestampExtractor())
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                .apply(new AllWindowFunction<Student, Student, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Student> values, Collector<Student> out) throws Exception {
                int sum = 0;
                for (Student student : values) {
                    sum++;
                    out.collect(student);
                }
                System.out.println("当前时间 : " + System.currentTimeMillis() + " values 数量 : " + sum);
            }
        });

        operator.print();
        env.execute("test Transformation : windowAll");
    }

    private static void union(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        // 合并两个流
        DataStream<String> unionSource = source.union(source);
        SingleOutputStreamOperator operator = unionSource
                .map((MapFunction<String, Student>) value -> parseTokens2Object(parseString2Tokens(value)))
                .keyBy("id")
                .reduce(new ReduceFunction<Student>() {
                    @Override
                    public Student reduce(Student value1, Student value2) throws Exception {
                        Student student = new Student();
                        student.setId(value1.getId());
                        student.setName(value1.getName() + " || " + value2.getName());
                        student.setAge(value1.getAge() + value2.getAge());
                        return student;
                    }
                });
        operator.print();
        env.execute("test Transformation : union");
    }

    private static void cogroup(StreamExecutionEnvironment env, DataStreamSource<String> source) throws  Exception {
        DataStreamSource<Student> studentDataStreamSource = env.fromCollection(Lists.newArrayList(
                new Student(1, "otherName1", 1, "a"),
                new Student(2, "otherName2", 2, "b"),
                new Student(3, "otherName3", 3, "c")
        ));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 分配时间提取器

        SingleOutputStreamOperator<Student> stream1 = source
                .map((MapFunction<String, Student>) value -> parseTokens2Object(parseString2Tokens(value)))
                .assignTimestampsAndWatermarks(new MyExtendTimestampExtractor<>());

        SingleOutputStreamOperator<Student> stream2 = studentDataStreamSource
                .assignTimestampsAndWatermarks(new MyExtendTimestampExtractor<>());


        stream1.coGroup(stream2)
                .where(
                        (KeySelector<Student, String>) value -> value.getAddress())
                .equalTo(
                        (KeySelector<Student, String>) value -> value.getAddress())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .apply(new CoGroupFunction<Student, Student, Object>() {
                    @Override
                    public void coGroup(Iterable<Student> first, Iterable<Student> second, Collector<Object> out) throws Exception {
                        out.collect(first);
                        out.collect(second);
                    }
                })
        .print();
        env.execute("test cogroup");
    }

    private static void connectAndCogroupMap(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        DataStreamSource<Student> studentDataStreamSource = env.fromCollection(Lists.newArrayList(
                new Student(1, "otherName1", 1, ""),
                new Student(2, "otherName2", 2, ""),
                new Student(3, "otherName3", 3, "")
        ));

        // “连接”两个保存其类型的数据流。连接允许两个流之间的共享状态。
        // 例如自定义一个 MyCoFlatMapFunction 类，继承自 RichCoFlatMapFunction，在里面实现自己的逻辑
        // 可以参考这篇文章 https://training.ververica.com/lessons/connected-streams.html
        ConnectedStreams<String, Student> connectedStreams = source.connect(studentDataStreamSource);
        connectedStreams.flatMap(new CoFlatMapFunction<String, Student, Object>() {
            @Override
            public void flatMap1(String value, Collector<Object> out) throws Exception {
                // 状态 1
                out.collect("Add prefix : " + value);
            }

            @Override
            public void flatMap2(Student value, Collector<Object> out) throws Exception {
                // 状态 2
                if (value.getId() % 2 != 0) {
                    out.collect(value);
                }
            }
        }).print();

        env.execute("test Transformation : connection");
    }

    private static void splitAndSelect(StreamExecutionEnvironment env, DataStreamSource<String> source) throws Exception {
        SplitStream<String> splitStream = source.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                String[] tokens = parseString2Tokens(value);
                int num = Integer.valueOf(tokens[0]);
                List<String> output = new ArrayList<>();
                if (num % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<String> even = splitStream.select("even");
        DataStream<String> odd = splitStream.select("odd");
        DataStream<String> all = splitStream.select("even","odd");

        even.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String value, Collector<Object> out) throws Exception {
                System.out.println("even stream : " + value);
            }
        });

        odd.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String value, Collector<Object> out) throws Exception {
                System.out.println("odd stream : " + value);
            }
        });

        all.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String value, Collector<Object> out) throws Exception {
                System.out.println("all stream : " + value);
            }
        });

        env.execute("test Transformation : split & select");
    }

    private static void project(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Tuple4<String, Integer, Long, BigDecimal>> customSource = env.fromCollection(
                Lists.newArrayList(
                        Tuple4.of("one", 1, 1L, BigDecimal.ONE),
                        Tuple4.of("two", 2, 2L, BigDecimal.ZERO),
                        Tuple4.of("three", 3, 3L, BigDecimal.TEN),
                        Tuple4.of("four", 4, 4L, BigDecimal.TEN)
                )
        );
        // 分离下标 1，3 到新到数据流
        DataStream<Tuple2<Integer, BigDecimal>> tuple2DataStreamSource = customSource.project(1, 3);
        tuple2DataStreamSource.print();
        env.execute("test Transformation : project");
    }


    private static Student parseTokens2Object(String[] tokens) {
        if (tokens == null) {
            return null;
        }
        Student stu = new Student();
        stu.setId(Integer.valueOf(tokens[0]));
        stu.setName(tokens[1]);
        stu.setAge(Integer.valueOf(tokens[2]));
        stu.setAddress(tokens[3]);
        return stu;
    }

    private static String[] parseString2Tokens(String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        return s.split("\\W+");
    }
}
