package cn.sevenyuan.datasource;

import cn.sevenyuan.domain.Student;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;


import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * 文件输入流
 * @author JingQ at 2019-09-22
 */
public class DataSourceFromFile {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取文件地址
        URL fileUrl = DataSourceFromFile.class.getClassLoader().getResource("datasource/student.txt");
        String filePath = fileUrl.getPath();

        // 简单的文字文件输入流
//        DataStreamSource<String> textFileSource =
//                env.readTextFile(filePath);
//        SingleOutputStreamOperator<Student> textFileOperator = textFileSource.map(new MapFunction<String, Student>() {
//            @Override
//            public Student map(String s) throws Exception {
//                String[] tokens = s.split("\\W+");
//                return new Student(Integer.valueOf(tokens[0]), tokens[1], Integer.valueOf(tokens[2]), "加密地址");
//            }
//        });
//        textFileOperator.print();


        // 指定格式和监听类型
        Path pa = new Path(filePath);
        TextInputFormat inputFormat = new TextInputFormat(pa);
        DataStreamSource<String> complexFileSource =
                env.readFile(inputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100L,
                        TypeExtractor.getInputFormatTypes(inputFormat));
        SingleOutputStreamOperator<Student> complexFileOperator = complexFileSource.flatMap(new FlatMapFunction<String, Student>() {
            @Override
            public void flatMap(String value, Collector<Student> out) throws Exception {
                String[] tokens = value.split("\\W+");
                if (tokens.length > 1) {
                    out.collect(new Student(Integer.valueOf(tokens[0]), tokens[1], Integer.valueOf(tokens[2]), "加密地址"));
                }
            }
        });
        complexFileOperator.print();


        env.execute("test file source");
    }


}
