package cn.sevenyuan.datasource;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 读出 socket 数据源
 * @author JingQ at 2019-09-22
 */
public class DataSourceFromSocket {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env
                .socketTextStream("localhost", 9000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] tokens = value.split("\\W+");
                        for (String token : tokens) {
                            out.collect(Tuple2.of(token, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);
        operator.print();

        env.execute("test socket datasource");
    }
}
