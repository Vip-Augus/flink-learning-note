package cn.sevenyuan.datasource.custom;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义 DataSource，测试从 redis 取数据
 * @author JingQ at 2019-09-22
 */
public class DataSourceFromRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> customSource = env.addSource(new MyRedisDataSourceFunction());
        SingleOutputStreamOperator<String> operator = customSource
                .map((MapFunction<String, String>) value -> "当前最大值为 : " + value);
        operator.print();
        env.execute("test custom redis datasource function");
    }
}
