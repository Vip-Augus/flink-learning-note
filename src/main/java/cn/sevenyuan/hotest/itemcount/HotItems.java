package cn.sevenyuan.hotest.itemcount;

import cn.sevenyuan.datasource.DataSourceFromFile;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.net.URL;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * reference http://wuchong.me/blog/2018/11/07/use-flink-calculate-hot-items/
 * @author JingQ at 2019-09-28
 */
public class HotItems {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了打印到控制台的结果不乱序，配置全局的并发度为 1（对正确性没有影响）
        env.setParallelism(1);
        // 以下步骤，是为了使用 PojoCsvInputFormat，读取 csv 文件并转成 POJO
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File("/Users/jingqi/Deploy/Project/IdeaProject/flink-quick-start/src/main/resources/UserBehavior.csv"));
        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出来的字段顺序是不确定的，需要显示指定文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fieldOrder);

        // 开始创建输入源
        DataStream<UserBehavior> dataSource = env.createInput(csvInputFormat, pojoTypeInfo);

        // 一、设置 EventTime 模式，用基于数据中自带的时间戳（默认是【事件处理】processing time）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 二、指定获取业务时间，以及生成 Watermark（用来追踪事件的概念，用来指示当前处理到什么时刻的数据）
        DataStream<UserBehavior> timeData = dataSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        // 原始单位是秒，需要乘以 1000 ，转换成毫秒
                        return element.getTimestamp() * 1000;
                    }
                });

        // 使用过滤算子 filter，筛选出操作行为中是 pv 的数据
        DataStream<UserBehavior> pvData = timeData
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.getBehavior());
                    }
                });

        // 设定滑动窗口 sliding window，每隔五分钟统计最近一个小时的每个商品的点击量
        // 经历过程 dataStream -> keyStream -> dataStream
        DataStream<ItemViewCount> windowData = pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());

        // 统计最热门商品
        DataStream<String> topItems = windowData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));

        topItems.print();
        env.execute("Test Hot Items Job");
    }


}
