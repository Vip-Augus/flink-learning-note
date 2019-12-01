package cn.sevenyuan.watermark;

import com.google.common.base.Joiner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 乱序事件时间窗口，验证 Watermark，解决超过窗口时间达到的事件，成功加入对应的窗口，触发窗口计算
 * <p>
 * 参考链接 https://juejin.im/post/5bf95810e51d452d705fef33
 *
 * @author JingQ at 2019-11-26
 */
public class UnOrderEventTimeWindowDemo {

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        int port = 9010;
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用eventtime，默认是使用processtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1,默认并行度是当前机器的cpu数量
        env.setParallelism(1);
        //连接socket获取输入的数据
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");

        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        //抽取timestamp和生成watermark，封装成继承接口的类
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new WordCountPeriodicWatermarks());

        DataStream<String> window = waterMarkStream.keyBy(0)
                //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对window内的数据进行排序，保证数据的顺序
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<>();
                        List<String> eventTimeList = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next.f1);
                            eventTimeList.add(String.valueOf(next.f1).substring(8,10));
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = "\n 键值 : " + key + "\n              " +
                                "触发窗内数据个数 : " + arrarList.size() + "\n              " +
                                "触发窗起始数据： " + sdf.format(arrarList.get(0)) + "\n              " +
                                "触发窗最后（可能是延时）数据：" + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "\n              " +
                                "窗口内的事件数据：" + Joiner.on(",").join(eventTimeList) + "\n" +
                                "实际窗起始和结束时间： " + sdf.format(window.getStart()) + "《----》" + sdf.format(window.getEnd()) + " \n \n ";

                        out.collect(result);
                    }
                });
        //测试-把结果打印到控制台即可
        window.print();

        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("eventtime-watermark");

    }
}
