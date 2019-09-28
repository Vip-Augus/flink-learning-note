package cn.sevenyuan.hotest.itemcount;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用于输出窗口的结果
 *
 * WindowFunction 函数定义 WindowFunction<IN, OUT, KEY, W extends Window>
 * @author JingQ at 2019-09-28
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(
            Tuple tuple,                    // 窗口的主键，即 itemId
            TimeWindow window,              // 窗口
            Iterable<Long> input,           // 聚合函数，即 count 值
            Collector<ItemViewCount> out)   // 输出类型是 ItemViewCount
            throws Exception {
        Long itemId = ((Tuple1<Long>) tuple).f0;
        Long count = input.iterator().next();
        out.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
}
