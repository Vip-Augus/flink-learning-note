package cn.sevenyuan.hotest.itemcount;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间，输出 TopN 的字符结果串
 * @author JingQ at 2019-09-29
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    private final int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    /**
     * 用于存储商品与点击数的状态，待收取同一个窗口的数据后，再触发 TopN 计算
     */
    private ListState<ItemViewCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 重载 open 方法，注册状态
        ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor =
                new ListStateDescriptor<ItemViewCount>(
                        "itemState-state",
                        ItemViewCount.class
                );
        // 用来存储收到的每条 ItemViewCount 状态，保证在发生故障时，状态数据的不丢失和一致性
        itemState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 每条数据都保存到状态中
        itemState.add(value);
        // 注册 windowEnd + 1 的 EventTime Timer，当触发时，说明收起了属于 windowEnd 窗口的所有数据
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 获取收到的所有商品点击量
        List<ItemViewCount> allItems = Lists.newArrayList(itemState.get());
        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序（也就是按照某字段正向排序，然后进行反序）
        allItems.sort(Comparator.comparing(ItemViewCount::getViewCount).reversed());
        // 将排名信息格式化成 String
        StringBuilder result = new StringBuilder();
        result.append("================================== TEST =================================\n");
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        // 遍历点击事件到结果中
        int realSize = allItems.size() < topSize ? allItems.size() : topSize;
        for (int i = 0; i < realSize; i++) {
            ItemViewCount item = allItems.get(i);
            if (item == null) {
                continue;
            }
            result.append("No ").append(i).append(":")
                    .append("    商品 ID=").append(item.getItemId())
                    .append("    游览量=").append(item.getViewCount())
                    .append("\n");
        }
        result.append("================================== END =================================\n\n");
        out.collect(result.toString());
    }
}
