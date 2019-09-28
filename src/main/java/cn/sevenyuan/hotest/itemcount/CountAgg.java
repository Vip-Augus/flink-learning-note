package cn.sevenyuan.hotest.itemcount;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * COUNT 统计的聚合函数实现，每出现一条记录加一
 * @author JingQ at 2019-09-28
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        // 返回输出结果，这里提前聚合掉数据，减少 state 的存储压力
        return a + b;
    }
}
