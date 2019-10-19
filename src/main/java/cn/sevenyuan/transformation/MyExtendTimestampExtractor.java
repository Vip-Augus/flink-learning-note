package cn.sevenyuan.transformation;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 简单时间抽取器，返回当前时间即可，不需要类型限制
 * @author JingQ at 2019-10-19
 */
public class MyExtendTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
        return System.currentTimeMillis();
    }
}
