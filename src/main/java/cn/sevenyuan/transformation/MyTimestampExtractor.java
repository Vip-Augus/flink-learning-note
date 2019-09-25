package cn.sevenyuan.transformation;

import cn.sevenyuan.domain.Student;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author JingQ at 2019-09-24
 */
public class MyTimestampExtractor implements AssignerWithPeriodicWatermarks<Student> {


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }

    @Override
    public long extractTimestamp(Student element, long previousElementTimestamp) {
        return System.currentTimeMillis();
    }
}
