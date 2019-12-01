package cn.sevenyuan.watermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.SimpleDateFormat;

/**
 * 计数，周期性生成水印
 *
 * @author JingQ at 2019-12-01
 */
public class WordCountPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

    private Long currentMaxTimestamp = 0L;

    // 最大允许的乱序时间是 3 s
    private final Long maxOutOfOrderness = 3000L;

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }


    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
        //定义如何提取timestamp
        long timestamp = element.f1;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        long id = Thread.currentThread().getId();
        System.out.println("线程 ID ："+ id +
                " 键值 :" + element.f0 +
                ",事件事件:[ "+sdf.format(element.f1)+
                " ],currentMaxTimestamp:[ "+
                sdf.format(currentMaxTimestamp)+" ],水印时间:[ "+
                sdf.format(getCurrentWatermark().getTimestamp())+" ]");
        return timestamp;
    }
}
