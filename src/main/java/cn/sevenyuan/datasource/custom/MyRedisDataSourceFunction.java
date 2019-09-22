package cn.sevenyuan.datasource.custom;

import cn.sevenyuan.util.RedisUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author JingQ at 2019-09-22
 */
public class MyRedisDataSourceFunction extends RichSourceFunction<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // noop
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (true) {
            String maxNumber = RedisUtils.get("maxNumber", String.class);
            ctx.collect(StringUtils.isBlank(maxNumber) ? "0" : maxNumber);
            // 隔 1 s 执行程序
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        // noop
    }

    @Override
    public void close() throws Exception {
        super.close();
        RedisUtils.close();
    }
}
