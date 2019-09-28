package cn.sevenyuan.transformation.studentwindow;

import cn.sevenyuan.domain.Student;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * COUNT 统计的聚合函数实现，将结果累加，每遇到一条记录进行加一
 *
 *  reference http://wuchong.me/blog/2018/11/07/use-flink-calculate-hot-items/
 *
 * @author JingQ at 2019-09-28
 */
public class CountStudentAgg implements AggregateFunction<Student, Long ,Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Student value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
