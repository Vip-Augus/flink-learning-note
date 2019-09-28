package cn.sevenyuan.transformation.studentwindow;

import lombok.Data;

/**
 * 学生的统计基础类
 *
 * @author JingQ at 2019-09-28
 */
@Data
public class StudentViewCount {

    private int id;

    /**
     * 窗口结束时间
     */
    private long windowEnd;

    /**
     * 同一个 id 下的统计数量
     */
    private long viewCount;

    public static StudentViewCount of(int id, long windowEnd, long count) {
        StudentViewCount result = new StudentViewCount();
        result.setId(id);
        result.setWindowEnd(windowEnd);
        result.setViewCount(count);
        return result;
    }
}
