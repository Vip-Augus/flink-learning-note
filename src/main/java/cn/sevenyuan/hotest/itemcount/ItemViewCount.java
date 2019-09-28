package cn.sevenyuan.hotest.itemcount;

import lombok.Data;

/**
 * 商品点击量（窗口操作的输出类型）
 *
 * @author JingQ at 2019-09-28
 */
@Data
public class ItemViewCount {

    /**
     * 商品 ID
     */
    private long itemId;

    /**
     * 窗口结束时间戳
     */
    private long windowEnd;

    /**
     * 商品的点击数
     */
    private long viewCount;

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.setItemId(itemId);
        result.setWindowEnd(windowEnd);
        result.setViewCount(viewCount);
        return result;
    }
}
