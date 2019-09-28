package cn.sevenyuan.hotest.itemcount;

import lombok.Data;

/**
 * 简单的用户行为
 * @author JingQ at 2019-09-28
 */
@Data
public class UserBehavior {

    /**
     * 用户 ID
     */
    private long userId;

    /**
     * 商品 ID
     */
    private long itemId;

    /**
     * 商品类目 ID
     */
    private int categoryId;

    /**
     * 用户行为，包括("pv", "buy", "cart", "fav")
     */
    private String behavior;

    /**
     * 行为发生的时间戳，模拟数据中自带了时间属性
     */
    private long timestamp;

}
