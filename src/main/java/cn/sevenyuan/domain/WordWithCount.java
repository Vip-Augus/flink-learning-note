package cn.sevenyuan.domain;

import lombok.Data;

/**
 * @author JingQ at 2019-09-18
 */
@Data
public class WordWithCount {

    private String word;

    private long count;

    public WordWithCount() {}

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }

}
