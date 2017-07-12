package com.github.xuzw.forexroo_scheduler;

/**
 * @author 徐泽威 xuzewei_2012@126.com
 * @time 2017年6月14日 下午12:35:45
 */
public interface NamedValue {
    int getValue();

    String getComment();

    default String toText() {
        return this.getValue() + ":" + this.getComment();
    }
}
