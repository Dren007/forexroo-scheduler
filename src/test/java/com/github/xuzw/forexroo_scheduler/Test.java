package com.github.xuzw.forexroo_scheduler;

/**
 * @author 徐泽威 xuzewei_2012@126.com
 * @time 2017年6月27日 下午6:33:07
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Druid.init();
        new MasterTraderDailyClearing().execute(null);
    }
}
