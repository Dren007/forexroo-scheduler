package com.github.xuzw.forexroo_scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 徐泽威 xuzewei_2012@126.com
 * @time 2017年6月27日 下午2:24:00
 */
public class MasterTraderDailyClearing implements Job {
    private static final Logger log = LoggerFactory.getLogger(MasterTraderDailyClearing.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("execute");
    }
}
