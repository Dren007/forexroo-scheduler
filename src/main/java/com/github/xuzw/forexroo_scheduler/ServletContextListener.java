package com.github.xuzw.forexroo_scheduler;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContextEvent;
import javax.servlet.annotation.WebListener;

import org.apache.activemq.ActiveMQConnection;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.Scheduler;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.xuzw.activemq_utils.ActiveMq;

/**
 * @author 徐泽威 xuzewei_2012@126.com
 * @time 2017年6月5日 下午6:24:03
 */
@WebListener
public class ServletContextListener implements javax.servlet.ServletContextListener {
    private static final Logger log = LoggerFactory.getLogger(ServletContextListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        log.info("contextInitialized");
        try {
            Druid.init();
            // ----
            String brokerUrl = "failover:tcp://119.23.62.18:61616";
            List<String> responseTopics = new ArrayList<>();
            responseTopics.add("Deposit_User_Info_Result_Topic");
            ActiveMq.init(ActiveMQConnection.DEFAULT_USER, ActiveMQConnection.DEFAULT_PASSWORD, brokerUrl, responseTopics);
            // ----
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.scheduleJob(JobBuilder.newJob(MasterTraderDailyClearing.class).build(), TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.dailyAtHourAndMinute(0, 1)).build());
            scheduler.scheduleJob(JobBuilder.newJob(CommissionDailyClearing.class).build(), TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.dailyAtHourAndMinute(0, 1)).build());
            scheduler.start();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.info("contextDestroyed");
    }
}
