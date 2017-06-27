package com.github.xuzw.forexroo_scheduler;

import static com.github.xuzw.forexroo.entity.Tables.MT4_HISTORY_ORDER;
import static com.github.xuzw.forexroo.entity.Tables.USER;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.xuzw.forexroo.entity.tables.daos.MasterTraderRankingsHistoryDao;
import com.github.xuzw.forexroo.entity.tables.pojos.MasterTraderRankingsHistory;

/**
 * @author 徐泽威 xuzewei_2012@126.com
 * @time 2017年6月27日 下午2:24:00
 */
public class MasterTraderDailyClearing implements Job {
    private static final Logger log = LoggerFactory.getLogger(MasterTraderDailyClearing.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("execute");
        try {
            DSLContext db = DSL.using(Jooq.buildConfiguration());
            Condition orderCondition = MT4_HISTORY_ORDER.CLOSE_TIME.gt(0L);
            YyyyMmDd today = YyyyMmDd.today();
            Condition dateStartCondition = MT4_HISTORY_ORDER.CLOSE_TIME.ge(today.firstMillsecond());
            Condition dateEndCondition = MT4_HISTORY_ORDER.CLOSE_TIME.le(today.lastMillsecond());
            Condition finalCondition = Jooq.and(orderCondition, dateStartCondition, dateEndCondition);
            List<Field<?>> fields = new ArrayList<>();
            fields.add(USER.ID.as("userId"));
            fields.add(MT4_HISTORY_ORDER.LOGIN);
            fields.add(DSL.choose().when(MT4_HISTORY_ORDER.PROFIT.gt(0.0), 1).otherwise(0).sum().as("profitCount"));
            fields.add(DSL.count().as("orderCount"));
            Field<?> totalProfitField = DSL.sum(MT4_HISTORY_ORDER.PROFIT).as("totalProfit");
            fields.add(totalProfitField);
            List<MasterTraderDailyClearingRecord> rows = new ArrayList<>();
            rows.addAll(db.select().from(MT4_HISTORY_ORDER).leftJoin(USER).on(MT4_HISTORY_ORDER.LOGIN.eq(USER.MT4_REAL_ACCOUNT)).where(finalCondition).groupBy(MT4_HISTORY_ORDER.LOGIN).orderBy(totalProfitField.asc()).limit(0, 10).fetchInto(MasterTraderDailyClearingRecord.class));
            rows.addAll(db.select().from(MT4_HISTORY_ORDER).leftJoin(USER).on(MT4_HISTORY_ORDER.LOGIN.eq(USER.MT4_REAL_ACCOUNT)).where(finalCondition).groupBy(MT4_HISTORY_ORDER.LOGIN).orderBy(totalProfitField.desc()).limit(0, 10).fetchInto(MasterTraderDailyClearingRecord.class));
            MasterTraderRankingsHistoryDao masterTraderRankingsHistoryDao = new MasterTraderRankingsHistoryDao(Jooq.buildConfiguration());
            for (MasterTraderDailyClearingRecord x : rows) {
                MasterTraderRankingsHistory object = new MasterTraderRankingsHistory();
                object.setUserId(x.getUserId());
                object.setMt4RealAccount(x.getLogin());
                object.setTotalProfit(String.valueOf(x.getTotalProfit()));
                object.setSingleProfit(String.valueOf(x.getTotalProfit() / x.getOrderCount()));
                NumberFormat nt = NumberFormat.getPercentInstance();
                nt.setMinimumFractionDigits(2);
                object.setSuccessRate(nt.format(x.getProfitCount() * 1.0 / x.getOrderCount()));
                object.setTime(today.format("yyyy-MM-dd"));
                masterTraderRankingsHistoryDao.insert(object);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static class MasterTraderDailyClearingRecord {
        private Long userId;
        private Integer login;
        private Integer orderCount;
        private Integer profitCount;
        private Double totalProfit;

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Integer getLogin() {
            return login;
        }

        public void setLogin(Integer login) {
            this.login = login;
        }

        public Integer getOrderCount() {
            return orderCount;
        }

        public void setOrderCount(Integer orderCount) {
            this.orderCount = orderCount;
        }

        public Integer getProfitCount() {
            return profitCount;
        }

        public void setProfitCount(Integer profitCount) {
            this.profitCount = profitCount;
        }

        public Double getTotalProfit() {
            return totalProfit;
        }

        public void setTotalProfit(Double totalProfit) {
            this.totalProfit = totalProfit;
        }
    }
}
