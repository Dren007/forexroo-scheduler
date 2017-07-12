package com.github.xuzw.forexroo_scheduler;

import static com.github.xuzw.forexroo.entity.Tables.MT4_HISTORY_ORDER;
import static com.github.xuzw.forexroo.entity.Tables.USER;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.github.xuzw.activemq_utils.ActiveMq;
import com.github.xuzw.commons.YyyyMmDd;
import com.github.xuzw.forexroo.entity.tables.daos.BrokerCommissionSettingsDao;
import com.github.xuzw.forexroo.entity.tables.daos.DepositAndWithdrawDao;
import com.github.xuzw.forexroo.entity.tables.daos.UserDao;
import com.github.xuzw.forexroo.entity.tables.pojos.BrokerCommissionSettings;
import com.github.xuzw.forexroo.entity.tables.pojos.DepositAndWithdraw;
import com.github.xuzw.forexroo.entity.tables.pojos.User;

/**
 * @author 徐泽威 xuzewei_2012@126.com
 * @time 2017年7月12日 上午10:08:38
 */
public class CommissionDailyClearing implements Job {
    private static final Logger log = LoggerFactory.getLogger(CommissionDailyClearing.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("execute");
        try {
            Condition symbolTypeCondition = MT4_HISTORY_ORDER.SYMBOL_TYPE.isNotNull();
            Condition orderCondition = MT4_HISTORY_ORDER.CLOSE_TIME.gt(0L);
            YyyyMmDd yesterday = YyyyMmDd.now().yesterday();
            Condition dateStartCondition = MT4_HISTORY_ORDER.CLOSE_TIME.ge(yesterday.firstMillsecond() / 1000);
            Condition dateEndCondition = MT4_HISTORY_ORDER.CLOSE_TIME.le(yesterday.lastMillsecond() / 1000);
            Condition finalCondition = Jooq.and(symbolTypeCondition, orderCondition, dateStartCondition, dateEndCondition);
            List<Field<?>> fields = new ArrayList<>();
            fields.add(USER.ID.as("userId"));
            fields.add(USER.MY_BROKER_ID);
            fields.add(MT4_HISTORY_ORDER.SYMBOL_TYPE);
            fields.add(DSL.sum(MT4_HISTORY_ORDER.VOLUME).as("volume"));
            DSLContext db = DSL.using(Jooq.buildConfiguration());
            List<CommissionDailyClearingRecord> rows = db.select(fields).from(MT4_HISTORY_ORDER).leftJoin(USER).on(MT4_HISTORY_ORDER.LOGIN.eq(USER.MT4_REAL_ACCOUNT)).where(finalCondition).groupBy(MT4_HISTORY_ORDER.LOGIN, MT4_HISTORY_ORDER.SYMBOL_TYPE).fetchInto(CommissionDailyClearingRecord.class);
            DepositAndWithdrawDao depositAndWithdrawDao = new DepositAndWithdrawDao(Jooq.buildConfiguration());
            BrokerCommissionSettingsDao brokerCommissionSettingsDao = new BrokerCommissionSettingsDao(Jooq.buildConfiguration());
            BrokerCommissionSettings globalSettings = brokerCommissionSettingsDao.fetchOneByBrokerId(0L);
            UserDao userDao = new UserDao(Jooq.buildConfiguration());
            for (CommissionDailyClearingRecord x : rows) {
                Long myBrokerId = x.getMyBrokerId();
                if (myBrokerId == null || myBrokerId == 0) {
                    continue;
                }
                User myBroker = userDao.fetchOneById(myBrokerId);
                try {
                    double commissionPerVolume = 0;
                    BrokerCommissionSettings privateSettings = brokerCommissionSettingsDao.fetchOneByBrokerId(myBrokerId);
                    switch (x.getSymbolType()) {
                    case "cfd":
                        commissionPerVolume = (privateSettings != null && privateSettings.getAmountCfd() != null) ? privateSettings.getAmountCfd() : globalSettings.getAmountCfd();
                        break;
                    case "forex":
                        commissionPerVolume = (privateSettings != null && privateSettings.getAmountForex() != null) ? privateSettings.getAmountForex() : globalSettings.getAmountForex();
                        break;
                    case "metals":
                        commissionPerVolume = (privateSettings != null && privateSettings.getAmountMetals() != null) ? privateSettings.getAmountMetals() : globalSettings.getAmountMetals();
                        break;
                    case "oil":
                        commissionPerVolume = (privateSettings != null && privateSettings.getAmountOil() != null) ? privateSettings.getAmountOil() : globalSettings.getAmountOil();
                        break;
                    }
                    double amount = commissionPerVolume * x.getVolume();
                    DepositAndWithdrawTypeEnum type = DepositAndWithdrawTypeEnum.commission_deposit;
                    JSONObject mt4Request = new JSONObject();
                    mt4Request.put("login", myBroker.getMt4RealAccount());
                    mt4Request.put("operationtype", type.getValue());
                    mt4Request.put("amount", amount);
                    DepositAndWithdraw entity = new DepositAndWithdraw();
                    entity.setUserId(myBrokerId);
                    entity.setType(type.getValue());
                    entity.setAmount(String.valueOf(amount));
                    entity.setMt4RawRequest(mt4Request.toJSONString());
                    entity.setMt4RequestTime(System.currentTimeMillis());
                    JSONObject mt4Response = ActiveMq.sendRequestAndAwait("Deposit_User_Info_Topic", mt4Request);
                    entity.setMt4RawResponse(mt4Response.toJSONString());
                    entity.setMt4ResponseTime(System.currentTimeMillis());
                    String orderId = mt4Response.getString("orderid");
                    entity.setOrderId(orderId);
                    boolean success = StringUtils.isNotBlank(orderId);
                    if (!success) {
                        throw new Exception(String.format("mt4_middleware_error, userId=%s, myBrokerId=%s, message=%s", x.getUserId(), myBrokerId, mt4Response.getString("error")));
                    }
                    entity.setTime(System.currentTimeMillis());
                    depositAndWithdrawDao.insert(entity);
                } catch (Exception e) {
                    log.error("Deposit Error", e);
                }
            }
        } catch (Exception e) {
            log.error("Error", e);
        }
    }

    public static class CommissionDailyClearingRecord {
        private Long userId;
        private Long myBrokerId;
        private String symbolType;
        private Integer volume;

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getMyBrokerId() {
            return myBrokerId;
        }

        public void setMyBrokerId(Long myBrokerId) {
            this.myBrokerId = myBrokerId;
        }

        public String getSymbolType() {
            return symbolType;
        }

        public void setSymbolType(String symbolType) {
            this.symbolType = symbolType;
        }

        public Integer getVolume() {
            return volume;
        }

        public void setVolume(Integer volume) {
            this.volume = volume;
        }
    }
}
