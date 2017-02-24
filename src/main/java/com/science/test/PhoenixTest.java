package com.science.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.util.InstanceResolver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PhoenixTest {

    /**
     * 1: thread 2: records
     *
     * @param args
     * @throws InterruptedException
     */

    public static void main(String[] args) throws InterruptedException {
        final String envType;
        int threadnum;
        final int recordsPerThread;
        final int numPerCommit;
        final int slatnum;

        if (args.length == 0) {
            log.info("run initial program:");
            envType = "policy";
            threadnum = 2;
            recordsPerThread = 10;
            numPerCommit = 1;
            slatnum = 6;
        } else {
            log.info("----------param,envType:[{}],thread:[{}],records:[{}],numPerCommit:[{}],slat:[{}]", args[0], args[1], args[2], args[3],
                    (args.length > 4 ? Integer.parseInt(args[4]) : 0));
            envType = args[0];
            threadnum = Integer.parseInt(args[1]);
            recordsPerThread = Integer.parseInt(args[2]);
            numPerCommit = Integer.parseInt(args[3]);
            slatnum = args.length > 4 ? Integer.parseInt(args[4]) : 0;
        }

        final PhoenixTest phoenixTest = new PhoenixTest();
        final CountDownLatch countDownLatch = new CountDownLatch(threadnum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadnum);
        phoenixTest.init();
        long curtime = System.currentTimeMillis();
        final AtomicInteger numall = new AtomicInteger(0);

        for (int i = 0; i < threadnum; i++) {
            final Integer threadId = Integer.valueOf(i);
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    String threadName = Thread.currentThread().getName();
                    try {
                        final Connection conn = DriverManager
                                .getConnection("jdbc:phoenix:10.253.11.207,10.139.113.47,10.253.12.4:2181");
                        int suc;
                        switch (envType) {
                            case "policy":
                                suc = phoenixTest.testUpsetPolicy(conn, threadName, recordsPerThread, numPerCommit, slatnum, threadId);
                                break;
                            case "user":
                                suc = phoenixTest.testUpset(conn, threadName, recordsPerThread, numPerCommit, slatnum);
                                break;
                            case "userBatch":
                                suc = phoenixTest.testBatch(conn, threadName, recordsPerThread, numPerCommit, slatnum);
                                break;
                            default:
                                suc = phoenixTest.testUpsetPolicy(conn, threadName, recordsPerThread, numPerCommit, slatnum, threadId);
                                break;
                        }

//						int suc = phoenixTest.testUpset(conn, threadName, recordsPerThread, numPerCommit, slatnum);
//						int suc = phoenixTest.testUpsetPolicy(conn, threadName, recordsPerThread, numPerCommit, slatnum, threadId);
                        // int suc = phoenixTest.testBatch(conn, threadName,
                        // recordsPerThread, numPerCommit);
                        numall.addAndGet(suc);
                    } catch (SQLException e) {
                        log.error("init error", e);
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            });
        }
        countDownLatch.await();
        long usetime = System.currentTimeMillis() - curtime;
        log.info("----------time:{} ,numall:{},tps:{}", usetime, numall.get(), numall.get() / (usetime / 1000));
    }

    public int testBatch(Connection conn, String threadName, int recordsPerThread, int numPerCommit, int slatnum)
            throws SQLException {

        PhoenixStatement stmt = (PhoenixStatement) conn.createStatement();
        int suc = 0;
        int error = 0;
        long curtime = System.currentTimeMillis();
        String slatstr = slatnum > 0 ? String.valueOf(slatnum) : "";
        log.info("----------batch test,use table user{}", slatstr);
        for (int j = 0; j < recordsPerThread; j++) {
            try {
                String id = String.format("%s-%s", threadName, j);
                stmt.addBatch("UPSERT INTO user" + slatstr + "(id, firstname,lastname) values('" + id + "','zjh','zhou')");
                if (j % numPerCommit == 0 || j == recordsPerThread - 1) {
//					System.out.println("no:" + j);
                    stmt.executeBatch();
                    conn.commit();
                }
                suc++;
            } catch (Exception e) {
                error++;
                log.error("error", e);
            }
        }
        long usetime = System.currentTimeMillis() - curtime;
        log.info("thread {} ,suc {} ,error:{},time:{} ,tps:{}", threadName, suc, error, usetime,
                suc / (usetime / 1000));
        stmt.close();
        return suc;
    }

    public int testUpset(Connection conn, String threadName, int recordsPerThread, int numPerCommit, int slatnum)
            throws SQLException {
        Statement stmt = conn.createStatement();
        int suc = 0;
        int error = 0;
        long curtime = System.currentTimeMillis();
        String slatstr = slatnum > 0 ? String.valueOf(slatnum) : "";
        log.info("----------use table user{}", slatstr);
        for (int j = 0; j < recordsPerThread; j++) {
            try {
                String id = String.format("%s-%s", threadName, j);
                stmt.execute("UPSERT INTO user" + slatstr + "(id, firstname,lastname) values('" + id + "','zjh','zhou')");
                if (j % numPerCommit == 0 || j == recordsPerThread - 1) {
//					System.out.println("no:" + j);
                    conn.commit();
                }
                suc++;
            } catch (Exception e) {
                error++;
                log.error("error", e);
            }
        }
        long usetime = System.currentTimeMillis() - curtime;
        log.info("thread {} ,suc:{} ,error:{},time:{} ,tps:{}", threadName, suc, error, usetime,
                suc / (usetime / 1000));
        stmt.close();
        return suc;
    }


    public int testUpsetPolicy(Connection conn, String threadName, int recordsPerThread, int numPerCommit, int slatnum, Integer threadId)
            throws SQLException {
        Statement stmt = conn.createStatement();
        int suc = 0;
        int error = 0;
        long curtime = System.currentTimeMillis();
        String slatstr = slatnum > 0 ? String.valueOf(slatnum) : "";
        log.info("------------use table policy{}", slatstr);
        for (int j = 0; j < recordsPerThread; j++) {
            try {
//				int id = 1234;
                Integer id = Integer.parseInt(String.valueOf(j) + String.valueOf(threadId));
                String upsertStr = "UPSERT INTO policy6(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,parent_policy_no," +
                        "parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place," +
                        "remark,uw_remark,uw_operator,uw_date,insurant_id,policy_holder_id,benefitids,insured_type,order_user_id,insured_id,insured_no,channel_policy_no," +
                        "channel_policy_end_time,log_date,is_deleted,extra_info,extra_common_info,creator,gmt_create,modifier,gmt_modified," +
                        "used_sum_insured,policy_package_id,package_def_id,campaign_def_id,substituted_money_vat,primary_premium,no_tax_premium,is_multiple_payer,total_pay_limit,is_present," +
                        "comment,single_pay_limit,commission_rate,commission_amount,invoice_type,is_free_tax,is_foreign_currency,is_periodic_settlement,billing_cycle,system_code,standard_premium,waiting_period,discount_rate" +
                        ") values(" + id + " , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +
                        "2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','shanghai'," +
                        "'','','system','2014-05-23',NULL,NULL,'',NULL,NULL,'15042710271148','',''," +
                        "'','','N','','','liulei','2015-11-06','liulei','2015-11-06'," +
                        "'',0,170001,200003,'','','','','',''," +
                        "'','','','',NULL,'','','','','','','','')";
//				String upsertStr = "UPSERT INTO policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id," +
//						"product_id,contract_id,parent_policy_id,parent_policy_no,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place" +
//						") values(300000035 , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +
//						"2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','上海市')";
                stmt.execute(upsertStr);
                if (j % numPerCommit == 0 || j == recordsPerThread - 1) {
//					System.out.println("no:" + j);
                    conn.commit();
                }
                suc++;
            } catch (Exception e) {
                error++;
                log.error("error", e);
            }
        }
        long usetime = System.currentTimeMillis() - curtime;
        log.info("thread {} ,suc {} ,error:{},time:{} ,tps:{}", threadName, suc, error, usetime,
                suc / (usetime / 1000));
        stmt.close();
        return suc;
    }

    public void init() {
        // 10.253.11.207 master
        // 10.139.113.47 slave1
        // 10.253.12.4 slave2
        // 10.253.3.12 slave3
        // KerBeroos
        //测试

        final Configuration conf = HBaseConfiguration.create();
        // conf.set("hbase.master", "master:60000");
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        // conf.set("hbase.zookeeper.quorum",
        // "10.253.11.207,10.139.113.47,10.253.12.4");
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conf.setBoolean("hbase.table.sanity.checks", false);// TODO sanity检查
        conf.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");// TODO
        // 多个二级索引

        try {
            UserGroupInformation.setConfiguration(conf);
        } catch (Exception e) {

        }

        // Clear the cached singletons so we can inject our own.
        // InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default
        // Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }

            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });

    }
}
