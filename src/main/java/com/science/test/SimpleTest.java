package com.science.test;

import com.science.util.ScienceUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class SimpleTest {
	private static Connection conn;

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		ScienceUtil.init();
		conn = DriverManager.getConnection("jdbc:phoenix:10.253.11.207,10.139.113.47,10.253.12.4:2181");
//
		SimpleTest test = new SimpleTest();
		test.testUpset();
//		test.testCoreUpset();
	}

	public void testUpset() throws SQLException {
		Statement stmt = conn.createStatement();

//		CONSTRAINT uk_policy_channelid_productid_channelpolicyno PRIMARY KEY  (id,channel_id,product_id,channel_policy_no
// 		stmt.execute(createStr);
//		stmt.execute("UPSERT INTO zhaixt_test(id, firstname,lastname) values('pool1','zjh','翟')");
		Integer id = 14;
		String upsertStr19 = "upsert into policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id," +
				"product_id,contract_id,parent_policy_no,parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place" +
				") values(12 , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +
				"2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','上海市')";

		String upsertStr3 = "upsert into policy_0000(id, policy_no,apply_no" +
				") values("+id+" , '8261230403273160','816123039825838111')";
		String upsertStr = "upsert into policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,parent_policy_no,parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place" +
				") values("+id+" , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405',2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','shanghai')";
//		stmt.execute(upsertStr);
//		stmt.execute("create table ZHAIXT_TEST ( id bigint(20)  not null primary key , firstname varchar(20) , lastname varchar(20),age integer(11)) ");
//		stmt.execute("create table ZHAIXT_TEST ( id varchar(20)  not null primary key , firstname bigint(20) , lastname varchar(20)) ");

//		stmt.execute("UPSERT into  ZHAIXT_TEST( id,firstname,lastname,age) values (123,'晓彤','翟',18)  ON DUPLICATE KEY UPDATE age = eage + 1");
		stmt.execute("UPSERT into  ZHAIXT_TEST( id,firstname,lastname,age) values (123,'晓彤','翟',14)");
//		stmt.execute("UPDATE  ZHAIXT_TEST Set age = age +5 where id = 123 ");
		/*有null*/
//		stmt.execute("UPSERT into  ZHAIXT_TEST( id,firstname,lastname,age) values (123,NULL,'lala',NULL)");
		/*insert字段不全*/
//		stmt.execute("UPSERT into  ZHAIXT_TEST( id,firstname,lastname) values (123,NULL,'lala')");

//		stmt.execute("create table ZHAIXT_TEST ( id varchar(20)  not null primary key , firstname integer(20) , lastname varchar(20), date_time Date) ");
//		stmt.execute("UPSERT into  ZHAIXT_TEST( id,firstname,lastname,date_time) values ('123',12,'lala','2015-04-29')");

		conn.commit();
		stmt.close();
		log.info("upset sucess");
	}
	public void testCoreUpset() throws SQLException {
		Statement stmt = conn.createStatement();
//		String createStr= "CREATE TABLE policy_0000 (\n" +
//				"  id bigint(20) NOT NULL primary key ,\n" +
//				"  policy_no varchar(50) ,\n" +
//				"  apply_no varchar(50),\n" +
//				"  insurance_certi_no varchar(50) ,\n" +
//				"  policy_type tinyint(4) ,\n" +
//				"  policy_status tinyint(4) ,\n" +
//				"  channel_id integer(11),\n" +
//				"  product_id bigint(20)  ,\n" +
//				"  contract_id bigint(20)  ,\n" +
//				"  parent_policy_no varchar(50)  ,\n" +
//				"  parent_policy_id bigint(20)  ,\n" +
//				"  sum_insured varchar(50)  ,\n" +
//				"  premium varchar(50)  ,\n" +
//				"  pay_frequency tinyint(4)  ,\n" +
//				"  insure_date date  ,\n" +
//				"  effective_date date  ,\n" +
//				"  expiry_date date  ,\n" +
//				"  issue_date date  ,\n" +
//				"  insure_place varchar(255)  ,\n" +
//				"  remark varchar(4000)  ,\n" +
//				"  uw_remark varchar(255)  ,\n" +
//				"  uw_operator varchar(50)  ,\n" +
//				"  uw_date date  ,\n" +
//				"  insurant_id bigint(20)  ,\n" +
//				"  policy_holder_id bigint(20)  ,\n" +
//				"  benefitids varchar(255)  ,\n" +
//				"  insured_type integer(11)  ,\n" +
//				"  order_user_id bigint(20)  ,\n" +
//				"  insured_id varchar(128) ,\n" +
//				"  insured_no varchar(50) ,\n" +
//				"  channel_policy_no varchar(50)  ,\n" +
//				"  channel_policy_end_time date,  \n" +
//				"  log_date date ,\n" +
//				"  is_deleted char(1)  ,\n" +
//				"  extra_info varchar(1024) ,\n" +
//				"  extra_common_info varchar(512) ,\n" +
//				"  creator varchar(50) ,\n" +
//				"  gmt_create date ,\n" +
//				"  modifier varchar(50) ,\n" +
//				"  gmt_modified date ,\n" +
//				"  used_sum_insured varchar(50)  ,\n" +
//				"  policy_package_id bigint(20)  ,\n" +
//				"  package_def_id bigint(20)  ,\n" +
//				"  campaign_def_id bigint(20)  ,\n" +
//				"  substituted_money_vat decimal(18,6)  ,\n" +
//				"  primary_premium decimal(18,6)  ,\n" +
//				"  no_tax_premium decimal(18,6)  ,\n" +
//				"  is_multiple_payer char(1)  ,\n" +
//				"  total_pay_limit varchar(50)  ,\n" +
//				"  is_present char(1)  ,\n" +
//				"  comment varchar(512)  ,\n" +
//				"  single_pay_limit varchar(50)  ,\n" +
//				"  commission_rate varchar(50)  ,\n" +
//				"  commission_amount varchar(50) ,\n" +
//				"  invoice_type tinyint(4)  ,\n" +
//				"  is_free_tax char(1)  ,\n" +
//				"  is_foreign_currency char(1) ,\n" +
//				"  is_periodic_settlement char(1)  ,\n" +
//				"  billing_cycle varchar(1)  ,\n" +
//				"  system_code varchar(6) ,\n" +
//				"  standard_premium decimal(16,2)  ,\n" +
//				"  waiting_period varchar(20)  ,\n" +
//				"  discount_rate decimal(12,8)  \n" +
//				" )";
//		CONSTRAINT uk_policy_channelid_productid_channelpolicyno PRIMARY KEY  (id,channel_id,product_id,channel_policy_no
//		stmt.execute(createStr);
//		String threadName = Thread.currentThread().getName();
//		Integer id = Integer.parseInt(UUIDUtil.getUuid());
		// 		stmt.execute(createStr);
		stmt.execute("UPSERT INTO zhaixt(id, firstname,lastname) values('pool1','zjh','zhou')");
		Integer id = 14;
		String upsertStr19 = "upsert into policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id," +
				"product_id,contract_id,parent_policy_no,parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place" +
				") values(12 , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +
				"2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','上海市')";

		String upsertStr3 = "upsert into policy_0000(id, policy_no,apply_no" +
				") values("+id+" , '8261230403273160','816123039825838111')";
		String upsertStr = "upsert into policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,parent_policy_no,parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place" +
				") values("+id+" , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405',2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','shanghai')";
		stmt.execute(upsertStr);

//		String upsertStr = "UPSERT INTO policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,parent_policy_no," +
//				"parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place," +
//				"remark,uw_remark,uw_operator,uw_date,insurant_id,policy_holder_id,benefitids,insured_type,order_user_id,insured_id,insured_no,channel_policy_no," +
//				"channel_policy_end_time,log_date,is_deleted,extra_info,extra_common_info,creator,gmt_create,modifier,gmt_modified,"+
//				"used_sum_insured,policy_package_id,package_def_id,campaign_def_id,substituted_money_vat,primary_premium,no_tax_premium,is_multiple_payer,total_pay_limit,is_present,"+
//				"comment,single_pay_limit,commission_rate,commission_amount,invoice_type,is_free_tax,is_foreign_currency,is_periodic_settlement,billing_cycle,system_code,standard_premium,waiting_period,discount_rate" +
//				") values("+id+" , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +
//				"2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','上海市'," +
//				"'','','system','2014-05-23',NULL,NULL,'',NULL,NULL,'15042710271148','',''," +
//				"'','','N','','','liulei','2015-11-06','liulei','2015-11-06'," +
//				"'',0,170001,200003,'','','','','',''," +
//				"'','','','',NULL,'','','','','','','','')";
		/*全部53字段*/
//		stmt.execute(upsertStr);

		/*这是我自己创建的sql语句*/
//		stmt.execute("UPSERT INTO policy_0000(id, policy_no" +
//				") values(300000035 ,'8261230403273160')");

//		stmt.execute("UPSERT INTO policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id," +
//				"product_id) values(300000035 , '8261230403273160','816123039825838111','',2,10,42,10146)");
//		id = 10;
		/*19个字段*/
//		String upsertStr19 = "upsert into policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,parent_policy_no,parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place" +
//				") values("+id+" , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405',2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','shanghai')";
//		System.out.println("upsert string is :"+upsertStr19);
//		stmt.execute(upsertStr19);
		/*40个字符*/
//		stmt.execute("UPSERT INTO policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,pareknt_policy_no," +  //10
//				"parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place," +                                      //9
//						"remark,uw_remark,uw_operator,uw_date,insurant_id,policy_holder_id,benefitids,insured_type,order_user_id,insured_id,insured_no,channel_policy_no," +  //12
//						"channel_policy_end_time,log_date,is_deleted,extra_info,extra_common_info,creator,gmt_create,modifier,gmt_modified"+			                      //9
//				") values(300000035 , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +                                            //10
//				"2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','上海市'," +														//9
//				"'','','system','2014-05-23',NULL,NULL,'',NULL,NULL,'15042710271148','',''," +                                                                       //12
//				"'','','N','','','liulei','2015-11-06','liulei','2015-11-06')");																						//9					//7

		/*50个字段*/
//		stmt.execute("UPSERT INTO policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,parent_policy_no," +  //10
//				"parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place," +                                      //9
//				"remark,uw_remark,uw_operator,uw_date,insurant_id,policy_holder_id,benefitids,insured_type,order_user_id,insured_id,insured_no,channel_policy_no," +  //12
//				"channel_policy_end_time,log_date,is_deleted,extra_info,extra_common_info,creator,gmt_create,modifier,gmt_modified,"+
//				"used_sum_insured,policy_package_id,package_def_id,campaign_def_id,substituted_money_vat,primary_premium,no_tax_premium,is_multiple_payer,total_pay_limit,is_present"+    //10           //9
//				") values(300000035 , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +                                            //10
//				"2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','上海市'," +														//9
//				"'','','system','2014-05-23',NULL,NULL,'',NULL,NULL,'15042710271148','',''," +                                                                       //12
//				"'','','N','','','liulei','2015-11-06','liulei','2015-11-06'," +                                                                                       //9
//				"'',0,170001,200003,'','','','','','')");																						                               //10


		/*全部53字段*/
//		stmt.execute("UPSERT INTO policy_0000(id, policy_no,apply_no,insurance_certi_no,policy_type,policy_status,channel_id,product_id,contract_id,parent_policy_no," +  //10
//				"parent_policy_id,sum_insured,premium,pay_frequency,insure_date,effective_date,expiry_date,issue_date,insure_place," +                                      //9
//				"remark,uw_remark,uw_operator,uw_date,insurant_id,policy_holder_id,benefitids,insured_type,order_user_id,insured_id,insured_no,channel_policy_no," +  //12
//				"channel_policy_end_time,log_date,is_deleted,extra_info,extra_common_info,creator,gmt_create,modifier,gmt_modified,"+                                     //9
//				"used_sum_insured,policy_package_id,package_def_id,campaign_def_id,substituted_money_vat,primary_premium,no_tax_premium,is_multiple_payer,total_pay_limit,is_present,"+    //10
//				"comment,single_pay_limit,commission_rate,commission_amount,invoice_type,is_free_tax,is_foreign_currency,is_periodic_settlement,billing_cycle,system_code,standard_premium,waiting_period,discount_rate" +  //13
//				") values(300000035 , '8261230403273160','816123039825838111','',2,10,42,10146,NULL,'885116151030502405'," +                                            //10
//				"2114841689,'5000','0.15',0,'2015-04-29','2015-04-29','2015-04-29','2015-04-29','上海市'," +														//9
//				"'','','system','2014-05-23',NULL,NULL,'',NULL,NULL,'15042710271148','',''," +                                                                       //12
//				"'','','N','','','liulei','2015-11-06','liulei','2015-11-06'," +                                                                                       //9
//				"'',0,170001,200003,'','','','','',''," +                 																										//10
//				"'','','','',NULL,'','','','','','','','')");																											//13



		/*这是核心给我的sql语句*/
//		String policyUpsert = "upsert INTO POLICY_0000 (`id`, `policy_no`, `apply_no`, `insurance_certi_no`, `policy_type`, `policy_status`, `channel_id`, `product_id`, `contract_id`, `parent_policy_no`, `parent_policy_id`, `sum_insured`, `premium`, `pay_frequency`, `insure_date`, `effective_date`, `expiry_date`, `issue_date`, `insure_place`, `remark`, `uw_remark`, `uw_operator`, `uw_date`, `insurant_id`, `policy_holder_id`, `benefitids`, `insured_type`, `order_user_id`, `insured_id`, `insured_no`, `channel_policy_no`, `channel_policy_end_time`, `log_date`, `is_deleted`, `extra_info`, `extra_common_info`, `creator`, `gmt_create`, `modifier`, `gmt_modified`, `used_sum_insured`, `policy_package_id`, `package_def_id`, `campaign_def_id`, `substituted_money_vat`, `primary_premium`, `no_tax_premium`, `is_multiple_payer`, `total_pay_limit`, `is_present`, `comment`, `single_pay_limit`, `commission_rate`, `commission_amount`, `invoice_type`, `is_free_tax`, `is_foreign_currency`, `is_periodic_settlement`, `billing_cycle`, `system_code`, `standard_premium`, `waiting_period`, `discount_rate`) VALUES (495173, '825051400053531919', '815051400053528436', NULL, 3, 3, 1, 10029, 10000001, NULL, NULL, '2228.0', '19.28', 0, '2014-8-1 22:26:22', '2016-1-1 00:00:00', '2017-1-1 00:00:00', '2014-8-1 00:00:00', '浙江省', NULL, '', 'system', '2014-8-8 19:12:16', 815343, 815348, NULL, 3, NULL, '222198261133122', NULL, 'TBB-222198261133122', '2017-7-31 00:00:00', NULL, 'N', '{\\\"benifitCertNo\\\":\\\"\\\",\\\"benifitCertType\\\":\\\"\\\",\\\"benifitEmail\\\":\\\"o***t@126.com\\\",\\\"benifitId\\\":\\\"86993099\\\",\\\"benifitName\\\":\\\"l***\\\",\\\"benifitPhone\\\":\\\"138****0004\\\",\\\"benifitSex\\\":\\\"男\\\",\\\"bizData\\\":\\\"{\\\\\\\"item_number\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"order_receiver\\\\\\\":\\\\\\\"甘雪萍\\\\\\\",\\\\\\\"order_sub_order\\\\\\\":\\\\\\\"722198261133122\\\\\\\",\\\\\\\"item_rootcat\\\\\\\":\\\\\\\"1101\\\\\\\",\\\\\\\"order_main_order\\\\\\\":\\\\\\\"722198261133122\\\\\\\",\\\\\\\"item_realrootcat\\\\\\\":\\\\\\\"1101\\\\\\\",\\\\\\\"item_price\\\\\\\":\\\\\\\"222800\\\\\\\",\\\\\\\"item_brand\\\\\\\":\\\\\\\"Lenovo/联想\\\\\\\",\\\\\\\"order_to\\\\\\\":\\\\\\\"浙江省温州市\\\\\\\",\\\\\\\"item_title\\\\\\\":\\\\\\\"Lenovo/联想S405-AEI(O)联想笔记本电脑14寸超极本超薄游戏上网本\\\\\\\",\\\\\\\"item_model\\\\\\\":\\\\\\\"S405-AEI(O)\\\\\\\",\\\\\\\"seller_id\\\\\\\":\\\\\\\"1797951006\\\\\\\"}\\\",\\\"bizId\\\":\\\"222198261133122\\\",\\\"holderCertNo\\\":\\\"\\\",\\\"holderCertType\\\":\\\"\\\",\\\"holderEmail\\\":\\\"o***t@126.com\\\",\\\"holderId\\\":\\\"86993099\\\",\\\"holderName\\\":\\\"l***\\\",\\\"holderPhone\\\":\\\"138****0004\\\",\\\"holderSex\\\":\\\"男\\\",\\\"itemName\\\":\\\"4102\\\",\\\"logDate\\\":\\\"20140711\\\",\\\"openPolicyNo\\\":\\\"\\\",\\\"protocloId\\\":\\\"\\\",\\\"rate\\\":\\\"0.0\\\"}', '{\\\"primaryExpiryDate\\\":\\\"2015-07-31 23:59:59\\\",\\\"primarySumInsured\\\":\\\"2228.0\\\",\\\"primaryPremium\\\":\\\"19.28\\\"}', 'system', '2014-8-8 19:12:17', 'system', '2016-4-18 14:23:22', NULL, 0, 10029, 10029, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);";
//		stmt.execute(policyUpsert);
		conn.commit();
		stmt.close();
		log.info("upset sucess");
	}
}
