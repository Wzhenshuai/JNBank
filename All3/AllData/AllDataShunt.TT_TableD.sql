--- 本文件: AllDataShunt.TT_TableD.sqlCREATE DATABASE IF NOT EXISTS CoreBankHist COMMENT '总行.历史库';use CoreBankHist;create table IF NOT EXISTS TT_TableD(
rowKeyStr string comment '联合主键(corporation,XT_BR_NO,XT_CURR_COD拼接)',DataDay_ID String COMMENT'数据的时间',tdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',corporation varchar(20) comment'表头_法人主体.主键',
DAY_ID varchar(20) comment'表头_批量日期',
XT_DATE date comment'日期',
XT_BR_NO varchar(20) comment'机构代码.主键',
XT_ACCT_NO bigint comment'帐号',
XT_CURR_COD varchar(11) comment'币别.主键',
XT_CURR_IDEN string comment'钞汇鉴别',
XTBAL_DB_TIMESTAMP timestamp comment'',
XT_CURR bigint comment'币种',
XT_CR_AMT decimal(20,3) comment'贷方发生额',
XT_DR_AMT varchar(20) comment'借方发生额',
XT_BAL decimal(20,3) comment'余额',
Data_source String COMMENT'数据来源')comment 'TT_TableD汉语注解' partitioned by(partition_month string)clustered by (rowKeyStr) into 13 buckets stored as orc TBLPROPERTIES ('transactional'='true') ;CREATE DATABASE IF NOT EXISTS TownBankHist COMMENT '村镇.历史库';use TownBankHist;create table IF NOT EXISTS TT_TableD(
rowKeyStr string comment '联合主键(corporation,XT_BR_NO,XT_CURR_COD拼接)',DataDay_ID String COMMENT'数据的时间',tdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',corporation varchar(20) comment'表头_法人主体.主键',
DAY_ID varchar(20) comment'表头_批量日期',
XT_DATE date comment'日期',
XT_BR_NO varchar(20) comment'机构代码.主键',
XT_ACCT_NO bigint comment'帐号',
XT_CURR_COD varchar(11) comment'币别.主键',
XT_CURR_IDEN string comment'钞汇鉴别',
XTBAL_DB_TIMESTAMP timestamp comment'',
XT_CURR bigint comment'币种',
XT_CR_AMT decimal(20,3) comment'贷方发生额',
XT_DR_AMT varchar(20) comment'借方发生额',
XT_BAL decimal(20,3) comment'余额',
Data_source String COMMENT'数据来源')comment 'TT_TableD汉语注解' partitioned by(partition_month string)clustered by (rowKeyStr) into 13 buckets stored as orc TBLPROPERTIES ('transactional'='true') ;set hive.enforce.bucketing = true;set hive.exec.dynamic.partition=true;set hive.exec.dynamic.partition.mode=nonstrict;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;insert into CoreBankHist.TT_TableD PARTITION(partition_month) select
 rowkeystr,dataday_id,tdh_load_timestamp,corporation,
DAY_ID,
XT_DATE,
XT_BR_NO,
XT_ACCT_NO,
XT_CURR_COD,
XT_CURR_IDEN,
XTBAL_DB_TIMESTAMP,
XT_CURR,
XT_CR_AMT,
XT_DR_AMT,
XT_BAL,
data_source,substr(DataDay_ID,1,6) as partition_monthfrom AllAnticipate.TT_TableD where partition_corporation in (select distinct CORPORATION from AddBuffer.dic_CORPORATION where CORPORATION_NAME in ('公共','总行' ));insert into TownBankHist.TT_TableD PARTITION(partition_month) select
 rowkeystr,dataday_id,tdh_load_timestamp,corporation,
DAY_ID,
XT_DATE,
XT_BR_NO,
XT_ACCT_NO,
XT_CURR_COD,
XT_CURR_IDEN,
XTBAL_DB_TIMESTAMP,
XT_CURR,
XT_CR_AMT,
XT_DR_AMT,
XT_BAL,
data_source,substr(DataDay_ID,1,6) as partition_monthfrom AllAnticipate.TT_TableD where partition_corporation in (select distinct CORPORATION from AddBuffer.dic_CORPORATION where CORPORATION_NAME in ('公共','村镇' ));!q