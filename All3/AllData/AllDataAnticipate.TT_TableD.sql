--- 本文件: AllDataAnticipate.TT_TableD.sqlCREATE DATABASE IF NOT EXISTS AllAnticipate COMMENT '全量.预处理库';use AllAnticipate;drop table TT_TableD;create table IF NOT EXISTS TT_TableD(
rowKeyStr string comment '联合主键(corporation,XT_BR_NO,XT_CURR_COD拼接)' ,DataDay_ID String COMMENT'数据的时间',tdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',corporation varchar(20) comment '表头_法人主体.主键',
DAY_ID varchar(20) comment '表头_批量日期',
XT_DATE date comment '日期',
XT_BR_NO varchar(20) comment '机构代码.主键',
XT_ACCT_NO bigint comment '帐号',
XT_CURR_COD varchar(11) comment '币别.主键',
XT_CURR_IDEN string comment '钞汇鉴别',
XTBAL_DB_TIMESTAMP timestamp comment '',
XT_CURR bigint comment '币种',
XT_CR_AMT decimal(20,3) comment '贷方发生额',
XT_DR_AMT varchar(20) comment '借方发生额',
XT_BAL decimal(20,3) comment '余额',
Data_source String COMMENT'数据来源')comment 'TT_TableD汉语注解' partitioned by(partition_corporation string)STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='');set hive.enforce.bucketing = true;set hive.exec.dynamic.partition=true;set hive.exec.dynamic.partition.mode=nonstrict;SET hive.exec.max.dynamic.partitions=100000;SET hive.exec.max.dynamic.partitions.pernode=100000;insert into AllAnticipate.TT_TableD PARTITION(partition_corporation) select
 concat_ws('^',trim(corporation),trim(XT_BR_NO),trim(XT_CURR_COD)),TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyyMMdd'),to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss'),trim(corporation),
trim(DAY_ID),
case when date(trim(XT_DATE)) is null then date(trim('1970-01-01 08:00:00')) else date(trim(XT_DATE)) end,
trim(XT_BR_NO),
trim(XT_ACCT_NO),
trim(XT_CURR_COD),
trim(XT_CURR_IDEN),
case when date(trim(XTBAL_DB_TIMESTAMP)) is null then CAST(trim('1970-01-01 08:00:00') as TIMESTAMP) else CAST(trim(XTBAL_DB_TIMESTAMP) as TIMESTAMP) end ,
trim(XT_CURR),
trim(XT_CR_AMT),
trim(XT_DR_AMT),
trim(XT_BAL),
'TT' as Data_source,CORPORATION as partition_corporationfrom AllAnalyze.TT_TableD;-->>> AllAnticipateTablesCount 统计全量数据预处理表.全量数据量 [时间戳、将库表名称、数据条数、解析时间(yyyy-MM-dd HH:mm:ss)]insert into AllAnticipateTablesCount select
 to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')
, 'AllAnticipate.TT_TableD'
, count(1)
, SYSDATE from TT_TableD;!q