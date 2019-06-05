--- 本文件: AllDataAnalyze.TT_TableD.sqlCREATE DATABASE IF NOT EXISTS AllAnalyze COMMENT '全量.解析库';use AllAnalyze;create external table IF NOT EXISTS TT_TableD(
corporation string comment '表头_法人主体.主键',
DAY_ID string comment '表头_批量日期',
XT_DATE string comment '日期',
XT_BR_NO string comment '机构代码.主键',
XT_ACCT_NO string comment '帐号',
XT_CURR_COD string comment '币别.主键',
XT_CURR_IDEN string comment '钞汇鉴别',
XTBAL_DB_TIMESTAMP string comment '',
XT_CURR string comment '币种',
XT_CR_AMT string comment '贷方发生额',
XT_DR_AMT string comment '借方发生额',
XT_BAL string comment '余额')comment 'TT_TableD汉语注解' row format delimited fields terminated by '\u0003' stored as textfile location '/tmp/DATACENTER/AllData/CORE/TT_TableD';insert into AllAnalyzeTablesCount select
 to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')
, 'AllAnalyze.TT_TableD'
, count(1)
, SYSDATE from TT_TableD;!q--- ���ļ�: AllDataAnalyze.TT_tabled.sqlCREATE DATABASE IF NOT EXISTS AllAnalyze COMMENT 'ȫ��.������';use AllAnalyze;create external table IF NOT EXISTS TT_tabled(
corporation string comment '��ͷ_��������.����',
DAY_ID string comment '��ͷ_��������',
XT_DATE string comment '����',
XT_BR_NO string comment '��������.����',
XT_ACCT_NO string comment '�ʺ�',
XT_CURR_COD string comment '�ұ�.����',
XT_CURR_IDEN string comment '�������',
XTBAL_DB_TIMESTAMP string comment '',
XT_CURR string comment '����',
XT_CR_AMT string comment '����������',
XT_DR_AMT string comment '�跽������',
XT_BAL string comment '���')comment 'TT_tabled����ע��' row format delimited fields terminated by '\u0003' stored as textfile location '/tmp/DATACENTER/AllData/CORE/TT_tabled';insert into AllAnalyzeTablesCount select
 to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')
, 'AllAnalyze.TT_tabled'
, count(1)
, SYSDATE from TT_tabled;!q