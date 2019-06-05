--- æœ¬æ–‡ä»¶: AllDataAnalyze.TT_TableD.sqlCREATE DATABASE IF NOT EXISTS AllAnalyze COMMENT 'å…¨é‡.è§£æåº“';use AllAnalyze;create external table IF NOT EXISTS TT_TableD(
corporation string comment 'è¡¨å¤´_æ³•äººä¸»ä½“.ä¸»é”®',
DAY_ID string comment 'è¡¨å¤´_æ‰¹é‡æ—¥æœŸ',
XT_DATE string comment 'æ—¥æœŸ',
XT_BR_NO string comment 'æœºæ„ä»£ç .ä¸»é”®',
XT_ACCT_NO string comment 'å¸å·',
XT_CURR_COD string comment 'å¸åˆ«.ä¸»é”®',
XT_CURR_IDEN string comment 'é’æ±‡é‰´åˆ«',
XTBAL_DB_TIMESTAMP string comment '',
XT_CURR string comment 'å¸ç§',
XT_CR_AMT string comment 'è´·æ–¹å‘ç”Ÿé¢',
XT_DR_AMT string comment 'å€Ÿæ–¹å‘ç”Ÿé¢',
XT_BAL string comment 'ä½™é¢')comment 'TT_TableDæ±‰è¯­æ³¨è§£' row format delimited fields terminated by '\u0003' stored as textfile location '/tmp/DATACENTER/AllData/CORE/TT_TableD';insert into AllAnalyzeTablesCount select
 to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')
, 'AllAnalyze.TT_TableD'
, count(1)
, SYSDATE from TT_TableD;!q--- ±¾ÎÄ¼ş: AllDataAnalyze.TT_tabled.sqlCREATE DATABASE IF NOT EXISTS AllAnalyze COMMENT 'È«Á¿.½âÎö¿â';use AllAnalyze;create external table IF NOT EXISTS TT_tabled(
corporation string comment '±íÍ·_·¨ÈËÖ÷Ìå.Ö÷¼ü',
DAY_ID string comment '±íÍ·_ÅúÁ¿ÈÕÆÚ',
XT_DATE string comment 'ÈÕÆÚ',
XT_BR_NO string comment '»ú¹¹´úÂë.Ö÷¼ü',
XT_ACCT_NO string comment 'ÕÊºÅ',
XT_CURR_COD string comment '±Ò±ğ.Ö÷¼ü',
XT_CURR_IDEN string comment '³®»ã¼ø±ğ',
XTBAL_DB_TIMESTAMP string comment '',
XT_CURR string comment '±ÒÖÖ',
XT_CR_AMT string comment '´û·½·¢Éú¶î',
XT_DR_AMT string comment '½è·½·¢Éú¶î',
XT_BAL string comment 'Óà¶î')comment 'TT_tabledººÓï×¢½â' row format delimited fields terminated by '\u0003' stored as textfile location '/tmp/DATACENTER/AllData/CORE/TT_tabled';insert into AllAnalyzeTablesCount select
 to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')
, 'AllAnalyze.TT_tabled'
, count(1)
, SYSDATE from TT_tabled;!q