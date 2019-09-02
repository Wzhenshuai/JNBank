def findTableDayId(tableName):
    day_id = ''
    if tableName == 'TTRD_ACCOUNTING_SECU_OBJ_HIS':
        day_id = 'BEG_DATE'
    elif tableName == 'DEV_TRANS_LOG':
        day_id = 'OPER_DATE'
    elif tableName == 'HOSTTRANLIST':
        day_id = 'TRNDATE'
    elif tableName == 'BANK_JN_CARDBAL_HIS':
        day_id = 'PEDATE'
    elif tableName == 'OPERATIONLOG':
        day_id = 'OPTIMEDATETIME'
    elif tableName == 'TRANSTYPEPOINTSTOTAL':
        day_id = 'COUNTDATE'
    elif tableName == 'VISTIORSREC':
        day_id = 'VISDATE'
    elif tableName == 'T_EMG_REFUND_JRN':
        day_id = 'CREATE_DT'
    elif tableName == 'T_EMG_OPERLOG':
        day_id = "to_char(OPERTIME,'yyyyMMdd')"
    elif tableName == 'T_EMG_PAY_JRN':
        day_id = 'CREAT_DATE'
    elif tableName == 'JNTCR_ZWLS':
        day_id = 'JYRQ'
    elif tableName == 'IBANK_TRADEFLOWLIST':
        day_id = "to_char(TRADETIME,'yyyyMMdd')"
    elif tableName == 'PARAMOPERATELOG':
        day_id = "to_char(OPERATEDATE,'yyyyMMdd')"
    elif tableName == 'GOODSINFO_LOG':
        day_id = 'GOODSTIME'
    elif tableName == 'MMTMTLOG':
        day_id = 'MMTMTXDATE'
    elif tableName == 'RULESEXECUTEHISTORY':
        day_id = 'EXEC_START_TIME'
    elif tableName == 'CUSRULESEXECUTEHIS':
        day_id = 'EXECSTARTTIME'
    elif tableName == 'CUSTCHARGETRANS':
        day_id = 'TIME_START'
    elif tableName == 'CSNDTLOG':
        day_id = 'PLATTXDATE'
    elif tableName in ('IBANK_PMC_MACHOPLOG', 'IBANK_PMC_MEDIUMOPLOG'):
        day_id = "to_char(OPTIME,'yyyyMMdd')"
    elif tableName in ('DEV_TRANS_COMMENT', 'RIGHTSTRANSMINUTES', 'RIGHTSTRANS', 'MARKETINGCLEARINGLIST'):
        day_id = 'TRANS_DATE'
    elif tableName in ('REDPACCLEHIS', 'REFUNDACTIONTABLE', 'TRANSACTIONTABLE'):
        day_id = 'CREATETIME'
    elif tableName in ('STOCKSTRANS', 'POINTSTRANS', 'TRANSTATEMENT'):
        day_id = 'TRANSDATE'
    elif tableName in ('DC_LOG', 'IN_MST_HST', 'LN_MST_HST', 'TRACE_LOG'):
        day_id = 'TX_DATE'
    elif tableName in ('CRM_DM_CUST_ATTR_INFO_TMP','CRM_DM_CUST_ASSET_BAL_OUT_IN','CRM_DM_CUST_ALG_SUM','CRM_DM_CUST_MAN_HIS','CRM_DM_CUST_BASE_INFO_P',
                       'CRM_DM_INDEX_DATA_VALUE','CRM_DM_CUST_CONTRI_TREND',
                       'CRM_DM_USER_ORDER_INFO','CRM_DM_USER_ORDER_INFO_TOP50'):
        day_id = 'DATA_DATE'
    elif tableName in ('DB_BIZ_BANK_WITHHOLD_TRANSACTION_LOG','DB_BIZ_BANK_WITHHOLD_TRANSACTION'):
        day_id = "DATE_FORMAT(update_at,'%Y%m%d')"
    elif tableName == 'DB_BIZ_INTEREST_ACCRUAL':
        day_id = "DATE_FORMAT(update_time,'%Y%m%d')"
    elif tableName == 'DB_MSGSERVER_MSG_RECORD':
        day_id = "DATE_FORMAT(last_modify_time,'%Y%m%d')"
    return day_id
