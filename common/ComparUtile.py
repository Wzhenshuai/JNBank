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
    elif tableName in ('CRM_CRM_DM_cust_attr_info_tmp','CRM_CRM_DM_cust_attr_info_tmp','CRM_CRM_DM_cust_alg_sum','CRM_CRM_DM_cust_man_his',
                       'CRM_CRM_DM_index_data_value','CRM_CRM_DM_cust_contri_trend','CRM_CRM_DM_cust_base_info_p'):
        day_id = 'DATA_DATE'
    return day_id
