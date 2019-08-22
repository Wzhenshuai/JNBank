

setHiveStr = "\r\r " \
             "set hive.merge.mapfiles=true;\r" \
             "set hive.merge.mapredfiles=true;\r" \
             "set hive.enforce.bucketing = true;\r" \
             "set hive.exec.dynamic.partition=true;\r" \
             "set hive.exec.dynamic.partition.mode=nonstrict;\r" \
             "SET hive.exec.max.dynamic.partitions=100000;\r" \
             "SET hive.exec.max.dynamic.partitions.pernode=100000;\r"

custmerStr = "'CORE_DS_ACCOUNTING_FLOW','CORE_TM_ACCOUNT','CORE_TM_CUST_LIMIT_O','CORE_TM_CUSTOMER','CORE_TM_LOAN'," \
             "'CORE_TM_PSB_PERSONAL_INFO','CORE_TT_TXN_POST','CORE_QRY_080','CORE_DICTIONARY','CORE_ORGANIZATION'," \
             "'CORE_JN_CRD_OLD_NEW','CORE_JNBANK_PASLCGX','CORE_V_ORGANIZATION_TREE','CORE_XD_BAIDUXD_LOAN','CORE_XD_BAIDUXD_LOAN_RATE'"

#custmerStr = "'CORE_JN_CRD_OLD_NEW','CORE_JNBANK_PASLCGX','CORE_V_ORGANIZATION_TREE','CORE_XD_BAIDUXD_LOAN','CORE_XD_BAIDUXD_LOAN_RATE'"
