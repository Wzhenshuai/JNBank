

setHiveStr = "\n\n " \
            "set plsql.server.dialect = DB2;\n" \
             "set hive.merge.mapfiles=true;\n" \
             "set hive.merge.mapredfiles=true;\n" \
             "set hive.enforce.bucketing = true;\n" \
             "set hive.exec.dynamic.partition=true;\n" \
             "set hive.exec.dynamic.partition.mode=nonstrict;\n" \
             "SET hive.exec.max.dynamic.partitions=100000;\n" \
             "SET hive.exec.max.dynamic.partitions.pernode=100000;\n"

custmerStr = "'CORE_DS_ACCOUNTING_FLOW','CORE_TM_ACCOUNT','CORE_TM_CUST_LIMIT_O','CORE_TM_CUSTOMER','CORE_TM_LOAN'," \
             "'CORE_TM_PSB_PERSONAL_INFO','CORE_TT_TXN_POST','CORE_QRY_080','CORE_DICTIONARY','CORE_ORGANIZATION'," \
             "'CORE_JN_CRD_OLD_NEW','CORE_JNBANK_PASLCGX','CORE_V_ORGANIZATION_TREE','CORE_XD_BAIDUXD_LOAN','CORE_XD_BAIDUXD_LOAN_RATE'"

#custmerStr = "'CORE_JN_CRD_OLD_NEW','CORE_JNBANK_PASLCGX','CORE_V_ORGANIZATION_TREE','CORE_XD_BAIDUXD_LOAN','CORE_XD_BAIDUXD_LOAN_RATE'"
