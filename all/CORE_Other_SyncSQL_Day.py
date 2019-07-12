#coding=utf-8
## 仅用于 数整 其它表的同步，指定一天的数据
import pymysql
import os, sys

conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

system_nu = sys.argv[1]
system_name = 'CORE'

# 获取所有表
dictSql = "SELECT sql_path,system_code FROM dic_info_mapping WHERE transfer_mode ='全量铺底数据' AND system_code='" + system_name + "'"
cursor.execute(dictSql)
table_data = cursor.fetchall()

sqlPath = 'E:\mnt\JN_shell\Create_tables\AddDataSyncDay'
shortName = system_name

selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_en_name in ('CORE_DS_ACCOUNTING_FLOW'," \
                 "'CORE_TM_ACCOUNT','CORE_TM_CUST_LIMIT_O','CORE_TM_CUSTOMER','CORE_TM_LOAN','CORE_TM_PSB_PERSONAL_INFO','CORE_TT_TXN_POST')"
cursor.execute(selectTableSql)
allTable = cursor.fetchall()

if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)

numIndex = 0
for ta in allTable:
    numIndex += 1
    schemeKey = ta[0]
    tableName = ta[1]
    cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag "
                   "FROM table_field where scheme_key ='%s' order by cast(ord_number as SIGNED INTEGER)" % (ta[0]))
    allField = cursor.fetchall()
    table_name = shortName + "_" + tableName.lower()
    file_sql_name = "AllDataSyncDay.Core.%s.sql" % table_name
    ## 拼接创建表 语句操作
    insert_CoreBankHist_str = "insert into %s select\n " % table_name

    unite_key_file = ""
    insert_table_str = ""
    insert_fieldStr = ''
    create_fieldStr = ''
    aaa = 0
    for fie in allField:
        key_comm = fie[5]
        if fie[0].upper() == 'CORPORATION':
            aaa = 1
        if key_comm == '是':
            unite_key_file = unite_key_file + fie[0] + ','
        insert_fieldStr = insert_fieldStr + '`'+fie[0] + '`,\n'
    if aaa == 0:
        unite_key_file = 'CORPORATION,' + unite_key_file
    if unite_key_file == "":
        insert_table_str = "uniq() as rowkeystr,\r" \
                           + "TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyyMMdd') as dataday_id,\r" \
                           + "to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss') as tdh_load_timestamp, \r"
    else:
        insert_table_str = "concat(" + unite_key_file.rstrip(",") + ')as rowkeystr,\r' \
                           + "TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyyMMdd') as dataday_id,\r" \
                           + "to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss') as tdh_load_timestamp, \r"
    if aaa == 0:
        insert_table_str = insert_table_str + 'CORPORATION,\r' + insert_fieldStr
    else:
        insert_table_str = insert_table_str + insert_fieldStr

    insert_table_str = insert_table_str +"'%s' as data_source_str\r "% shortName
    insert_CoreBankHist_str = insert_CoreBankHist_str+insert_table_str + "from AllAnalyze.%s \r" \
                 "where DAY_ID in (select distinct CycleId from AddBuffer.AddDateCycleId );" % ("Core_"+table_name)
    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\r truncate table %s;\r" % table_name)

    f.write("\r\r\r"+insert_CoreBankHist_str)

    f.write("\r\r")
    f.write("\r\r!q")
    f.close()
print(str(numIndex)+"张表操作完成！！！")
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()