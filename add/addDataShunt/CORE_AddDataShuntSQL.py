#coding=utf-8
## 仅用于 增量表的分流
import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道
system_nu = sys.argv[1]
system_name = 'CORE'

# 获取所有表
dictSql = "SELECT sql_path,system_code FROM dic_info_mapping WHERE transfer_mode ='增量数据' AND system_code='" + system_name + "'"
cursor.execute(dictSql)
table_data = cursor.fetchall()

sqlPath = table_data[0][0].upper()
shortName = table_data[0][1].upper()

selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是' and  substring_index(system_en_name,'_',2)='%s'" % (system_name,system_nu)
cursor.execute(selectTableSql)
allTable = cursor.fetchall()

if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)

numIndex = 0
for ta in allTable:
    if ta[1].upper() == 'F_CM_SPSRC_VIEW':
        continue
    numIndex += 1
    schemeKey = ta[0]
    tableName = ta[1]
    cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag "
                   "FROM table_field where scheme_key ='%s' order by cast(ord_number as SIGNED INTEGER)" % (ta[0]))
    allField = cursor.fetchall()
    table_name = shortName + "_" + tableName.lower()
    file_sql_name = "AddDataShunt.Core.%s.sql" % table_name
    ## 拼接创建表 语句操作
    insert_CoreBankHist_str = "insert into CoreBankHistTest.%s PARTITION(partition_year) select\n " % table_name
    insert_TownBankHist_str = "insert into TownBankHistTest.%s PARTITION(partition_year) select\n " % table_name
    insert_AddRollData_str = "insert into AddRollData.%s PARTITION(partition_year) select\n " % table_name

    unite_key_file = ""
    insert_table_str = ""
    insert_fieldStr = ''
    create_fieldStr = ''
    aaa = 0
    for fie in allField:
        key_comm = fie[5]
        fieldName = fie[0].upper()
        if fieldName == 'CORPORATION':
            aaa = 1

        if key_comm == '是':
            unite_key_file = unite_key_file + fie[0] + ','
        insert_fieldStr = insert_fieldStr + '`'+fie[0] + '`,\n'
    if aaa == 0:
        unite_key_file = 'CORPORATION,' + unite_key_file
    if unite_key_file == "":
        insert_table_str = "uniq() as rowkeystr,\r" \
                           + "(select distinct CycleId from AddBuffer.AddDateCycleId) as dataday_id,\r" \
                           + "SYSDATE  as tdh_load_timestamp, \r"
    else:
        insert_table_str = "concat(" + unite_key_file.rstrip(",") + ')as rowkeystr,\r' \
                           + "(select distinct CycleId from AddBuffer.AddDateCycleId) as dataday_id,\r" \
                           + "SYSDATE  as tdh_load_timestamp, \r"
    if aaa == 0:
        insert_table_str = insert_table_str + 'CORPORATION,\r' + insert_fieldStr
    else:
        insert_table_str = insert_table_str + insert_fieldStr
    insert_AddRollData_ss = insert_table_str + "(select distinct CycleId from AddBuffer.AddDateCycleId) as partition_day \r"
    insert_table_str = insert_table_str + "'%s' as data_source_str,\r (select distinct substr(CycleId,1,4) from AddBuffer.AddDateCycleId) as partition_year \r" %shortName

    insert_CoreBankHist_str = insert_CoreBankHist_str+insert_table_str + "from AddRollData.%s where corporation in ('800','815');" % ("Core_"+table_name)

    insert_TownBankHist_str = insert_TownBankHist_str+insert_table_str + "from AddRollData.%s where corporation in ('800','615');" % ("Core_"+table_name)

    insert_AddRollData_str = insert_AddRollData_str+insert_AddRollData_ss + "from AddAnalyze.Core_%s ; " %table_name
    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\r truncate table CoreBankHist.%s;\r" % table_name)
    f.write("truncate table TownBankHist.%s;\r" % table_name)

    f.write("\r\r\rset hive.enforce.bucketing = true;\r"
            "set hive.exec.dynamic.partition=true;\r"
            "set hive.exec.dynamic.partition.mode=nonstrict;\r"
            "SET hive.exec.max.dynamic.partitions=100000;\r"
            "SET hive.exec.max.dynamic.partitions.pernode=100000;\r")

    f.write("\r\r\r"+insert_CoreBankHist_str)
    f.write("\r\r")
    f.write("\r\r\r" + insert_TownBankHist_str)
    f.write("\r\r")
    f.write("\r\r\r" + insert_AddRollData_str)
    f.write("\r\r!q")
    f.close()
print(str(numIndex)+"张表操作完成！！！")
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()