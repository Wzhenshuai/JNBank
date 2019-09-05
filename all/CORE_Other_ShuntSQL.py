#coding=utf-8
## 仅用于 数整 其它表的分流

import os
import sys

import pymysql

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import ConstantUtile

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

sqlPath = table_data[0][0].upper()
shortName = table_data[0][1].upper()
custmerStr = ConstantUtile.custmerStr

selectTableSql = "SELECT system_en_name,en_name FROM table_scheme where system_en_name in ("+custmerStr+")"
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
    file_sql_name = "AllDataShunt.Core.%s.sql" % table_name
    ## 拼接创建表 语句操作
    insert_CoreBankHist_str = "insert into CoreBankHist.%s PARTITION(partition_year) select\n " % table_name
    insert_TownBankHist_str = "insert into TownBankHist.%s PARTITION(partition_year) select\n " % table_name

    unite_key_file = ""
    insert_table_str = ""
    insert_fieldStr = ''
    create_fieldStr = ''
    aaa = 0
    sub_flag = ""
    for fie in allField:
        key_comm = fie[5]
        fieldName = fie[0].upper()
        if fieldName == 'CORPORATION':
            aaa = 1
        if fieldName in ('DAY_ID','BEGIN_DATE','RPT_HEAD_DATE'):
            sub_flag = "substr(%s, 1, 4) as partition_year"%fieldName
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
    if sub_flag == "":
        sub_flag = "TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyy') as partition_year"
    insert_table_str = insert_table_str +"'%s' as data_source_str,\r %s \r" % (shortName,sub_flag)
    insert_CoreBankHist_str = insert_CoreBankHist_str+insert_table_str + "from AllAnalyze.%s where corporation in ('800','815');" % ("Core_"+table_name)

    insert_TownBankHist_str = insert_TownBankHist_str+insert_table_str + "from AllAnalyze.%s where corporation in ('800','615');" % ("Core_"+table_name)
    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\r truncate table CoreBankHist.%s;\r" % table_name)
    f.write("truncate table TownBankHist.%s;\r" % table_name)

    f.write(ConstantUtile.setHiveStr)

    f.write("\r\r\r"+insert_CoreBankHist_str)

    f.write("\r\r")
    f.write("\r\r\r" + insert_TownBankHist_str)
    f.write("\r\r!q")
    f.close()
print(str(numIndex)+"张表操作完成！！！")
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()