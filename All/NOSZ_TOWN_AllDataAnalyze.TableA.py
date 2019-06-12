#coding=utf-8

import pymysql
import os, sys
from All3 import coverField

conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

#system_name = sys.argv[1]
system_name = 'credit'

# 获取所有表
dictSql = "SELECT sql_path,system_code FROM dic_info_mapping WHERE transfer_mode ='全量铺底数据' AND system_code='" + system_name + "'"
cursor.execute(dictSql)
table_data = cursor.fetchall()

sqlPath = table_data[0][0]
shortName = table_data[0][1].upper()

selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是'" % system_name
cursor.execute(selectTableSql)
allTable = cursor.fetchall()

if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)
aa = 0
for ta in allTable:
    aa = aa+1
    if aa >3:
        break
    schemeKey = ta[0]
    tableName = ta[1]
    cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag "
                   "FROM table_field where scheme_key ='%s'" % (ta[0]))
    allField = cursor.fetchall()
    #table_name = shortName + "_" + tableName.lower()
    catalog = shortName + "_" + tableName.lower()
    table_name = "Town_" + catalog
    file_sql_name = "AllDataAnalyze.%s.sql" % ("Town." + catalog)

    exe_table_name = (ta[0])

    #print(fields)
    ## 拼接创建表 语句操作
    create_table_str = "create external table IF NOT EXISTS %s(\n" % table_name
    i = 0
    for i in allField:
        comm = i[4]
        if comm == '':
            com = "''"
        key_comm = i[5]
        if key_comm == '是':
            comm = comm+'.主键'
        if i[0] == 'corporation':
            i = 1
        create_table_str = create_table_str+("%s string comment '%s',\n")%('`'+i[0]+'`',comm)
    if i == 0:
        create_table_str = "'corporation' String comment '法人行号_主鍵',\n"+create_table_str
    create_table_str = create_table_str.rstrip(",\n")+"\r)comment '%s汉语注解' row format delimited fields terminated by '\\u0003' \r" \
                                                      "stored as textfile location '/DATACENTER/AllData/%s/TownBank/%s/';" % (table_name,shortName,catalog)
    ## 插入语句拼接
    insert_str = "insert into AllAnalyzeTablesCount select\n " \
                 "to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')\n" \
                 ", 'AllAnalyze.%s'\n" \
                 ", count(1)\n" \
                 ", SYSDATE from %s;"%(table_name,table_name)
    #print(insert_str)
    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding='utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS AllAnalyze COMMENT '全量.解析库';\r"
            "use AllAnalyze;\r\r")
    f.write(create_table_str)
    f.write("\r\r")
    f.write(insert_str)
    f.write("\r\r!q")
    f.close()

#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()
