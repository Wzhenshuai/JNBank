#coding=utf-8

import pymysql
import os, sys
from All3 import coverField

conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

system_name = sys.argv[1]

# 获取所有表
dictSql = "SELECT sql_path,system_code FROM dic_info_mapping WHERE transfer_mode ='全量铺底数据' AND system_code='" + system_name + "'"
cursor.execute(dictSql)
table_data = cursor.fetchall()

sqlPath = table_data[0][0]
shortName = table_data[0][1].upper()

selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是'" % (
    system_name)
cursor.execute(selectTableSql)
allTable = cursor.fetchall()

if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)
for ta in allTable:
    schemeKey = ta[0]
    tableName = ta[1]
    cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag "
                   "FROM table_field where scheme_key ='%s'" % (ta[0]))
    allField = cursor.fetchall()
    table_name = shortName + "_" + tableName.lower()
    file_sql_name = "AllDataShunt.%s.sql" % table_name
    ## 拼接创建表 语句操作
    create_table_str = "create table IF NOT EXISTS %s(\n" %table_name
    insert_CoreBankHist_str = "insert into CoreBankHist.%s PARTITION(partition_month) select\n " % table_name
    insert_TownBankHist_str = "insert into TownBankHist.%s PARTITION(partition_month) select\n " % table_name

    unite_key_file = "联合主键("
    unite_key_value = "concat_ws('^',"
    insert_table_str = ""
    for fie in allField:
        key_comm = fie[5]
        if key_comm == '是':
            unite_key_file = unite_key_file + fie[0] + ','
            unite_key_value = unite_key_value + fie[0] + ','
    create_table_str = create_table_str + "rowKeyStr varchar(333) comment '"
    create_table_str = create_table_str + unite_key_file.rstrip(",") + "拼接)'," \
                                                                       "\rDataDay_ID varchar(33) COMMENT'数据的时间'," \
                                                                       "\rtdh_load_timestamp  varchar(33)  COMMENT'加载到TDH时的时间戳',\r" \
                                                                       "corporation varchar(33) comment '法人行号.主键'"

    insert_table_str = 'rowkeystr,\r'+'dataday_id,\r'+'tdh_load_timestamp,\r'+ 'corporation,\r'
    for i in allField:
        comm = i[4]
        if comm == '':
            com = "''"
        key_comm = i[5]
        if key_comm == '是':
            comm = comm+'.主键'
        field_type = coverField.convert_fieldType(i)
        create_table_str = create_table_str + ("%s %s comment'%s',\n")%(i[0],field_type,comm)
        insert_table_str = insert_table_str+"%s,\n"%i[0]
    create_table_str = create_table_str+"Data_source_str varchar(33) COMMENT'数据来源'"+\
                                                        "\r)comment '%s汉语注解' partitioned by(partition_month varchar(33))\r" \
                                                      "clustered by (rowKeyStr) into 13 buckets stored as orc TBLPROPERTIES ('transactional'='true') ;"%(table_name)

    insert_table_str = insert_table_str +"data_source,\r substr(DataDay_ID,1,6) as partition_month \r"
    insert_CoreBankHist_str = insert_CoreBankHist_str+insert_table_str + "from AllAnticipate.%s where partition_corporation in (" \
                                                 "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                 "where CORPORATION_NAME in ('公共','总行' ));" % (table_name)
    insert_TownBankHist_str = insert_TownBankHist_str+insert_table_str + "from AllAnticipate.%s where partition_corporation in (" \
                                                 "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                 "where CORPORATION_NAME in ('公共','村镇' ));" % (table_name)

    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS CoreBankHist COMMENT '总行.历史库';\r"
            "use CoreBankHist;\r")
    f.write("\r\r\r"+create_table_str)

    f.write("\r\r\rCREATE DATABASE IF NOT EXISTS TownBankHist COMMENT '村镇.历史库';\r"
            "use TownBankHist;\r")
    f.write("\r\r\r" + create_table_str)


    f.write("\r\r\rset hive.enforce.bucketing = true;\r"
            "set hive.exec.dynamic.partition=true;\r"
            "set hive.exec.dynamic.partition.mode=nonstrict;\r"
            "SET hive.exec.max.dynamic.partitions=100000;\r"
            "SET hive.exec.max.dynamic.partitions.pernode=100000;\r")

    f.write("\r\r\r"+insert_CoreBankHist_str)
    f.write("\r\r\r" + insert_TownBankHist_str)
    f.write("\r\r!q")
    f.close()

#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()