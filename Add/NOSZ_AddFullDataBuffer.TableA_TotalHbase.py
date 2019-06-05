#coding=utf-8

import pymysql
import os,sys
from All3 import coverField

conn = pymysql.connect(host='127.0.0.1',user='root', password='woshibangbangde',db='datams',charset='utf8',port=3306)
#第二步：创建游标  对象
cursor = conn.cursor()

system_name = sys.argv[1]

#获取所有表
dictSql = "SELECT sql_path,system_code FROM dic_info_mapping WHERE transfer_mode ='增量数据' AND system_code='"+system_name+"'"
cursor.execute(dictSql)
table_data = cursor.fetchall()

sqlPath = table_data[0][0]
shortName = table_data[0][1].upper()

selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是'"%(system_name)
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
    table_name = (shortName + "_" + tableName.lower())

    file_sql_name = "AddFullDataBuffer.%s_TotalHbase.sql" % table_name

    ## 拼接创建表 语句操作
    create_table_str = "create table IF NOT EXISTS %s_totalHbase(\n" %(table_name)
    insert_table_str = "insert into AddBuffer.%s_totalHbase select * from AddAnticipate.%s;\n "%(table_name,table_name)

    unite_key_file = "联合主键("
    unite_key_value = "concat_ws('^',"
    for fie in allField:
        key_comm = fie[5]
        if key_comm == '是':
            unite_key_file = unite_key_file + fie[0]+','
            unite_key_value = unite_key_value+fie[0]+','
    create_table_str = create_table_str+"rowKeyStr varchar(333) comment '"
    create_table_str = create_table_str+unite_key_file.rstrip(",")+"拼接)',\r" \
                                          "DataDay_ID varchar(33) COMMENT '数据的时间',\r" \
                                          "tdh_load_timestamp  varchar(33)  COMMENT'加载到TDH时的时间戳',\r" \
                                                                   "corporation varchar(33) COMMENT '法人行号.主键',\r"
    for i in allField:
        comm = i[4]
        if comm == '':
            com = "''"
        key_comm = i[5]
        if key_comm == '是':
            comm = comm+'.主键'
        field_type = coverField.convert_fieldType(i)
        create_table_str = create_table_str+("%s %s comment'%s',\n")%(i[0],field_type,comm)

    create_table_str = create_table_str+"Data_source_str varchar(33) COMMENT'数据来源'\r)comment '%s汉语注解.贴源层全量表' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler';"%(table_name)

    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS AddBuffer COMMENT 'hive缓冲库';\r"
            "use AddBuffer;\r"
            "truncate table %s_TotalHbase;"%(table_name))
    f.write("\r\r"+create_table_str)
    f.write("\r\r\r"+insert_table_str)
    f.write("\r\r!q")
    f.close()
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()
