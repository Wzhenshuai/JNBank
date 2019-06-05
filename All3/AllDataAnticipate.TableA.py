#coding=utf-8

import pymysql
import os, sys
from All3 import coverField
#第一步：连接到mysql数据库
conn = pymysql.connect(host='127.0.0.1',user='root',password='123456',db='mysql',charset='utf8',port=33061)
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

#操作的数据库
data_name = 'test_table'

#数据库缩写
short_name = 'TT'

conn.select_db(data_name);
#获取所有表
cursor.execute('SHOW TABLES')
all_table = cursor.fetchall()
#print(all_table)
#创建、切换目录
save_dir = os.getcwd()+"/AllData/"
if os.path.exists(save_dir) is False:
    os.makedirs(save_dir)
os.chdir(save_dir)
for ta in all_table:

    table_name = (short_name + "_" + ta[0])
    exe_table_name = (ta[0])
    file_sql_name = "AllDataAnticipate.%s.sql" % (table_name)
    # 查询表的注解
    table_comm = "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE table_schema = '%s' and table_name = '%s'" %(data_name,exe_table_name)
    cursor.execute(table_comm)
    table_comm_b = cursor.fetchall()[0][1]
    # 查询表的结构
    sql ="SELECT COLUMN_NAME,column_comment,COLUMN_TYPE,column_key FROM INFORMATION_SCHEMA.Columns WHERE table_name = '%s' AND table_schema='%s'" %(exe_table_name,data_name);
    #print(sql);
    cursor.execute(sql)
    # 表结构的数据
    fields = cursor.fetchall()
    #print(fields)
    ## 拼接创建表 语句操作
    create_table_str = "create table IF NOT EXISTS %s(\n" %(table_name)
    insert_table_str = "insert into AllAnticipate.%s PARTITION(partition_corporation) select\n "%(table_name)

    unite_key_file = "联合主键("
    unite_key_value = "concat_ws('^',"
    for fie in fields:
        key_comm = fie[3]
        if key_comm == 'PRI':
            unite_key_file = unite_key_file + fie[0] + ','
            unite_key_value = unite_key_value + 'trim('+fie[0] +')'+ ','
    create_table_str = create_table_str + "rowKeyStr string comment '"
    create_table_str = create_table_str + unite_key_file.rstrip(",") + "拼接)' ," \
                                                                       "\rDataDay_ID String COMMENT'数据的时间'," \
                                                                       "\rtdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',\r"

    insert_table_str = insert_table_str + unite_key_value.rstrip(",") + ")," \
                                                                        "\rTDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyyMMdd')," \
                                                                        "\rto_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss'),\r"

    for i in fields:
        comm = i[1]
        if (comm == ''):
            com = "''"
        key_comm = i[3]
        if key_comm == 'PRI':
            comm = comm+'.主键'
        field_type = i[2];
        field_type = coverField.convert_fieldType(field_type)
        create_table_str = create_table_str+("%s %s comment '%s',\n")%(i[0],field_type,comm)

        insert_field_str = ''
        if i[2] == 'date':
            insert_field_str = "case when date(trim(XT_DATE)) is null then date(trim('1970-01-01 08:00:00')) " \
                               "else date(trim(XT_DATE)) end"
        elif i[2] == 'timestamp':
            insert_field_str = "case when date(trim(XTBAL_DB_TIMESTAMP)) " \
                                "is null then CAST(trim('1970-01-01 08:00:00') as TIMESTAMP) " \
                                "else CAST(trim(XTBAL_DB_TIMESTAMP) as TIMESTAMP) end "
        else:
            insert_field_str = "trim(%s)"%i[0]
        insert_table_str = insert_table_str+insert_field_str+',\n'

    create_table_str = create_table_str+"Data_source String COMMENT'数据来源'"+\
                                           "\r)comment '%s汉语注解' partitioned by(partition_corporation string)\r" \
                                                      "STORED AS TEXTFILE TBLPROPERTIES('serialization.null.format'='');"%(table_name)

    insert_table_str = insert_table_str+"'%s' as Data_source,\r"\
                       "CORPORATION as partition_corporation\r" \
                                        "from AllAnalyze.%s;"%(short_name,table_name)

    ## 插入AddAnticipateTablesCount语句拼接
    insert_str = "insert into AllAnticipateTablesCount select\n " \
                 "to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')\n" \
                 ", 'AllAnticipate.%s'\n" \
                 ", count(1)\n" \
                 ", SYSDATE from %s;" % (table_name, table_name)
    #print(insert_str)
    ## 数据写入文件
    if (os._exists(file_sql_name)):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+")
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS AllAnticipate COMMENT '全量.预处理库';\r"
            "use AllAnticipate;\r"
            "drop table %s;"%(table_name))
    f.write("\r\r\r" + create_table_str)

    f.write("\r\r\rset hive.enforce.bucketing = true;\r"
            "set hive.exec.dynamic.partition=true;\r"
            "set hive.exec.dynamic.partition.mode=nonstrict;\r"
            "SET hive.exec.max.dynamic.partitions=100000;\r"
            "SET hive.exec.max.dynamic.partitions.pernode=100000;\r")

    f.write("\r\r\r"+insert_table_str)
    f.write("\r\r-->>> AllAnticipateTablesCount 统计全量数据预处理表.全量数据量 [时间戳、将库表名称、数据条数、解析时间(yyyy-MM-dd HH:mm:ss)]\r\r\r")
    f.write(insert_str)
    f.write("\r\r!q")
    f.close()

#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()
