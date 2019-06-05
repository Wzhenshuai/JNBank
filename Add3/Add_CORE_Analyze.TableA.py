#coding=utf-8

import pymysql
import os, sys
import json
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
save_dir = os.getcwd()+"/AddData/"
if os.path.exists(save_dir) is False:
    os.makedirs(save_dir)
os.chdir(save_dir)

for ta in all_table:
    table_name = (short_name + "_" + ta[0])
    exe_table_name = (ta[0])
    file_sql_name = "Add_CORE_Analyze.%s.sql" % (table_name)
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
    for i in fields:
        comm = i[1]
        if (comm == ''):
            com = "''"
        key_comm = i[3]
        if key_comm == 'PRI':
            comm = comm+'.主键'
        create_table_str = create_table_str+("%s string comment'%s',\n")%(i[0],comm)
    create_table_str = create_table_str.rstrip(",\n")+")comment '%s汉语注解' row format delimited fields terminated by '\\u0003' " \
                                                      "\rstored as textfile location '/tmp/DATACENTER/AddData/CORE/%s';"%(table_name,table_name)
    ## 插入语句拼接
    insert_str = "insert into AddAnalyzeTablesCount select\n " \
                 "to_timestamp(SYSDATE, 'yyyy-MM-dd HH:mm:ss')\n" \
                 ", 'AddAnalyze.%s'\n" \
                 ", count(1)\n" \
                 ", SYSDATE from %s;"%(table_name,table_name)
    #print(insert_str)
    ## 数据写入文件
    if (os._exists(file_sql_name)):
        os.remove(file_sql_name)

    f = open(file_sql_name, "a+")
    f.write("--- 本文件: "+file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS AddAnalyze COMMENT'全量.解析库';\r"
            "use AddAnalyze;\r\r")
    f.write(create_table_str)
    f.write("\r\r-->>> AddAnalyzeTablesCount 统计增量数据解析表.数据量 [时间戳、将库表名称、数据条数、解析时间(yyyy-MM-dd HH:mm:ss)]\r\r\r")
    f.write(insert_str)
    f.write("\r\r!q")
    f.close()

#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()
