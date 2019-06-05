#coding=utf-8

import pymysql
import os
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
save_dir = os.getcwd()+"/AddData/"
if os.path.exists(save_dir) is False:
    os.makedirs(save_dir)
os.chdir(save_dir)
for ta in all_table:

    table_name = (short_name + "_" + ta[0])
    exe_table_name = (ta[0])
    file_sql_name = "Add_CORE_Buffer.%s_PureAdd.sql" % (table_name)
    # 查询表的注解
    table_comm = "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE table_schema = '%s' and table_name = '%s'" %(data_name,exe_table_name)
    cursor.execute(table_comm)
    #table_comm = cursor.fetchall()[0][1]
    # 查询表的结构
    sql ="SELECT COLUMN_NAME,column_comment,COLUMN_TYPE,column_key FROM INFORMATION_SCHEMA.Columns WHERE table_name = '%s' AND table_schema='%s'" %(exe_table_name,data_name);
    cursor.execute(sql)
    # 表结构的数据
    fields = cursor.fetchall()
    #print(fields)
    ## 拼接创建表 语句操作
    create_table_str = "create table IF NOT EXISTS %s_pureAdd(\r" %(table_name)
    insert_buffer_str = "insert into AddBuffer.%s_pureAdd select\r "%(table_name)
    insert_CoreBankHist_str = "insert into CoreBankHist.%s PARTITION(partition_month) select\r " % (table_name)
    insert_TownBankHist_str = "insert into TownBankHist.%s PARTITION(partition_month) select\r " % (table_name)

    #unite_key_value = "concat_ws('^',"
    # insert_table_str = ""
    # for fie in fields:
    #     key_comm = fie[3]
    #     if key_comm == 'PRI':
    #         unite_key_value = unite_key_value + fie[0] + ','

    #unite_key_value = unite_key_value.rstrip(",") + "),\r"
    #insert_table_str = insert_table_str + unite_key_value.rstrip(",") + "),\r"

    unite_key_file = "联合主键("
    for fie in fields:
        key_comm = fie[3]
        if key_comm == 'PRI':
            unite_key_file = unite_key_file + fie[0] + ','
    create_table_str = create_table_str + "rowKeyStr string comment '"
    create_table_str = create_table_str + unite_key_file.rstrip(",") + "拼接)' ," \
                                                                       "\rDataDay_ID String COMMENT'数据的时间'," \
                                                                       "\rtdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',\r"

    for i in fields:
        comm = i[1]
        if (comm == ''):
            com = "''"
        key_comm = i[3]
        if key_comm == 'PRI':
            comm = comm+'.主键'
        field_type = i[2];
        field_type = coverField.convert_fieldType(field_type)
        create_table_str = create_table_str+("%s %s comment'%s',\n")%(i[0],field_type,comm)
        #insert_table_str = insert_table_str+"%s,\n"%i[0]
        #insert_buffer_str = insert_buffer_str+"%s,\n"%i[0]

    create_table_str = create_table_str+"\rData_source String COMMENT'数据来源'" \
                                        "\r)comment '%s汉语注解.单纯增量表' stored as ORC;"%(table_name)
    insert_table_str = ''
    insert_buffer_str = insert_buffer_str +" * from AddAnticipate.%s;"%(table_name)
    insert_table_str = insert_table_str +"*,substr(DataDay_ID,1,6) as partition_month\r"
    insert_CoreBankHist_str = insert_CoreBankHist_str +insert_table_str+"from AddBuffer.%s_pureAdd where corporation in (" \
                                                     "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                     "where CORPORATION_NAME in ('公共','总行' ));" % (table_name)
    insert_TownBankHist_str = insert_TownBankHist_str +insert_table_str+"from AddBuffer.%s_pureAdd where corporation in (" \
                                                     "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                     "where CORPORATION_NAME in ('公共','村镇' ));" % (table_name)

    ## 数据写入文件
    if (os._exists(file_sql_name)):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+")
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS AddBuffer COMMENT 'hive缓冲库';\r"
            "use AddBuffer;\r")
    f.write("drop table %s_pureAdd;\r"%(table_name))

    f.write("\r"+create_table_str)
    f.write("\r\r\r----数据存放入：HIVE缓冲库.纯增量表")
    f.write("\r\r\r" + insert_buffer_str)
    f.write("\r\r\rset hive.enforce.bucketing = true;\r"
            "set hive.exec.dynamic.partition=true;\r"
            "set hive.exec.dynamic.partition.mode=nonstrict;\r"
            "SET hive.exec.max.dynamic.partitions=100000;\r"
            "SET hive.exec.max.dynamic.partitions.pernode=100000;\r")

    f.write("\r\r"+insert_CoreBankHist_str)
    f.write("\r\r\r" + insert_TownBankHist_str)
    f.write("\r!q")
    f.close()

#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()
