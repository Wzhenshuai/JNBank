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
#预处理库
data_addAnticipate_name = 'AddAnticipate'

conn.select_db(data_name);
#获取所有表
cursor.execute('SHOW TABLES')
all_table = cursor.fetchall()

#创建、切换目录
save_dir = os.getcwd()+"/AddData/"
if os.path.exists(save_dir) is False:
    os.makedirs(save_dir)
os.chdir(save_dir)
for ta in all_table:

    table_name = (short_name + "_" + ta[0])
    exe_table_name = (ta[0])
    file_sql_name = "Add_CORE_Buffer.%s_TotalHbase.sql" % (table_name)
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
    create_table_str = "create table IF NOT EXISTS %s_totalHbase(\n" %(table_name)
    insert_table_str = "insert into AddBuffer.%s_totalHbase select\n "%(table_name)

    unite_key_file = "联合主键("
    unite_key_value = "concat_ws('^',"
    for fie in fields:
        key_comm = fie[3]
        if key_comm =='PRI':
            unite_key_file = unite_key_file + fie[0]+','
            unite_key_value = unite_key_value+fie[0]+','
    create_table_str = create_table_str+"rowKeyStr string comment '"
    create_table_str = create_table_str+unite_key_file.rstrip(",")+"拼接)'," \
                                                                   "\rDataDay_ID String COMMENT '数据的时间'," \
                                                                   "\rtdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',\r"
    #insert_table_str = insert_table_str+unite_key_value.rstrip(",")+"),\rto_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss'),\r"
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
        #insert_table_str = insert_table_str+("%s,\n"%i[0])

    create_table_str = create_table_str+"\rData_source String COMMENT'数据来源'" \
                                        "\r)comment '%s汉语注解.贴源层全量表' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler';"%(table_name)

    insert_table_str = insert_table_str+"\r * from AddAnticipate.%s;"%(table_name)


    ## 数据写入文件
    if (os._exists(file_sql_name)):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+")
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS AddBuffer COMMENT 'hive缓冲库';\r"
            "use AddBuffer;")
    f.write("\r\r"+create_table_str)
    f.write("\r\r\r"+insert_table_str)
    f.write("\r\r!q")
    f.close()
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()
