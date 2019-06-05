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
#data_addAnticipate_name = 'AddAnticipate'

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

    file_sql_name = "AddFull_CORE_Buffer.%s_PureAdd.sql" % (table_name)
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
    create_table_str = "create table IF NOT EXISTS %s_PureAdd(\n" %(table_name)
    insert_table_pureAdd_str = "insert into AddBuffer.%s_PureAdd \r" \
                               "select sss.rowKeyStr,\r" \
                               "TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyyMMdd'),\r" \
                               "to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss')," %(table_name)
                              # "\rfrom (select\r"
    insert_table_pureDelete_str = "insert into AddBuffer.AddPureDelete \r" \
                                  "select to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss')\r" \
                                  ",'AddBuffer.%s'\r" \
                                  ",sss.rowKeyStr\r " \
                                  ",SYSDATE \r" \
                                  "from (select rowKeyStr\r" \
                                  "from AddBuffer.%s_TotalHbase\r" \
                                  "EXCEPT\r" \
                                  "select rowKeyStr from AddAnticipate.%s) sss;"%(table_name,table_name,table_name)
    insert_table_town_str = "insert into TownBankHist.%s PARTITION(partition_month) select \r"%(table_name)
    insert_table_core_str = "insert into CoreBankHist.%s PARTITION(partition_month) select  \r"%(table_name)


    #create_table_str = create_table_str+"tdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',\n"
    #insert_table_str = insert_table_str+unite_key_value.rstrip(",")+"),\n"

    ##concat_ws('^',corporation,xt_br_no,xt_curr_cod)#
    unite_key_value_str = coverField.unite_key_value(fields)
    unite_key_file = "联合主键("
    unite_key_value = "concat_ws('^',"
    for fie in fields:
        key_comm = fie[3]
        if key_comm == 'PRI':
            unite_key_file = unite_key_file + fie[0] + ','
            #unite_key_value = unite_key_value + fie[0] + ','
    create_table_str = create_table_str + "rowKeyStr string comment '"
    create_table_str = create_table_str + unite_key_file.rstrip(",") + "拼接)'," \
                                                                       "\rDataDay_ID String COMMENT '数据的时间'," \
                                                                       "\rtdh_load_timestamp  String  COMMENT'加载到TDH时的时间戳',\r"

    # arr_fields = coverField.get_fields_str(fields)
    # create_fiels_str = arr_fields[0]
    # insert_field_str = arr_fields[1]

    create_field_str = ''
    inster_field_str = ''
    inster_field_str_sss = ''
    for i in fields:
        comm = i[1]
        if (comm == ''):
            com = "''"
        key_comm = i[3]
        if key_comm == 'PRI':
            comm = comm + '.主键'
        field_type = i[2];
        field_type = coverField.convert_fieldType(field_type)

        create_field_str = create_field_str + ("%s %s comment'%s',\n") % (i[0], field_type, comm)
        inster_field_str_sss = inster_field_str_sss + ("sss."+"%s,\r" % i[0])
        inster_field_str = inster_field_str + ("%s,\r" % i[0])



    create_table_str = create_table_str+create_field_str
    insert_town_core_table_str = unite_key_value_str +"\r,to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss')\r"+inster_field_str+"\r,substr(day_id,1,6) as partition_month "
##-------------###
    create_table_str = create_table_str + "\rData_source String COMMENT'数据来源'" \
                                          ")comment '%s汉语注解.单纯增量表' stored as ORC;"%(table_name)

    insert_table_pureAdd_str = insert_table_pureAdd_str+inster_field_str_sss+"sss.Data_source \r" \
                                            "from (select rowKeyStr,\r"+ inster_field_str +"Data_source\r" \
                                            "from AddAnticipate.%s\rEXCEPT\rselect rowKeyStr,\r"%(table_name)
    insert_table_pureAdd_str = insert_table_pureAdd_str+inster_field_str+"\rData_source \rfrom AddBuffer.%s_TotalHbase\r) sss ;"%(table_name)

    #insert_table_pureDelete_str = insert_table_pureDelete_str+unite_key_value_str+"as rowKeyStr\rfrom AddAnticipate.%s\r) sss ;"%(table_name)

    insert_table_town_str = insert_table_town_str  + "*,TDH_TODATE(DataDay_ID,'yyyyMM') as partition_month \r" \
                                                     "from AddAnticipate.%s where corporation in (" \
                                                     "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                     "where CORPORATION_NAME in ('公共','村镇' ));" % (table_name)
    insert_table_core_str = insert_table_core_str  + "*,TDH_TODATE(DataDay_ID,'yyyyMM') as partition_month \r" \
                                                     "from AddAnticipate.%s where corporation in (" \
                                                     "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                     "where CORPORATION_NAME in ('公共','总行' ));" % (table_name)

    ## 数据写入文件
    if (os._exists(file_sql_name)):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+")
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS AddBuffer COMMENT 'hive缓冲库';\r"
            "use AddBuffer;\r"
            "drop table %s_PureAdd;"%(table_name))
    f.write("\r\r"+create_table_str)

    f.write("\r\r----集合运算。计算出：本次全增量中的单纯增量/删除档。并进行分类存储。"
            "\r-------集合运算中：使用最新的 全增量 和 昨天的贴源全量数据层 进行运算。"
            "\r-------单纯增量计算结束后，在进行更新 最新的贴源全量数据层表。")
    f.write("\r\r\r"+insert_table_pureAdd_str)
    f.write("\r\r ----将删除掉的数据，插入到删除档。长期积累。")
    f.write("\r\r"+insert_table_pureDelete_str)

    f.write("\r\r\rset hive.enforce.bucketing = true;")
    f.write("\rset hive.exec.dynamic.partition=true;")
    f.write("\rset hive.exec.dynamic.partition.mode=nonstrict;")
    f.write("\rSET hive.exec.max.dynamic.partitions=100000;")
    f.write("\rSET hive.exec.max.dynamic.partitions.pernode=100000;")


    f.write("\r\r"+insert_table_core_str)
    f.write("\r\r"+insert_table_town_str)
    f.write("\r\r!q")
    f.close()
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()
