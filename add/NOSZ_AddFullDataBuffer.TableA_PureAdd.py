#coding=utf-8

import pymysql
import os,sys
from All3 import coverField
conn = pymysql.connect(host='127.0.0.1',user='root',password='woshibangbangde',db='datams',charset='utf8',port=3306)
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

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
    file_sql_name = "AddFullDataBuffer.%s_PureAdd.sql" % table_name

    ## 拼接创建表 语句操作
    create_table_str = "create table IF NOT EXISTS %s_PureAdd(\n" % table_name
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


    unite_key_value_str = coverField.unite_key_value(allField)
    unite_key_file = "联合主键("
    unite_key_value = "concat_ws('^',"
    for fie in allField:
        key_comm = fie[5]
        if key_comm == '是':
            unite_key_file = unite_key_file + fie[0] + ','
    create_table_str = create_table_str + "rowKeyStr varchar(333) comment '"
    create_table_str = create_table_str + unite_key_file.rstrip(",") + "拼接)'," \
                                                                       "\rDataDay_ID varchar(33) COMMENT '数据的时间'," \
                                                                       "\rtdh_load_timestamp  varchar(33)  COMMENT'加载到TDH时的时间戳',\r" \
                                                                       "corporation varchar(33) COMMENT '法人行号.主键',\r"


    create_field_str = ''
    inster_field_str = ''
    inster_field_str_sss = 'sss.corporation,\r'
    for i in allField:
        comm = i[1]
        if comm == '':
            com = "''"
        key_comm = i[5]
        if key_comm == '是':
            comm = comm + '.主键'
        field_type = coverField.convert_fieldType(i)

        create_field_str = create_field_str + ("%s %s comment'%s',\n") % (i[0], field_type, comm)
        inster_field_str_sss = inster_field_str_sss + ("sss."+"%s,\r" % i[0])
        inster_field_str = inster_field_str + ("%s,\r" % i[0])



    create_table_str = create_table_str+create_field_str
    insert_town_core_table_str = unite_key_value_str +"\r,to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss')\r"+inster_field_str+"\r,substr(day_id,1,6) as partition_month "

    create_table_str = create_table_str + "\rData_source_str varchar(33) COMMENT'数据来源'" \
                                          ")comment '%s汉语注解.单纯增量表' stored as ORC;"%(table_name)

    insert_table_pureAdd_str = insert_table_pureAdd_str+inster_field_str_sss+"sss.Data_source \r" \
                                            "from (select rowKeyStr,\r corporation,\r"+ inster_field_str +"Data_source\r" \
                                            "from AddAnticipate.%s\rEXCEPT\rselect rowKeyStr,\r corporation,\r"%(table_name)
    insert_table_pureAdd_str = insert_table_pureAdd_str+inster_field_str+"\rData_source \rfrom AddBuffer.%s_TotalHbase\r) sss ;"%(table_name)

    insert_table_town_str = insert_table_town_str  + "*,TDH_TODATE(DataDay_ID,'yyyyMM') as partition_month \r" \
                                                     "from AddAnticipate.%s where corporation in (" \
                                                     "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                     "where CORPORATION_NAME in ('公共','村镇' ));" % (table_name)
    insert_table_core_str = insert_table_core_str  + "*,TDH_TODATE(DataDay_ID,'yyyyMM') as partition_month \r" \
                                                     "from AddAnticipate.%s where corporation in (" \
                                                     "select distinct CORPORATION from AddBuffer.dic_CORPORATION " \
                                                     "where CORPORATION_NAME in ('公共','总行' ));" % (table_name)

    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding='utf-8')
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
