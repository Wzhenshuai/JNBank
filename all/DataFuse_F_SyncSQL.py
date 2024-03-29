#coding=utf-8
## 仅用于 数整 F表的同步
import pymysql
import os, sys

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

sqlPath = 'E:\mnt\JN_shell\Create_tables\AddDataSync'
shortName = system_name

selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是' and  substring_index(system_en_name,'_',2)='%s'" % (system_name,system_nu)
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
    file_sql_name = "AllDataSync.Core.%s.sql" % table_name
    ## 拼接创建表 语句操作
    insert_CoreBankHist_str = "insert into %s select\n " % table_name

    insert_table_str = ""
    insert_fieldStr = ''
    create_fieldStr = ''
    aaa = 0
    for fie in allField:
        key_comm = fie[5]
        if fie[0].upper() == 'CORPORATION':
            aaa = 1
        insert_fieldStr = insert_fieldStr + '`'+fie[0] + '`,\n'

    insert_table_str = "uniq() as rowkeystr,\r"

    if aaa == 0:
        insert_table_str = insert_table_str + 'CORPORATION,\r' + insert_fieldStr
    else:
        insert_table_str = insert_table_str + insert_fieldStr

    ##insert_table_str = insert_table_str +"'%s' as data_source_str\r "% shortName
    insert_CoreBankHist_str = insert_CoreBankHist_str+insert_table_str.rstrip(',\n') + "\rfrom AllAnalyze.%s;" % ("Core_"+table_name)
    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\r truncate table %s;\r" % table_name)

    f.write("\r\r\r"+insert_CoreBankHist_str)

    f.write("\r\r")
    f.write("\r\r!q")
    f.close()
print(str(numIndex)+"张表操作完成！！！")
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()