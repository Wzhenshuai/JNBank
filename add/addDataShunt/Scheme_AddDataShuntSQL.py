#coding=utf-8
## 仅用于 增量表的分流
import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile, ConstantUtile

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道
SHORTNAME = sys.argv[1].upper()
# 获取sql 路径
AllSchemeResultData = SqlUtile.getALLSchemeData(cursor, SHORTNAME)

dicResultData = SqlUtile.getDicInfo(cursor, SHORTNAME)
sqlPath = dicResultData[0][0].upper()

if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)

numIndex = 0
for ta in AllSchemeResultData:
    numIndex += 1
    schemeKey = ta[0]
    tableName = ta[1]
    DBNAME = ta[3].upper()
    fieldResultData = SqlUtile.getTableFieldByKey(cursor, schemeKey)

    SHORT_DBNAME_tableName = SHORTNAME + "_" + DBNAME + '_' + tableName.lower()
    file_sql_name = "AddDataShunt.%s.sql" % SHORT_DBNAME_tableName
    ## 拼接创建表 语句操作
    insert_CoreBankHist_str = "insert into CoreBankHistTest.%s PARTITION(partition_year) select\n " % SHORT_DBNAME_tableName
    #insert_TownBankHist_str = "insert into TownBankHistTest.%s PARTITION(partition_year) select\n " % SHORT_DBNAME_tableName
    insert_AddRollData_str = "insert into AddRollData.%s_HisAdd PARTITION(partition_day) select\n " % SHORT_DBNAME_tableName

    insert_table_str = ""
    insert_fieldStr = ''
    create_fieldStr = ''
    aaa = "NO_CORPORATION"
    unite_key_oth = ""
    for fie in fieldResultData:
        key_comm = fie[5]
        fieldName = fie[0].upper()
        if fieldName == 'CORPORATION':
            aaa = "YES_CORPORATION"
        insert_fieldStr = insert_fieldStr + '`'+fie[0] + '`,\n'
    insert_table_str = "uniq() as rowkeystr,\r" \
                       + "(select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') dataday_id,\r" % SHORTNAME\
                       + "SYSDATE  as tdh_load_timestamp, \r"

    if aaa == "NO_CORPORATION":
        insert_table_str = insert_table_str + 'CORPORATION,\r' + insert_fieldStr
    else:
        insert_table_str = insert_table_str + insert_fieldStr
    insert_AddRollData_ss = insert_table_str + "'%s' as data_source_str,\r (select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') as partition_day \r" % ( SHORTNAME,SHORTNAME)
    insert_table_str = insert_table_str + "'%s' as data_source_str,\r substr(dataday_id,1,4) as partition_year \r" % SHORTNAME

    insert_CoreBankHist_str = insert_CoreBankHist_str+insert_table_str + "from AddRollData.%s where corporation in ('800','815');" % SHORT_DBNAME_tableName

    #insert_TownBankHist_str = insert_TownBankHist_str+insert_table_str + "from AddRollData.%s where corporation in ('800','615');" % SHORT_DBNAME_tableName

    insert_AddRollData_str = insert_AddRollData_str+insert_AddRollData_ss + "from AddAnalyze.%s ; " % SHORT_DBNAME_tableName
    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: %s \r" % file_sql_name)
    f.write(" use CoreBankHistTest;  ---临时使用\r")
    f.write(ConstantUtile.setHiveStr)

    f.write("\r\r"+insert_CoreBankHist_str)
    f.write("\r\r")
    #f.write("\r\r\r" + insert_TownBankHist_str)
    f.write("\r\r")
    f.write("\r\r\r" + insert_AddRollData_str)
    f.write("\r\r!q")
    f.close()
print(str(numIndex)+"张表操作完成！！！")
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()