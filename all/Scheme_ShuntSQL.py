#coding=utf-8
## 用于星云贷、CRM。SHORTNAME_SCHEME_tablename
import os
import sys

Path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile,ConstantUtile

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

SHORTNAME = sys.argv[1].upper()

# 获取所有表
dicResultData = SqlUtile.getPDDicInfo(cursor,SHORTNAME)

sqlPath = dicResultData[0][0]
shortName = dicResultData[0][1].upper()

AllSchemeResultData = SqlUtile.getALLSchemeData(cursor,SHORTNAME)

if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)

numIndex = 0
for ta in AllSchemeResultData:
    numIndex += 1
    schemeKey = ta[0]
    tableName = ta[1].lower()
    dbName = ta[3].upper()
    fieldResultData = SqlUtile.getTableFieldByKey(cursor,schemeKey)
    SHORTNAME_DBNAME_tableName = SHORTNAME + "_" + dbName + "_" + tableName.lower()
    file_sql_name = "AllDataShunt.%s.sql" % SHORTNAME_DBNAME_tableName
    ## 拼接创建表 语句操作
    insert_CoreBankHist_str = "insert into CoreBankHist.%s PARTITION(partition_year) select\n " % SHORTNAME_DBNAME_tableName

    unite_key_file = ""
    insert_table_str = ""
    insert_fieldStr = ''
    create_fieldStr = ''
    aaa = 0
    for fie in fieldResultData:
        key_comm = fie[5]
        if fie[0].upper() == 'CORPORATION':
            aaa = 1
        if key_comm == '是':
            unite_key_file = unite_key_file + fie[0] + ','
        insert_fieldStr = insert_fieldStr + '`'+fie[0] + '`,\n'
    if aaa == 0:
        unite_key_file = 'CORPORATION,' + unite_key_file
    if unite_key_file == "":
        insert_table_str = "uniq() as rowkeystr,\r" \
                           + "TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyyMMdd') as dataday_id,\r" \
                           + "to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss') as tdh_load_timestamp, \r"
    else:
        insert_table_str = "concat(" + unite_key_file.rstrip(",") + ')as rowkeystr,\r' \
                           + "TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyyMMdd') as dataday_id,\r" \
                           + "to_timestamp(SYSDATE,'yyyy-MM-dd HH:mm:ss') as tdh_load_timestamp, \r"
    if aaa == 0:
        insert_table_str = insert_table_str + 'CORPORATION,\r' + insert_fieldStr
    else:
        insert_table_str = insert_table_str + insert_fieldStr

    insert_table_str = insert_table_str +"'%s' as data_source_str,\r TDH_TODATE(SYSDATE+TO_DAY_INTERVAL(-1),'yyyy') as partition_year \r" % shortName
    insert_CoreBankHist_str = insert_CoreBankHist_str+insert_table_str + "from AllAnalyze.%s where corporation in ('800','815');" % SHORTNAME_DBNAME_tableName

    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write("\rCREATE DATABASE IF NOT EXISTS CoreBankHist COMMENT '总行.历史库';\r"
            "use CoreBankHist;\r"
            "truncate table %s;\r" % SHORTNAME_DBNAME_tableName )

    f.write(ConstantUtile.setHiveStr)

    f.write("\r\r\r"+insert_CoreBankHist_str)
    f.write("\r\r!q")
    f.close()
print(str(numIndex)+"张表操作完成！！！")
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()