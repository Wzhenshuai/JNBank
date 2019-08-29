#coding=utf-8
## 缓冲数据层:计算该表的 单纯增量数据，以及贴源层全量数据
import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile, ConstantUtile

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道
SHORTNAME = sys.argv[1].upper()

# 获取所有表
## 获取增量数据
AllSchemeResultData = SqlUtile.getZLData(cursor, SHORTNAME)

dicResultData = SqlUtile.getDicInfo(cursor, SHORTNAME)
sqlPath = dicResultData[0][0].upper()

if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)
numIndex = 0
for ta in AllSchemeResultData:
    numIndex += 1
    schemeKey = ta[0]
    tableName = ta[1].lower()
    DBNAME = ta[2].upper()
    ##  获得该表的表 字段
    fieldResultData = SqlUtile.getTableFieldByKey(cursor, schemeKey)

    SHORT_DBNAME_tableName = SHORTNAME+'_'+DBNAME+'_'+tableName.lower()
    file_sql_name = "AddDataBuffer.%s.sql" % SHORT_DBNAME_tableName
    ## 拼接创建表 语句操作
    insert_tableName_str = "insert into AddRollData.%s \r " % SHORT_DBNAME_tableName
    insert_tableName_Hbase_str = "insert into AddRollData.%s_Hbase select\r " % SHORT_DBNAME_tableName


    unite_key_file = ""
    insert_table_str = ""
    insert_fieldStr = ''
    create_fieldStr = ''
    aaa = 0
    for fie in fieldResultData:
        key_comm = fie[5]
        fieldName = fie[0].upper()
        if fieldName == 'CORPORATION':
            aaa = 1
        if key_comm == '是':
            unite_key_file = unite_key_file + fie[0] + ','
        insert_fieldStr = insert_fieldStr + '`'+fie[0] + '`,\n'
    if aaa == 0:
        unite_key_file = 'CORPORATION,' + unite_key_file
    if unite_key_file == "":
        insert_table_str = "uniq() as rowkeystr,\r" \
                           + "(select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') as dataday_id,\r" \
                           + "SYSDATE  as tdh_load_timestamp, \r" % SHORTNAME
    else:
        insert_table_str = "select concat(" + unite_key_file.rstrip(",") + ')as rowkeystr,\r' \
                           + "(select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') as dataday_id,\r" % SHORTNAME\
                           + "SYSDATE  as tdh_load_timestamp, \r"
    if aaa == 0:
        insert_table_str = insert_table_str + 'CORPORATION,\r' + insert_fieldStr
        insert_fieldStr = 'CORPORATION,\r' + insert_fieldStr
    else:
        insert_table_str = insert_table_str + insert_fieldStr

    insert_table_str = insert_table_str + "'%s' as data_source_str \r" % SHORTNAME

    insert_tableName_Hbase_field_str = "rowkeystr,\r dataday_id,\r tdh_load_timestamp,\r"+ insert_fieldStr+ "'%s' as data_source_str \r" % SHORTNAME

    insert_tableName_str = insert_tableName_str+insert_table_str + "from AddAnalyze.%s ;" % SHORT_DBNAME_tableName

    insert_tableName_Hbase_str = insert_tableName_Hbase_str+insert_tableName_Hbase_field_str + "from AddRollData.%s ;" % SHORT_DBNAME_tableName

    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding= 'utf-8')
    f.write("--- 本文件: " + file_sql_name)

    f.write(ConstantUtile.setHiveStr)

    f.write("truncate table AddRollData.%s;\r" % SHORT_DBNAME_tableName)

    f.write("\r"+insert_tableName_str)

    f.write("\r\r" + insert_tableName_Hbase_str)
    f.write("\r\r!q")
    f.close()
print(str(numIndex)+"张表操作完成！！！")
#第六步：关闭所有的连接
#关闭游标
cursor.close()
#关闭数据库
conn.close()