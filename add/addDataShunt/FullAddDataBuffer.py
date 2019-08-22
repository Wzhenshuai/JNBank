# coding=utf-8
## 仅用于 增量表的分流
import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile,ConstantUtile

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道
SHORTNAME = sys.argv[1].upper()

# 查询sql 存入的路径
dicResultData = SqlUtile.getDicInfo(cursor, SHORTNAME)
sqlPath = dicResultData[0][0].upper()

## 查询增量表数据

if SHORTNAME == 'CREDITCORE':
    FullResultData = SqlUtile.getQLData(cursor,'CREDIT')
elif SHORTNAME == 'CREDITTOWN':
    FullResultData = SqlUtile.getQLCREDITTOWNData(cursor)
else:
    FullResultData = SqlUtile.getQLData(cursor, SHORTNAME)
    # ZLResultData = SqlUtile.otherTmp(cursor)
if os.path.exists(sqlPath) is False:
    os.makedirs(sqlPath)
os.chdir(sqlPath)

numIndex = 0
for ta in FullResultData:
    numIndex += 1
    schemeKey = ta[0]
    tableName = ta[1]
    ## 查询该表 字段
    fieldResultData = SqlUtile.getTableFieldByKey(cursor, schemeKey)

    SHORT_tableName = SHORTNAME + "_" + tableName.lower()
    file_sql_name = "FullAddDataBuffer.%s.sql" % SHORT_tableName
    ## 拼接创建表 语句操作
    part_insert_1_1 = "truncate table AddRollData.%s;\r insert into AddRollData.%s \r select \r " % (SHORT_tableName,SHORT_tableName)
    part_insert_2_1 = "insert into AddBuffer.AddPureDelete \r select uniq() \r ,to_timestamp(SYSDATE)\r ,SYSDATE \r ,'AddRollData.%s'\r,"% SHORT_tableName
    part_insert_3_1 = "truncate table AddRollData.%s_Hbase;\r insert into AddRollData.%s_Hbase select\n " % (SHORT_tableName,SHORT_tableName)

    unite_key_file = ""
    select_unite_key_file = ""
    insert_table_str = ""
    insert_fieldStr = ''
    select_insert_fieldStr = ''
    create_fieldStr = ''
    insert_rowKey_str = ""
    aaa = 0
    for fie in fieldResultData:
        key_comm = fie[5]
        fieldName = fie[0].upper()
        fieldType = fie[1].upper()

        if fieldName == 'CORPORATION':
            aaa = 1
        if key_comm == '是':
            unite_key_file = unite_key_file + "%s," % fieldName
            select_unite_key_file = select_unite_key_file + "%s," % fieldName
        select_insert_fieldStr = select_insert_fieldStr + "`%s`,\r" % fieldName
        if fieldType in ("CLOB", "BLOB"):
            continue
        insert_fieldStr = insert_fieldStr + "COALESCE(`%s`,'ThisIsNULL') as `%s`,\r" % (fieldName,fieldName)
    if aaa == 0:
        unite_key_file = 'CORPORATION,' + unite_key_file
        select_unite_key_file = 'CORPORATION,' + select_unite_key_file
        select_insert_fieldStr = 'CORPORATION,\r' + select_insert_fieldStr
    if unite_key_file == "":
        #没有主键 处理一下
        part_insert_1_2 = "uniq() as rowkeystr,\r"
        part_insert_2_2 = "concat("+select_insert_fieldStr.rstrip(",\r")+"'%s')" % SHORTNAME
        part_insert_3_2 = part_insert_1_2
    else:
        part_insert_1_2 = "concat(" + unite_key_file.rstrip(",\r") + ')as rowkeystr,\r'
        part_insert_2_2 = "concat(" + select_unite_key_file.rstrip(",\r") + ')as rowkeystr\r'
        part_insert_3_2 = part_insert_1_2;

    part_insert_1_3 = "(select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') as dataday_id," % SHORTNAME
    part_insert_3_3 = part_insert_1_3
    part_insert_1_3_1 = "\r SYSDATE, \r "
    part_insert_3_3_1 = "\r SYSDATE  as tdh_load_timestamp, \r"
    if aaa == 0:
        insert_table_str = insert_table_str + 'CORPORATION,\r'
        insert_fieldStr = 'CORPORATION,\r' + insert_fieldStr
    insert_table_str = insert_table_str + select_insert_fieldStr

    part_insert_1_5 = "'%s' as data_source_str \r from AddAnalyze.%s \r where ("% (SHORTNAME,SHORT_tableName)

    part_insert_1_6 =  " from AddAnalyze.%s \r  "% SHORT_tableName

    part_insert_1_6_1 = "EXCEPT \r select "

    part_insert_2_6 = part_insert_1_6

    part_insert_1_7 =  " from AddRollData.%s_Hbase\r"% SHORT_tableName

    part_insert_2_7 = part_insert_1_7
    part_insert_1_5_1 = insert_fieldStr.rstrip(',\r').replace('\r', '')
    part_insert_1 = part_insert_1_1+part_insert_1_2+part_insert_1_3+part_insert_1_3_1+select_insert_fieldStr+part_insert_1_5+part_insert_1_5_1+") \r in (select " +part_insert_1_5_1+part_insert_1_6 + part_insert_1_6_1+insert_fieldStr.rstrip(',\r').replace('\r','')+part_insert_1_7 + ") ;"

    part_insert_2_3 = ",CORPORATION \r ,(select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') as DELETE_DAY_ID " \
                      "\r from ( \r select " % SHORTNAME

    part_insert_2_5 = "from AddAnalyze.%s \r ) ;\r\r" % SHORT_tableName

    part_insert_2 = part_insert_2_1+part_insert_2_2+part_insert_2_3 + insert_fieldStr.rstrip(',\r').replace('\r','') + part_insert_2_7+ part_insert_1_6_1+ insert_fieldStr.rstrip(',\r').replace('\r','') + part_insert_2_6 +") ;"

    part_insert_3_5 = "'CORE' as data_source_str from AddAnalyze.%s ;\r"% SHORT_tableName

    part_insert_3 = part_insert_3_1+part_insert_3_2+part_insert_3_3+part_insert_3_3_1+select_insert_fieldStr+part_insert_3_5


    ## 数据写入文件
    if os.path.exists(file_sql_name):
        os.remove(file_sql_name)
    f = open(file_sql_name, "a+", encoding='utf-8')
    f.write("--- 本文件: " + file_sql_name)
    f.write(ConstantUtile.setHiveStr)

    f.write("\r\r\r" + part_insert_1)
    f.write("\r\r")
    f.write("\r\r\r" + part_insert_2)
    f.write("\r\r")
    f.write("\r\r\r" + part_insert_3)
    f.write("\r\r!q")
    f.close()
print(str(numIndex) + "张表操作完成！！！")
# 第六步：关闭所有的连接
# 关闭游标
cursor.close()
# 关闭数据库
conn.close()
