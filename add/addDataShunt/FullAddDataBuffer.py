# coding=utf-8
## 仅用于 增量表的分流
import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile,ConstantUtile,CoverField

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道
SHORTNAME = sys.argv[1].upper()

# 查询sql 存入的路径
dicResultData = SqlUtile.getDicInfo(cursor, SHORTNAME)
sqlPath = dicResultData[0][0].upper()

## 查询增量表数据

FullResultData = SqlUtile.getQLSchemeData(cursor, SHORTNAME)
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

    yes_keyFlag_fieldStr = ""
    no_big_fieldStr = ""
    select_unite_key_file = ""
    insert_table_str = ""
    insert_fieldStr1 = ''
    insert_fieldStr2 = ''
    select_insert_fieldStr = ''
    create_fieldStr = ''
    insert_rowKey_str = ""
    aaa = "NO_CORPORATION"
    key_flag = "无主键"
    for fie in fieldResultData:
        ## field_code,field_type,field_len,field_accuracy,field_name,key_flag
        keyFlag = fie[5]
        fieldCode = fie[0].upper()
        fieldType = fie[1].upper()

        if keyFlag == '是':
            key_flag = "有主键"
            yes_keyFlag_fieldStr = yes_keyFlag_fieldStr + "%s," % fieldCode
            insert_fieldStr2 = insert_fieldStr2 + "COALESCE(`%s`,'ThisIsNULL') as `%s`,\r" % (fieldCode, fieldCode)
        else:
            fieldTypeOth = CoverField.conver_field_oth(fieldType)
            if fieldTypeOth == '1':
            ## 不是大字段
                no_big_fieldStr = no_big_fieldStr + "'%s'," % fieldCode
        select_unite_key_file = select_unite_key_file + "%s," % fieldCode
        select_insert_fieldStr = select_insert_fieldStr + "`%s`,\r" % fieldCode
        if fieldType in ("CLOB", "BLOB","LONG RAW"):
           continue
        if fieldCode == 'CORPORATION':
            aaa = "YES_CORPORATION"
        if key_flag == "无主键":
            insert_fieldStr2 = insert_fieldStr2 + "COALESCE(`%s`,'ThisIsNULL') as `%s`,\r" % (fieldCode, fieldCode)
        insert_fieldStr1 = insert_fieldStr1 + "COALESCE(`%s`,'ThisIsNULL'),\r" % fieldCode

    if aaa == "NO_CORPORATION":
        # 没有CORPORATION 处理
        yes_keyFlag_fieldStr = 'CORPORATION,' + yes_keyFlag_fieldStr
        #select_unite_key_file = 'CORPORATION,' + select_unite_key_file
        select_insert_fieldStr = 'CORPORATION,\r' + select_insert_fieldStr
        no_big_fieldStr = "'CORPORATION'," + no_big_fieldStr
    if key_flag == "无主键":
        #没有主键 处理一下
        part_insert_1_2 = "uniq() as rowkeystr,\r"
        part_insert_2_2 = "concat("+no_big_fieldStr.rstrip(",\r")+")as rowkeystr\r"
        part_insert_3_2 = part_insert_1_2
    else:
        part_insert_1_2 = "concat(" + yes_keyFlag_fieldStr.rstrip(",\r") + ')as rowkeystr,\r'
        part_insert_2_2 = "concat(" + yes_keyFlag_fieldStr.rstrip(",\r") + ')as rowkeystr\r'
        part_insert_3_2 = part_insert_1_2

    part_insert_1_3 = "(select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') as dataday_id," % SHORTNAME
    part_insert_3_3 = part_insert_1_3
    part_insert_1_3_1 = "\r SYSDATE, \r "
    part_insert_3_3_1 = "\r SYSDATE  as tdh_load_timestamp, \r"
    if aaa == 'NO_CORPORATION':
        insert_table_str = insert_table_str + 'CORPORATION,\r'
        insert_fieldStr1 = 'CORPORATION,\r' + insert_fieldStr1
        insert_fieldStr2 = 'CORPORATION,\r' + insert_fieldStr2
    insert_table_str = insert_table_str + select_insert_fieldStr

    part_insert_1_5 = "'%s' as data_source_str \r from AddAnalyze.%s \r where ("% (SHORTNAME,SHORT_tableName)

    part_insert_1_6 =  " from AddAnalyze.%s \r  "% SHORT_tableName

    part_insert_1_6_1 = "EXCEPT \r select "

    part_insert_2_6 = part_insert_1_6

    part_insert_1_7 =  " from AddRollData.%s_Hbase\r"% SHORT_tableName

    part_insert_2_7 = part_insert_1_7
    part_insert_1_5_1 = insert_fieldStr1.rstrip(',\r').replace('\r', '')
    part_insert_1 = part_insert_1_1+part_insert_1_2+part_insert_1_3+part_insert_1_3_1+select_insert_fieldStr+part_insert_1_5+part_insert_1_5_1+") \r in (select " +part_insert_1_5_1+part_insert_1_6 + part_insert_1_6_1+insert_fieldStr1.rstrip(',\r').replace('\r','')+part_insert_1_7 + ") ;"

    part_insert_2_3 = ",CORPORATION \r ,(select distinct CycleId from AddBuffer.AddDateCycleId WHERE System_JC='%s') as DELETE_DAY_ID " \
                      "\r from ( \r select " % SHORTNAME

    part_insert_2_5 = "from AddAnalyze.%s \r ) ;\r\r" % SHORT_tableName

    part_insert_2 = part_insert_2_1+part_insert_2_2+part_insert_2_3 + insert_fieldStr2.rstrip(',\r').replace('\r','') + part_insert_2_7+ \
                    part_insert_1_6_1+ insert_fieldStr2.rstrip(',\r').replace('\r','') + part_insert_2_6 +") ;"

    part_insert_3_5 = "'%s' as data_source_str from AddAnalyze.%s ;\r"% (SHORTNAME,SHORT_tableName)

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
