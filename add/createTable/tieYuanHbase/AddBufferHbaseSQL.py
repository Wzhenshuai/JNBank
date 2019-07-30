#coding=utf-8
### 贴源层全量数据用于数整 ODS表生成铺底 建表
import os
import sys

from common import FieldUtile,SqlUtile

SHORTNAME = sys.argv[1].upper()

conn = SqlUtile.mysqlLogin()
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

if SHORTNAME.startswith('CORE'):
    AllSchemeResultData = SqlUtile.getCORESchemeData(cursor, SHORTNAME)
else:
    AllSchemeResultData = SqlUtile.getALLSchemeData(cursor,SHORTNAME)

path = r"E:\mnt\JN_shell\Create_tables\AddBuffer"

out_file_path = os.path.join(path, "%s.AddBufferHbase.sql" % SHORTNAME)

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)
numIndex = 0
for td in AllSchemeResultData:
    numIndex = numIndex + 1
    schemeKey = td[0]
    tableName = td[1].lower()
    tableChName = td[2]
    SHORT_tableName = SHORTNAME + '_' + tableName

    fieldResultData = SqlUtile.getTableFieldByKey(cursor, schemeKey)
    headStr = "DROP TABLE IF EXISTS %s_hbase; \r create table IF NOT EXISTS %s_hbase (\r" % (SHORT_tableName, SHORT_tableName)
    bodayStr = FieldUtile.getAllfieldStr(fieldResultData)
    footStr = "`Data_source_str` varchar(33) COMMENT '数据来源' \r)comment '%s'\r" \
              " clustered by (rowKeyStr) into 13 buckets stored as orc TBLPROPERTIES ('transactional'='true');\r\r" % tableChName
    fieldStr = headStr + bodayStr + footStr
    f = open(out_file_path, "a+", encoding='utf-8')
    f.write(fieldStr)
    f.close()
print(str(numIndex) + "张表操作完成！！！")
cursor.close()
conn.close()