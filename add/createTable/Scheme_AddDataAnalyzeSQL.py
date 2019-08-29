
# coding=utf-8
### 增量生成数整 ODS 解析sql
import os
import sys
Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile, FieldUtile

SHORTNAME = sys.argv[1].upper()

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

AllSchemeResultData = SqlUtile.getALLSchemeData(cursor, SHORTNAME)

path = r"E:\mnt\JN_shell\Create_tables\AddAnalyze"

out_file_path = os.path.join(path, "%s.AddDataAnalyze.sql" % SHORTNAME)

if os.path.exists(out_file_path):
    os.remove(out_file_path)
numIndex = 0
for td in AllSchemeResultData:
    numIndex += 1
    schemeKey = td[0]

    tableChName = td[2]
    DBNAME = td[3].upper()
    SHORT_DBNAME_tableName = SHORTNAME + '_' + DBNAME + '_'+ td[1].lower()
    headStr = "DROP TABLE IF EXISTS %s; \r create external table IF NOT EXISTS %s (\r" %(SHORT_DBNAME_tableName,SHORT_DBNAME_tableName)
    fieldResultData = SqlUtile.getTableFieldByKey(cursor, schemeKey)
    bodayStr = FieldUtile.getfieldString(fieldResultData)

    footStr = "\r )comment '%s' row format delimited fields terminated by '\\u0003' lines terminated by '\\u0005'\r" \
              "stored as textfile location '/DATACENTER/AddData/%s/%s/';\r\r" %(tableChName,SHORTNAME,SHORT_DBNAME_tableName)

    fieldStr =headStr+bodayStr.strip(',\r')+footStr

    f = open(out_file_path, "a+", encoding='utf-8')
    f.write(fieldStr)
    f.close()
print(str(numIndex)+"张表操作完成！！！")
cursor.close()
conn.close()

