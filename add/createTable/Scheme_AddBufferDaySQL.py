#coding=utf-8
### 增量用于数整 ODS表生成铺底 建表（历史库）
import os
import sys
Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile, FieldUtile

SHORTNAME = sys.argv[1].upper()

#第二步：创建游标  对象
conn = SqlUtile.mysqlLogin()   #cursor当前的程序到数据之间连接管道
cursor = conn.cursor()

AllSchemeResultData = SqlUtile.getALLSchemeData(cursor, SHORTNAME)
path = r"E:\mnt\JN_shell\Create_tables\AddBuffer"
out_file_path = os.path.join(path, "%s.AddBufferDay.sql" % SHORTNAME)

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)
numIndex = 0
for td in AllSchemeResultData:
    numIndex = numIndex + 1
    schemeKey = td[0]
    tableName = td[1].lower()
    tableChName = td[2]
    DBNAME = td[3].upper()
    SHORT_DBNAME_tableName = SHORTNAME + '_' + DBNAME + '_' +tableName

    tableCommenStr = td[1]+td[2]

    field_datas = SqlUtile.getTableFieldByKey(cursor, schemeKey)
    createBody = FieldUtile.getAllfieldStr(field_datas)
    headStr = "DROP TABLE IF EXISTS %s;\r create table IF NOT EXISTS %s ( \r " %(SHORT_DBNAME_tableName,SHORT_DBNAME_tableName)
    footStr = "`Data_source_str` varchar(33) COMMENT '数据来源' \r)comment '%s' \r clustered by (rowKeyStr) into 6 buckets stored as orc TBLPROPERTIES ('transactional'='true');\r\r"% tableChName
    fieldStr = headStr + createBody + footStr
    f = open(out_file_path, "a+", encoding='utf-8')
    f.write(fieldStr)
    f.close()
print(str(numIndex) + "张表操作完成！！！")
cursor.close()
conn.close()