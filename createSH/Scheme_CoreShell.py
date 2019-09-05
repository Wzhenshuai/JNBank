# coding=utf-8

import os
import sys

Path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile
## 用于除（数整、信贷村镇、ERPCW）的数据生成
SHORTNAME = sys.argv[1].upper()
# systemUpper = 'credit'.upper()
conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

AllSchemeResultData = SqlUtile.getALLSchemeData(cursor,SHORTNAME)

dictSql = SqlUtile.getPDDicInfo(cursor,SHORTNAME)

outPath = dictSql[0][2]
# 模版路径

allTempFilePath = r"E:\mnt\template\all\AllData.Scheme.SHORTNAME_tablename.sh"

filePath = ''
out_file_path = ''
tableName = ''
DBNAME = ''
numIndex = 0
for ta in AllSchemeResultData:
    field1 = "select '815' as corporation ,"

    numIndex += 1
    schemeKey = ta[0]
    tableName = ta[1].lower()
    DBNAME = ta[3].upper()
    SHORTNAME_DBNAME_tableName = SHORTNAME + "_" + DBNAME + "_" + tableName

    fieldResultData = SqlUtile.getTableFieldByKey(cursor,schemeKey)

    fieldStr = ''
    for fd in fieldResultData:
        ty = fd[0].upper()
        if ty == 'INDEX':
            ty = '\\"INDEX\\"'
        if fd[1].upper() in ('CHAR', 'NCHAR', 'VARCHAR', 'NVARCHAR', 'GRAPHIC', 'VARBRAPHIC', 'CHARACTER','VARCHAR2',
                             'NVARCHAR2','LANG','EVALUATE_RECORD','MAINTAIN_INFO','MAINTAIN_INFO',
                             'LAWSUIT_APPLY','LAWCASE_INFO','BUSINESS_CONTRACT','ASSET_INFO','BUSINESS_APPROVE'):
            fieldStr = fieldStr + "trim("+ty+"),"
        else:
            fieldStr = fieldStr + ty + ','
        if fd[0].upper() == 'CORPORATION':
            field1 = "select "
    fieldStr = field1 + fieldStr.rstrip(',')+' from ${source_db}.${source_Table} where \$CONDITIONS'

    out_file_path = os.path.join(outPath, "AllData.%s.sh" % SHORTNAME_DBNAME_tableName)

    if os.path.exists(out_file_path):
        os.remove(out_file_path)

    f = open(allTempFilePath, 'r', encoding='utf-8')
    lines = f.readlines()

    wf = open(out_file_path, 'w', encoding='utf-8')
    for li in lines:
        rli = li.replace('SYSTEMSHORT', SHORTNAME).replace('TABLENAME', tableName.upper()).replace('sqoopQueryStr', fieldStr).replace('DBNAME',DBNAME)
        wf.write(rli)
    wf.close()
print(str(numIndex) + "张表操作完成！！！")
