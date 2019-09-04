# coding=utf-8

import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile
## 用于除（数整、信贷村镇、ERPCW）的数据生成
SHORTNAME = sys.argv[1].upper()
conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

allSchemeResultData = SqlUtile.getALLSchemeData(cursor,SHORTNAME)

dicResultData = SqlUtile.getPDDicInfo(cursor,SHORTNAME)

outPath = dicResultData[0][2]
# 模版路径

allTempFilePath = r"E:\mnt\template\CREDIT.shell\Core_agentShell\AllData.Core.SHORTNAME_tablename.sh"

filePath = ''
out_file_path = ''
numIndex = 0
if SHORTNAME == 'CREDITTOWN':
    field1 = "select '615' as corporation ,"
else:
    field1 = "select '815' as corporation ,"
for td in allSchemeResultData:

    tableLower = td[1].lower()
    tableUpper = td[1].upper()
    scheme_key = td[0]
    numIndex += 1
    fieldResultData = SqlUtile.getTableFieldByKey(cursor,scheme_key)
    fieldStr = ''
    for fd in fieldResultData:
        ty = fd[0].upper()
        if ty == 'INDEX':
            ty = '\\"INDEX\\"'
        if tableUpper in ('CHAR','CHARACTER','NVARCHAR2','VARCHAR','VARCHAR2'):
            fieldStr = fieldStr + "trim("+ty+"),"
        else:
            fieldStr = fieldStr + ty + ','
        if ty == 'CORPORATION':
            field1 = "select "
    fieldStr = field1 + fieldStr.rstrip(',')+' from ${source_Table} where \$CONDITIONS'
    systemTable = SHORTNAME + '_' + tableLower

    out_file_path = os.path.join(outPath, "AllData.%s.sh" % systemTable)

    if os.path.exists(out_file_path):
        os.remove(out_file_path)

    f = open(allTempFilePath, 'r', encoding='utf-8')
    lines = f.readlines()

    wf = open(out_file_path, 'w', encoding='utf-8')
    for li in lines:
        rli = li.replace('SYSTEMSHORT', SHORTNAME).replace('TABLENAME', tableUpper).replace('sqoopQueryStr', fieldStr).replace('SHORT_tableName', systemTable)
        wf.write(rli)
    wf.close()
print(str(numIndex) + "张表操作完成！！！")
