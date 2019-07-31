# coding=utf-8

### 仅用于 数整 ODS 生成shell的抽取

import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile, ComparUtile

SHORTNANE = sys.argv[1].upper()

conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

dicResultData = SqlUtile.getDicInfo(cursor, SHORTNANE)
AllSchemeResultData = SqlUtile.getALLSchemeData(cursor, SHORTNANE)
outPath = dicResultData[0][2]
# 模版路径

allTempFilePath = r"E:\mnt\template\add\AddData.SHORTtable_approve_relative.sh"

filePath = ''
out_file_path = ''
numIndex = 0
for td in AllSchemeResultData:
    field1 = "select '800' as corporation ,"
    tableLower = td[0].lower()
    SHORTtableName = SHORTNANE + '_'+tableLower
    tableUpper = td[0].upper()
    scheme_key = td[1]
    numIndex += 1
    tableDate = ComparUtile.findTableDayId(tableUpper)
    if tableDate == '':
        tableDate = 'DAY_ID'
    fieldResultData = SqlUtile.getTableFieldByKey(cursor, scheme_key)
    fieldStr = ''
    for fd in fieldResultData:
        fileCode = fd[0].upper()
        fileType = fd[1].upper()
        if fileCode == 'INDEX':
            fileCode = '"INDEX"'
        if fileType in ('CHAR', 'NCHAR', 'VARCHAR', 'NVARCHAR', 'GRAPHIC', 'VARBRAPHIC', 'CHARACTER','VARCHAR2',
                             'NVARCHAR2','LANG','EVALUATE_RECORD','MAINTAIN_INFO','MAINTAIN_INFO','LAWSUIT_APPLY',
                             'LAWCASE_INFO','BUSINESS_CONTRACT','ASSET_INFO','BUSINESS_APPROVE'):
            fieldStr = fieldStr + "trim("+fileCode+"),"
        else:
            fieldStr = fieldStr + fileCode + ','
        if fileCode == 'CORPORATION':
            field1 = "select "
    fieldStr = field1 + fieldStr.rstrip(',')

    out_file_path = os.path.join(outPath, "AllData_%s_approve_relative.sh" % SHORTtableName)

    if os.path.exists(out_file_path):
        os.remove(out_file_path)

    f = open(allTempFilePath, 'r', encoding='utf-8')
    lines = f.readlines()

    wf = open(out_file_path, 'w', encoding='utf-8')
    for li in lines:
        rli = li.replace('SHORTNAME', SHORTNANE).replace('TABLENAME', tableUpper).replace('sqoopQueryStr', fieldStr)
        wf.write(rli)
    wf.close()

print(str(numIndex) + "张表操作完成！！！")
cursor.close()
conn.close()
