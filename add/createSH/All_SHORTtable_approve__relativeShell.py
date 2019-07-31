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

filePath = ''
out_file_path = ''
numIndex = 0
if SHORTNANE == 'CREDITTOWN':
    field1 = "select '615' as corporation ,"
else:
    field1 = "select '815' as corporation ,"
for td in AllSchemeResultData:
    tableLower = td[1].lower()
    SHORTtableName = SHORTNANE + '_'+tableLower
    tableUpper = td[1].upper()
    scheme_key = td[0]
    numIndex += 1
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
    sqoopQueryStr = field1 + fieldStr.rstrip(',')

    tableDate = ComparUtile.findTableDayId(tableUpper)
    if tableDate == '':
        allTempFilePath = r"E:\mnt\template\add\FullAddData.SHORTtable.sh"
        out_file_path = os.path.join(outPath, "FullAddData_%s.sh" % SHORTtableName)
       # sqoopQueryStr = sqoopQueryStr + "from ${source_Table} where \$CONDITIONS "
    else:
        allTempFilePath = r"E:\mnt\template\add\AddData.SHORTtable.sh"
        out_file_path = os.path.join(outPath, "AddData_%s.sh" % SHORTtableName)
       # sqoopQueryStr = sqoopQueryStr + "from ${source_Table} where ${Date_Dolumns} = '${cycle_id}' and \$CONDITIONS"
    if os.path.exists(out_file_path):
        os.remove(out_file_path)
    f = open(allTempFilePath, 'r', encoding='utf-8')
    lines = f.readlines()

    wf = open(out_file_path, 'w', encoding='utf-8')
    for li in lines:
        if tableDate != '':
            li = li.replace('DAY_ID',tableDate)
        rli = li.replace('SHORTNAME', SHORTNANE).replace('TABLENAME', tableUpper).replace('sqoopQueryStr', sqoopQueryStr).replace('SHORTtableName',SHORTtableName)
        wf.write(rli)
    wf.close()

print(str(numIndex) + "张表操作完成！！！")
cursor.close()
conn.close()
