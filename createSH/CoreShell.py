# coding=utf-8

import pymysql
import os, sys

## 用于除（数整、信贷村镇、ERPCW）的数据生成
systemUpper = sys.argv[1].upper()
# systemUpper = 'credit'.upper()
conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

cursor.execute(
    "SELECT en_name,system_en_name FROM table_scheme WHERE system_name = '%s' AND or_extract = '是'" %systemUpper)
table_datas = cursor.fetchall()

dictSql = "SELECT shell_path,system_code FROM dic_info_mapping WHERE transfer_mode ='全量铺底数据' AND system_code='" + systemUpper + "'"
cursor.execute(dictSql)
path_data = cursor.fetchall()

if systemUpper == 'CREDIT':
    outPath = path_data[0][0].split('|')[0]
else:
    outPath = path_data[0][0]
# 模版路径

allTempFilePath = r"E:\mnt\template\CREDIT.shell\Core_agentShell\AllData.Core.SHORTNAME_tablename.sh"

filePath = ''
out_file_path = ''
numIndex = 0
for td in table_datas:
    field1 = "select '815' as corporation ,"
    tableLower = td[0].lower()
    tableUpper = td[0].upper()
    if tableUpper == 'ACCT_FLOW':
        print(tableLower)
    scheme_key = td[1]
    numIndex += 1
    cursor.execute("SELECT  field_code,field_type FROM table_field WHERE scheme_key = '%s' ORDER BY cast(ord_number as SIGNED INTEGER)" % scheme_key)
    field_datas = cursor.fetchall()
    fieldStr = ''
    for fd in field_datas:
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
    fieldStr = field1 + fieldStr.rstrip(',')+' from ${source_Table} where \$CONDITIONS'
    systemTable = systemUpper + '_' + tableLower

    out_file_path = os.path.join(outPath, "AllData.Core.%s.sh" % systemTable)

    if os.path.exists(out_file_path):
        os.remove(out_file_path)

    f = open(allTempFilePath, 'r', encoding='utf-8')
    lines = f.readlines()

    wf = open(out_file_path, 'w', encoding='utf-8')
    for li in lines:
        rli = li.replace('SYSTEMSHORT', systemUpper).replace('TABLENAME', tableUpper).replace('sqoopQueryStr', fieldStr).replace('SHORT_tableName', systemTable)
        wf.write(rli)
    wf.close()
print(str(numIndex) + "张表操作完成！！！")
