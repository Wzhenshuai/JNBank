# coding=utf-8

import pymysql
import os, sys

systemUpper = sys.argv[1].upper()
# systemUpper = 'credit'.upper()
conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

cursor.execute(
    "SELECT en_name,system_en_name FROM table_scheme WHERE system_name = '%s' AND or_extract = '是' and core_town = '1'" %systemUpper)
table_datas = cursor.fetchall()

dictSql = "SELECT shell_path,system_code FROM dic_info_mapping WHERE transfer_mode ='全量铺底数据' AND system_code='" + systemUpper + "'"
cursor.execute(dictSql)
path_data = cursor.fetchall()

outPath = path_data[0][0].split('|')[1]

# 模版路径
allTempFilePath = r"E:\mnt\template\CREDIT.shell\Town_agentShell\AllData.Town.SHORTNAME_tablename.sh"

## 输出路径
#outPath = r"E:\济宁银行\pudi\All_shell\CREDIT\Town_agentShell/"

filePath = ''
out_file_path = ''
field1 = "select '615' as corporation ,"
numIndex = 0
for td in table_datas:
    numIndex += 1
    tableLower = td[0].lower()
    tableUpper = td[0].upper()
    if tableLower in ('rate_info_his','location_info','lawcase_urge','lawcase_time','lawcase_process',
                      'lawcase_loanback','eds_rwal_entindlist_t','crq_apply','lawcase_guaranty',
                      'lawcase_fee','gc_app_query_history','evaluate_record','maintain_info',
                      'lawsuit_apply','lawcase_info','business_contract','asset_info','business_approve'):
        continue
    scheme_key = td[1]
    cursor.execute("SELECT  field_code,field_type FROM table_field WHERE scheme_key = '%s'and core_town = '1' ORDER BY cast(ord_number as SIGNED INTEGER)" % scheme_key)
    field_datas = cursor.fetchall()
    fieldStr = ''
    for fd in field_datas:
        if fd[1].upper() in ('CHAR', 'NCHAR', 'VARCHAR', 'NVARCHAR', 'GRAPHIC', 'VARBRAPHIC', 'CHARACTER',
                             'VARCHAR2','NVARCHAR2','XMLTYPE','LONG VARCHAR','LANG'):
            fieldStr = fieldStr + "trim("+fd[0]+"),"
        else:
            fieldStr = fieldStr + fd[0] + ','
        if fd[0].upper() == 'CORPORATION':
            field1 = "select "

    fieldStr = field1 + fieldStr.rstrip(',')+' from ${source_Table} where \$CONDITIONS'
    systemTable = systemUpper + '_' + tableLower

    out_file_path = os.path.join(outPath, "AllData.Town.%s.sh" % systemTable)

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
