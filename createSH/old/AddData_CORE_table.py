# coding=utf-8

import pymysql
import os, sys

conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

cursor.execute(
    "SELECT system_en_name,en_name,ch_name,provideDate_way FROM table_scheme WHERE system_name ='credit' AND or_extract='是'")
table_datas = cursor.fetchall()
# systemUpper = sys.argv[1].upper()
systemUpper = 'CORE'

# 模版路径
fullTempfilePath = r"E:\济宁银行\0.数据迁移样例V3\调度脚本.取数上传\增量.调度脚本.数整平台\AddFullData.CORE_table.sh"
addTempFilePath = r"E:\济宁银行\0.数据迁移样例V3\调度脚本.取数上传\增量.调度脚本.数整平台\AddData.CORE_table.sh"

## 输出路径
outPath = r"E:\济宁银行\0.数据迁移样例V3\Add_shell\%s\agentShell\/" % systemUpper

filePath = ''
out_file_path = ''
for td in table_datas:
    tableLower = td[1].lower()
    tableUpper = td[1].upper()
    systemTable = systemUpper + '_' + tableLower
    if td[3] == '增量':
        filePath = addTempFilePath
        out_file_path = os.path.join(outPath, "AddData.%s.sh" % systemTable)
    else:
        filePath = fullTempfilePath
        out_file_path = os.path.join(outPath, "AddFullData.%s.sh" % systemTable)
    if os.path.exists(out_file_path):
        os.remove(out_file_path)

    f = open(filePath, 'r', encoding='utf-8')
    lines = f.readlines()

    if os.path.exists(out_file_path):
        os.remove(out_file_path)

    wf = open(out_file_path, 'w', encoding='utf-8')
    for li in lines:
        rli = li.replace('CORE', systemUpper).replace('tabled', tableLower).replace('TABLED', tableUpper)
        wf.write(rli)
    wf.close()
