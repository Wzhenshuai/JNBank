# coding=utf-8

import os

ttm = 'XD_BAIDUXD_LOAN_RATE'
# 模版路径
filePath = r"E:\tmp\0731\%s.sql"%ttm

## 输出路径
out_file_path = r"\\USER-20190220BH\mnt\%s.sql"%ttm


f = open(filePath, 'r', encoding='utf-8')
lines = f.readlines()

if os.path.exists(out_file_path):
    os.remove(out_file_path)

wf = open(out_file_path, 'w', encoding='utf-8')
for li in lines:
    dd = li.split(',')
    print(dd[0],dd[1],dd[2],dd[3],dd[4],dd[5],dd[6])
    cloName = dd[0].strip()
    cloNo = int(dd[1].strip())+1
    cloType = dd[2].strip()
    cloLength = dd[3].strip()
    cloScle = dd[4].strip()
    cloKey = dd[5].strip()
    cloRmark = dd[6].strip()
    sch_key = 'CORE_%s'%ttm
    if cloKey != '' and cloKey != '0' and cloKey != 'null':
        cloKey = 'XX'
    if cloScle == '0':
        cloScle = ''
    ss = "INSERT INTO table_field(ord_number, field_code, field_type, field_len, field_accuracy, key_flag, scheme_key)" \
    "VALUES('%s','%s','%s','%s','%s','%s','%s');\r" %(cloNo,cloName,cloType,cloLength,cloScle,cloKey,sch_key)
    wf.write(ss)
wf.close()
