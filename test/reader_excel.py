# coding=utf-8

import os

import xlrd

in_execle_path = r"E:\济宁银行\第一轮梳理表结构\CRM\CRM系统.xlsx"

## 输出路径
out_execle_path = r"E:\济宁银行\第一轮梳理表结构\CRM\IN.sql"

if os.path.exists(out_execle_path):
    os.remove(out_execle_path)

workbook = xlrd.open_workbook(in_execle_path)


# 根据sheet索引或者名称获取sheet内容
sheet1 = workbook.sheet_by_index(3)  # sheet索引从2开始
wf = open(out_execle_path, 'w', encoding='utf-8')
i = 0;
for sheet_no in workbook.sheet_names():
    i += 1;
    if i < 4:
        continue
    sheetVal = workbook.sheet_by_name(sheet_no)
    for row_no in sheetVal._cell_values:

        cloCode = repr(row_no[0])
        cloType = repr(row_no[1])
        cloLength = repr(row_no[2])
        cloScle = repr(row_no[3])
        cloNo = repr(row_no[4])
        sch_key = "CRM_"+ sheet_no

        ss = "INSERT INTO table_field(ord_number, field_code, field_type, field_len, field_accuracy, system_en_name)" \
        "VALUES('%s','%s','%s','%s','%s','%s');\r" %(cloNo,cloCode,cloType,cloLength,cloScle,sch_key)

        wf.write(ss)
wf.close()
