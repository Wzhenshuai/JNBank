# -*- coding: utf-8 -*-
import xlrd
import os, sys




def read_file(fileDir,outDir):
    StringStr = ""
    f = open(fileDir, 'r', encoding='utf-8')
    lines = f.readlines()
    #for li in lines:
    #    enName = li.split(';')[0].upper()
    #    cnName = li.split(';')[1]
    #    StringStr = StringStr + "INSERT INTO table_scheme (`system_name`, `ch_name`, `en_name`, `or_extract`, `system_en_name`) " \
    #                            "VALUES ('ERP', '%s', '%s', '是', 'ERP_%s');\r" %(cnName,enName,enName)
    for li in lines:
        enName = li.split(';')[0].upper()
        enNames = 'ERP_%s'%enName
        StringStr = StringStr +"'"+enNames+"',"
    wf = open(outDir, 'w', encoding='utf-8')
    wf.write(StringStr)
    f.close()


if __name__ == '__main__':
    fileDir = r'E:\济宁银行\数据\erpTT.txt'
    outDir = r'E:\济宁银行\数据\erpTT_out_file.sql'
    read_file(fileDir,outDir)