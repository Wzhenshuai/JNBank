# -*- coding: utf-8 -*-
import xlrd
import os, sys




def read_file(fileDir,outDir):

    StringStr = "HHHHHHHHHHHHHHHNNNNNNNNNNNNN \n nnnnnnnnnnnnn \n\n"
    StringStr2 = "RRRRRRRRRRRRRRRRRRRRRRRRRRR \r\r XXXXXXXXXXXXXX \r\r"
    wf = open(outDir, 'w', encoding='utf-8')
    wf.write(StringStr)
    wf.write(StringStr2)


if __name__ == '__main__':
    fileDir = r'E:\济宁银行\数据\erpTT.txt'
    outDir = r'E:\济宁银行\数据\erpTT_out_file.sql'
    read_file(fileDir,outDir)