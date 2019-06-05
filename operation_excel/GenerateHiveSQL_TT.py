#coding=utf-8

import pymysql
import os, sys
import coverField

conn = pymysql.connect(host='127.0.0.1',user='root',password='woshibangbangde',db='datams',charset='utf8',port=3306)
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

cursor.execute("SELECT * FROM table_field WHERE scheme_key LIKE '%,%'")
field_datas = cursor.fetchall()

cursor.execute("SELECT * FROM table_scheme WHERE  system_en_name LIKE '%,%'")
table_datas = cursor.fetchall()
ty = 1

path = r"E:\济宁银行\\"
out_file_path = os.path.join(path, "xxxFF.sql")

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)

if ty == 1 :
    for td in field_datas:
        print(td)
        #codeStr = td[3]
        schemeKey = td[12]
        upStr = ''
        if schemeKey.find(',') > -1:
            #codeStr = codeStr.replace(',','')
            #upStr = "\r update table_field set field_code = '"+codeStr+"' where field_id ="+ str(td[0])+";"
            schemeKey = schemeKey.replace(',', '')
            upStr = "\r update table_field set scheme_key = '" + schemeKey + "' where field_id =" + str(td[0]) + ";"
            f = open(out_file_path, "a+", encoding='utf-8')
            f.write(upStr)
            f.close()
if ty == 0:
    for td in table_datas:
        enStr = td[3]
        upStr = ''
        if enStr.find(',') > -1:
            enStr = enStr.replace(',', '')
            upStr = "\r update table_scheme set en_name = '" + enStr + "',system_en_name = '" + td[
                10] + "' where scheme_id =" + str(td[0]) + ";"
            f = open(out_file_path, "a+", encoding='utf-8')
            f.write(upStr)
            f.close()
