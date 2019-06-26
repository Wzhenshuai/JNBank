#coding=utf-8

import pymysql
import os, sys
from allHiveSQL import coverField

system_nu = sys.argv[1]
conn = pymysql.connect(host='127.0.0.1',user='root',password='woshibangbangde',db='datams',charset='utf8',port=3306)
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

#cursor.execute("SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_name ='credit' AND or_extract='是'")
cursor.execute("SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_name ='CORE' and  substring_index(system_en_name,'_',2)='%s' AND or_extract = '是'" % system_nu)
table_datas = cursor.fetchall()


path = r"E:\mnt\JN_shell\Create_tables\AllData"

out_file_path = os.path.join(path, "CORE_F_all_SQL.sql")

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)

for td in table_datas:
    if td[1].upper() == 'F_CM_SPSRC_VIEW':
        continue
    tableName = system_nu.upper() + '_' + td[1].lower()

    tableCommenStr = td[1]+td[2]
    #if tableName =='BUSINESS_UNDERWRINTING':
    #    print(tableName)
    cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag FROM table_field "
                   "where scheme_key = '%s' ORDER BY cast(ord_number as SIGNED INTEGER)"% td[0])

    field_datas = cursor.fetchall()

    rowKeyStrC = '联合主键('
    for fd in field_datas:
        if fd[5] == '是':
            rowKeyStrC = rowKeyStrC + fd[0] + ','

    fieldStr = "`rowKeyStr` varchar(333) comment '" + rowKeyStrC.rstrip(',') + ")',\r"


    for fd in field_datas:
        pr_key = ''
        field_code = fd[0]
        field_type = fd[1]
        field_len = fd[2]
        field_accuracy = fd[3]
        field_comment = fd[4]
        key_flag = fd[5]
        if field_type == 'clob':
            print('xxxo')
        if key_flag == '是':
            rowKeyStrC = rowKeyStrC + fd[0] + ','
            pr_key = pr_key + field_code + ','
        tieldTypeStr = coverField.convert_fieldType(field_type, field_len, field_accuracy)
        #if field_type == 'NUMBER':
        #    filed_type = 'decimal'
        if field_accuracy == "" or field_accuracy == " ":
            if key_flag == '是':
                fieldStr = fieldStr + '`' + fd[0] + '` ' + tieldTypeStr + " comment '" + fd[4] + '_主鍵'+"',\r"
            else:
                fieldStr = fieldStr + '`' + fd[0] + '` ' + tieldTypeStr + " comment '" + fd[4] + "',\r"
        else:
            if key_flag == '是':
                fieldStr = fieldStr + '`' + fd[0] + '` ' + tieldTypeStr + " comment '" + fd[4] + '_主鍵' + "',\r"
            else:
                fieldStr = fieldStr + '`' + fd[0] + '` ' + tieldTypeStr + " comment '" + fd[4] + "',\r"

    drop_table_str = 'DROP TABLE IF EXISTS ' + tableName + ';\r'
    create_str = drop_table_str + 'create table IF NOT EXISTS ' + tableName + ' (\r' + fieldStr.rstrip(',\r') + "\r)comment '"+ tableCommenStr + "'\r" \
                          "clustered by (rowKeyStr) into 13 buckets stored as orc TBLPROPERTIES ('transactional'='true');\r\r\r"

    f = open(out_file_path, "a+", encoding='utf-8')
    f.write(create_str)
    f.close()