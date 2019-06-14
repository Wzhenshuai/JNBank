#生成村镇 解析sql
# coding=utf-8

import pymysql
import os
import coverField

# system_nu = sys.argv[1].upper()
system_nu = 'CREDIT'
conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

cursor.execute(
    "SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是'" % system_nu)

table_datas = cursor.fetchall()

path = r"E:\mnt\JN_shell\Create_tables\DataAnalyze\%s" % system_nu

out_file_path = os.path.join(path, "%s.Town.DataAnalyze.sql" % system_nu)

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)

def exec():
    header = "--- 本文件: AllDataAnalyze.Town.%s.sql \r CREATE DATABASE IF NOT EXISTS AllAnalyze COMMENT '全量.解析库';\r" % system_nu
    header = header + "use AllAnalyze;\r\r"

    f = open(out_file_path, "a+", encoding='utf-8')
    f.write(header)
    f.close()
    numIndex = 0
    for td in table_datas:
        numIndex += 1
        if td[1].upper() == 'F_CM_SPSRC_VIEW':
            continue
        tableName = system_nu + '_' + td[1].lower()

        cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag FROM table_field where scheme_key ='%s'" % td[0])

        field_datas = cursor.fetchall()
        boday = "create external table IF NOT EXISTS Town_%s(\r" % tableName
        corporationStr = "`corporation` String comment '法人行号_主鍵',\r"

        fieldStr = ''
        for fd in field_datas:
            field_code = fd[0]
            key_flag = fd[5]
            field_comment = fd[4]
            if key_flag == '是':
                fieldStr = fieldStr + '`' + field_code + '` ' + " string comment '" + field_comment + '_主鍵' + "',\r"
            else:
                fieldStr = fieldStr + '`' + field_code + '` ' + " string comment '" + field_comment + "',\r"
            if fd[0].upper() == 'CORPORATION':
                corporationStr = ''
        if corporationStr != '':
            fieldStr = corporationStr + fieldStr

        fieldStr = fieldStr.rstrip(',\r') + "\r)comment 'Town_%s汉语注解' row format delimited fields terminated by '\\u0003' \r" \
                                          "stored as textfile location '/DATACENTER/AllData/%s/TownBank/%s/';\r\r" % (tableName,system_nu,tableName)

        f = open(out_file_path, "a+", encoding='utf-8')
        f.write(boday+fieldStr)
        f.close()
    return numIndex


if __name__ == '__main__':
    numIndex = exec()
    print('SUCCESS===' + str(numIndex))
