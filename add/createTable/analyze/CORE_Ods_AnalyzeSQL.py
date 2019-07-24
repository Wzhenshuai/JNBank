
# coding=utf-8
### 增量生成数整 ODS 解析sql
import os
import sys

import pymysql

system_st = sys.argv[1].upper()
system_nu = 'CORE'
system_en = 'CORE_'+system_st
conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

cursor.execute(
    "SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是' and  substring_index(system_en_name,'_',2)='%s'" %(system_nu,system_en))

table_datas = cursor.fetchall()

path = r"E:\mnt\JN_shell\Create_tables\AddAnalyze"

out_file_path = os.path.join(path, "CORE.Core.%s.Analyze.sql" % system_st)

if os.path.exists(out_file_path):
    os.remove(out_file_path)

def exec():
    numIndex = 0

    for td in table_datas:
        numIndex += 1
        tableName = system_nu + '_' + td[1].lower()

        cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag FROM table_field where scheme_key ='%s' order by cast(ord_number as SIGNED INTEGER)" % td[0])

        field_datas = cursor.fetchall()
        boday = "DROP TABLE IF EXISTS Core_%s; \r create external table IF NOT EXISTS Core_%s(\r" %(tableName,tableName)
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

        fieldStr = fieldStr.rstrip(',\r') + "\r)comment 'Core_%s汉语注解' row format delimited fields terminated by '\\u0003'  lines terminated by '\\u0005' \r" \
                                          "stored as textfile location '/DATACENTER/AddData/%s/CoreBank/%s/';\r\r" % (tableName,system_nu,tableName)

        f = open(out_file_path, "a+", encoding='utf-8')
        f.write(boday+fieldStr)
        f.close()
    return str(numIndex)+"张表操作完成！！！"


if __name__ == '__main__':
    numIndex = exec()
    print('SUCCESS===' + str(numIndex))
