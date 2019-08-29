
# coding=utf-8
## 用于星云贷、CRM。SHORTNAME_SCHEME_tablename
import os
import sys

Path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile

SHORTNAME = sys.argv[1].upper()
#system_nu = sys.argv[1].upper()
#system_nu = 'JFCORE'
conn = SqlUtile.mysqlLogin()
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道
AllSchemeResultData = SqlUtile.getALLSchemeData(cursor,SHORTNAME)

path = r"E:\mnt\JN_shell\Create_tables\AllAnalyze"

out_file_path = os.path.join(path, "%s.DataAnalyze.sql" % SHORTNAME)
if (os.path.exists(out_file_path)):
    os.remove(out_file_path)

def exec():
    numIndex = 0

    header = "--- 本文件: %s.DataAnalyze.sql \r CREATE DATABASE IF NOT EXISTS AllAnalyze COMMENT '全量.解析库';\r" % SHORTNAME
    header = header + "use AllAnalyze;\r\r"

    f = open(out_file_path, "a+", encoding='utf-8')
    f.write(header)
    f.close()
    for ta in AllSchemeResultData:

        if ta[1].upper() == 'F_CM_SPSRC_VIEW':
            continue
        numIndex += 1
        schemeKey = ta[0]
        tableName = ta[1].lower()
        DBNAME = ta[3].upper()
        SHORTNAME_DBNAME_tableName = SHORTNAME+"_"+DBNAME+"_"+tableName
        fieldResultData = SqlUtile.getTableFieldByKey(cursor,schemeKey)
        boday = "DROP TABLE IF EXISTS %s; \r create external table IF NOT EXISTS %s(\r" % (SHORTNAME_DBNAME_tableName, SHORTNAME_DBNAME_tableName)
        corporationStr = "`corporation` String comment '法人行号_主鍵',\r"

        fieldStr = ''
        for fd in fieldResultData:
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

        fieldStr = fieldStr.rstrip(',\r') + "\r)comment '%s汉语注解' row format delimited fields terminated by '\\u0003' \r" \
                                          "stored as textfile location '/DATACENTER/AllData/%s/%s/';\r\r" % (SHORTNAME_DBNAME_tableName,SHORTNAME,SHORTNAME_DBNAME_tableName)

        f = open(out_file_path, "a+", encoding='utf-8')
        f.write(boday+fieldStr)
        f.close()
    return str(numIndex)+"张表操作完成！！！"


if __name__ == '__main__':
    numIndex = exec()
    print('SUCCESS===' + str(numIndex))
