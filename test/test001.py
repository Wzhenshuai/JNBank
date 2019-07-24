#coding=utf-8

import pymysql
import os, sys
import coverField

system_nu = sys.argv[1]
conn = pymysql.connect(host='127.0.0.1',user='root',password='woshibangbangde',db='datams',charset='utf8',port=3306)
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

#cursor.execute("SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_name ='credit' AND or_extract='是'")
cursor.execute("SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_name ='%s'" % system_nu)
table_datas = cursor.fetchall()


path = r"E:\济宁银行\第一轮梳理表结构\%s" % system_nu

out_file_path = os.path.join(path, "%s_hive_SQL.sql" % system_nu)

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)

cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag FROM table_field where scheme_key like '"+ system_nu +"_%'" )

field_datas = cursor.fetchall()
corporationStr = "`corporation` varchar(33) comment'法人主体.主键',\r"

def exec():
    numIndex = 0
    for td in table_datas:
        numIndex += 1
        if td[1].upper() == 'F_CM_SPSRC_VIEW':
            continue
        tableName = system_nu.upper() + '_' + td[1].lower()
        tableCommenStr = td[1]+td[2]



        rowKeyStrC = '联合主键(corporation,'
        for fd in field_datas:
            if fd[5] == '是':
                rowKeyStrC = rowKeyStrC + fd[0] + ','
            if fd[0] == 'CORPORATION':
                corporationStr = ''
        fieldStr = "`rowKeyStr` varchar(333) comment '" + rowKeyStrC.rstrip(',') + ")',\r" \
                   "`DataDay_ID` varchar(33) COMMENT'数据的时间',\r" \
                   "`tdh_load_timestamp`  varchar(33)  COMMENT'加载到TDH时的时间戳',\r" + corporationStr


        for fd in field_datas:
            pr_key = ''
            field_code = fd[0]
            field_type = fd[1]
            field_len = fd[2]
            field_accuracy = fd[3]
            field_comment = fd[4]
            key_flag = fd[5]
            if key_flag == '是':
                rowKeyStrC = rowKeyStrC + fd[0] + ','
                pr_key = pr_key + field_code + ','
            tieldTypeStr = convert_fieldTypes(field_type, field_len, field_accuracy)
            #tieldTypeStr = convert_fieldTypes(field_type, field_len, field_accuracy)
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
        fieldStr += "`Data_source_str` varchar(33) COMMENT'数据来源'"
        create_str = drop_table_str + 'create table IF NOT EXISTS ' + tableName + ' (\r' + fieldStr.rstrip(',\r') + "\r)comment '"+ tableCommenStr + "' partitioned by(partition_month varchar(33))\r " \
                              "clustered by (rowKeyStr) into 13 buckets stored as orc TBLPROPERTIES ('transactional'='true');\r\r\r"

        f = open(out_file_path, "a+", encoding='utf-8')
        f.write(create_str)
        f.close()
    return numIndex


def convert_fieldTypes(fieldType,fieldLen,fieldAccuracy):
    fieldType = fieldType.lower()
    if fieldType in ('char', 'nchar', 'varchar', 'nvarchar', 'graphic', 'varbraphic', 'character','varchar2','nvarchar2','xmltype','long varchar'):
        field_type = 'varchar(' + fieldLen + ')'
    elif fieldType in ('time', 'date', 'timestamp','timestamp(6)'):
        field_type = 'varchar(30)'
    elif fieldType in ('smallint', 'integer', 'bigint', 'int', 'tinyint'):
        field_type = 'bigint'
    elif fieldType == "boolean":
        field_type = 'boolean'
    elif fieldType in ('clob', 'blob'):
        field_type = fieldType
    elif fieldType == 'nclob':
        field_type = 'clob'
    elif fieldType in ('long raw','longraw','raw'):
        field_type = 'blob'
    elif fieldType == 'long':
        field_type = 'varchar(1000)'
    elif fieldType in ('numeric', 'decimal', 'decfloat', 'real', 'float', 'double','number'):
        if fieldAccuracy == "" or fieldAccuracy == " ":
            field_type = 'decimal(' + fieldLen + ')'
        else:
            field_type = 'decimal(' + fieldLen + ',' + fieldAccuracy + ')'
    else:
        field_type = fieldType + '(' + fieldLen + ')'
    return field_type;

if __name__ == '__main__':
    numIndex = exec()
    print('SUCCESS==='+str(numIndex))