#生成历史库建表语句 不区分 or_extract='是'or 否
#coding=utf-8

import pymysql
import os,sys
import coverField

system_nu = sys.argv[1].upper()
shortName = 'CREDIT'
conn = pymysql.connect(host='127.0.0.1',user='root',password='woshibangbangde',db='datams',charset='utf8',port=3306)
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

cursor.execute("SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_name ='CREDIT' AND or_extract = '是' " )
table_datas = cursor.fetchall()


path = r"E:\mnt\JN_shell\Create_tables\AllData"

out_file_path = os.path.join(path, "%s_AllData_SQL.sql" % system_nu)

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)

def exec():
    numIndex = 0
    f = open(out_file_path, "a+", encoding='utf-8')
    f.write("CREATE DATABASE IF NOT EXISTS CoreBankHist COMMENT '总行.历史库';\n use CoreBankHist;\r")
    f.close()
    for td in table_datas:
        numIndex += 1
        if td[1].upper() == 'F_CM_SPSRC_VIEW':
            continue
        tableName = shortName.upper() + '_' + td[1].lower()
        tableCommenStr = td[1]+td[2]

        cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag FROM table_field where scheme_key ='%s' order by cast(ord_number as SIGNED INTEGER)" % td[0])

        field_datas = cursor.fetchall()
        corporationStr = "`corporation` varchar(33) comment'法人主体.主键',\r"

        rowKeyStr = ''

        for fd in field_datas:
            if fd[5] == '是':
                rowKeyStr = rowKeyStr + fd[0] + ','
            if fd[0].upper() == 'CORPORATION':
                corporationStr = ''
        if corporationStr != '':
            rowKeyStr = '联合主键(CORPORATION,'+ rowKeyStr
        fieldStr = "`rowKeyStr` varchar(333) comment '" + rowKeyStr.rstrip(',') + ")',\r" \
                   "`DataDay_ID` varchar(33) COMMENT'数据的时间',\r" \
                   "`tdh_load_timestamp`  varchar(33)  COMMENT'加载到TDH时的时间戳',\r" + corporationStr

        for fd in field_datas:
            field_code = fd[0]
            field_type = fd[1]
            field_len = fd[2]
            field_accuracy = fd[3]
            key_flag = fd[5]
            field_comment = fd[4]
            tieldTypeStr = coverField.convert_fieldType(field_type, field_len, field_accuracy)
            if key_flag == '是':
                fieldStr = fieldStr + '`' + field_code + '` ' + tieldTypeStr + " comment '" + field_comment + '_主鍵'+"',\r"
            else:
                fieldStr = fieldStr + '`' + field_code + '` ' + tieldTypeStr + " comment '" + field_comment + "',\r"

        drop_table_str = 'DROP TABLE IF EXISTS ' + tableName + ';\r'
        fieldStr = fieldStr + "`Data_source_str` varchar(33) COMMENT'数据来源'"
        create_str = 'create table IF NOT EXISTS ' + tableName + ' (' \
                    '\r' + fieldStr + "\r" \
                    ")comment '"+ tableCommenStr + "' partitioned by(partition_month varchar(33))\r " \
                     "clustered by (rowKeyStr) into 13 buckets stored as orc TBLPROPERTIES ('transactional'='true');\r\r\r"

        f = open(out_file_path, "a+", encoding='utf-8')
        f.write(drop_table_str)
        f.write(create_str)

        f.close()
    return numIndex

if __name__ == '__main__':
    numIndex = exec()
    print('SUCCESS==='+str(numIndex))