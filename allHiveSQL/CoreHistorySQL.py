
#coding=utf-8

import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile, CoverField

SHORTNAME = sys.argv[1].upper()

conn = SqlUtile.mysqlLogin()
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

allSchemeResultData = SqlUtile.getALLSchemeData(cursor,SHORTNAME)

#dicResultData = SqlUtile.getPDDicInfo(cursor,SHORTNAME)
path = 'E:\mnt\JN_shell\Create_tables\AllData'

out_file_path = os.path.join(path, "%s_History_SQL.sql" % SHORTNAME)

if (os.path.exists(out_file_path)):
    os.remove(out_file_path)

def exec():
    numIndex = 0
    for td in allSchemeResultData:
        numIndex += 1
        if td[1].upper() == 'F_CM_SPSRC_VIEW':
            continue
        tableName = SHORTNAME + '_' + td[1].lower()
        schemeKey = td[0]
        tableCommenStr = td[1]+str(td[2])
        fieldResultData = SqlUtile.getTableFieldByKey(cursor,schemeKey)
        corporationStr = "`corporation` varchar(33) comment'法人主体.主键',\r"
        rowKeyStr = ''
        for fd in fieldResultData:
            if fd[5] == '是':
                rowKeyStr = rowKeyStr + fd[0] + ','
            if fd[0].upper() == 'CORPORATION':
                corporationStr = ''
        if corporationStr != '':
            rowKeyStr = '联合主键(CORPORATION,'+ rowKeyStr
        fieldStr = "`rowKeyStr` varchar(333) comment '" + rowKeyStr.rstrip(',') + ")',\r" \
                   "`DataDay_ID` varchar(33) COMMENT'数据的时间',\r" \
                   "`tdh_load_timestamp`  varchar(33)  COMMENT'加载到TDH时的时间戳',\r" + corporationStr

        for fd in fieldResultData:
            field_code = fd[0]
            field_type = fd[1]
            field_len = fd[2]
            field_accuracy = fd[3]
            key_flag = fd[5]
            field_comment = fd[4]
            tieldTypeStr = CoverField.convert_fieldTypeAll(field_type, field_len, field_accuracy)
            if key_flag == '是':
                fieldStr = fieldStr + '`' + field_code + '` ' + tieldTypeStr + " comment '" + field_comment + '_主鍵'+"',\r"
            else:
                fieldStr = fieldStr + '`' + field_code + '` ' + tieldTypeStr + " comment '" + field_comment + "',\r"

        drop_table_str = 'DROP TABLE IF EXISTS ' + tableName + ';\r'
        fieldStr = fieldStr + "`Data_source_str` varchar(33) COMMENT'数据来源'"
        create_str = 'create table IF NOT EXISTS ' + tableName + ' (' \
                    '\r' + fieldStr + "\r" \
                    ")comment '"+ tableCommenStr + "' partitioned by(partition_year varchar(33))\r " \
                     "clustered by (rowKeyStr) into 6 buckets stored as orc TBLPROPERTIES ('transactional'='true');\r\r\r"

        f = open(out_file_path, "a+", encoding='utf-8')

        f.write(drop_table_str)
        f.write(create_str)

        f.close()
    return numIndex

if __name__ == '__main__':
    numIndex = exec()
    print('SUCCESS==='+str(numIndex))