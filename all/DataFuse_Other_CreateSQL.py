#coding=utf-8
### 用于数整 数据融合库，other层建表
import os
import sys

Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(Path)
from common import SqlUtile,ConstantUtile,CoverField

SHORTNAME = sys.argv[1].upper()
conn = SqlUtile.mysqlLogin()
#第二步：创建游标  对象
cursor = conn.cursor()   #cursor当前的程序到数据之间连接管道

custmerStr = ConstantUtile.custmerStr
selectTableSql = "SELECT system_en_name,en_name,ch_name FROM table_scheme WHERE system_en_name in ("+custmerStr+")"
cursor.execute(selectTableSql)
table_datas = cursor.fetchall()

path = r"E:\mnt\JN_shell\Create_tables\AllData"
out_file_path = os.path.join(path, "%s_DateFuseSQL.sql" % SHORTNAME)
if (os.path.exists(out_file_path)):
    os.remove(out_file_path)
numIndex = 0
for td in table_datas:
    numIndex = numIndex + 1
    SHORT_tableName = SHORTNAME + '_' + td[1].lower()
    tableCommenStr = td[2]
    cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag "
                    "FROM table_field where scheme_key ='%s' ORDER BY cast(ord_number as SIGNED INTEGER)"%(td[0]))
    field_datas = cursor.fetchall()
    corporationStr = "`corporation` varchar(33) comment'法人主体.主键',\r"

    rowKeyStrC = ''
    rowKeyStr = ''
    for fd in field_datas:
        if fd[5] == '是':
            rowKeyStr = rowKeyStr + fd[0] + ','
        if fd[0] == 'CORPORATION':
            corporationStr = ''
    if corporationStr != '':
        rowKeyStrC = '联合主键(corporation,' + rowKeyStr
    else:
        rowKeyStrC = '联合主键(' + rowKeyStr
    fieldStr = "`rowKeyStr` varchar(333) comment '主键(uniq()函数取的唯一)',\r"

    for fd in field_datas:
        field_code = fd[0]
        field_type = fd[1]
        field_len = fd[2]
        field_accuracy = fd[3]
        field_comment = fd[4]
        key_flag = fd[5]
        tieldTypeStr = CoverField.convert_fieldTypeAll(field_type, field_len, field_accuracy)

        if key_flag == '是':
            fieldStr = fieldStr + '`' + fd[0] + '` ' + tieldTypeStr + " comment '" + fd[4] + '_主鍵'+"',\r"
        else:
            fieldStr = fieldStr + '`' + fd[0] + '` ' + tieldTypeStr + " comment '" + fd[4] + "',\r"

    drop_table_str = 'DROP TABLE IF EXISTS ' + SHORT_tableName + ';\r'
    create_str = drop_table_str + 'create table IF NOT EXISTS ' + SHORT_tableName + ' (\r' + fieldStr.rstrip(',\r') + "\r)comment '"+ tableCommenStr + "'\r " \
                          "clustered by (rowKeyStr) into 6 buckets stored as orc TBLPROPERTIES ('transactional'='true');\r\r\r"

    f = open(out_file_path, "a+", encoding='utf-8')
    f.write(create_str)
    f.close()
print(str(numIndex) + "张表操作完成！！！")