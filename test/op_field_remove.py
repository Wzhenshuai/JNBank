import pymysql
import os,sys
from All3 import coverField

conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                       port=3306)
# 第二步：创建游标  对象
cursor = conn.cursor()  # cursor当前的程序到数据之间连接管道

system_name = sys.argv[1]

# 获取所有表
dictSql = "source"
cursor.execute(dictSql)