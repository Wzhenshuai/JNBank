#coding=utf-8

import pymysql
import os, sys
import cx_Oracle
import coverField

conn = cx_Oracle.connect('reader/reader@172.16.100.196/jnbank')    #连接数据库
c = conn.cursor()                                           #获取cursor
x = c.execute('select sysdate from dual')                   #使用cursor进行各种操作
x.fetchone()
c.close()                                                 #关闭cursor
conn.close()
