import pymysql


def mysqlLogin():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                           port=3306)
    return conn
