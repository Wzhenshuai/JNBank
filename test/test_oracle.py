import cx_Oracle

conn = cx_Oracle.connect('cmstmp20190501' , 'cmstmp20190501' , '10.188.16.25:1521/jnbank')
cur = conn.cursor()
sql = "select * form tablel"
cur.execute(sql)
rs = cur.fetchall()
