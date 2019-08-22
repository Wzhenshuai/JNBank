import pymysql

conn = pymysql.connect(host='172.16.100.182', user='tobuser', password='ts@123', db='', charset='utf8',port=3306)

cursor = conn.cursor()

selectTableSql = "select TABLE_SCHEMA,table_name,COLUMN_NAME,column_type,ordinal_position,column_key,column_comment from " \
                 "columns where table_schema = 'common' AND table_name ='bank'"
cursor.execute('use information_schema')
cursor.execute(selectTableSql)
AllSchemeResultData = cursor.fetchall()
print(AllSchemeResultData)

for td in AllSchemeResultData:
    tab1 = td[2] + " "
    tab2 = td[3] + " "
    tab3 = td[4]
    tab4 =  " " + td[5]

    print(tab1,tab2,tab3,tab4)