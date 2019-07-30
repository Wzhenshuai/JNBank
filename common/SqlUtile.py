import pymysql


def mysqlLogin():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='woshibangbangde', db='datams', charset='utf8',
                           port=3306)
    return conn
## 获取sql 路径
def getDicInfo(cursor,SHORTNANE):
    dictSql = "SELECT sql_path,system_code,shell_path FROM dic_info_mapping WHERE transfer_mode ='增量数据' AND system_code='%s'" % SHORTNANE
    cursor.execute(dictSql)
    dicResultData = cursor.fetchall()
    return dicResultData
## 获取非增量数据
def getQLData(cursor,SHORTNANE):
    selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_name ='%s' and provideDate_way != '增量' AND or_extract='是'" % SHORTNANE
    cursor.execute(selectTableSql)
    QLResultData = cursor.fetchall()
    return QLResultData

## 获取增量数据
def getZLData(cursor,SHORTNANE):
    selectTableSql = "SELECT system_en_name,en_name FROM table_scheme WHERE system_name ='%s' and provideDate_way = '增量' AND or_extract='是'" % SHORTNANE
    cursor.execute(selectTableSql)
    ZLResultData = cursor.fetchall()
    return ZLResultData

## 获取所有表数据
def getALLSchemeData(cursor,SHORTNANE):
    selectTableSql = "SELECT system_en_name,en_name ,ch_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是'" % SHORTNANE
    cursor.execute(selectTableSql)
    AllSchemeResultData = cursor.fetchall()
    return AllSchemeResultData

def getTableFieldByKey(cursor,scheme_key):
    cursor.execute("SELECT field_code,field_type,field_len,field_accuracy,field_name,key_flag "
                   "FROM table_field where scheme_key ='%s' order by cast(ord_number as SIGNED INTEGER)" % scheme_key)
    fieldResultData = cursor.fetchall()
    return fieldResultData

## 获取数整指定类型数据
def getCORESchemeData(cursor,SHORTNANE):
    selectTableSql = "SELECT system_en_name,en_name ,ch_name FROM table_scheme WHERE system_name ='CORE' AND or_extract='是' and substring_index(system_en_name,'_',2)='%s'" % SHORTNANE
    cursor.execute(selectTableSql)
    AllSchemeResultData = cursor.fetchall()
    return AllSchemeResultData