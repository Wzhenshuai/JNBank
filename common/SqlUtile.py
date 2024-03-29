import pymysql

##  mysql登录
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

## 获取铺底sql 路径
def getPDDicInfo(cursor,SHORTNANE):
    dictSql = "SELECT sql_path,system_code,shell_path FROM dic_info_mapping WHERE transfer_mode ='全量铺底数据' AND system_code='%s'" % SHORTNANE
    cursor.execute(dictSql)
    dicResultData = cursor.fetchall()
    return dicResultData


## 获取非增量数据
def getQLSchemeData(cursor,SHORTNAME):
    selectTableSql = "SELECT system_en_name,en_name,db_name FROM table_scheme WHERE system_name ='%s' and provideDate_way != '增量' AND or_extract='是'" % SHORTNAME
    cursor.execute(selectTableSql)
    QLResultData = cursor.fetchall()
    return QLResultData

## 获取增量数据scheme
def getZLSchemeData(cursor,SHORTNANE):
    selectTableSql = "SELECT system_en_name,en_name ,ch_name ,db_name FROM table_scheme WHERE system_name ='%s' AND or_extract='是' and provideDate_way = '增量'" % SHORTNANE
    cursor.execute(selectTableSql)
    AllSchemeResultData = cursor.fetchall()
    return AllSchemeResultData

## 获取所有表数据 scheme
def getALLSchemeData(cursor,SHORTNAME):
    selectTableSql = "SELECT system_en_name,en_name ,ch_name,db_name,provideDate_way FROM table_scheme WHERE system_name ='%s' AND or_extract='是'" % SHORTNAME
    cursor.execute(selectTableSql)
    AllSchemeResultData = cursor.fetchall()
    return AllSchemeResultData

## 获取以field 根据orNumber 排序
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

## 获取数整指定表
def otherTmp(cursor):
    selectTableSql = "SELECT system_en_name,en_name ,ch_name FROM table_scheme WHERE system_name ='CORE' AND en_name in ('ODS_UNIONALOD','ODS_CMMSCCST','ODS_YEP_FMS_ACCRUE_LOG')"
    cursor.execute(selectTableSql)
    AllSchemeResultData = cursor.fetchall()
    return AllSchemeResultData
