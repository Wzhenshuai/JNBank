# -*- coding: utf-8 -*-
import os

import xlrd


def list_file(rootdir):
    list = os.listdir(rootdir)
    for i in range(0, len(list)):
        if list[i].endswith('.xls'):
            read_excel(list[i],rootdir)

def out_path(file_name,path):
    out_file_sql = file_name.replace('.xls', '.sql')
    r_path = os.path.join(path, r'schemeResult/')
    if not os.path.isdir(r_path):
        os.mkdir(r_path)
    out_file_path = os.path.join(r_path, "scheme_"+out_file_sql)
    return out_file_path

def out_path_all(file_name,path):
    out_file_sql = 'All3.sql';
    r_path = path
    if not os.path.isdir(r_path):
        os.mkdir(r_path)
    # else:
    #     os.remove(r_path)
    #     os.mkdir(r_path)
    out_file_path = os.path.join(r_path, "scheme_"+out_file_sql)
    return out_file_path

def read_excel(file_name,rootdir):
    #file_sql_name = "scheme_"+file_sql_name.replace('.xls', '.sql')
    path = os.path.join(rootdir, file_name)
    # 打开文件
    workbook = xlrd.open_workbook(path)
    # 获取所有sheet
    print(workbook.sheet_names())
    index_sheet_name = workbook.sheet_names()[1]

    # 根据sheet索引或者名称获取sheet内容
    sheet1 = workbook.sheet_by_index(1)  # sheet索引从2开始

    # sheet的名称，行数，列数
    #print(sheet1.name, sheet1.nrows, sheet1.ncols)
    out_file_sql = out_path(file_name,rootdir)

    if os.path.exists(out_file_sql):
         os.remove(out_file_sql)
    f = open(out_file_sql, "a+",encoding='utf-8')
    a = 0
    for row_no in sheet1._cell_values:
        a = a+1;
        if a<=1:
            continue
        system_name = repr(row_no[0])
        ch_name = repr(row_no[1])
        en_name = repr(row_no[2])
        provideDate_way =  repr(row_no[5])
        or_extract = repr(row_no[7])
        system_en_name = repr(row_no[0]+"_"+row_no[2])
        insertStr = "insert INTO table_scheme (system_name,ch_name,en_name,provideDate_way,or_extract,system_en_name) " \
                    "VALUES("+system_name+","+ch_name+","+en_name+","+provideDate_way+","+or_extract+","+system_en_name+");\r"
        print(insertStr)
        inn = "insert into table values("+ch_name+")"
        print(inn)
        f.write(insertStr)
    f.close()



if __name__ == '__main__':
    busi_name = 'WEBCREDIT'
    busi_nu = ['BIll', 'credit', 'csnd', 'ctr', 'jf_mall', 'jf_sysbols', 'mmtm', 'trdj', 'utan', 'vts', 'xbus']

    rootdir = r"E:\济宁银行\第一轮梳理表结构\%s" % busi_name
    rootdir = rootdir + "\\"
    # system_name = "C##BILL"
    list_file(rootdir)

    #for i in range(len(busi_nu)):
     #   busi_name = busi_nu[i]
        #rootdir = r'/Users/freer/Documents/网智天元科技股份有限公司/济宁项目组/第一轮梳理表结构/%s/' % busi_name
     #   rootdir = r"E:\济宁银行\第一轮梳理表结构\%s" % busi_name
      #  rootdir = rootdir + "\\"
        #system_name = "C##BILL"
       # list_file(rootdir)