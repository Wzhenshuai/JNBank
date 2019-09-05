# -*- coding: utf-8 -*-
import xlrd
import os, sys


def list_file(busi_name, rootdir):
    list = os.listdir(rootdir)
    for i in range(0, len(list)):
        if list[i].endswith('.xls'):
            read_excel(busi_name, list[i], rootdir)


def out_path(file_name, path):
    out_file_sql = file_name + '_All.sql'
    if not os.path.isdir(path):
        os.mkdir(path)
    out_file_path = os.path.join(path, "scheme_" + out_file_sql)
    return out_file_path


def read_excel(busi_name, file_name, rootdir):
    path = os.path.join(rootdir, file_name)
    print(path)
    # 打开文件
    workbook = xlrd.open_workbook(path)

    # 根据sheet索引或者名称获取sheet内容
    sheet1 = workbook.sheet_by_index(0)  # sheet索引从2开始

    # sheet的名称，行数，列数
    out_path_all = out_path(busi_name, rootdir)

    f = open(out_path_all, "a+", encoding='utf-8')
    # f.write('use table_scheme;\r')
    for row_no in sheet1._cell_values:

        system_name = '\''+busi_name.upper() + '\''
        en_name = repr(row_no[0])
        ch_name = repr(row_no[1])

        system_en_name = repr("CRM_" + en_name)
        insertStr = "insert INTO table_scheme (system_name,ch_name,en_name, or_extract, system_en_name,db_name) " \
                    "VALUES(" + system_name + "," + ch_name + "," + repr(en_name) + "," + "是," + system_en_name + ");\r"
        f.write(insertStr)
    f.close()


if __name__ == '__main__':
    # busi_nu = ['BIll', 'credit', 'csnd', 'ctr', 'jf_mall', 'jf_sysbols', 'mmtm', 'trdj', 'utan', 'vts', 'xbus']
    busi_nu = 'CRM'

    rootdir = r"E:\济宁银行\第一轮梳理表结构\%s" % busi_nu
    rootdir = rootdir + "\\"

    out_file_sql = out_path(busi_nu, rootdir);
    if os.path.exists(out_file_sql):
        os.remove(out_file_sql)
    list_file(busi_nu, rootdir)
    print('------SUCCESS-----')
