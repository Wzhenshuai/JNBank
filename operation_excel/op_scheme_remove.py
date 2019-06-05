# -*- coding: utf-8 -*-
import xlrd
import os, sys



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

    out_file_sql = out_path_all(file_name,rootdir)

    if os.path.exists(out_file_sql):
        os.remove(out_file_sql)


if __name__ == '__main__':
    #busi_name = 'credit'
    busi_nu = ['BIll', 'credit', 'csnd', 'ctr', 'jf_mall', 'jf_sysbols', 'mmtm', 'trdj', 'utan', 'vts', 'xbus']

    for i in range(len(busi_nu)):
        busi_name = busi_nu[i]
        #rootdir = r'/Users/freer/Documents/网智天元科技股份有限公司/济宁项目组/第一轮梳理表结构/%s/' % busi_name
        rootdir = r"E:\济宁银行\第一轮梳理表结构\%s" % busi_name
        rootdir = rootdir + "\\"
        #system_name = "C##BILL"
        list_file(rootdir)