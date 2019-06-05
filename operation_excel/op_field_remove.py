# -*- coding: utf-8 -*-
import os, sys


def list_file(rootdir,system_name):
    list = os.listdir(rootdir)
    for i in range(0, len(list)):
        if list[i].endswith('.xls'):
            read_excel(list[i],rootdir,system_name)

def out_path(file_name,path):
    out_file_sql = file_name.replace('.xls', '.sql')
    r_path = os.path.join(path, r'filedResult/')
    if not os.path.isdir(r_path):
        os.mkdir(r_path)
    # else:
    #     os.remove(r_path)
    #     os.mkdir(r_path)
    out_file_path = os.path.join(r_path, "field_"+out_file_sql)
    return out_file_path

def out_path_all(file_name,path):
    out_file_sql = 'All3.sql';
    r_path = path
    if not os.path.isdir(r_path):
        os.mkdir(r_path)

    out_file_path = os.path.join(r_path, "field_"+out_file_sql)
    return out_file_path

def read_excel(file_name,path,system_name):
    out_file_name = out_path_all(system_name,path)
    if (os.path.exists(out_file_name)):
        os.remove(out_file_name)

if __name__ == '__main__':
    #busi_nu = ['xbus']
    busi_nu = ['BIll','credit','csnd','ctr','jf_mall','jf_sysbols','mmtm','trdj','utan','vts','xbus']

    for i in range(len(busi_nu)):
        busi_name = busi_nu[i]

        if busi_name == 'BIll':
            system_name = "BILL"
        elif busi_name == 'credit':
            system_name = "credit"
        elif busi_name == 'csnd':
            system_name = "CSDN"
        elif busi_name == 'ctr':
            system_name = "TCR"
        elif busi_name == 'jf_mall':
            system_name = "INTEGRALMALL"
        elif busi_name == 'jf_sysbols':
            system_name = "INTEGRALSYSBOLS"
        elif busi_name == 'mmtm':
            system_name = "MMTM"
        elif busi_name == 'trdj':
            system_name = "TRDJ"
        elif busi_name == 'utan':
            system_name = "UTAN"
        elif busi_name == 'vts':
            system_name = "VTS"
        elif busi_name == 'xbus':
            system_name = "CORE_XBUS_ZDY"
        #rootdir = r'/Users/freer/Documents/网智天元科技股份有限公司/济宁项目组/第一轮梳理表结构/%s/'%busi_name
        rootdir = r"E:\济宁银行\第一轮梳理表结构\%s"%busi_name
        rootdir = rootdir+"\\"

        print(rootdir)
        list_file(rootdir,system_name);