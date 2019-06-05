# -*- coding: utf-8 -*-
import xlrd
import os, sys

#rootdir = r'/Users/freer/Documents/网智天元科技股份有限公司/济宁项目组/第一轮梳理表结构/BIll'

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

def read_excel(file_name,path,system_name):
    path_file = os.path.join(path,file_name)

    # 打开文件
    workbook = xlrd.open_workbook(path_file)

    out_file_name = out_path(file_name,path)
    i= 0;
    if (os.path.exists(out_file_name)):
        os.remove(out_file_name)
    f = open(out_file_name, "a+",encoding='utf-8')
    for sheet_no in workbook.sheet_names():
        i =i+1;
        if i<4:
            continue
        print(sheet_no)
        sheetVal = workbook.sheet_by_name(sheet_no)
        print(sheetVal.name, sheetVal.nrows, sheetVal.ncols)
        a = 0;
        for row_no in sheetVal._cell_values:
            a = a +1;
            if a < 5:
                continue
            ord_number = repr(row_no[0])
            filed_name = repr(row_no[1])
            filed_code = repr(row_no[2])
            filed_type = repr(row_no[3])
            filed_len = repr(row_no[4])
            filed_accuracy = repr(row_no[5])
            key_flag = repr(row_no[6])
            or_dict = repr(row_no[7])
            dict_content = repr(row_no[8])
            remarks = repr(row_no[9])
            or_empty = repr('')
            scheme_key = repr(system_name+"_"+sheet_no)
            # print(system_name,row_no[1],row_no[2],row_no[5],row_no[7])

            insertStr = "INSERT INTO table_field (ord_number,field_name,field_code,field_type,field_len,field_accuracy,key_flag,or_dict,dict_content,remarks,scheme_key,or_empty)" \
                        "VALUES(" + ord_number + "," + filed_name + "," + filed_code + "," + filed_type + "," + filed_len + "," + filed_accuracy + "," + key_flag + "," + or_dict\
                        + "," + dict_content + "," + remarks + "," + scheme_key + "," + or_empty + ");\r"
            print(insertStr)
            f.write(insertStr)

    f.close()


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