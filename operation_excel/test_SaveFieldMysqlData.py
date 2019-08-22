# -*- coding: utf-8 -*-
import os

import xlrd


def list_file(path, system_name):
    list = os.listdir(path)
    out_file_name = out_path_all(system_name, path)
    f = open(out_file_name, "a+", encoding='utf-8')
    nu_index = 0;
    for i in range(0, len(list)):
        nu_index += 1
        if list[i].endswith('.xls'):
            fiel_name = list[i]
            path_file = os.path.join(rootdir, list[i])
            print(path_file)

            # 打开文件
            workbook = xlrd.open_workbook(path_file)
            i = 0;
            for sheet_no in workbook.sheet_names():
                i = i + 1;
                if i < 3:
                    continue
                if sheet_no == 'ODS_ECCRCSHT':
                    print(sheet_no)
                sheetVal = workbook.sheet_by_name(sheet_no)
                a = 0;
                tableName = ''
                for row_no in sheetVal._cell_values:
                    a = a + 1;
                    if a == 2 :
                        tableName = row_no[1].replace(',', '')
                    if a >= 5:
                        if row_no[0] == '' or row_no[0] == ' ':
                            ord_number = row_no[0]
                        else:
                            ord_number = str(int(row_no[0]))
                        # ord_number = str(int(row_no[0]))
                        filed_name = row_no[1].replace("'", "\"").replace(",", "\,").replace(";", ":").replace("；", ":")
                        filed_code = row_no[2].replace(',', '')
                        filed_type = row_no[3]
                        if row_no[4] == '' or row_no[4] == ' ':
                            filed_len = row_no[4]
                        else:
                            filed_len = str(int(row_no[4]))
                        if row_no[5] == '' or row_no[5] == ' ':
                            filed_accuracy = row_no[5]
                        else:
                            filed_accuracy = str(int(row_no[5]))

                        key_flag = row_no[6]
                        or_dict = row_no[7]
                        dict_content = row_no[8].replace("'", "\"").replace(",", "\,").replace(";", ":").replace("；", ":").replace("\'", "/")
                        remarks = row_no[9].replace("'", "\"").replace("\n","")
                        or_empty = "''"
                        if tableName == '':
                            print(fiel_name)
                        scheme_key = "'" + system_name.upper() + "_" + tableName + "'"
                        # print(system_name,row_no[1],row_no[2],row_no[5],row_no[7])

                        insertStr = "INSERT INTO table_field (ord_number,field_name,field_code,field_type,field_len,field_accuracy,key_flag,or_dict,dict_content,remarks,scheme_key,or_empty)" \
                                    "VALUES('" + ord_number + "','" + filed_name + "','" + filed_code + "','" + filed_type + "','" + filed_len + "','" + filed_accuracy + "','" + key_flag + "','" + or_dict \
                                    + "','" + dict_content + "','" + remarks + "'," + scheme_key + "," + or_empty + ");\r"
                        # print(insertStr)
                        f.write(insertStr)
    f.close()
    print('=======SUCCESS=======')


def out_path_all(file_name, path):
    out_file_sql = file_name + '_All.sql'
    if not os.path.isdir(path):
        os.mkdir(path)
    out_file_path = os.path.join(path, "field_" + out_file_sql)
    return out_file_path


if __name__ == '__main__':
    # busi_nu = ['BIll','credit','csnd','ctr','jf_mall','jf_sysbols','mmtm','trdj','utan','vts','xbus']
    #busi_nu = sys.argv[1]
    busi_nu = 'WEBCREDIT'
    system_name = busi_nu

    rootdir = r"E:\济宁银行\第一轮梳理表结构\%s" % busi_nu
    rootdir = rootdir + "\\"
    out_file_sql = out_path_all(system_name, rootdir);
    if os.path.exists(out_file_sql):
        os.remove(out_file_sql)
    list_file(rootdir, system_name)
