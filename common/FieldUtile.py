from common import coverField

def getAllfieldStr(field_datas):
    rowKeyStrC = ''
    pr_key = ''
    fieldStr = ''
    corporationStr = "`corporation` varchar(33) comment'法人主体.主键',\r"
    for fd in field_datas:
        field_code = fd[0]
        field_type = fd[1]
        field_len = fd[2]
        field_accuracy = fd[3]
        field_comment = fd[4]
        key_flag = fd[5]
        if key_flag == '是':
            rowKeyStrC = rowKeyStrC + field_code + ','
            pr_key = pr_key + field_code + ','
        if fd[0] == 'CORPORATION':
            corporationStr = ''
        tieldTypeStr = coverField.convert_fieldTypeAll(field_type, field_len, field_accuracy)

        if key_flag == '是':
            fieldStr = fieldStr + '`' + field_code + '` ' + tieldTypeStr + " comment '" + field_comment + '_主鍵' + "',\r"
        else:
            fieldStr = fieldStr + '`' + field_code + '` ' + tieldTypeStr + " comment '" + field_comment + "',\r"
    ##如果没有corporatin
    if corporationStr != '':
        rowKeyComment = '联合主键(corporation,' + pr_key
    else:
        rowKeyComment = '联合主键(' + pr_key
    fieldStrTop = "`rowKeyStr` varchar(333) comment '" + rowKeyComment.rstrip(',') + ")',\r" \
                                  "`DataDay_ID` varchar(33) COMMENT'数据的时间',\r" \
                                  "`tdh_load_timestamp`  varchar(33)  COMMENT'加载到TDH时的时间戳',\r" + corporationStr

    return fieldStrTop + fieldStr
