import re


def convert_fieldType(fieldType):
    field_type = fieldType.lower()
    fieldLen = re.findall(r'[(](.*?)[)]',field_type)
    #print(fieldLen[0])
    field_type_dis = field_type.split("(")[0]
    # if field_type_dis in ('char','nchar','varchar','nvarchar','clob','blob',
    #                   'nclob','dbclob','graphic','varbraphic','date','timestamp','time','xml'):
    #     field_type = 'string'
    # elif field_type_dis in ('smallint','integer','bigint','int','tinyint'):
    #     field_type = 'bigint'
    # elif field_type_dis == "boolean":
    #     field_type = 'boolean'
    # elif field_type_dis in('numeric','decimal','decfloat','real','float','double'):
    #     field_type = 'decimal(21,8)'
    # else:
    #     field_type = field_type_dis;

    if field_type_dis in ('char','nchar','varchar','nvarchar','graphic','varbraphic'):
        field_type = 'varchar('+fieldLen[0]+')'
    elif field_type_dis in ('date','timestamp'):
        field_type = field_type
    elif field_type_dis in ('time'):
        field_type = 'string'
    elif field_type_dis in ('smallint','integer','bigint','int','tinyint'):
        field_type = 'bigint'
    elif field_type_dis == "boolean":
        field_type = 'boolean'
    elif field_type_dis in('numeric','decimal','decfloat','real','float','double'):
        if len(fieldLen) == 1:
            field_type = 'decimal('+fieldLen[0]+')'
        elif len(fieldLen) == 2:
            field_type = 'decimal('+fieldLen[0]+','+fieldLen[1]+')'
        else:
            field_type = field_type

    else:
        field_type = field_type_dis;
    return field_type;

def unite_key_value(fields):
    unite_key_value = "concat_ws('^',"
    for fie in fields:
        key_comm = fie[3]
        if key_comm == 'PRI':
            unite_key_value = unite_key_value + fie[0] + ','
    unite_key_value = unite_key_value.rstrip(",") + ")"
    return unite_key_value

def get_fields_str(fields):
    create_field_str = ''
    inster_field_str = ''
    for i in fields:
        comm = i[1]
        if (comm == ''):
            com = "''"
        key_comm = i[3]
        if key_comm == 'PRI':
            comm = comm+'.主键'
        field_type = i[2];
        field_type = convert_fieldType(field_type)

        create_field_str = create_field_str+("%s %s comment'%s',\n")%(i[0],field_type,comm)
        inster_field_str = inster_field_str+("%s,\n"%i[0])

    return create_field_str.rstrip(",\n"),inster_field_str.rstrip(",\n")