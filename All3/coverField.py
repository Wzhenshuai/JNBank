import re


def convert_fieldType(field):
    fieldType = field[1]
    fieldLen = field[2]
    fieldSAccuracy = field[3]

    type = fieldType.lower()
    #fieldLen = re.findall(r'[(](.*?)[)]',field_type)

    #field_type_dis = field_type.split("(")[0]

    if type in ('char','nchar','varchar','nvarchar','graphic','varbraphic','varchar2'):
        field_type = 'varchar('+ fieldLen +')'
    elif type in ('date','timestamp','time'):
        field_type = 'varchar(30)'
    elif type in ('smallint','integer','bigint','int','tinyint'):
        field_type = 'bigint'
    elif type == "boolean":
        field_type = 'boolean'
    elif type in('numeric','decimal','decfloat','real','float','double','number'):
        if fieldSAccuracy == '' or fieldSAccuracy == ' ':
            field_type = 'decimal('+fieldLen+')'
        else :
            field_type = 'decimal('+fieldLen+','+fieldSAccuracy+')'
    else:
        field_type = type;
    return field_type;

def unite_key_value(fields):
    unite_key_value = "concat_ws('^',"
    for fie in fields:
        key_comm = fie[5]
        if key_comm == '是':
            unite_key_value = unite_key_value + fie[0] + ','
    unite_key_value = unite_key_value.rstrip(",") + ")"
    return unite_key_value

def get_fields_str(fields):
    create_field_str = ''
    inster_field_str = ''
    for i in fields:
        comm = i[4]
        if (comm == ''):
            com = "''"
        key_comm = i[5]
        if key_comm == '是':
            comm = comm+'.主键'
        field_type = i[1];
        field_type = convert_fieldType(field_type)

        create_field_str = create_field_str+("%s %s comment'%s',\n")%(i[4],field_type,comm)
        inster_field_str = inster_field_str+("%s,\n"%i[0])

    return create_field_str.rstrip(",\n"),inster_field_str.rstrip(",\n")