
def convert_fieldType(fieldType,fieldLen,fieldAccuracy):
    fieldType = fieldType.lower()
    #if field_accuracy == "" or field_accuracy == " ":

    if fieldType in ('char','nchar','varchar','nvarchar','graphic','varbraphic',
                     'character','varchar2','nvarchar2','xmltype','long varchar','long raw'):
        field_type = 'varchar('+fieldLen+')'
    elif fieldType in ('time','date','timestamp','timestamp(6)'):
        field_type = 'varchar(30)'
    elif fieldType in ('smallint','integer','bigint','int','tinyint'):
        field_type = 'bigint'
    elif fieldType == "boolean":
        field_type = 'boolean'
    elif fieldType in ('clob','blob'):
        field_type = fieldType
    elif fieldType in('clob','blob','float','decfloat'):
        if fieldAccuracy == "" or fieldAccuracy == " ":
            field_type = 'decimal('+fieldLen+')'
        else:
            field_type = 'decimal('+fieldLen+','+fieldAccuracy+')'
    else:
        field_type = fieldType + '(' +fieldLen+ ')'
    return field_type;

