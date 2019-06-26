
def convert_fieldType(fieldType,fieldLen,fieldAccuracy):
    fieldType = fieldType.lower()
    if fieldType in (
    'char', 'nchar', 'varchar', 'nvarchar', 'graphic', 'varbraphic', 'character', 'varchar2', 'nvarchar2', 'xmltype'):
        field_type = 'varchar(' + fieldLen + ')'
    elif fieldType in ('time', 'date', 'timestamp', 'timestamp(6)'):
        field_type = 'varchar(30)'
    elif fieldType in ('smallint', 'integer', 'bigint', 'int', 'tinyint'):
        field_type = 'bigint'
    elif fieldType == "boolean":
        field_type = 'boolean'
    elif fieldType in ('clob', 'blob'):
        field_type = fieldType
    elif fieldType in ('nclob', 'long varchar'):
        field_type = 'clob'
    elif fieldType in ('long raw', 'longraw', 'raw'):
        field_type = 'blob'
    elif fieldType == 'long':
        field_type = 'varchar(1000)'
    elif fieldType in ('numeric', 'decimal', 'decfloat', 'real', 'float', 'double', 'number'):
        if fieldAccuracy == "" or fieldAccuracy == " ":
            field_type = 'decimal(' + fieldLen + ')'
        else:
            field_type = 'decimal(' + fieldLen + ',' + fieldAccuracy + ')'
    else:
        field_type = fieldType + '(' + fieldLen + ')'
    return field_type;

