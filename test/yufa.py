import re
import sys

print(sys.path)
a = "123abc4a6"
b = '币别 ;  货币代号'
t = 'timestamp'

print(re.search("([0-9]*)([a-z]*)([0-9]*)",a).group(0))   #123abc456,返回整体
print(re.search("([0-9]*)([a-z]*)([0-9]*)", a).group(1))  # 123
print(re.search("([0-9]*)([a-z]*)([0-9]*)", a).group(2))  # abc
print(re.search("([0-9]*)([a-z]*)([0-9]*)", a).group(3))  # 456

#a.search("(.+)+b")

#print(re.search(u'a(.+)', a).group(0))  # 456

#a = a.replace('a','A').replace('二','四').replace('b','B')
#print(b.replace(';', ':'))
fieldType = 'timestamp(30)'
aa = fieldType.split('(')[0]
print(aa)
if fieldType in ('time', 'date', 'timestamp','timestamp(30)'):
    print(fieldType)
ty = 'INDEX'
if ty == 'INDEX':
    ty = '"INDEX"'
print(ty)