#coding=utf-8

import os
import sys

print(sys.path)
Path=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(Path)
sys.path.append(Path)
from common import ConstantUtile

print(sys.path)

print(ConstantUtile.custmerStr)
