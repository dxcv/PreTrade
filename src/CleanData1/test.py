# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 9:44
# @Author  : ZouJunLin
import os
from utils.InfoApi import  *

cnfpath=os.path.abspath('../config.conf')
print cnfpath
info=InfoApi().GetDbHistoryConnect()

print info