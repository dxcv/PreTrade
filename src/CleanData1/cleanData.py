# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 9:20
# @Author  : ZouJunLin
"""金数源数据清洗"""
# encoding: UTF-8
import datetime
from ReadData import dealCsvfile


if __name__ == '__main__':
    times1 = datetime.datetime.now()
    dealCsvfile()     #数据清洗
    times2=datetime.datetime.now()
    print "Time spent: ",str(times2-times1)
