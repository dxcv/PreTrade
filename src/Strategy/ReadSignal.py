# -*- coding: utf-8 -*-
# @Time    : 2018/12/11 16:21
# @Author  : ZouJunLin
"""读取信号量相关数据"""
import xlrd
import pandas as pd

class Signal:
    def ___init__(self):
        pass

    def ReadContract(self):
        """相同品种不同合约之间套利"""
        contractdata=""
        try:
            contractdata=pd.read_excel('signal/Contract1.xlsx')
        except:
            print "111111"
        return contractdata





