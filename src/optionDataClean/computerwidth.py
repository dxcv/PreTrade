# -*- coding: utf-8 -*-
# @Time    : 2019/5/17 13:41
# @Author  : ZouJunLin
import pandas as pd
import sys,csv,datetime


def GetAverageWidth(Directory,filename):
    df=pd.read_csv(Directory+"/"+filename)
    print df


if __name__ == '__main__':

    Directory = "D:/GitData/PreTrade/src/optionDataClean/CleanedData/"
    filename="cu1906C44000_20190516.csv"

    temp=GetAverageWidth(Directory,filename)