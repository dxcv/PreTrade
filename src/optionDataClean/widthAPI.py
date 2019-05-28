# -*- coding: utf-8 -*-
# @Time    : 2019/5/27 16:35
# @Author  : ZouJunLin
import pandas as pd
import numpy as np


def GetWidth(BP):
    if BP<500:
        return max(20,BP*0.12)
    elif BP<1000:
        return max(60,BP*0.1)
    elif BP<3000:
        return max(100,BP*0.08)
    else:
        return max(240,BP*0.06)

def GetRespondWidth(BP):
    if BP<500:
        return max(30,BP*0.14)
    elif BP<1000:
        return max(70,BP*0.12)
    elif BP<3000:
        return max(120,BP*0.10)
    else:
        return max(300,BP*0.08)

def GetAverageWidth(Directory,filename):
    df=pd.read_csv(Directory+"/"+filename,encoding='gbk')
    InstrumentID=str(filename).split("_")[0]
    length=len(df)
    BP = sum(df['BP1']) / length
    BPWidth = GetWidth(BP)
    width = sum(df['SP1'] - df['BP1']) / length

    return [InstrumentID,width,BPWidth,'']

def GetResponseAverageWidth(Directory,filename):
    df = pd.read_csv(Directory + "/" + filename, encoding='gbk')
    InstrumentID = str(filename).split("_")[0]
    length = len(df)
    for i in df.index:
        if df.at[i,u'申买价一']==0:
            df.at[i, u'申买价一']=df.at[i, u'跌停板价']
        if df.at[i,u'申卖价一']==0:
            df.at[i, u'申卖价一']=df.at[i, u'涨停板价']
    BP = sum(df[u'申买价一']) / length
    BPWidth = GetRespondWidth(BP)
    width = sum(df[u'申卖价一'] - df[u'申买价一']) / length
    return [InstrumentID, width,'', BPWidth]