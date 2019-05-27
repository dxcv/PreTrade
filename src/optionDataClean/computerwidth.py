# -*- coding: utf-8 -*-
# @Time    : 2019/5/17 13:41
# @Author  : ZouJunLin
import pandas as pd
import os
import sys,csv,datetime

def GetWidth(BP):
    if BP<500:
        return max(20,BP*0.12)
    elif BP<1000:
        return max(60,BP*0.1)
    elif BP<3000:
        return max(100,BP*0.08)
    else:
        return max(240,BP*0.06)

def GetAverageWidth(Directory,filename):
    df=pd.read_csv(Directory+"/"+filename)
    InstrumentID=str(filename).split("_")[0]
    length=len(df)
    BP=sum(df['BP1'])/length
    BPWidth=GetWidth(BP)
    width=sum(df['SP1']-df['BP1'])/length
    return InstrumentID,width,BPWidth


if __name__ == '__main__':

    columns=[u'合约',u'平均报价宽度',u'义务宽度']
    Directory = "D:/GitData/PreTrade/src/optionDataClean/CleanedData/"

    saveDirector = "./"
    savafile = saveDirector + "/"+u"cu期权报价宽度.xlsx"
    writer = pd.ExcelWriter(savafile, engine='xlsxwriter')

    abspath = os.path.abspath(Directory)
    templist=[]
    for filenames in os.walk(abspath):
        for filename in filenames[2][:-1]:
            filename = filename.decode(encoding='gbk')
            if str(filename).endswith("csv"):
                # print filename
                temp=GetAverageWidth(Directory,filename)
                templist.append(temp)
    df =pd.DataFrame(data=templist,columns=columns)

    df.to_excel(writer,sheet_name='Sheet1', startrow=1, startcol=0, index=None)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column('A:C', 20)

    # Add a header format.
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'vcenter',
        'border': 1
    })
    header_format.set_align('center')
    header_format.set_align('vcenter')
    writer.save()