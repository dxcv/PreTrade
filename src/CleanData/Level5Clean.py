# encoding:utf-8
import datetime
import os,re,sys
import csv
from tqdm import tqdm
import pandas as pd
from utils.InfoApi import  *
from CleanAPI import *
import thread
import time



#全局变量保存临时数据
templist=[]
TradingDay = ""
LastTradingDay=""
InstrumentId= ""
columns0={'交易日':'TradingDay','合约代码':'InstrumentId','最新价':'Price','上次结算价':'Price3','昨收盘':'LastClose','昨持仓量':'LastTotalVol','今开盘':'Open','最高价':'High','最低价':'Low','数量':'Volume','成交金额':'Amount','持仓量':'OpenInt','今收盘':'ClosePrice ','本次结算价':'Price3','涨停板价':'HighLimitPrice','跌停板价':'LowLimitPrice','昨虚实度':'PreDelta','今虚实度':'Delta',
          '后修改时间':'Time','申买价一':'BP1','申买量一':'BV1','申卖价一':'SP1','申卖量一':'SV1','申买价二':'BP2','申买量二':'BP2','申卖价二':'SP2','申卖量二':'SV2','申买价三':'BP3','申买量三':'BV3','申卖价三':'SP3','申卖量三':'SV3','申买价四':'BP4','申买量四':'BV4','申卖价四':'SP4','申卖量四':'SV4','申买价五':'BP5',
         '申买量五':'BV5','申卖价五':'SP5','申卖量五':'SV5','当日均价':'AveragePrice','业务日期':'TradingDay'}
columns=['Time','Price','Volume','Amount','OpenInt','TotalVol','TotalAmount','Price2','Price3','LastClose','Open','High','Low','SP1','SP2','SP3','SP4','SP5','SV1','SV2','SV3','SV4','SV5','BP1','BP2','BP3','BP4','BP5','BV1','BV2','BV3','BV4','BV5','isBuy']


def Myappend(csv_data,i,col,TradingDay,InstrumentId):
    newcol = []
    newcol.append(TradingDay)
    newcol.append(InstrumentId)
    for column in columns:
        newcol.append(csv_data.at[i, column])
    col.append(tuple(newcol))

#读取数据
def  Level_5_clean(filename, fileDirectory, info):
    print filename,fileDirectory
    #源文件名字
    daylist=[]
    templist=[]
    temp=0
    theoryhms=""
    lasthms="-1"
    col=[]
    InstrumentId = info.cleanDatadict[2]
    TradingDay = info.cleanDatadict[0]
    try:
        csv_data=pd.read_csv(fileDirectory+"\\"+filename)
    except Exception, e:
        print  "u'源数据文件不存在,或请先关闭文件",info.cleanDatadict[0],info.cleanDatadict[1]
        return
    length = len(csv_data) - 1
    backup_name =info.cleanDatadict[2]+"_"+info.cleanDatadict[0]+".csv"
    backup_csvfile = IshaveFile(backup_name, info.cleanDatadict[3], False, "wb")
    writer = csv.writer(backup_csvfile)
    code=info.cleanDatadict[1]
    sql = """ select [DayTradTime],[NightTradTime] from ContractCode where InstrumentCode='%s'""" % code
    TradTime = info.mysql.ExecQuery(sql)[0]
    firstdata = datetime.datetime.strptime(str(csv_data.at[0, 'Time']), '%Y-%m-%d %H:%M:%S').strftime("%H:%M:%S")
    starthms, endhms = GetSEndHms(TradTime,firstdata)
    """"""
    col = []
    col.append('InstrumentId')
    col.append('TradingDay')
    for i in columns:
        col.append(i)
    writer.writerow(col)

    for i in csv_data.index:
        date=datetime.datetime.strptime(str(csv_data.at[i,'Time']),'%Y-%m-%d %H:%M:%S')
        hms=date.strftime("%H%M%S")
        if temp==hms:
            continue
        if isintimerange(hms + "000", TradTime):
            if starthms != "":
                if starthms[:6] == hms:
                    if int(csv_data.at[i, 'SP1']) == 0:
                        csv_data.at[i, 'SP1'] = csv_data.at[i, 'LastClose']
                    if int(csv_data.at[i, 'BP1']) == 0:
                        csv_data.at[i, 'BP1'] = csv_data.at[i, 'LastClose']
                    lasthms = theorynextdate(starthms, TradTime)
                    starthms = ""
                    csv_data.at[i, 'Time'] = str('%09d' % int(hms + "000"))
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                else:
                    """补开盘时缺失的数据"""
                    while starthms!=hms+'000':
                        if i==0:
                            if int(csv_data.at[i, 'SP1']) == 0:
                                csv_data.at[i, 'SP1'] = csv_data.at[i, 'LastClose']
                            if int(csv_data.at[i, 'BP1']) == 0:
                                csv_data.at[i, 'BP1'] = csv_data.at[i, 'LastClose']
                            csv_data.at[i, 'Time'] = starthms
                            Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                            starthms=theorynextdate(starthms,TradTime)
                        else:
                            if int(csv_data.at[i, 'SP1']) == 0:
                                csv_data.at[i, 'SP1'] = csv_data.at[i, 'LastClose']
                            if int(csv_data.at[i, 'BP1']) == 0:
                                csv_data.at[i, 'BP1'] = csv_data.at[i, 'LastClose']
                            csv_data.at[i-1, 'Time'] = starthms
                            Myappend(csv_data, i-1, templist, InstrumentId, TradingDay)
                            starthms = theorynextdate(starthms, TradTime)
                    csv_data.at[i, 'Time'] = starthms
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                    # if lasthms == endhms:
                    #     # daylist = [InstrumentId, TradingDay, csv_data.at[i, 'TotalVol'], csv_data.at[i, 'TotalAmount'],csv_data.at[i, 'Price2'],(csv_data.at[i,'SP1']+csv_data.at[i,'BP1'])/2, csv_data.at[i, 'Price3'],csv_data.at[i, 'LastClose']]
                    #     break
                    lasthms = theorynextdate(starthms, TradTime)
                    starthms=""

            elif hms !=temp:
                if hms!=lasthms[:6] :
                    while lasthms !=hms + "000" and lasthms!=endhms:
                        csv_data.at[i-1, 'Time'] = lasthms
                        Myappend(csv_data, i-1, templist, InstrumentId, TradingDay)
                        lasthms = theorynextdate(lasthms, TradTime)
                    if specialdata(hms,TradTime):
                            temp = hms
                    csv_data.at[i, 'Time'] = lasthms
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                    if lasthms == endhms:
                        # daylist = [InstrumentId, TradingDay, csv_data.at[i, 'TotalVol'], csv_data.at[i, 'TotalAmount'],csv_data.at[i, 'Price2'], (csv_data.at[i, 'SP1'] + csv_data.at[i, 'BP1']) / 2,csv_data.at[i, 'Price3'], csv_data.at[i, 'LastClose']]
                        break
                    lasthms = theorynextdate(lasthms, TradTime)
                    if specialdata(hms,TradTime):
                        temp = hms
                elif lasthms==hms+"000":
                    csv_data.at[i, 'Time'] = lasthms
                    Myappend(csv_data, i, templist,InstrumentId,TradingDay)
                    lasthms = theorynextdate(lasthms, TradTime)
                    if specialdata(hms,TradTime):
                        temp=hms
                elif lasthms==hms+"500":
                    csv_data.at[i, 'Time'] = lasthms
                    Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                    lasthms = theorynextdate(lasthms, TradTime)
                    temp=hms

        "结束循环的两种情况"
        if lasthms == "":
            # daylist = [InstrumentId, TradingDay, csv_data.at[i, 'TotalVol'], csv_data.at[i, 'TotalAmount'],csv_data.at[i, 'Price2'], (csv_data.at[i, 'SP1'] + csv_data.at[i, 'BP1']) / 2,csv_data.at[i, 'Price3'], csv_data.at[i, 'LastClose']]
            break
        if i == length and lasthms != endhms:
            while lasthms != endhms and lasthms != "":
                csv_data.at[i, 'Time'] = lasthms
                Myappend(csv_data, i, templist, InstrumentId, TradingDay)
                lasthms = theorynextdate(lasthms, TradTime)
            csv_data.at[i, 'Time'] = lasthms
            Myappend(csv_data, i, templist, InstrumentId, TradingDay)
            if lasthms == "":
                break
            lasthms = theorynextdate(lasthms, TradTime)
    while lasthms!="":
        csv_data.at[i, 'Time'] = lasthms
        Myappend(csv_data, i, templist, InstrumentId, TradingDay)
        lasthms = theorynextdate(lasthms, TradTime)
    writer.writerows(templist)


