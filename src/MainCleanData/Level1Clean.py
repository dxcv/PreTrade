# encoding:utf-8
import datetime
import os,re,sys
import csv
from tqdm import tqdm
import pandas as pd
from utils.InfoApi import  *
import thread
import time

insertFuturesql="""
        INSERT INTO FutureHistoryTdData
           (InstrumentId,TradingDay,Time,Price,Volume,Amount,OpenInt,TotalVol,TotalAmount,Price2,Price3,LastClose,[Open],High,Low,SP1,SP2,SP3,SP4,SP5,SV1,SV2,SV3 ,SV4,SV5 ,BP1,BP2,BP3,BP4,BP5,BV1,BV2,BV3,BV4,BV5,isBuy)
        VALUES ('%s','%s',%s,%s,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%s)
    """

insertDayFuturesql="""
        INSERT INTO FutureHistoryTdDayData
           (InstrumentId,TradingDay,TotalVol,[TotalAmount],SettlementPrice,ClosePrice,LastSettlementPrice,[LastClosePrice])
        VALUES ('%s','%s',%d,%d,'%s','%s','%s','%s')
    """

#全局变量保存临时数据
templist=[]
TradingDay = ""
LastTradingDay=""
InstrumentId= ""
str_file=os.getcwd()+"\DATA"
columns0={'交易日':'TradingDay','合约代码':'InstrumentId','最新价':'Price','上次结算价':'Price3','昨收盘':'LastClose','昨持仓量':'LastTotalVol','今开盘':'Open','最高价':'High','最低价':'Low','数量':'Volume','成交金额':'Amount','持仓量':'OpenInt','今收盘':'ClosePrice ','本次结算价':'Price3','涨停板价':'HighLimitPrice','跌停板价':'LowLimitPrice','昨虚实度':'PreDelta','今虚实度':'Delta',
          '后修改时间':'Time','申买价一':'BP1','申买量一':'BV1','申卖价一':'SP1','申卖量一':'SV1','申买价二':'BP2','申买量二':'BP2','申卖价二':'SP2','申卖量二':'SV2','申买价三':'BP3','申买量三':'BV3','申卖价三':'SP3','申卖量三':'SV3','申买价四':'BP4','申买量四':'BV4','申卖价四':'SP4','申卖量四':'SV4','申买价五':'BP5',
         '申买量五':'BV5','申卖价五':'SP5','申卖量五':'SV5','当日均价':'AveragePrice','业务日期':'TradingDay'}
columns=['Time','Price','Volume','Amount','OpenInt','TotalVol','TotalAmount','Price2','Price3','LastClose','Open','High','Low','SP1','SP2','SP3','SP4','SP5','SV1','SV2','SV3','SV4','SV5','BP1','BP2','BP3','BP4','BP5','BV1','BV2','BV3','BV4','BV5','isBuy']

def  GetSEndHms(TradTime):
    """获取合约的交易开始时间"""
    startime={'0':['090000000','091500000','093000000']}
    endtime=['150000000','151500000','150000000']
    if TradTime[1]=='0':
        return startime['0'][int(TradTime[0])],endtime[int(TradTime[0])]
    else:
        return '210000000',endtime[int(TradTime[0])]

def isintimerange(hms,TradTime):
    try:
        if TradTime[0]=='0' and TradTime[1]=='0':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='1':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("230000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='2':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("233000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='3':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("235959500"):
                return True
            elif mystrptime(hms) >= mystrptime("000000000") and mystrptime(hms) <= mystrptime("010000000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='4':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            elif mystrptime(hms) >= mystrptime("210000000") and mystrptime(hms) <= mystrptime("235959500"):
                return True
            elif mystrptime(hms) >= mystrptime("000000000") and mystrptime(hms) <= mystrptime("023000000"):
                return True
            else:
                return False
        elif TradTime[0]=='1' and TradTime[1]=='0':
            if mystrptime(hms) >= mystrptime("091500000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("151500000"):
                return True
            else:
                return False
        elif TradTime[0] == '2' and TradTime[1] == '0':
            if mystrptime(hms) >= mystrptime("093000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("150000000"):
                return True
            else:
                return False
    except Exception, e:
        # print e.message,hms,type(hms),"循环里面时间溢出，正常情况"
        return False

def mystrptime(hms):
    return datetime.datetime.strptime(hms.strip(),"%H%M%S%f")

def theorynextdate(lasthms,TradTime):
    """
    #理论上下一个时间点的数,当然有时间范围限制
    :param lasthms:
    :return:
    """
    time=str('%09d'%(int(lasthms)+500))
    if time[2:4]=="60" or time[4:6]=="60":
        hour=int(time[:2])
        minute=int(time[2:4])
        second=int(time[4:6])
        if second==60:
            minute=str('%02d'%(minute+1))
            second=str("00")
            if minute=="60":
                hour = str('%02d' % (hour + 1))
                minute = str("00")
                if (hour == "24"):
                    hour = str("00")

        if minute==60:
            hour=str('%02d'%(hour+1))
            minute=str("00")
            if (hour == "24"):
                hour = str("00")
        if(hour==24):
            hour=str("00")
        hour='%02d'%int(hour)
        minute = '%02d'%int(minute)
        second='%02d'%int(second)
        time=str(hour)+str(minute)+str(second)+time[-3:]

    if isintimerange(time,TradTime):
        return time
    else:
        time=nextdurationTime(time,TradTime)
        return time

def nextdurationTime(time,TradTime):
    """计算时间断层问题，将时间链接起来"""
    dict1=dict()
    dict0={'0':{"101500500":'103000000','113000500':'133000000'},'1':{'113000500':'133000000'},'2':{'113000500':'133000000'}}
    list0=["090000000",'091500000','093000000']

    dict1['0']={}
    dict1['1']={'230000500':list0[int(TradTime[0])]}
    dict1['2']={'233000500':list0[int(TradTime[0])]}
    dict1['3']={'010000500':list0[int(TradTime[0])]}
    dict1['4']={'023000500':list0[int(TradTime[0])]}

    tempdict=dict(dict0[TradTime[0]].items()+dict1[TradTime[1]].items())
    try:
        return tempdict[time]
    except:
        return ""

def specialdata(hms,TradTime):
    temp=dict()
    temp['0']=['090000000','103000000','133000000']
    temp['1']=['091500000','130000000']
    temp['2']=['093000000','130000000']
    if hms in temp[TradTime[0]]:
        return True
    else:
        return False

def Myappend(csv_data,i,col,TradingDay,InstrumentId):
    newcol = []
    newcol.append(TradingDay)
    newcol.append(InstrumentId)
    for column in columns:
        newcol.append(csv_data.at[i, column])
    col.append(tuple(newcol))

def IshaveFile(name, fileDirectory, close, method):
    """
   判断文件是否存在
   :param name: 
   :param fileDirectory: 
   :param close: 
   :param method: 
   :return: 
    """""
    fileDirectory = fileDirectory.split("\\")
    fileDirectory[fileDirectory.index("DATA")] = "cleanedData"
    fileDirectory = "\\".join(fileDirectory)
    filename=fileDirectory + "\\" + name

    try:
        file_backup = open(filename, method)
    except Exception, e:
        file_backup = open(filename, method)
        print e.message
    if close:
        file_backup.close()
    return file_backup

#读取数据
def  Level_1_clean(filename, fileDirectory, info):
    print filename,fileDirectory
    #源文件名字
    daylist=[]
    templist=[]
    temp=0
    theoryhms=""
    lasthms="-1"
    col=[]
    try:
        csv_data=pd.read_csv(fileDirectory+"\\"+file)
        t=fileDirectory.split("\\")
        TradingDay=t[len(t)-1]
    except Exception, e:
        print  "u'源数据文件不存在,或请先关闭文件",info.cleanDatadict[0],info.cleanDatadict[1]
        return
    length = len(csv_data) - 1
    backup_name =info.cleanDatadict[1]+"_"+info.cleanDatadict[0]+".csv"
    backup_csvfile = IshaveFile(backup_name, fileDirectory, False, "wb")
    writer = csv.writer(backup_csvfile)
    code=info.cleanDatadict[1]
    sql = """ select [DayTradTime],[NightTradTime] from ContractCode where InstrumentCode='%s'""" % code
    TradTime = info.mysql.ExecQuery(sql)[0]
    starthms, endhms = GetSEndHms(TradTime)

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
                    if lasthms == endhms:
                        # daylist = [InstrumentId, TradingDay, csv_data.at[i, 'TotalVol'], csv_data.at[i, 'TotalAmount'],csv_data.at[i, 'Price2'],(csv_data.at[i,'SP1']+csv_data.at[i,'BP1'])/2, csv_data.at[i, 'Price3'],csv_data.at[i, 'LastClose']]
                        break
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
    # backup_csvfile.close()
    #print templist
    # InsertFutureData(templist,tuple(daylist),insertFuturesql,insertDayFuturesql)
    # if len(daylist) > 0:
    #     info.mysql.ExecmanyNonQuery(insertDayFuturesql, tuple(daylist))
    # if len(templist) > 0:
    #     info.mysql.ExecmanysNonQuery(insertFuturesql, templist)



