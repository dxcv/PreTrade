# -*- coding: utf-8 -*-
# @Time    : 2018/12/17 15:22
# @Author  : ZouJunLin

from pandas import DataFrame,Series
import datetime

def GetDf1(df,gep,TradTime):
    TradTime=['0','0']
    starthms, endhms = GetSEndHms(TradTime)

    df1=DataFrame([],columns=['bid1','ask1','midpx','bid11','ask11','midpx1','dif','Correlation','difavg'])
    startTime=starthms
    i=0
    tempdf=DataFrame([],columns=['bid1','ask1','midpx','bid11','ask11','midpx1','dif'])
    while mystrptime(startTime) !=mystrptime(endhms):
        try:
            temp=df.loc[int(startTime)]
            i+=1
            tempdf = tempdf.append(temp)
            tempdf.at[int(startTime), 'dif'] = int(tempdf.at[int(startTime ), 'midpx']) - int(tempdf.at[int(startTime), 'midpx1'])
            df1 = df1.append(temp)
            df1.at[int(startTime),'dif']=int( df1.at[int(startTime),'midpx'])-int( df1.at[int(startTime),'midpx1'])
            if i == 30:
                i=0
                Correlation = tempdf.midpx.corr(tempdf.midpx1)
                if str(Correlation).strip()=="":
                    Correlation=-1
                df1.at[int(startTime), 'Correlation'] = Correlation
                df1.at[int(startTime),'difavg']=tempdf['dif'].mean()
                tempdf = DataFrame([], columns=[ 'bid1', 'ask1', 'midpx', 'bid11', 'ask11', 'midpx1', 'dif'])
        except:
            pass
        startTime = GetaddTime(startTime, gep,TradTime,endhms)
    return df1

def GetaddTime(time,gep,TradTime,endhms):
    temp=2*gep
    i=0
    while i<temp:
        try:
            time=theorynextdate(time,TradTime)
            i+=1
        except:
            return endhms
    return time

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

# def Myappend(csv_data,i,col,TradingDay,InstrumentId):
#     newcol=[]
#     # newcol.append(TradingDay)
#     # newcol.append(InstrumentId)
#     for column in columns:
#         newcol.append(csv_data.at[i,column])
#     col.append(tuple(newcol))


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