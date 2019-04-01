# -*- coding: utf-8 -*-
# @Time    : 2019/3/28 21:51
# @Author  : ZouJunLin
import datetime

def nextdurationTime(time,TradTime):
    """计算时间断层问题，将时间链接起来"""
    dict1=dict()
    dict0={'0':{"1016":'1030','1131':'1330'},'1':{'1131':'1330'},'2':{'1131':'1330'}}
    list0=["0905",'0915','0930']

    dict1['0']={}
    dict1['1']={'2301':list0[int(TradTime[0])]}
    dict1['2']={'2331':list0[int(TradTime[0])]}
    dict1['3']={'0056':list0[int(TradTime[0])]}
    dict1['4']={'0231':list0[int(TradTime[0])]}

    tempdict=dict(dict0[TradTime[0]].items()+dict1[TradTime[1]].items())
    try:
        return tempdict[time]
    except:
        return ""

def theorynextdate(lasthms,TradeTime):
    """
    #理论上下一个时间点的数,当然有时间范围限制
    :param lasthms:
    :return:
    """
    lasthms=str(lasthms).replace(":","")[:4]

    time=str('%04d'%(int(lasthms)+1))
    if time[2:4]=="60":
        hour=int(time[:2])
        minute=int(time[2:4])
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
        time=str(hour)+str(minute)
    if isintimerange(time,TradeTime):
        time=time[:2]+":"+time[2:4]+":"+"00"
        return time
    else:
        time=nextdurationTime(time,TradeTime)
        if time!="":
            time = time[:2] + ":" + time[2:4] + ":" + "00"
        return time

def mystrptime(hms):
    return datetime.datetime.strptime(hms.strip(),"%H%M%S%f")

def isintimerange(hms,TradTime):
    hms=str(hms.strip()+"00000").strip()

    try:
        if TradTime[0]=='0' and TradTime[1]=='0':
            if mystrptime(hms) >= mystrptime("090000000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("145500000"):
                return True
            else:
                return False
        elif TradTime[0]=='0' and TradTime[1]=='1':
            if mystrptime(hms) >= mystrptime("090500000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("145500000"):
                return True
            elif mystrptime(hms) >= mystrptime("210500000") and mystrptime(hms) <= mystrptime("225500000"):
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
            if mystrptime(hms) >= mystrptime("090500000") and mystrptime(hms) <= mystrptime("101500000"):
                return True
            elif mystrptime(hms) >= mystrptime("103000000") and mystrptime(hms) <= mystrptime("113000000"):
                return True
            elif mystrptime(hms) >= mystrptime("133000000") and mystrptime(hms) <= mystrptime("145500000"):
                return True
            elif mystrptime(hms) >= mystrptime("210500000") and mystrptime(hms) <= mystrptime("235959500"):
                return True
            elif mystrptime(hms) >= mystrptime("000000000") and mystrptime(hms) <= mystrptime("005500000"):
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
        print e.message,hms,type(hms),"循环里面时间溢出，正常情况"
        return False