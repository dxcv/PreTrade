# -*- coding: utf-8 -*-
# @Time    : 2019/2/19 15:40
# @Author  : ZouJunLin
"""数据处理的管用方法"""
import datetime


def  GetSEndHms(TradTime,firstdata):
    """获取合约的交易开始时间"""
    startime={'0':['090000000','091500000','093000000']}
    endtime=['150000000','151500000','150000000']
    if TradTime[1]=='0':
        """No Night Trade"""
        return startime['0'][int(TradTime[0])],endtime[int(TradTime[0])]
    if firstdata<"09:00:00":
        return startime['0'][int(TradTime[0])], endtime[int(TradTime[0])]
    else:
        return '210000000',endtime[int(TradTime[0])]

def IshaveFile(name, fileDirectory, close, method):
    """
   判断文件是否存在
   :param name: 
   :param fileDirectory: 
   :param close: 
   :param method: 
   :return: 
    """""
    filename=fileDirectory + name

    try:
        file_backup = open(filename, method)
    except Exception, e:
        file_backup = open(filename, method)
        print e.message
    if close:
        file_backup.close()
    return file_backup

def mystrptime(hms):
    return datetime.datetime.strptime(hms.strip(),"%H%M%S%f")

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
    temp['0']=['090000000','101500','103000000','113000','133000000']
    temp['1']=['091500000','113000','130000000']
    temp['2']=['093000000','113000','130000000']

    night=dict()
    night['1']=['230000']
    night['2']=['233000']
    night['3']=['010000']
    night['4']=['023000']
    if hms in temp[TradTime[0]]:
        return True
    if hms in night[TradTime[1]]:
        return True

    return False



