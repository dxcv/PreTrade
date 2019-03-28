# -*- coding: utf-8 -*-
# @Time    : 2019/2/18 15:49
# @Author  : ZouJunLin

"""主力合约数据处理脚本"""
from utils.InfoApi import *
import datetime
from utils.TradingDay import  NextTradingDay
from Level5Clean import *
from Level1Clean import *
from utils.BasicAPI import *

def Getdirector(info):
    if info.cleanDatadict[1] in info.setting.level5:
        return "T:/level_5/"+info.cleanDatadict[0]+"/","DC"+info.cleanDatadict[2]+".csv",5
    else:
        return "T:/level_1/"+info.cleanDatadict[0]+"/",info.cleanDatadict[2]+"_"+info.cleanDatadict[0]+".csv",1

def CleanData(info):
    directory,filename,type=Getdirector(info)
    print directory,filename
    if type==5:
        """type   5 大商所五档行情        1   一档行情"""
        Level_5_clean(filename, directory, info)
    elif type==1:
        Level_1_Clean(filename, directory, info)

if __name__=='__main__':

    ProductCodeList=['ni','au','ag','rb','i','m','TA']
    # ProductCodeList = ['ni']
    StartDay="20180801"
    # StartDay = "20190201"
    storeDirectory="D:/DATA/MainIstrument/some/"

    """Read SqlServer Get Main InstrumentID by ProductCode"""
    pass

    info = InfoApi()
    info.GetDbHistoryConnect()

    t = NextTradingDay.TradingDay(info)
    # enddate = datetime.datetime.now()-datetime.timedelta(days=1)
    enddate = datetime.datetime.strptime("20190201","%Y%m%d")
    for i in tqdm(ProductCodeList):
        StartDay = "20180801"
        startdate = datetime.datetime.strptime(StartDay, "%Y%m%d")
        while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
            print startdate
            IsExistdiretory(startdate.strftime("%Y%m%d"),storeDirectory)
            MainInstrument=info.GetMainInstrumentIdByProductCode(i,startdate.strftime("%Y-%m-%d"))
            info.cleanDatadict = [startdate.strftime("%Y%m%d"), i,MainInstrument,storeDirectory+startdate.strftime("%Y%m%d")+"/"]
            CleanData(info)
            startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
            startdate = datetime.datetime.strptime(startdate, "%Y%m%d")

    info.mysql.Disconnect()

