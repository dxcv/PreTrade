# -*- coding: utf-8 -*-
# @Time    : 2019/5/17 11:37
# @Author  : ZouJunLin


from utils.InfoApi import *
import datetime
from utils.TradingDay import  NextTradingDay
from Level1Clean import *
from utils.BasicAPI import *
import threading
directory=os.getcwd()+"\data"




if __name__ == '__main__':


    storeDirectory="D:/GitData/PreTrade/src/optionDataClean/CleanedData/"
    dataDiretory=""
    abspath = os.path.abspath(directory)
    info = InfoApi()
    info.GetDbHistoryConnect()
    t = NextTradingDay.TradingDay(info)
    for filenames in os.walk(abspath):
        print filenames
        if not os.path.exists("./CleanedData"):
            os.mkdir("./CleanedData")
        ## 新建清洗后的数据的存储路径
        for i in filenames[1]:
            path = filenames[0].replace(abspath, "") + "\\" + i + "\\"
            path = os.getcwd() + "\\CleanedData" + path
            if not os.path.exists(path):
                os.mkdir(path)
        fileDirectory = filenames[0]
        for filename in filenames[2]:
            filename = filename.decode(encoding='gbk')
            if str(filename).endswith("csv"):
                # data = pd.read_csv(fileDirectory + "\\" + filename)

                info.cleanDatadict = [filename.split("_")[1].replace(".csv",""), None, filename.split("_")[0],storeDirectory ]
                Level_1_Clean(filename, fileDirectory, info)