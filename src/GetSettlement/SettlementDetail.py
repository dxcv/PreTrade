# -*- coding: utf-8 -*-
# @Time    : 2018/11/5 15:49
# @Author  : ZouJunLin
"""
4大交易所手续费写入一个数据库中
"""

from utils.TradingDay.NextTradingDay import *
from utils.InfoApi import *
from SettlementDetailAPI import *
reload(sys)
from utils.Mysplider import *
sys.setdefaultencoding('utf-8')

hreflist={}
daylist=[]

def GetCFFEXFutureSettlementDetail(TradingDay, infoapi):
    url = "http://www.cffex.com.cn/jscs/"
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Host': 'www.cffex.com.cn',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }
    sql = """    
             INSERT INTO [dbo].[SettlementDetail]([TradingDay],[InstrumentID],[ExchangeID],[SettlementPrice],[IsFutures],[IsShort],[MarginMethod],[MarginSpeculateBuy],[MarginSpeculateSale],[MarginHedgingBuy],
               [MarginHedgingSale],[SettlementMethod],[SettlementOpen],[SettlementClose],[SettlementOpenShort],[SettlementCloseShort],[DeliveMethod],[DeliveCharge],[CloseTodayMethod],[CloseTodayPrice])
           VALUES('%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           """


    j = 0
    print url
    if len(daylist) == 0:
        html = infoapi.mysplider.getUrlcontent(url, header=headers)
        if not len(html):
            print "中金所结算数据爬取失败，请重新运行！"
            return
        soup = BeautifulSoup(html, "lxml")
        templist = soup.findAll("a", class_="list_a_text")
        for href in templist:
            day = href.text
            day = list(str(day.encode("utf-8")).strip())[-11:-3]
            day.insert(4, "-")
            day.insert(7, "-")
            day = "".join(day)
            hreflist[day] = "http://www.cffex.com.cn" + href['href']
            daylist.append(day)
        daylist.sort(reverse=False)
    TradingDaytemp = datetime.datetime.strftime(TradingDay, "%Y-%m-%d")
    # ##从中金所给出的时间中选出一个正确的时间
    daylisttemp = []
    for i in daylist:
        if i <= TradingDaytemp:
            daylisttemp.append(i)

    csvurl = hreflist[daylisttemp[len(daylisttemp) - 1]]
    print csvurl
    try:
        open("./data/" + str(daylisttemp[len(daylisttemp) - 1]) + ".txt", "r")
    except:
        html = infoapi.mysplider.getUrlcontent(csvurl, header=headers)
        file = open("./data/" + str(daylisttemp[len(daylisttemp) - 1]) + ".txt", "w")
        file.write(html)
        file.flush()
        file.close()

    SettlementPrice = GetCFFEXFutureSettlementPrice(TradingDay.strftime("%Y-%m-%d"),infoapi)
    Instrumentlist=GetCFFEXDayInstrument(TradingDay.strftime("%Y-%m-%d"), infoapi)
    #raise Exception
    sqllist = []
    addlist=[]
    for line in open("./data/" + str(daylisttemp[len(daylisttemp) - 1]) + ".txt", "r"):
        col = []
        klist = line.strip().split(',')

        if j != 0 and j != 1 and j != "" and len(line) != 1:
            if str(klist[0]).encode('utf-8').strip() in Instrumentlist:
                for k in klist:
                    temp = k.replace(",", "").strip()
                    if temp.find("元/手") != -1:
                        temp = temp.replace("元/手", "")
                        col.append(0)
                    elif temp.find("万分之") != -1:
                        temp = temp.replace("万分之", "")
                        col.append(1)
                    if temp.find(".") == 0:
                        temp = "0" + temp
                    if temp.find("%") != -1:
                        temp = temp.replace("%", "")
                        temp = str(float(temp) / 100)
                    col.append(temp)
                col.insert(0, TradingDay.strftime("%Y-%m-%d"))
                col.insert(2, "CFFEX")
                col.insert(3, SettlementPrice[col[1]])
                col.insert(4, 1)
                col.insert(5, 0)
                col.insert(6, 1)
                col.insert(9, col[7])
                col.insert(10, col[8])
                col.insert(13, col[12])
                col.insert(14, col[12])
                col.insert(15, col[12])
                col.insert(18, 1)
                sqllist.append(tuple(col))
                addlist.append(str(klist[0]).encode('utf-8').strip())
        j += 1
    infoapi.mysplider.dataToSqlserver(sqllist, sql,infoapi)

    addsql=""" 
        SELECT  top 1 [TradingDay],[InstrumentID],[ExchangeID],[SettlementPrice],[IsFutures]*1,[IsShort]*1,[MarginMethod]*1,[MarginSpeculateBuy],[MarginSpeculateSale],[MarginHedgingBuy],
               [MarginHedgingSale],[SettlementMethod]*1,[SettlementOpen],[SettlementClose],[SettlementOpenShort],[SettlementCloseShort],[DeliveMethod]*1,[DeliveCharge],[CloseTodayMethod]*1,[CloseTodayPrice] FROM [PreTrade].[dbo].[SettlementDetail] where  InstrumentID like '%s' and TradingDay='%s'
    """
    sqllist=[]
    addlist=list(set(Instrumentlist)-set(addlist))
    for i in addlist:
        col=[]
        num = re.search(r"\d+\.?", str(i).strip()).group()
        code = str(i).strip().replace(num, "")+"%"
        col=IsExistData(addsql%(code,TradingDaytemp))
        col=list(col[0])
        col[1]=i
        col[3]=SettlementPrice[i]
        sqllist.append(tuple(col))
    infoapi.mysplider.dataToSqlserver(sqllist, sql,infoapi)





def CFFEXSettlementDetail(TradingDay, infoapi):

    sql1 = """
                   select TradingDay from [PreTrade].[dbo].[SettlementDetail] where TradingDay='%s' and ExchangeID='CFFEX'
               """

    list1 = IsExistData(sql1 % TradingDay.strftime("%Y-%m-%d"),infoapi)
    if len(list1) == 0:
        ###爬取中金所期货信息
        GetCFFEXFutureSettlementDetail(TradingDay, infoapi)
    else:
        print TradingDay, "中金所期货结算结算数据已经存在"


def main(startdate, infoapi):
    CFFEXSettlementDetail(startdate, infoapi)  # 上期所保证金手续费相关信息

    CZCESettlementDetail(startdate, infoapi)              # 郑商所保证金手续费信息

    DCESettlementDetail(startdate, infoapi)              # 大商所保证金手续费信息

    SHFESettlementDetail(startdate, infoapi)              #上期所保证金手续费相关信息



if __name__=="__main__":
    infoapi=InfoApi()
    t = TradingDay(infoapi)
    startdate = datetime.datetime.strptime("20190114", "%Y%m%d")
    enddate = datetime.datetime.now()-datetime.timedelta(days=1)
    while startdate.strftime("%Y%m%d") <= enddate.strftime("%Y%m%d"):
        print startdate
        main(startdate, infoapi)
        startdate = t.NextTradingDay(startdate.strftime("%Y%m%d"), True)
        startdate = datetime.datetime.strptime(startdate, "%Y%m%d")


