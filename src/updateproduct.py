# -*- coding: utf-8 -*-
# @Time    : 2019/1/4 17:06
# @Author  : ZouJunLin
"""更新交易品种数据库"""
from utils.sqlServer import *
from utils.InfoApi import *
from utils.Mysplider import *
from utils.BasicAPI import *

def GetCZCEProduct(mysplider,info,ExchangeID):
    temp={}
    url=info.GetExchangeWebsite(ExchangeID)
    html=mysplider.getUrlcontent(url,info.GetExchangeHeader(ExchangeID))
    bs = BeautifulSoup(html, "html.parser")
    topnypz = bs.find("div",class_="topnypz").find("ul").findAll("a",href=True)+bs.find("div",class_="fnypz").find("ul").findAll("a",href=True)
    for i in topnypz:
        InstrumentName = str(i.get_text().encode('utf-8')).strip()
        if InstrumentName=='菜籽油':
            InstrumentName="菜油"
        elif InstrumentName=="菜籽粕":
            InstrumentName="菜粕"
        elif InstrumentName=="油菜籽":
            InstrumentName="菜籽"
        elif InstrumentName=="晚籼稻":
            InstrumentName="晚稻"
        href=url+i['href'].encode("utf-8").strip()
        if not basic.StrinList(InstrumentName, existlist):
            mysql.ExecNonQuery(insertsql % (ExchangeID, InstrumentName, href))
        else:
            mysql.ExecNonQuery(updatesql % (href, InstrumentName))
        print InstrumentName, href


def GetDCEProduct(mysplider, info,ExchangeID):
    temp={}
    url = info.GetExchangeWebsite(ExchangeID)
    header= info.GetExchangeHeader(ExchangeID)
    html = mysplider.getUrlcontent(url,header)
    bs = BeautifulSoup(html, "html.parser")
    topnypz = bs.find("div", class_="pzzx_box01").find("ul").findAll("a", href=True) + bs.find("div", class_="pzzx_box03").find("ul").findAll("a", href=True)
    for i in topnypz:
        InstrumentName=str(i.get_text().encode('utf-8')).strip()
        if InstrumentName=='黄大豆1号':
            InstrumentName="豆一"
        elif InstrumentName=='黄大豆2号':
            InstrumentName="豆二"
        url1=url + i['href'].encode("utf-8").strip()
        html=mysplider.getUrlcontent(url1, header)
        bs = BeautifulSoup(html, "html.parser")
        href=url+str(bs.find("ul",class_="list_tpye").find("a",href=True)['href'].encode("utf-8")).strip()
        if not basic.StrinList(InstrumentName, existlist):
            mysql.ExecNonQuery(insertsql % (ExchangeID, InstrumentName, href))
        else:
            mysql.ExecNonQuery(updatesql % (href, InstrumentName))
        print InstrumentName,href



def GetSHFEProduct(mysplider, info, ExchangeID):
    temp = {}
    ExchangeIDurl = info.GetExchangeWebsite(ExchangeID)
    # url="http://www.shfe.com.cn/js/mainmenu.js"
    html = mysplider.getUrlcontent(ExchangeIDurl, info.GetExchangeHeader(ExchangeID))
    bs = BeautifulSoup(html, "lxml")
    topnypz = bs.find("div",id="tabs").findAll("option")[:-1]
    for i in topnypz:
        url1=ExchangeIDurl+"/products/"+str(i['value'].encode("utf-8")).strip().lower()
        InstrumentName=str(i.get_text().encode("utf-8")).strip()
        if InstrumentName=="天胶":
            InstrumentName="天然橡胶"
        html = mysplider.getUrlcontent(url1, info.GetExchangeHeader(ExchangeID))
        bs=BeautifulSoup(html,"html.parser")
        href=ExchangeIDurl+str(bs.find("div","heyue_big").find("a")['href'].encode("utf-8")).strip()
        if not basic.StrinList(InstrumentName,existlist):
            mysql.ExecNonQuery(insertsql%(ExchangeID,InstrumentName,href))
        else:
            mysql.ExecNonQuery(updatesql%(href,InstrumentName))
        print InstrumentName,href

def GetCFFEXProduct(mysplider, info, ExchangeID):
    url = info.GetExchangeWebsite(ExchangeID)
    html = mysplider.getUrlcontent(url, info.GetExchangeHeader(ExchangeID))
    bs = BeautifulSoup(html, "lxml")
    topnypz = bs.find("div", class_="nav_product_div_left").findAll("a", href=True)
    for i in topnypz:
        InstrumentName = str(i.get_text().encode("utf-8")).strip()[:-12]
        print InstrumentName
        href= url + i['href']
        if not basic.StrinList(InstrumentName, existlist):
            mysql.ExecNonQuery(insertsql % (ExchangeID, InstrumentName, href))
        else:
            mysql.ExecNonQuery(updatesql % (href, InstrumentName))
    topnypz = bs.find("div",class_="nav_product_div_right").findAll("a", href=True)
    for i in topnypz:
        InstrumentName = str(i.get_text().encode("utf-8")).strip()[:-6]
        print InstrumentName
        href = url + i['href']
        if not basic.StrinList(InstrumentName, existlist):
            mysql.ExecNonQuery(insertsql % (ExchangeID, InstrumentName, href))
        else:
            mysql.ExecNonQuery(updatesql % (href, InstrumentName))

if __name__=='__main__':
    Instrument={}
    temp={}
    info=InfoApi()
    basic=BasicAPI()
    mysplider=MySplider()
    account=info.GetDbHistoryDataAccount()
    mysql=Mysql(account[0],account[1],account[2],account[3])
    existsql="select [InstrumentName] from [PreTrade].[dbo].[ContractCode]"
    insertsql="insert into [PreTrade].[dbo].[ContractCode](ExchangeID,[InstrumentName],[Website]) values('%s','%s','%s')"
    updatesql="update   [PreTrade].[dbo].[ContractCode] set [Website]='%s' where InstrumentName='%s'"
    existlist=mysql.ExecQueryGetList(existsql)
    ExchangeList=info.GetAllExchange()
    for i in ExchangeList:
        url=info.GetExchangeWebsite(i)
        if i =='CZCE':
            GetCZCEProduct(mysplider,info,i)
        elif i=='DCE':
            GetDCEProduct(mysplider, info, i)
        elif i=='SHFE':
            GetSHFEProduct(mysplider, info, i)
        elif i=='CFFEX':
            pass
            GetCFFEXProduct(mysplider, info, i)