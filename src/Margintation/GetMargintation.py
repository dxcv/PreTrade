# -*- coding: utf-8 -*-
# @Time    : 2019/1/16 16:02
# @Author  : ZouJunLin
"""获取部分公司的保证金标准"""
# 1、    银河  GetYhqh(info)
# 2、    中信
# 3、    申银万国
# 4、    渤海
# 5、    东航




from utils.InfoApi import *
from data.setting import *
import re
import datetime
from utils.TradingDay import *
from utils.BasicAPI import *
import json
YhqhUrl="https://www.yhqh.com.cn/list-431-1.html"
SywgUrl="http://www.sywgqh.com.cn/Pc/Common?page=1&rows=10&topicCode=Cover_Cost"
zhongxinUrl="https://www.citicsf.com/e-futures/csc/000205/article/list"
bohaiUrl="http://www.bhfcc.com/customer-center-tool_cid_47.html"
# donghangUrl="https://www.cesfutures.com/page/gsgg/#/index/jyfl/bzj"
donghangUrl="https://www.cesfutures.com/RESTfull/pz/bzj/list.json"

def main(info):
    # templist=GetSywg(info)
    # templist=GetYhqh(info)
    # templist=GetZhongxin(info)
    # templist=GetBohai(info,'2')
    templist=GetDonghang(info)
    # info.mysql.Disconnect()


def GetDonghang(info):
    Type='3'
    tabledict={'CZCE':0,'SHFE':1,'DCE':2,'CFFEX':3,'ine':4}
    tempdict=dict()
    header = {
        'Accept': 'text/html, application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Cache-Control': 'no-cache',
        'Content-Language': 'zh - CN',
        'Host': 'www.cesfutures.com',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
        'Pragma': 'no - cache',
    }
    html = info.mysplider.getUrlcontent(donghangUrl, header=header)
    jsondata=json.loads(html)
    tempdata=jsondata['data']['table']
    if datetime.datetime.now().strftime("%H%M%S") < "180000":
        date = datetime.datetime.now()
    else:
        date = (datetime.datetime.now() + datetime.timedelta(days=1))
    for i in tempdata:
        tempi=i['list']
        for j in tempi:
            code =str(j['1'].encode("utf-8")).strip().split(" ")[0]
            margintation = float(str(j['4'].encode("utf-8")).strip().replace("%", "")) / 100
            tempdict[code] =margintation
            # downuplimit=str(j['6'].encode("utf-8")).strip()
            # print code,margintation,downuplimit

    FutureInstrumentList = info.GetAllTradeInstrumentByTradingDay(date.strftime("%Y%m%d"))
    print len(FutureInstrumentList)
    templist = list()
    for i in FutureInstrumentList.keys():
        ExchangeId = FutureInstrumentList[i]
        code, num = info.GetDetailByInstrumentID(i, ExchangeId)
        margintation = tempdict[code]
        col = [date.strftime("%Y-%m-%d"), i, ExchangeId, margintation]
        templist.append(col)
    info.mysql.UpdateMarginExample(templist, Type)

def GetBohai(info,Type):
    tempdict=dict()
    header = {
        'Accept': 'text/html, application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Cache-Control': 'no-cache',
        'Content-Language': 'zh - CN',
        'Cookie': 'CITICSFID=5e234530-a085-4b66-93cf-0802b2fdd97a; CITICF_SESSION_EFUTURRES=C1ED2755FB3344115793DA52A14BC2AA; __jsluid=3a1e4391da46135c95e69a023c26201b; Hm_cv_eb9b2943105704fc985fd700527c1a9e=*!1*userType*%E6%B8%B8%E5%AE%A2; Hm_lvt_eb9b2943105704fc985fd700527c1a9e=1548990403,1548990557,1549002085; Hm_lpvt_eb9b2943105704fc985fd700527c1a9e=1549003175',
        'Host': 'www.bhfcc.com',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'http://www.bhfcc.com/customer-center-treaty_cid_49.html',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
        'Pragma': 'no - cache',
    }
    if datetime.datetime.now().strftime("%H%M%S")<"180000":
        date=datetime.datetime.now()
    else:
        date=(datetime.datetime.now()+datetime.timedelta(days=1))
    html = info.mysplider.getUrlcontent(bohaiUrl, header=header)
    bs = BeautifulSoup(html, "html.parser")
    divdata = bs.find("table")
    divdata=divdata.findAll("tr")[1:]
    for i in divdata[:-1]:
        td=i.findAll("td")
        try:
            product=td[-2].get_text()
            # product=re.findall(r'[(|（](.*?)[)|）]',product)[0]
            product = re.findall(r'[a-zA-Z]+', product)[0]
            margintation=float(str(td[-1].get_text()).strip().replace("%",""))/100
            tempdict[product]=margintation
        except:
            pass
    # for i in tempdict:
    #     print i,tempdict[i]

    FutureInstrumentList=info.GetAllTradeInstrumentByTradingDay(date.strftime("%Y%m%d"))
    print len(FutureInstrumentList)
    templist=list()
    for i in FutureInstrumentList.keys():
        ExchangeId = FutureInstrumentList[i]
        code, num = info.GetDetailByInstrumentID(i, ExchangeId)
        margintation=tempdict[code]
        col = [date.strftime("%Y-%m-%d"), i, ExchangeId, margintation]
        templist.append(col)
    info.mysql.UpdateMarginExample(templist, Type)




def  GetZhongxin(info):
    header = {
        'Accept': 'text/html, application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Request Method':'GET',
        'Cache-Control':'no - cache',
        'Content-Language': 'zh - CN',
        'Cookie':'CITICSFID=5e234530-a085-4b66-93cf-0802b2fdd97a; CITICF_SESSION_EFUTURRES=C1ED2755FB3344115793DA52A14BC2AA; __jsluid=3a1e4391da46135c95e69a023c26201b; Hm_cv_eb9b2943105704fc985fd700527c1a9e=*!1*userType*%E6%B8%B8%E5%AE%A2; Hm_lvt_eb9b2943105704fc985fd700527c1a9e=1548990403,1548990557,1549002085; Hm_lpvt_eb9b2943105704fc985fd700527c1a9e=1549003175',
        'Host': 'www.citicsf.com',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.citicsf.com/e-futures/csc/000205/article/list',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
        'Pragma':'no - cache',
    }
    html = info.mysplider.getUrlcontent(zhongxinUrl, header=header)
    bs = BeautifulSoup(html, "html.parser")
    content = bs.find("div", id="Section1").find("li").find("a",href=True)
    href="https://"+header['Host']+content['href']
    date = re.findall(r'\d+', str(content.get_text()).strip())[0].encode("utf-8")
    print href,date

def GetSywg(info):
    tempdict={}
    templist=[]
    header = {
        'Host': 'www.yhqh.com.cn',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
    }
    html = info.mysplider.getUrlcontent(SywgUrl, header=header)
    bs = BeautifulSoup(html, "html.parser")
    content=bs.find("ul",id="newsUl").find("li")
    href="http://www.sywgqh.com.cn"+content.find("a",href=True)['href']
    content=content.find("a",href=True).get_text()
    nowdate=re.findall(r'\d+', content)[0].encode("utf-8")
    # if not  date==datetime.datetime.now().strftime("%Y%m%d"):
    #     date=nowdate
    html = info.mysplider.getUrlcontent(href, header=header)
    bs = BeautifulSoup(html, "html.parser")
    content = bs.find("table",class_="ke-zeroborder").findAll("tr")
    for tr in content:
        tempdata=tr.findAll("td")[1:]
        col=[]
        tempdata=tempdata
        for j in range(len(tempdata)):
            if j not in [1,2]:
                col.append(str(tempdata[j].get_text().encode("utf-8")).strip())
        templist.append(col)
    templist=templist[:-1]
    BasicAPI().MyPrint(templist)
    tdict=BasicAPI().TwoList2Dict(templist)
    reduceDict(info,tdict,nowdate,'1')

def reduceDict(info,tdict,date,type):
    """处理爬取的dict数据，写入数据库"""
    templist=[]
    FutureInstrumentList=info.GetAllTradeInstrumentByTradingDay(date)
    date=str(date)[:4]+"-"+str(date)[4:6]+"-"+str(date[-2:])
    for i in FutureInstrumentList.keys():
        ExchangeId = FutureInstrumentList[i]
        code,num=info.GetDetailByInstrumentID(i,ExchangeId)
        try:
            margintation=str(tdict[code][num]).split(" ")[1]
        except:
            temp=info.GetAnotherInstrumentByInstrument(i)
            code, num = info.GetDetailByInstrumentID(temp, ExchangeId)
            margintation = str(tdict[code][num]).split(" ")[1]
        col=[date,i,ExchangeId,float(margintation)/100]
        templist.append(col)
    info.mysql.UpdateMarginExample(templist,type)

def GetYhqh(info):
    templist=[]
    header={
        'Host': 'www.yhqh.com.cn',
        'Upgrade - Insecure - Requests': '1',
        'User - Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
    }
    html=info.mysplider.getUrlcontent(YhqhUrl,header=header)
    bs = BeautifulSoup(html, "html.parser")
    divdata = bs.find("div",class_="zi_list").findAll("a")[0]
    url=divdata['href']
    text=divdata.get_text()
    date="".join(map(lambda x:str(x.encode("utf-8")).zfill(2),re.findall(r'\d+', text)))
    date = str(date)[:4] + "-" + str(date)[4:6] + "-" + str(date[-2:])
    templist=GetYhqhContent(info,date,url)

def GetYhqhContent(info,date,url):
    header={}
    html = info.mysplider.getUrlcontent(url, header=header)
    table=info.mysplider.tableTolist(html,"")
    templist=[]
    for i in table[1:-2]:
        try:
            a=float("".join(re.findall(r'[1-9.]', str(i[0]).strip())))
            if isinstance(a,float):
                temp=str(i[1]).strip().split("；") if str(i[1]).find("；")!=-1 else [str(i[1]).strip()]
                for j in temp:
                    if j in  info.setting.ProductName.keys():
                        tempname=info.setting.ProductName[j]
                    else:
                        tempname=j
                    # print tempname
                    tempcode=info.GetCodeByName(tempname)
                    if str(tempname).strip().find("(") != -1:
                        print "---------------------", tempname
                        if not "specialInstrumentId" in info.tempdata.keys():
                            info.tempdata['specialInstrumentId'] = dict()
                        info.tempdata['specialInstrumentId'].append(tempname)
                    elif str(tempname).strip().find("（") != -1:
                        print "---------------------", tempname
                        str(tempname).strip().replace("（","(").replace("）",")")
                        if not "specialInstrumentId" in info.tempdata.keys():
                            info.tempdata['specialInstrumentId'] = dict()
                        info.tempdata['specialInstrumentId'].append(tempname)
                        # print "---------------------", tempname
                        # specialMonth = re.findall(r'\d+', str(i[1]).strip())[0].encode("utf-8")
                        # specialInstrument =GetInstrumentIdByCodeDate(tempcode[0],tempcode[1],specialMonth)
                        # if not "specialInstrumentId" in info.tempdata.keys():
                        #     info.tempdata['specialInstrumentId']=dict()
                        # if not specialInstrument in info.tempdata['specialInstrumentId']:
                        #     print "---------------------",specialInstrument
                        #     info.tempdata['specialInstrumentId'].append(specialInstrument)
                    col = [date, tempcode[0], tempcode[1], float(i[0]) / 100]
                    print date, tempcode[0], tempcode[1], float(i[0]) / 100
                    templist.append(col)
        except:
            continue
    # print info.tempdata['specialInstrumentId']
    # info.mysql.UpdateMarginExample(templist, '2')

def GetInstrumentIdByCodeDate(code,ExchangeId,specialMonth):
    if ExchangeId=='CZCE':
        return code+str(specialMonth)[1:].zfill(3)
    else:
        return code+specialMonth

if __name__=='__main__':
    info=InfoApi()
    info.Get_Msplider()
    info.Get_BasicApi()
    main(info)
