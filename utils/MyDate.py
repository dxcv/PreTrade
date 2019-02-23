# -*- coding: utf-8 -*-
# @Time    : 2019/2/23 11:46
# @Author  : ZouJunLin
"""python date deal"""
import datetime

class MyDate:
    def __init__(self):
        self.now=datetime.datetime.now()

    def Get_PremonthB(self,date):
        """Get Preview month first day"""
        temp=date.strftime("%Y%m")+"01"
        temp=datetime.datetime.strptime(temp,"%Y%m%d")-datetime.timedelta(days=1)
        temp = temp.strftime("%Y%m") + "01"
        return datetime.datetime.strptime(temp,"%Y%m%d")

    def GetPremonthE(self,date):
        """Get Preview month end day"""
        temp = date.strftime("%Y%m") + "01"
        temp = datetime.datetime.strptime(temp, "%Y%m%d") - datetime.timedelta(days=1)
        return temp