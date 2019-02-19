# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 9:44
# @Author  : ZouJunLin
import os
from utils.InfoApi import  *
import csv
import pandas as pd


a="2019/2/14 20:59:00"
firstdata="8:59:00"
firstdata=datetime.datetime.strptime(str(a), '%Y/%m/%d %H:%M:%S').strftime("%H:%M:%S")
if firstdata < "09:00:00":
    print firstdata