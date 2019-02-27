# -*- coding: utf-8 -*-
# @Time    : 2019/2/22 17:56
# @Author  : ZouJunLin
"""MyXlwt 自己的版本"""
import xlwt
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


class MyXlwt:
    def __init__(self):
        self.style=xlwt.XFStyle()



    def GetMystyleTitle(self):
        al = xlwt.Alignment()

        al.horz = 0x02  # 设置水平居中
        al.vert = 0x01  # 设置垂直居中
        self.style.alignment = al
        self.style.alignment.wrap = 1
        return self.style


    def GetstyleRed(self):
        pattern = xlwt.Pattern()
        pattern.pattern = xlwt.Pattern.SOLID_PATTERN
        pattern.pattern_fore_colour = xlwt.Style.colour_map['red']  # 设置单元格背景色为红色
        self.style.pattern = pattern
        self.style.alignment.wrap = 1
        self.style.alignment.horz=0x02
        self.style.alignment.vert=0x01

        self.style.borders.left=4
        self.style.borders.right=4
        self.style.borders.top=4
        self.style.borders.bottom=4

        return self.style

    def set_style(self,name, height, bold=False):

        font = xlwt.Font()  # 为样式创建字体
        font.name = name  # 'Times New Roman'
        font.bold = bold
        font.color_index = 4
        font.height = height

        # borders= xlwt.Borders()
        # borders.left= 6
        # borders.right= 6
        # borders.top= 6
        # borders.bottom= 6

        self.style.font = font
        # style.borders = borders

        return style