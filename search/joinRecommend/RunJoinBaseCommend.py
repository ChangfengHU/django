#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/2 0:03
# @Author  : changfeng
from  search.joinRecommend import GetJoinBaseData
import sys
import random
def joinRecommend():
    # outfile_BS=GetJoinBaseData.getBSDate()
    # outfile_ZP=GetJoinBaseData.getZPDate(outfile_BS)
    # outfile_DP=GetJoinBaseData.getDPDate(outfile_ZP)
    # output_TTI=GetJoinBaseData.getTokenTOIdDate(outfile_DP)
    output_UIS=GetJoinBaseData.getUISIdDate()
    output_IUS=GetJoinBaseData.getIUSIdDate(output_UIS)
    output_UISAverage=GetJoinBaseData.getUISAverageDate(output_IUS)
    output_ItemTOItemSimilar=GetJoinBaseData.getItemTOItemSimilarDate(output_UISAverage)
    output_IBindIScore=GetJoinBaseData.getIBindIScoreDate(output_ItemTOItemSimilar)
    output_IIS=GetJoinBaseData.getIISIdDate(output_IBindIScore)
    output_IBindIScore=GetJoinBaseData.getIISGroupDate(output_IIS)


    # recommend=GetBaseData.getRamdomTitle()
    # print ("点击的文章:")
    # for title in recommend:
    #     print (title)
    # print ("推荐的文章:")
    # recommend_list= GetBaseData.testResult(recommend)
    # return recommend_list

if __name__ == '__main__':
    joinRecommend()