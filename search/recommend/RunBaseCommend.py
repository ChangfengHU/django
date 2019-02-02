#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/2 0:03
# @Author  : changfeng
from  search.recommend import GetBaseData
import sys
import random
def recommend(recommend):
    # outfile_BS=GetBaseData.getBSDate()
    # outfile_ZP=GetBaseData.getZPDate(outfile_BS)
    # outfile_DP=GetBaseData.getDPDate(outfile_ZP)
    # outfile_DPGP=GetBaseData.getDPGroupDate(outfile_DP)
    # output_TTI=GetBaseData.getTokenTOIdDate(outfile_DP)
    # output_RESULT=GetBaseData.getResultAllIdDate()
    print ("内容推荐的文章:")
    recommend_list= GetBaseData.testResult(recommend)
    return recommend_list
def JoinRecommend(recommend):
    print ("内容推荐的文章:")
    recommend_list= GetBaseData.testXietongResult(recommend)
    return recommend_list

def testRecommend():
    # outfile_BS=GetBaseData.getBSDate()
    # outfile_ZP=GetBaseData.getZPDate(outfile_BS)
    # outfile_DP=GetBaseData.getDPDate(outfile_ZP)
    # outfile_DPGP=GetBaseData.getDPGroupDate(outfile_DP)
    # output_TTI=GetBaseData.getTokenTOIdDate(outfile_DP)
    # output_RESULT=GetBaseData.getResultAllIdDate()
    recommend=GetBaseData.getOwnRamdomTitle()
    print ("点击的文章:")
    for title in recommend:
        print (title)
    print ("内容推荐的文章:")
    recommend_list= GetBaseData.testOwnResult(recommend)

    print("协同推荐的文章:")
    recommend_list= GetBaseData.testOwnXietongResult(recommend)
    return recommend_list

if __name__ == '__main__':
    testRecommend()