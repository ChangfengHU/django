#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/1 23:43
# @Author  : changfeng
import json
from django.http import HttpResponse
from django.views.generic.base import View
from  elasticsearch import Elasticsearch
from django.shortcuts import HttpResponse, render, redirect
from search.models import Article
from datetime import datetime
from search.recommend import RunBaseCommend,GetBaseData
client=Elasticsearch(hosts=["127.0.0.1"])
# Create your views here
class IndexView(View):
    """首页get请求top-n排行榜"""
    @staticmethod
    def get(request):
        topn_search_clean = []
        # topn_search = redis_cli.zrevrangebyscore(
        #     "search_keywords_set", "+inf", "-inf", start=0, num=5)
        recommend = GetBaseData.getRamdomTitle()
        topn_search=["java","学习","php"]
        topn_search=recommend
        for topn_key in topn_search:
            # topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean
        print("点击的文章:")
        for title in recommend:
            print(title)
        recommend_list=RunBaseCommend.recommend(recommend)
        JoinRecommend_list=RunBaseCommend.JoinRecommend(recommend)
        return render(request, "index.html", {"topn_search": topn_search,"recommend_list":recommend_list,"JoinRecommend_list":JoinRecommend_list})



class SearchSuggest(View):
    def get(self,request):
        key_words=request.GET.get("s",'')
        re_date=[]
        if key_words:
            search=Article.search()

            source = search.suggest('my_suggest', key_words, completion={"field":"suggest",
                "fuzzy":{
                    "fuzziness":2
                },
                "size":10})
            suggestions=source.execute()
            for match in suggestions.suggest.my_suggest[0].options:
                source=match._source
                re_date.append(source["title"])

            return  HttpResponse(json.dumps(re_date),content_type="application/json")

class SearchView(View):
    def get(self,request):
        key_words=request.GET.get("q",'')
        page=request.GET.get("p",'1')
        try:
            page=int(page)
        except:
            page=1
        start_time= datetime.now()
        response=client.search(
            index="jobbole",
            body={
                "query": {
                    "multi_match": {
                        "query":key_words,
                        "fields":[
                            "title",
                            "tags",
                            "content"
                        ]
                    }
                },
                "from": (page-1)*10,
                "size": 10,
                "highlight": {
                    "pre_tags": [
                        "<span class='keyWord'>"
                    ],
                    "post_tags": [
                        "</span>"
                    ],
                    "fields": {
                        "title": { },
                        "content": { },
                        "tags": {}
                    }
                }
            }
        )
        end_time = datetime.now()
        last_seconds=(end_time-start_time).total_seconds()
        hit_list=[]
        total_nums=response["hits"]["total"]
        if (page % 10)>0:
            page_nums=int(total_nums/10)+1
        else:
            page_nums = int(total_nums / 10)
        for hit in response["hits"]["hits"]:
            hit_dict={}
            if "title"  in hit["highlight"]:
                hit_dict["title"]="".join(hit["highlight"]["title"])
            else:
                hit_dict["title"] = hit["_source"]["title"]
            if "content"  in hit["highlight"]:
                hit_dict["content"]="".join(hit["highlight"]["content"][:500])
            else:
                hit_dict["content"] = hit["_source"]["content"][:500]
            hit_dict["create_date"]=hit["_source"]["create_date"]
            hit_dict["front_image_url"]=hit["_source"]["front_image_url"][0]
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]
            hit_list.append(hit_dict)

        return render(request,"result.html",{"all_hits":hit_list,"key_words":key_words,
                                             "last_seconds":last_seconds,"page_nums":page_nums,"total_nums":total_nums,"page":page })