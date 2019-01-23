import json

from django.http import HttpResponse
from django.views.generic.base import View
from  elasticsearch import Elasticsearch
from django.shortcuts import HttpResponse, render, redirect
from search.models import Article

client=Elasticsearch(hosts=["127.0.0.1"])
# Create your views here
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
                "from": 0,
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
        hit_list=[]
        total_nums=response["hits"]["total"]
        hit_list
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
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]
            hit_list.append(hit_dict)

        return render(request,"result.html",{"all_hits":hit_list,"key_words":key_words,
                                             "total_nums":total_nums })