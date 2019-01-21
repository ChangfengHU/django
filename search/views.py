from django.shortcuts import render
from django.views.generic.base import View
from search.models import Article
from django.http import HttpResponse
import json
# Create your views here
class SearchSuggest(View):
    def get(self,request):
        key_words=request.get("s",'')
        re_date=[]
        if key_words:
            search=Article.search()

            source = search.suggest('my_suggest', 'pyton', completion={"field":"suggest",
                "fuzzy":{
                    "fuzziness":2
                },
                "size":10})
            suggestions=source.execute()
            for match in suggestions.my_suggest[0].options:
                source=match._source
                re_date.append(source["title"])

            return  HttpResponse(json.dump(re_date),content_type="application/json")