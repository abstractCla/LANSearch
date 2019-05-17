import json
from django.shortcuts import render
from django.views.generic.base import View
from Search.models import NewsType
from django.http import HttpResponse
from elasticsearch import Elasticsearch
from datetime import datetime

# import redis

# 初始化Elasticsearch连接
client = Elasticsearch(hosts=["47.94.110.27"])


# Create your views here.
class SearchSuggestForKeyWod(View):
    def get(self, request):
        key_words = request.GET.get('s', '')
        re_datas = set()
        if key_words:
            s = NewsType.search()
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest", "fuzzy": {
                    "fuzziness": 1
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                re_datas.add(match.text)
                # source = match._source
                # re_datas.append(source["title"])
        return HttpResponse(json.dumps(list(re_datas)), content_type="application/json")


class SearchSuggest(View):
    def get(self, request):
        key_words = request.GET.get('s', '')
        re_datas = []
        if key_words:
            s = NewsType.search()
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest", "fuzzy": {
                    "fuzziness": 1
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                source = match._source
                re_datas.append(source["title"])
        return HttpResponse(json.dumps(re_datas), content_type="application/json")


class SearchView(View):
    def get(self, request):
        key_words = request.GET.get("q", "")
        # s_type = request.GET.get("s_type", "article")
        # redis_cli.zincrby("search_keywords_set", key_words)
        # topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=5)
        page = request.GET.get("p", "1")
        try:
            page = int(page)
        except:
            page = 1

        # start_time = datetime.now()
        response = client.search(
            index="xidian",
            body={
                "query": {
                    "function_score": {
                        "query": {
                            "multi_match": {
                                "query": key_words,
                                "fields": ["title^2", "content", "source"]
                            }
                        },
                        "field_value_factor": {
                            "field": "click_num",
                            "modifier": "log1p"
                        }
                    }
                },
                "min_score": 10,
                "from": (page - 1) * 10,
                "size": 10,
                "highlight": {
                    "pre_tags": ['<span class="keyWord">'],
                    "post_tags": ['</span>'],
                    "fields": {
                        "title": {},
                        "content": {},
                        "source": {},
                    }
                }
            }
            # body={
            #     "query": {
            #         "multi_match": {
            #             "query": key_words,
            #             "fields": ["title^2", "content", "source"]
            #         }
            #     },
            #     "from": (page - 1) * 10,
            #     "size": 10,
            #     # 高亮处理
            #     "highlight": {
            #         "pre_tags": ['<span class="keyWord">'],
            #         "post_tags": ['</span>'],
            #         "fields": {
            #             "title": {},
            #             "content": {},
            #             "source": {},
            #         }
            #     }
            # }
        )

        # end_time = datetime.now()
        # last_time = (end_time - start_time).total_seconds()
        last_time = response["took"] / 1000
        # 计算相应时间
        total_nums = response["hits"]["total"]
        if (page % 10) > 0:
            page_nums = int(total_nums / 10) + 1
        else:
            page_nums = int(total_nums / 10)
        hit_list = []
        hit_set = {}
        for hit in response["hits"]["hits"]:
            hit_dict = {}
            if "title" in hit["highlight"]:
                hit_dict["title"] = "".join(hit["highlight"]["title"])
            else:
                hit_dict["title"] = hit["_source"]["title"]
            if "content" in hit["highlight"]:
                hit_dict["content"] = "".join(hit["highlight"]["content"])[:500]
            else:
                hit_dict["content"] = hit["_source"]["content"][:500]

            hit_dict["date"] = hit["_source"]["date"]
            hit_dict["source"] = hit["_source"]["source"]
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]
            if hit["_source"]["url"] not in hit_set:
                hit_list.append(hit_dict)
            else:
                hit_set.add(hit["_source"]["url"])
        return render(request, "result.html", {"page": page,
                                               "all_hits": hit_list,
                                               "key_words": key_words,
                                               "total_nums": total_nums,
                                               "page_nums": page_nums,
                                               "last_time": last_time})
        # "jobbole_count":jobbole_count,
        # "topn_search":topn_search})
