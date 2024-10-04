from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import List
from datetime import datetime
from elasticsearch import Elasticsearch
from collections import defaultdict
from dotenv import load_dotenv
import os
from fuzzywuzzy import fuzz

load_dotenv()

app = FastAPI()

# Kết nối tới Elasticsearch
elasticsearch_db_url = os.getenv("ELASTICSEARCH_DB_URL")
es_db = Elasticsearch([elasticsearch_db_url], request_timeout=100)

# Mô hình dữ liệu yêu cầu từ người dùng
class KeywordRequest(BaseModel):
    type: List[str]
    start_date: str
    end_date: str
    type_top: str
    page: int
    page_size: int
    topic_ids: List[str]
class HashtagRequest(BaseModel):
    type: List[str]
    start_date: str
    end_date: str
    page: int
    page_size: int
    topic_ids: List[str]
class SearchRequest(BaseModel):
    keyword: str
    type: List[str]
    start_date: str
    end_date: str
    type_top: str
    page: int
    page_size: int
    topic_ids: List[str]
    
class SearchRequesthastag(BaseModel):
    hashtag: str
    type: List[str]
    start_date: str
    end_date: str
    page: int
    page_size: int
    topic_ids: List[str]

@app.post("/search_keywords/")
def search_keywords(request: SearchRequest, response: Response):
    try:
        if request.type_top not in ["popular", "trend"]:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid 'type_top' value provided. Choose 'popular' or 'trend'.", "sum_records": 0, "data": []}

        try:
            start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
            end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid date format, please use MM/DD/YYYY", "sum_records": 0, "data": []}

        index_name = "keyword_a" if request.type_top == "popular" else "keyword_a_trend_test"

        query = {
            "bool": {
                "must": [
                    {"terms": {"type": request.type}},
                    {"range": {"date": {"gte": start_date.strftime("%m_%d_%Y"), "lte": end_date.strftime("%m_%d_%Y")}}}
                ]
            }
        }
        if request.topic_ids:
            if index_name == "keyword_a":
                should_conditions = [{"exists": {"field": f"topic_ids.{topic_id}"}} for topic_id in request.topic_ids]
                query["bool"]["must"].append({"bool": {"should": should_conditions}})
            else:
                nested_should_conditions = [
                    {"exists": {"field": f"topic_ids.{topic_id}"}}
                    for topic_id in request.topic_ids
                ]
                query["bool"]["must"].append({
                    "nested": {
                        "path": "topic_ids",
                        "query": {
                            "bool": {
                                "should": nested_should_conditions
                            }
                        }
                    }
                })

        es_response = es_db.search(index=index_name, body={"query": query, "size": 10000})
        hits = es_response['hits']['hits']

        if request.type_top == "popular":
            keyword_aggregate = defaultdict(int)
            for hit in hits:
                if request.topic_ids:
                    for topic_id in request.topic_ids:
                        keywords_top = hit['_source']['topic_ids'].get(topic_id, {}).get('keywords_top', [])
                        for keyword_data in keywords_top:
                            keyword_aggregate[keyword_data['keyword']] += keyword_data['record']
                else:
                    keywords_top = hit['_source']['topic_ids'].get('all', {}).get('keywords_top', [])
                    for keyword_data in keywords_top:
                        keyword_aggregate[keyword_data['keyword']] += keyword_data['record']
        else:
            keyword_aggregate = defaultdict(lambda: {"record": 0, "top_20_days": 0, "trends": 0})
            for hit in hits:
                for topic_id, keywords in hit['_source']['topic_ids'].items():
                    if request.topic_ids and topic_id not in request.topic_ids:
                        continue
                    if request.topic_ids == [] and topic_id != "all":
                        continue

                    for i, keyword_data in enumerate(keywords):
                        keyword = keyword_data['keyword']
                        keyword_aggregate[keyword]["record"] += keyword_data['record']
                        if i < 20:  
                            keyword_aggregate[keyword]["top_20_days"] += 1
                        if keyword_data.get("isTrend", False):
                            keyword_aggregate[keyword]["trends"] += 1

        if request.type_top == "popular":
            # sorted_keywords = sorted(keyword_aggregate.items(), key=lambda x: x[1], reverse=True)
            sorted_keywords = sorted(
                                        {k.replace('_', ' '): v for k, v in keyword_aggregate.items()}.items(),
                                        key=lambda x: x[1], 
                                        reverse=True
                                    )

        else:
            # sorted_keywords = sorted(
            #     keyword_aggregate.items(),
            #     key=lambda x: (x[1]['top_20_days'], x[1]['trends'], x[1]['record']),
            #     reverse=True
            # )
                sorted_keywords = sorted(
                                            {k.replace('_', ' '): v for k, v in keyword_aggregate.items()}.items(),
                                            key=lambda x: (x[1]['top_20_days'], x[1]['trends'], x[1]['record']),
                                            reverse=True
                                        )

        if request.keyword:
            similar_keywords = [(k, v) for k, v in sorted_keywords if fuzz.ratio(request.keyword, k) > 90]
        else:
            similar_keywords = sorted_keywords
            # similar_keywords = [(k, v) for k, v in sorted_keywords]

        start_index = (request.page - 1) * request.page_size
        end_index = start_index + request.page_size
        paged_keywords = similar_keywords[start_index:end_index]

        data = [{"keyword": k.replace('_', ' '), "total_record": v["record"] if isinstance(v, dict) else v} for k, v in paged_keywords]

        result = {
            "status_code": 200,
            "message": "OK",
            "sum_records": sum(v["record"] if isinstance(v, dict) else v for _, v in similar_keywords),
            "data": data
        }

        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": "Internal server error", "error": str(e)}

# # API trả về dữ liệu theo yêu cầu
# @app.post("/aggregate_keywords/")
# def aggregate_keywords(request: KeywordRequest, response: Response):
#     try:
#         # Kiểm tra giá trị type_top
#         if request.type_top not in ["popular", "trend"]:
#             response.status_code = 400
#             return {"status_code": 400, "message": "Invalid 'type_top' value provided. Choose 'popular' or 'trend'.", "sum_records": 0, "data": []}

#         # Chuyển đổi định dạng ngày và kiểm tra
#         try:
#             start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
#             end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
#             start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
#             end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
#         except ValueError:
#             response.status_code = 400
#             return {"status_code": 400, "message": "Invalid date format, please use MM/DD/YYYY", "sum_records": 0, "data": []}

#         # Tạo truy vấn Elasticsearch
#         query = {
#             "bool": {
#                 "must": [
#                     {"terms": {"type": request.type}},
#                     {"range": {"date": {"gte": start_date.strftime("%m_%d_%Y"), "lte": end_date.strftime("%m_%d_%Y")}}}
#                 ]
#             }
#         }

#         # Thêm điều kiện cho topic_ids nếu không rỗng
#         if request.topic_ids:
#             should_conditions = [{"exists": {"field": f"topic_ids.{topic_id}"}} for topic_id in request.topic_ids]
#             query["bool"]["must"].append({"bool": {"should": should_conditions}})

#         # Thực hiện truy vấn Elasticsearch
#         es_response = es_db.search(index="keyword_a", body={"query": query, "size": 10000})
#         hits = es_response['hits']['hits']

#         # Xử lý dữ liệu từ Elasticsearch
#         keyword_aggregate = defaultdict(int)
#         for hit in hits:
#             if request.topic_ids:
#                 for topic_id in request.topic_ids:
#                     keywords_top = hit['_source']['topic_ids'].get(topic_id, {}).get('keywords_top', [])
#                     for keyword_data in keywords_top:
#                         keyword_aggregate[keyword_data['keyword']] += keyword_data['record']
#             else:
#                 # Nếu topic_ids là mảng rỗng, lấy dữ liệu từ key "all"
#                 keywords_top = hit['_source']['topic_ids'].get('all', {}).get('keywords_top', [])
#                 for keyword_data in keywords_top:
#                     keyword_aggregate[keyword_data['keyword']] += keyword_data['record']

#         # Sắp xếp kết quả theo số lần xuất hiện
#         sorted_keywords = sorted(keyword_aggregate.items(), key=lambda x: x[1], reverse=True)

#         # Phân trang kết quả
#         start_index = (request.page - 1) * request.page_size
#         end_index = start_index + request.page_size
#         paged_keywords = sorted_keywords[start_index:end_index]

#         result = {
#             "status_code": 200,
#             "message": "OK",
#             "sum_records": sum(keyword_aggregate.values()),
#             "data": [{"keyword": k, "total_record": v} for k, v in paged_keywords]
#         }

#         return result

#     except Exception as e:
#         response.status_code = 500
#         return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}

# API trả về dữ liệu theo yêu cầu

# API trả về dữ liệu theo yêu cầu

@app.post("/aggregate_keywords/")
def aggregate_keywords(request: KeywordRequest, response: Response):
    try:
        if request.type_top not in ["popular", "trend"]:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid 'type_top' value provided. Choose 'popular' or 'trend'.", "sum_records": 0, "data": []}

        try:
            start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
            end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid date format, please use MM/DD/YYYY", "sum_records": 0, "data": []}

        index_name = "keyword_a" if request.type_top == "popular" else "keyword_a_trend_test"

        # query = {
        #     "bool": {
        #         "must": [
        #             {"terms": {"type": request.type}},
        #             {"range": {"date": {"gte": start_date.strftime("%m_%d_%Y"), "lte": end_date.strftime("%m_%d_%Y")}}}
        #         ]
        #     }
        # }

        # if request.topic_ids:
        #     should_conditions = [{"exists": {"field": f"topic_ids.{topic_id}"}} for topic_id in request.topic_ids]
        #     query["bool"]["must"].append({"bool": {"should": should_conditions}})
        query = {
            "bool": {
                "must": [
                    {"terms": {"type": request.type}},
                    {"range": {"date": {"gte": start_date.strftime("%m_%d_%Y"), "lte": end_date.strftime("%m_%d_%Y")}}}
                ]
            }
        }
        if request.topic_ids:
            if index_name == "keyword_a":
                
                    should_conditions = [{"exists": {"field": f"topic_ids.{topic_id}"}} for topic_id in request.topic_ids]
                    query["bool"]["must"].append({"bool": {"should": should_conditions}})

            else:
                    nested_should_conditions = [
                        {"exists": {"field": f"topic_ids.{topic_id}"}}
                        for topic_id in request.topic_ids
                    ]
                    query["bool"]["must"].append({
                        "nested": {
                            "path": "topic_ids",
                            "query": {
                                "bool": {
                                    "should": nested_should_conditions
                                }
                            }
                        }
                    })

        es_response = es_db.search(index=index_name, body={"query": query, "size": 200})
        hits = es_response['hits']['hits']

        if request.type_top == "popular":
            keyword_aggregate = defaultdict(int)
            for hit in hits:
                if request.topic_ids:
                    for topic_id in request.topic_ids:
                        keywords_top = hit['_source']['topic_ids'].get(topic_id, {}).get('keywords_top', [])
                        for keyword_data in keywords_top:
                            keyword_aggregate[keyword_data['keyword']] += keyword_data['record']
                else:
                    keywords_top = hit['_source']['topic_ids'].get('all', {}).get('keywords_top', [])
                    for keyword_data in keywords_top:
                        keyword_aggregate[keyword_data['keyword']] += keyword_data['record']
        else:
            keyword_aggregate = defaultdict(lambda: {"record": 0, "top_20_days": 0, "trends": 0})
            for hit in hits:
                for topic_id, keywords in hit['_source']['topic_ids'].items():
                    if request.topic_ids and topic_id not in request.topic_ids:
                        continue
                    if request.topic_ids == [] and topic_id != "all":
                        continue

                    for i, keyword_data in enumerate(keywords):
                        keyword = keyword_data['keyword']
                        keyword_aggregate[keyword]["record"] += keyword_data['record']
                        if i < 20: 
                            keyword_aggregate[keyword]["top_20_days"] += 1
                        if keyword_data.get("isTrend", False):
                            keyword_aggregate[keyword]["trends"] += 1

        if request.type_top == "popular":
            sorted_keywords = sorted(keyword_aggregate.items(), key=lambda x: x[1], reverse=True)
        else:
            sorted_keywords = sorted(
                keyword_aggregate.items(),
                key=lambda x: (x[1]['top_20_days'], x[1]['trends'], x[1]['record']),
                reverse=True
            )

        start_index = (request.page - 1) * request.page_size
        end_index = start_index + request.page_size
        paged_keywords = sorted_keywords[start_index:end_index]

        if request.type_top == "trend":
            data = [
                {
                    # "keyword": k,
                    "keyword": k.replace('_', ' '),
                    "total_record": v["record"],
                    # "top_20_days": v["top_20_days"],
                    # "trends": v["trends"]
                }
                for k, v in paged_keywords
            ]
        else:
            data = [{"keyword": k.replace('_', ' '), "total_record": v} for k, v in paged_keywords]

        result = {
            "status_code": 200,
            "message": "OK",
            "sum_records": sum(v["record"] if request.type_top == "trend" else v for v in keyword_aggregate.values()),
            "data": data
        }

        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}


@app.post("/aggregate_hashtag/")
def aggregate_hashtags(request: HashtagRequest, response: Response):
    try:
        # if request.type_top not in ["popular", "trend"]:
        #     response.status_code = 400
        #     return {"status_code": 400, "message": "Invalid 'type_top' value provided. Choose 'popular' or 'trend'.", "sum_records": 0, "data": []}

        try:
            start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
            end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid date format, please use MM/DD/YYYY", "sum_records": 0, "data": []}

        query = {
            "bool": {
                "must": [
                    {"terms": {"type": request.type}},
                    {"range": {"date": {"gte": start_date.strftime("%m_%d_%Y"), "lte": end_date.strftime("%m_%d_%Y")}}}
                ]
            }
        }

        if request.topic_ids:
            should_conditions = [{"exists": {"field": f"topic_ids.{topic_id}"}} for topic_id in request.topic_ids]
            query["bool"]["must"].append({"bool": {"should": should_conditions}})

        es_response = es_db.search(index="keyword_a", body={"query": query, "size": 10000})
        hits = es_response['hits']['hits']

        hashtag_aggregate = defaultdict(int)
        for hit in hits:
            if request.topic_ids:
                for topic_id in request.topic_ids:
                    hashtags_top = hit['_source']['topic_ids'].get(topic_id, {}).get('hashtags_top', [])
                    for hashtag_data in hashtags_top:
                        hashtag_aggregate[hashtag_data['hashtag']] += hashtag_data['record']
            else:
        
                hashtags_top = hit['_source']['topic_ids'].get('all', {}).get('hashtags_top', [])
                for hashtag_data in hashtags_top:
                    hashtag_aggregate[hashtag_data['hashtag']] += hashtag_data['record']

    
        sorted_hashtags = sorted(hashtag_aggregate.items(), key=lambda x: x[1], reverse=True)

    
        start_index = (request.page - 1) * request.page_size
        end_index = start_index + request.page_size
        paged_hashtags = sorted_hashtags[start_index:end_index]

        result = {
            "status_code": 200,
            "message": "OK",
            "sum_records": sum(hashtag_aggregate.values()),
            "data": [{"hashtag": k, "total_record": v} for k, v in paged_hashtags]
        }

        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}
@app.post("/search_hashtag/")
def search_hashtags(request: SearchRequesthastag, response: Response):
    try:
        try:
            start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
            end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid date format, please use MM/DD/YYYY", "sum_records": 0, "data": []}

        query = {
            "bool": {
                "must": [
                    {"terms": {"type": request.type}},
                    {"range": {"date": {"gte": start_date.strftime("%m_%d_%Y"), "lte": end_date.strftime("%m_%d_%Y")}}}
                ]
            }
        }

        if request.topic_ids:
            should_conditions = [{"exists": {"field": f"topic_ids.{topic_id}"}} for topic_id in request.topic_ids]
            query["bool"]["must"].append({"bool": {"should": should_conditions}})

        es_response = es_db.search(index="keyword_a", body={"query": query, "size": 10000})
        hits = es_response['hits']['hits']

        hashtag_aggregate = defaultdict(int)
        for hit in hits:
            if request.topic_ids:
                for topic_id in request.topic_ids:
                    hashtags_top = hit['_source']['topic_ids'].get(topic_id, {}).get('hashtags_top', [])
                    for hashtag_data in hashtags_top:
                        hashtag_aggregate[hashtag_data['hashtag']] += hashtag_data['record']
            else:
                hashtags_top = hit['_source']['topic_ids'].get('all', {}).get('hashtags_top', [])
                for hashtag_data in hashtags_top:
                    hashtag_aggregate[hashtag_data['hashtag']] += hashtag_data['record']

        sorted_hashtags = sorted(hashtag_aggregate.items(), key=lambda x: x[1], reverse=True)
        if request.hashtag:
            similar_hashtags = [(k, v) for k, v in sorted_hashtags if fuzz.ratio(request.hashtag, k) > 65]
        else:
            similar_hashtags = sorted_hashtags


        start_index = (request.page - 1) * request.page_size
        end_index = start_index + request.page_size
        paged_hashtags = similar_hashtags[start_index:end_index]

        data = [{"keyword": k, "total_record": v["record"] if isinstance(v, dict) else v} for k, v in paged_hashtags]

        result = {
            "status_code": 200,
            "message": "OK",
            "sum_records": sum(v["record"] if isinstance(v, dict) else v for _, v in similar_hashtags),
            "data": data
        }




        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    