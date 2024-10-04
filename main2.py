from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import List
from datetime import datetime
from elasticsearch import Elasticsearch
from collections import defaultdict
from dotenv import load_dotenv
import os
from fuzzywuzzy import fuzz
import asyncio
import time
from datetime import datetime

load_dotenv()

app = FastAPI()
def read_white_list(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f)

# Đọc white_list từ file khi khởi động ứng dụng
white_list = read_white_list('white_list.txt')

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

async def call_with_retry(func, request, response, max_retries=5, timeout=1.5):
    for attempt in range(max_retries):
        try:
            if attempt < max_retries - 2:
                print(attempt)
                return await asyncio.wait_for(func(request, response), timeout)
            
            else:
                return await func(request, response)
        except asyncio.TimeoutError:
            print(f"Thử lại lần {attempt + 1}, vì quá thời gian chờ 1.5 giây")
        except Exception as e:
            print(f"Lỗi khác trong lần thử {attempt + 1}: {e}")

        await asyncio.sleep(0.5)

    raise Exception("Không thể kết nối sau tối đa số lần thử lại.")


@app.post("/search_keywords")
async def search_keywords_api(request: SearchRequest, response: Response):
    result =  await call_with_retry(search_keywords, request, response)
    return result

async def search_keywords(request: SearchRequest, response: Response):
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
        start_query_time = time.time()

        es_response = es_db.search(index=index_name, body={"query": query, "size": 10000},  timeout="60s" )
        end_query_time = time.time()
        query_duration = end_query_time - start_query_time
        print(f"Thời gian query Elasticsearch: {query_duration:.2f} giây")

        hits = es_response['hits']['hits']
        start_calc_time = time.time()

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
                    keyword_aggregate.items(),
                    key=lambda x: (
                        x[1]['top_20_days'], 
                        x[1]['trends'], 
                        x[1]['record']
                    ),
                    reverse=True
                )

                # Bước 2: Tách riêng các từ khóa không có dấu gạch dưới và không nằm trong white_list
                final_list = []
                non_white_list = []

                for k, v in sorted_keywords:
                    if "_" not in k and  k not in white_list:
                        non_white_list.append((k,v))  # Từ khóa không thỏa mãn điều kiện
                    else:
                        final_list.append((k,v))  # Từ khóa thỏa mãn điều kiện

                final_list.extend(non_white_list)
                final_list = [(k.replace('_', ' '), v) for k, v in final_list]

                sorted_keywords = final_list

                # sorted_keywords = sorted(
                #                             {k.replace('_', ' '): v for k, v in keyword_aggregate.items()}.items(),
                #                             key=lambda x: (x[1]['top_20_days'], x[1]['trends'], x[1]['record']),
                #                             reverse=True
                #                         )

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
            # "sum_records": sum(v["record"] if isinstance(v, dict) else v for _, v in similar_keywords),
            "sum_records": len(similar_keywords),

            "data": data
        }
        end_calc_time = time.time()
        calc_duration = end_calc_time - start_calc_time
        print(f"Thời gian tính toán: {calc_duration:.2f} giây")

        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": "Internal server error", "error": str(e)}



@app.post("/aggregate_keywords")
async def aggregate_keywords_api(request: KeywordRequest, response: Response):
    result =  await call_with_retry(aggregate_keywords, request, response)
    return result

async def aggregate_keywords(request: KeywordRequest, response: Response):
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
        start_query_time = time.time()

        es_response = es_db.search(index=index_name, body={"query": query, "size": 200})
        end_query_time = time.time()
        query_duration = end_query_time - start_query_time
        print(f"Thời gian query Elasticsearch: {query_duration:.2f} giây")

        hits = es_response['hits']['hits']
        start_calc_time = time.time()

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
            # "sum_records": sum(v["record"] if request.type_top == "trend" else v for v in keyword_aggregate.values()),
            "sum_records": len(keyword_aggregate),

            "data": data
        }
        end_calc_time = time.time()
        calc_duration = end_calc_time - start_calc_time
        print(f"Thời gian tính toán: {calc_duration:.2f} giây")

        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}


@app.post("/aggregate_hashtag")
async def aggregate_hashtag_api(request: HashtagRequest, response: Response):
    result =  await call_with_retry(aggregate_hashtag, request, response)
    return result

async def aggregate_hashtag(request: HashtagRequest, response: Response):
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
        start_query_time = time.time()

        es_response = es_db.search(index="keyword_a", body={"query": query, "size": 10000})
        end_query_time = time.time()
        query_duration = end_query_time - start_query_time
        print(f"Thời gian query Elasticsearch: {query_duration:.2f} giây")

        hits = es_response['hits']['hits']
        start_calc_time = time.time()

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
            # "sum_records": sum(hashtag_aggregate.values()),
            "sum_records": len(sorted_hashtags),

            "data": [{"hashtag": k, "total_record": v} for k, v in paged_hashtags]
        }
        end_calc_time = time.time()
        calc_duration = end_calc_time - start_calc_time
        print(f"Thời gian tính toán: {calc_duration:.2f} giây")

        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}
@app.post("/search_hashtag")
async def search_hashtag_api(request: SearchRequesthastag, response: Response):
    result = await call_with_retry(search_hashtag, request, response)
    return result

async def search_hashtag(request: SearchRequesthastag, response: Response):
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
        start_query_time = time.time()

        es_response = es_db.search(index="keyword_a", body={"query": query, "size": 10000})
        hits = es_response['hits']['hits']
        start_calc_time = time.time()

        end_query_time = time.time()
        query_duration = end_query_time - start_query_time
        print(f"Thời gian query Elasticsearch: {query_duration:.2f} giây")

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

        data = [{"hashtag": k, "total_record": v["record"] if isinstance(v, dict) else v} for k, v in paged_hashtags]

        result = {
            "status_code": 200,
            "message": "OK",
            # "sum_records": sum(v["record"] if isinstance(v, dict) else v for _, v in similar_hashtags),
            "sum_records": len(similar_hashtags),

            "data": data
        }

        end_calc_time = time.time()
        calc_duration = end_calc_time - start_calc_time
        print(f"Thời gian tính toán: {calc_duration:.2f} giây")



        return result

    except Exception as e:
        response.status_code = 500
        return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
