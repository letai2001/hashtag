from elasticsearch import Elasticsearch
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

# es = Elasticsearch(["http://172.168.200.202:9200"] , request_timeout=100)
import os
load_dotenv()

# Lấy URL Elasticsearch từ biến môi trường
elasticsearch_url = os.getenv("ELASTICSEARCH_URL")

# Khởi tạo Elasticsearch client
es = Elasticsearch([elasticsearch_url], request_timeout=100)

# current_date = datetime.now()

# # Tính ngày hôm qua (ngày trước của ngày hiện tại)
# yesterday = current_date - timedelta(days=1)

# # Định dạng ngày hôm qua theo định dạng yyyy-mm-dd
# selected_date = yesterday.strftime("%Y-%m-%d")

# # Kết thúc là ngày hiện tại
# end_date = current_date.strftime("%Y-%m-%d")


# # Định dạng lại để sử dụng trong truy vấn, bắt đầu từ 00:00:01 và kết thúc vào 00:00:00 ngày hôm sau
# start_date_str = selected_date.strftime("%Y/%m/%d 00:00:01")
# end_date_str = end_date.strftime("%Y/%m/%d 00:00:00")

list_link = ["https://vnexpress.net", "https://dantri.com.vn", "https://tuoitre.vn"  , "https://vtv.vn"  , "https://vov.vn" , "https://vietnamnet.vn" , 
             
             "https://ictnews.vietnamnet.vn" , "https://infonet.vietnamnet.vn" , "https://nhandan.vn" , "https://chinhphu.vn" , 
             "https://baochinhphu.vn" , "http://bocongan.gov.vn" , "https://baotintuc.vn" , "https://thethaovanhoa.vn" , 
             "https://www.vietnamplus.vn" , "https://thanhnien.vn" , "https://nghiencuuquocte.org" , "https://dangcongsan.vn" , 
             "http://cand.com.vn"  , "http://antg.cand.com.vn"  , "http://antgct.cand.com.vn" , "http://vnca.cand.com.vn" , 
             "http://cstc.cand.com.vn" , "https://nghiencuuchienluoc.org" , "https://bnews.vn" 
             ]
def query_day_2(start_date_str, end_date_str):
    body1 = {
  "_source": ["content" ,  "link" , "created_time" , "key_word"],
  "query": {
    "bool": {
      "must": [ 
        {
          "term": {
            "type.keyword": "youtube comment"
          }
        },
        {
          "range": {
                "created_time": {
                "gte": start_date_str,
                "lte": end_date_str,
                "format" : "MM/dd/uuuu HH:mm:ss"
                }
            }
        }
      ]
    }
  },
  "sort": [
    {
      "_id": "asc"
    }
  ]
}
    result = es.search(index="osint_posts", body=body1 , request_timeout=6000)
    dataFramse_Log = []
    for result_source in result['hits']['hits']:
    #   if contains_any_link(result_source['_source']['link'], list_link):

        dataFramse_Log.append(result_source)
    # Lấy kết quả tiếp theo bằng cách sử dụng search_after
    while len(result["hits"]["hits"]) > 0:
        last_hit = result["hits"]["hits"][-1]
        body1["search_after"] = [last_hit["_id"]]
        try:
            result = es.search(index="osint_posts", body=body1 ,  request_timeout=6000)
            for result_source in result['hits']['hits']:
            #   if contains_any_link(result_source['_source']['link'], list_link):
                dataFramse_Log.append(result_source)
                print(result_source) 
        except Exception as e:
                print(f"Lỗi xảy ra: {str(e)}")
    with open('content_test_newquery.filter.json', 'w', encoding='utf-8') as f:
        json.dump(dataFramse_Log, f, ensure_ascii=False, indent=4)
    return dataFramse_Log

def query_day(start_date_str, end_date_str):
    body1= {
    "query ": {
        "bool": {
            "must": [
                {"term": {"type.keyword": "youtube video"}},
                {
                    "range": {
                        "created_time": {
                            "gte": start_date_str,
                            "lte": end_date_str,
                            "format": "MM/dd/yyyy HH:mm:ss"
                        }
                    }
                }
            ]
        }
    },
    "size": 10000,
    "sort": [
        {
            "created_time": {
                "order": "asc"
            }
        }
    ],
    
    "_source": ["link", "content", "title", "created_time"  , "id"]
}

    # body1 = {
    # "_source": ["title" , "content" , "created_time"],
    # "query": {
    #     "bool": {
    #     "must": [
    #         {
    #         "range": {
    #             "created_time": {
    #             "gte": start_date_str,
    #             "lte": end_date_str,
    #             "format" : "MM/dd/uuuu HH:mm:ss"
    #             }
    #         }
    #         },
    #         {
    #         "term": {
    #             "type.keyword": "electronic media"
    #         }
    #         }
    #     ], 
    #     "should": [
    #         { "term": { "domain.keyword": "https://vnexpress.net" }},
    #         { "term": { "domain.keyword": "https://dantri.com.vn" }},
    #         { "term": { "domain.keyword": "https://tuoitre.vn" }},
    #         { "term": { "domain.keyword": "https://vtv.vn" }},
    #         { "term": { "domain.keyword": "https://vov.vn" }},
    #         { "term": { "domain.keyword": "https://ictnews.vietnamnet.vn" }},
    #         { "term": { "domain.keyword": "https://infonet.vietnamnet.vn" }},
    #         { "term": { "domain.keyword": "https://nhandan.vn" }},
    #         { "term": { "domain.keyword": "https://chinhphu.vn" }},
    #         { "term": { "domain.keyword": "https://baochinhphu.vn" }},
    #         { "term": { "domain.keyword": "http://bocongan.gov.vn" }},
    #         { "term": { "domain.keyword": "https://baotintuc.vn" }},
    #         { "term": { "domain.keyword": "https://thethaovanhoa.vn" }},
    #         { "term": { "domain.keyword": "https://www.vietnamplus.vn" }},
    #         { "term": { "domain.keyword": "https://thanhnien.vn" }},
    #         { "term": { "domain.keyword": "https://nghiencuuquocte.org" }},
    #         { "term": { "domain.keyword": "https://dangcongsan.vn" }},
    #         { "term": { "domain.keyword": "http://cand.com.vn" }},
    #         { "term": { "domain.keyword": "http://antg.cand.com.vn" }},
    #         { "term": { "domain.keyword": "http://antgct.cand.com.vn" }},
    #         { "term": { "domain.keyword": "http://vnca.cand.com.vn" }},
    #         { "term": { "domain.keyword": "http://cstc.cand.com.vn" }},
    #         { "term": { "domain.keyword": "https://nghiencuuchienluoc.org" }},
    #         { "term": { "domain.keyword": "https://bnews.vn" }}

    #     ],
    #     "minimum_should_match": 1
        
    #     }
    # },
    # "sort": [
    #     {
    #     "_id": "asc"
    #     }
    # ]
    # }

    # Lấy kết quả đầu tiên   result = es.search(index="osint_posts", body=body1)   dataFramse_Log = []
    result = es.search(index="posts", body=body1 , request_timeout=6000)
    dataFramse_Log = []
    for result_source in result['hits']['hits']:
    #   if contains_any_link(result_source['_source']['link'], list_link):

        dataFramse_Log.append(result_source)
    # Lấy kết quả tiếp theo bằng cách sử dụng search_after
    while len(result["hits"]["hits"]) > 0:
        last_hit = result["hits"]["hits"][-1]
        body1["search_after"] = [last_hit["_id"]]
        try:
            result = es.search(index="posts", body=body1 ,  request_timeout=6000)
            for result_source in result['hits']['hits']:
            #   if contains_any_link(result_source['_source']['link'], list_link):
                dataFramse_Log.append(result_source)
                print(result_source) 
        except Exception as e:
                print(f"Lỗi xảy ra: {str(e)}")
    with open('content_test_newquery.filter.json', 'w', encoding='utf-8') as f:
        json.dump(dataFramse_Log, f, ensure_ascii=False, indent=4)
    return dataFramse_Log
def query_day_3(start_date_str, end_date_str):
    body1= {
    "query": {
    "bool": {
      "must": [
        {
           "match_phrase": {
            "type": "youtube"
          }
        },
        {
          "range": {
            "created_time": {
              "gte": start_date_str
              , "lte": end_date_str
            }
          }
        }
      ]
    }
  },
    
    "sort": [
        {
            "created_time": {
                "order": "asc"
            }
        }
    ],
    
    "_source": ["content" ,   "created_time" , "title"]
}
    result = es.search(
          index='posts',
          scroll='2m',  # Giữ scroll mở trong 2 phút
          size=100,     # Số lượng records trên mỗi trang
          body=body1
      )
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    # Tạo một list chứa tất cả records
    records = []

    # Lặp qua scroll để lấy tất cả records
    while scroll_size > 0:
        # Thêm records vào list
        records.extend(result['hits']['hits'])
        
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Lưu records vào file JSON
    with open('comment.json', 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)

    return records
def query_spam(start_date_str , end_date_str):
    body1= {
    "query": {
        "bool": {
            "must": [
                {"term":  {
                        "post_classify.keyword": "normal"
                    }},
                {
                    "range": {
                        "created_time": {
                            "gte": start_date_str,
                            "lte": end_date_str,
                            "format": "MM/dd/yyyy HH:mm:ss"
                        }
                    }
                }
               
            ]
        }
    },
    
    "sort": [
        {
            "created_time": {
                "order": "asc"
            }
        }
    ],
    
    "_source": ["content"]
}

    result = es.search(
          index='posts',
          scroll='2m',  # Giữ scroll mở trong 2 phút
          size=100,     # Số lượng records trên mỗi trang
          body=body1
      )
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    # Tạo một list chứa tất cả records
    records = []

    # Lặp qua scroll để lấy tất cả records
    while scroll_size > 0:
        # Thêm records vào list
        records.extend(result['hits']['hits'])
        
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Lưu records vào file JSON
    with open('spam_filter_normal_2.json', 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)

    return records

def query_comment():
    body1= {
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "created_time": "04/18/2024"
          }
        },
        {
          "match_phrase": {
            "type": "youtube"
          }
        }
      ]
    }
  } ,
    
    "_source": [ "content",  "created_time" , "title"]
  
}

    result = es.search(
          index='comments',
          scroll='2m',  # Giữ scroll mở trong 2 phút
          size=100,     # Số lượng records trên mỗi trang
          body=body1
      )
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    # Tạo một list chứa tất cả records
    records = []

    # Lặp qua scroll để lấy tất cả records
    while scroll_size > 0:
        # Thêm records vào list
        records.extend(result['hits']['hits'])
        
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Lưu records vào file JSON
    with open('comment.json', 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)

    return records

def query_comment_by_dates(start_date_dt, end_date_dt):
    # Tạo danh sách các ngày từ start_date_dt đến end_date_dt
    date_range = []
    current_date = start_date_dt
    while current_date <= end_date_dt:
        date_range.append(current_date.strftime("%m/%d/%Y"))
        current_date += timedelta(days=1)

    # Tạo danh sách 'should' cho các ngày trong date_range
    should_clauses = []
    for date_str in date_range:
        should_clauses.append({
            "match_phrase": {
                "created_time": date_str
            }
        })

    # Tạo danh sách 'must' cho điều kiện khác (ví dụ: 'type' là 'youtube')
    must_clauses = [
        {
            "match_phrase": {
                "type": "youtube"
            }
        }
    ]

    # Tạo truy vấn Elasticsearch
    query_body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "should": should_clauses,
                "minimum_should_match": 1
            }
        },
        "_source": ["content", "created_time"]
    }
    result = es.search(
          index='comments',
          scroll='2m',  # Giữ scroll mở trong 2 phút
          size=100,     # Số lượng records trên mỗi trang
          body=query_body
      )
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    # Tạo một list chứa tất cả records
    records = []

    # Lặp qua scroll để lấy tất cả records
    while scroll_size > 0:
        # Thêm records vào list
        records.extend(result['hits']['hits'])
        
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Lưu records vào file JSON
    with open('comment.json', 'a', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)

    return records

def combined_queries( start_date, end_date):
    records = []

    # Truy vấn thứ nhất với 'range' cho created_time
    body1 = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "type": "tiktok"
                        }
                    },
                    {
                        "range": {
                            "created_time": {
                                "gte": start_date.strftime("%m/%d/%Y 00:00:01"),
                                "lte": end_date.strftime("%m/%d/%Y 23:59:59")
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "created_time": {
                    "order": "asc"
                }
            }
        ],
        "_source": ["content", "created_time", "title"]
    }

    # Thực hiện truy vấn và lưu vào records
    result = es.search(
        index='posts',
        scroll='2m',
        size=100,
        body=body1
    )
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    while scroll_size > 0:
        records.extend(result['hits']['hits'])
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Truy vấn thứ hai với 'should' cho từng ngày
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%m/%d/%Y"))
        current_date += timedelta(days=1)

    should_clauses = []
    for date_str in date_range:
        should_clauses.append({
            "match_phrase": {
                "created_time": date_str
            }
        })

    must_clauses = [
        {
            "match_phrase": {
                "type": "tiktok"
            }
        }
    ]

    query_body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "should": should_clauses,
                "minimum_should_match": 1
            }
        },
        "_source": ["content", "created_time", "title"]
    }

    # Thực hiện truy vấn và thêm vào records
    result = es.search(
        index='comments',
        scroll='2m',
        size=100,
        body=query_body
    )

    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    while scroll_size > 0:
        records.extend(result['hits']['hits'])
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Lưu tất cả records vào file JSON
    with open('comment_tiktok.json', 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)

    return records
def query_keyword_with_topic(es, start_date_str, end_date_str, type):
    try:
        # Định nghĩa body cho truy vấn Elasticsearch đầu tiên
        body1 = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"type": type}},
                        {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
                    ]
                }
            },
            "sort": [{"created_time": {"order": "asc"}}],
            "_source": ["keyword", "hashtag", "created_time", "topic_id"]
        }

        # Thực hiện truy vấn đầu tiên
        result = es.search(index='posts', scroll='2m', size=100, body=body1, request_timeout=2000)
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

        records = []

        while scroll_size > 0:
            for hit in result['hits']['hits']:
                # Bổ sung trường 'topic_id' nếu không tồn tại
                if 'topic_id' not in hit['_source']:
                    hit['_source']['topic_id'] = []
                if 'hashtag' not in hit['_source']:
                    hit['_source']['hashtag'] = []

                records.append(hit)

            result = es.scroll(scroll_id=scroll_id, scroll='2m', request_timeout=2000)
            scroll_id = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])

        # Tương tự, định nghĩa và thực hiện truy vấn thứ hai cho index 'comments'
        body2 = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"type": type}},
                        {"exists": {"field": "keywords"}},
                        {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
                    ]
                }
            },
            "sort": [{"created_time": {"order": "asc"}}],
            "_source": ["keywords", "hashtags", "created_time", "topic_id"]
        }

        result = es.search(index='comments', scroll='2m', size=100, body=body2, request_timeout=2000)
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

        while scroll_size > 0:
            for hit in result['hits']['hits']:
                if 'topic_id' not in hit['_source']:
                    hit['_source']['topic_id'] = []
                if 'hashtags' not in hit['_source']:
                    hit['_source']['hashtags'] = []

                records.append(hit)

            result = es.scroll(scroll_id=scroll_id, scroll='2m', request_timeout=2000)
            scroll_id = result['_scroll_id']
            scroll_size = len(result['hits']['hits'])

        # Lưu records vào file JSON
        # with open(query_data_file, 'w', encoding='utf-8') as f:
        #     json.dump(records, f, ensure_ascii=False, indent=4)

        return records
    
    except Exception as e:
        print(f"An error occurred: {e}")
        # with open(query_data_file, 'w', encoding='utf-8') as f:
        #     json.dump([], f, ensure_ascii=False, indent=4)


        return []
def query_keyword_2(start_date_str, end_date_str, type, query_data_file):
    # Khởi tạo Elasticsearch client
    # es = Elasticsearch('http://localhost:9200')

    # Định nghĩa body cho truy vấn Elasticsearch đầu tiên trên index 'posts'
    body1 = {
        "query": {
            "bool": {
                "must": [
                    {"match_phrase": {"type": type}},
                    {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}},
                    {"exists": {"field": "topic_id"}}  # Thêm điều kiện này để bắt buộc có trường topic_id
                ]
            }
        },
        "sort": [{"created_time": {"order": "asc"}}],
        "_source": ["keyword", "hashtag", "created_time", "topic_id"]
    }

    # Thực hiện truy vấn đầu tiên
    result = es.search(index='posts', scroll='2m', size=100, body=body1)
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    records = []

    # Sử dụng scroll để lấy toàn bộ kết quả
    while scroll_size > 0:
        records.extend(hit['_source'] for hit in result['hits']['hits'])

        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Định nghĩa và thực hiện truy vấn thứ hai trên index 'comments'
    body2 = {
        "query": {
            "bool": {
                "must": [
                    {"match_phrase": {"type": type}},
                    {"exists": {"field": "keywords"}},
                    {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}},
                    {"exists": {"field": "topic_id"}}  # Thêm điều kiện này để bắt buộc có trường topic_id
                ]
            }
        },
        "sort": [{"created_time": {"order": "asc"}}],
        "_source": ["keywords", "hashtags", "created_time", "topic_id"]
    }

    result = es.search(index='comments', scroll='2m', size=100, body=body2)
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    while scroll_size > 0:
        records.extend(hit['_source'] for hit in result['hits']['hits'])

        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    # Lưu records vào file JSON
    with open(query_data_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)

    return records

def query_keyword(start_date_str ,end_date_str , type , query_data_file ):
    body1= {
    "query": {
    "bool": {
      "must": [
        {
           "match_phrase": {
            "type": type
          }
        },
        {
          "range": {
            "created_time": {
              "gte": start_date_str
              , "lte": end_date_str
            }
          }
        }
      ]
    }
  },
    
    "sort": [
        {
            "created_time": {
                "order": "asc"
            }
        }
    ],
    
    "_source": ["keyword" ,  "hashtag"  ,  "created_time" ]
}
    result = es.search(
          index='posts',
          scroll='2m',  # Giữ scroll mở trong 2 phút
          size=100,     # Số lượng records trên mỗi trang
          body=body1
      )
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    # Tạo một list chứa tất cả records
    records = []

    # Lặp qua scroll để lấy tất cả records
    while scroll_size > 0:
        # Thêm records vào list
        records.extend(result['hits']['hits'])
        
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])
        
    body2 = {  
    "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "type": type
          }
        },
        {
          "exists": {
            "field": "keywords"
          }
        },
        {
          "range": {
            "created_time": {
              "gte":start_date_str,  
              "lte":  end_date_str
            }
          }
        }
      ]
    }
  },
     "sort": [
        {
            "created_time": {
                "order": "asc"
            }
        }
    ],
    
    "_source": ["keywords" ,"hashtags" ,"created_time" ]
}   
    result = es.search(
        index='comments',
        scroll='2m',
        size=100,
        body=body2
    )

    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    while scroll_size > 0:
        records.extend(result['hits']['hits'])
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])




    # Lưu records vào file JSON
    with open(query_data_file, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)

    return records

def get_link(gte,lte):
    
    body = {
    "query": {
        "bool": {
            "must": [
                {"term": {"type.keyword": "youtube video"}},
                {
                    "range": {
                        "created_time": {
                            "gte": gte,
                            "lte": lte,
                            "format": "MM/dd/yyyy HH:mm:ss"
                        }
                    }
                }
            ]
        }
    },
    "size": 10000,
    "sort": [
        {
            "created_time": {
                "order": "asc"
            }
        }
    ],
    
    "_source": ["link",  "title", "created_time"  ]
}

    result = es.search(index="posts", body=body)
    link=[]
    for hit in result['hits']['hits']:
        link.append(hit['_source']['id'])
    
    es.close()
    print(link)
if __name__ == '__main__':
    # start_date = "02-01-2024"
    # end_date = "02-25-2024"
    # start_date = datetime.strptime(start_date, "%m-%d-%Y")
    # end_date = datetime.strptime(end_date, "%m-%d-%Y")
    # start_date_str = start_date.strftime("%m/%d/%Y 00:00:01")
    # end_date_str = end_date.strftime("%m/%d/%Y 00:00:00")

    # query_day(start_date_str, end_date_str)
    
    start_date = "09-04-2024"
    end_date = "09-04-2024"
    start_date = datetime.strptime(start_date, "%m-%d-%Y")
    end_date = datetime.strptime(end_date, "%m-%d-%Y")
    start_date_str = start_date.strftime("%m/%d/%Y 00:00:01")
    end_date_str = end_date.strftime("%m/%d/%Y 23:59:59")
    # query_day_3(start_date_str , end_date_str)
    # query_comment_by_dates(start_date ,end_date )
    # combined_queries(start_date, end_date)
    query_data_file = ''
    query_keyword_with_topic(es, start_date_str ,end_date_str , "youtube" , "content_test_newquery_filter_ytb.json")
    