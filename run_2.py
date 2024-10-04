import numpy as np
import re
# from vncorenlp import VnCoreNLP
import json
# from langdetect import detect 
# from langdetect import detect as detect2
from main_query_es import query_day_3 , query_keyword , query_keyword_with_topic
from collections import defaultdict
from main_keyword_top import calculate_top_keywords_with_topic, calculate_top_keywords_with_topic_2_es, calculate_top_keywords_with_trend_logic_topic
import time
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from time import sleep
from keyword_save_es import fetch_all_records , update_historical_data_to_es , load_data_to_elasticsearch_new_data , delete_first_data, load_data_to_elasticsearch_kw_a,get_latest_date_from_elasticsearch
from keyword_save_mongo import connect_to_mongodb , create_or_get_collection , load_data_to_mongodb_new_data
from pymongo import MongoClient
import logging
from dotenv import load_dotenv
import os
load_dotenv()

# Lấy URL Elasticsearch từ biến môi trường
elasticsearch_url = os.getenv("ELASTICSEARCH_URL")
elasticsearch_db_url = os.getenv("ELASTICSEARCH_DB_URL")
logging.basicConfig(filename='keyword_summary.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')
restart_needed = False


es = Elasticsearch([elasticsearch_url], request_timeout=100)

es_db = Elasticsearch([elasticsearch_db_url], request_timeout=100)

historical_data_index = "keyword_a"
historical_data_index_trends = "keyword_a_trend_test"
keyword_top_file = 'keyword_percentages_main_title.json'
keyword_extract_file = 'keyword_test_27.1_filter_new_3.json'
keyword_today_file = 'keyword_percentages_main_title_noun_phase.json'
query_data_file_fb = 'content_test_newquery_filter_fb.json'
query_data_file_tik = 'content_test_newquery_filter_tik.json'
query_data_file_ytb = 'content_test_newquery_filter_ytb.json'
query_data_file_voz = 'content_test_newquery_filter_voz.json'
query_data_file_xamvn = 'content_test_newquery_filter_xam.json'
query_data_file_oto = 'content_test_newquery_filter_oto.json'
query_data_file_media = 'content_test_newquery_filter_media.json'

interval_hours = 2

def query_and_extract_keywords(es, start_time_str, end_time_str , type ):
    dataFramse_Log = query_keyword_with_topic(es, start_time_str, end_time_str , type )
    # with open("/usr/app/src/run.log", "a") as f:
    #     f.write("done query!")

    # extracted_keywords = convert(dataFramse_Log, keyword_extract_file)
    return dataFramse_Log


def run_keyword_all_day():
    today = datetime.today()
    # input_day = datetime.today() - timedelta(days=115)
    input_day = datetime.today()-timedelta(days=1)

    input_day_str = input_day.strftime("%m/%d/%Y")
    historical_data = []

    seven_days_before_input = input_day - timedelta(days=32)
    # seven_days_before_input = input_day


    # try:
    #     # historical_data = fetch_all_records(historical_data_index , es)
    #     historical_data = list(collection_fb.find({}, {'_id': False}).sort('_id', -1).limit(1))
    #     # historical_data = []
    #     if historical_data:
    #         last_day_str = historical_data[0]['date']
    #         last_day = datetime.strptime(last_day_str, "%m/%d/%Y")
    #         last_day += timedelta(days=1)
    #         if last_day <= seven_days_before_input:
    #             last_day = seven_days_before_input
    #     else:
    #         last_day = seven_days_before_input 
    # except Exception as e:
    #     print(e)
    #     last_day = seven_days_before_input
    # try:
    #     # Lấy ngày mới nhất từ Elasticsearch
    #     last_day_str = get_latest_date_from_elasticsearch(es_db , historical_data_index)

    #     if last_day_str:
    #         last_day = datetime.strptime(last_day_str, "%m/%d/%Y")
    #         last_day += timedelta(days=1)
    #         if last_day <= seven_days_before_input:
    #             last_day = seven_days_before_input
    #     else:
    last_day = seven_days_before_input 

    # except Exception as e:
    #     print(e)
    #     last_day = seven_days_before_input

    input_day_str = input_day.strftime("%m/%d/%Y 23:59:59")
    last_day_str = last_day.strftime("%m/%d/%Y 00:00:00")
    extracted_keywords_fb = query_and_extract_keywords(es, last_day_str , input_day_str , "facebook"  )
    extracted_keywords_tik = query_and_extract_keywords(es, last_day_str , input_day_str , "tiktok"  )
    extracted_keywords_ytb = query_and_extract_keywords(es, last_day_str , input_day_str , "youtube"  )
    extracted_keywords_voz = query_and_extract_keywords(es, last_day_str , input_day_str , "voz"  )
    extracted_keywords_xamvn = query_and_extract_keywords(es , last_day_str , input_day_str , "xamvn" )
    extracted_keywords_oto = query_and_extract_keywords(es, last_day_str , input_day_str ,"otofun" )
    extracted_keywords_media = query_and_extract_keywords(es ,last_day_str , input_day_str ,"media" )

    current_day = last_day
    
    while current_day <= input_day:
        current_day_str = current_day.strftime("%m/%d/%Y")
        if not any(record['date'] == current_day_str for record in historical_data):
            sleep(2)
            top_keywords_fb = calculate_top_keywords_with_topic_2_es(es_db, current_day_str, extracted_keywords_fb ,historical_data_index, "facebook"  )
            # print(f"Top Keywords for facebook {current_day_str}: {top_keywords_fb}")
            top_keywords_trends_fb = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, "facebook")
            # print(f"Top Keywords trend for facebook {current_day_str}: {top_keywords_trends_fb}")

            top_keywords_tik = calculate_top_keywords_with_topic_2_es(es_db ,current_day_str, extracted_keywords_tik , historical_data_index, "tiktok"   )
            # print(f"Top Keywords for tiktok {current_day_str}: {top_keywords_tik}")
            top_keywords_trends_tik = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, "tiktok")
            # print(f"Top Keywords trend for tiktok {current_day_str}: {top_keywords_trends_tik}")

            top_keywords_ytb = calculate_top_keywords_with_topic_2_es(es_db , current_day_str, extracted_keywords_ytb ,  historical_data_index, "youtube"  )
            # print(f"Top Keywords for youtube {current_day_str}: {top_keywords_ytb}")
            top_keywords_trends_ytb = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, "youtube")
            # print(f"Top Keywords trend for youtube {current_day_str}: {top_keywords_trends_ytb}")

            
            top_keywords_voz = calculate_top_keywords_with_topic_2_es(es_db ,current_day_str, extracted_keywords_voz ,  historical_data_index, "voz"  )
            # print(f"Top Keywords for voz {current_day_str}: {top_keywords_voz}")
            top_keywords_trends_voz = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, "voz")
            # print(f"Top Keywords trend for voz {current_day_str}: {top_keywords_trends_voz}")

            top_keywords_xamvn = calculate_top_keywords_with_topic_2_es(es_db , current_day_str, extracted_keywords_xamvn ,  historical_data_index, "xamvn"   )
            # print(f"Top Keywords for xamvn {current_day_str}: {top_keywords_xamvn}")
            top_keywords_trends_xamvn = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, "xamvn")
            # print(f"Top Keywords trend for xamvn {current_day_str}: {top_keywords_trends_xamvn}")

            top_keywords_oto = calculate_top_keywords_with_topic_2_es(es_db , current_day_str, extracted_keywords_oto ,  historical_data_index, "otofun"  )
            # print(f"Top Keywords for otofun {current_day_str}: {top_keywords_oto}")
            top_keywords_trends_otofun = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, "otofun")
            # print(f"Top Keywords trend for otofun {current_day_str}: {top_keywords_trends_otofun}")


            top_keywords_media = calculate_top_keywords_with_topic_2_es(es_db , current_day_str, extracted_keywords_media , historical_data_index, "media"   )
            # print(f"Top Keywords for media {current_day_str}: {top_keywords_media}")
            top_keywords_trends_media = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, "media")
            # print(f"Top Keywords trend for media {current_day_str}: {top_keywords_trends_media}")
            
            
            # with open("/usr/app/src/run.log", "a") as f:
            #     f.write("done update!\n")


            
        current_day += timedelta(days=1)
        
    input_day_str = input_day.strftime("%m/%d/%Y")
    # keyword_week = get_top_keywords_for_week(input_day_str ,keyword_today_file )
    # with open(query_data_file_fb, 'w', encoding='utf-8') as file:
    #         json.dump([], file, ensure_ascii=False, indent=4)
    # with open(query_data_file_tik, 'w', encoding='utf-8') as file:
    #         json.dump([], file, ensure_ascii=False, indent=4)
    # with open(query_data_file_ytb, 'w', encoding='utf-8') as file:
    #         json.dump([], file, ensure_ascii=False, indent=4)
    # with open(query_data_file_voz, 'w', encoding='utf-8') as file:
    #         json.dump([], file, ensure_ascii=False, indent=4)
    # with open(query_data_file_xamvn, 'w', encoding='utf-8') as file:
    #         json.dump([], file, ensure_ascii=False, indent=4)
    # with open(query_data_file_oto, 'w', encoding='utf-8') as file:
    #         json.dump([], file, ensure_ascii=False, indent=4)
    # with open(query_data_file_media, 'w', encoding='utf-8') as file:
    #         json.dump([], file, ensure_ascii=False, indent=4)



# def get_latest_hour_from_data(data):
#     latest_datetime = None

#     for entry in data.values():
#         created_time_str = entry.get("created_time", "")
#         if created_time_str:  
#             created_time = datetime.strptime(created_time_str, "%m/%d/%Y %H:%M:%S")
#             if not latest_datetime or created_time > latest_datetime:
#                 latest_datetime = created_time

#     if latest_datetime:
#         return latest_datetime.hour
#     else:
#         return 0  
def get_latest_hour_from_data(data):
    latest_datetime = None

    for entry in data:
        created_time_str = entry.get('_source', {}).get('created_time', '')
        
        if created_time_str:
            created_time = datetime.strptime(created_time_str, "%m/%d/%Y %H:%M:%S")
            
            if not latest_datetime or created_time > latest_datetime:
                latest_datetime = created_time

    if latest_datetime:
        return latest_datetime.hour
    else:
        return 0
def get_latest_datetime_from_data(data):
    latest_datetime = None

    for entry in data:
        created_time_str = entry.get('_source', {}).get('created_time', '')
        
        if created_time_str:
            created_time = datetime.strptime(created_time_str, "%m/%d/%Y %H:%M:%S")
            
            if not latest_datetime or created_time > latest_datetime:
                latest_datetime = created_time

    return latest_datetime


def merge_extracted_keywords(old_data, new_data): 
    for key, value in new_data.items():
        if key not in old_data:
            old_data[key] = value
        else:
            old_keywords = set(old_data[key]['hashtag'])
            new_keywords = set(value['hashtag'])
            combined_keywords = list(old_keywords.union(new_keywords))
            old_data[key]['hashtag'] = combined_keywords
            if old_data[key]['created_time'] < value['created_time']:
                old_data[key]['title'] = value['title']
                old_data[key]['created_time'] = value['created_time']
    return old_data

# def summarize_keywords_in_intervals(collection , type , query_data_file):
def summarize_keywords_in_intervals(type ,  old_extracted_keywords):
    start_bool = False
    top_keywords = {}
    print('haha')
    if old_extracted_keywords is None:
        old_extracted_keywords = []  # Khởi tạo danh sách rỗng nếu giá trị là None

    # try:
    #     with open(query_data_file, 'r', encoding='utf-8') as file:
    #         old_extracted_keywords =  json.load(file)
    # except Exception as e:
    #     print(e)
    #     old_extracted_keywords =  []
    latest_datetime = get_latest_datetime_from_data(old_extracted_keywords)
    print(latest_datetime)

    # now = datetime.now()- timedelta(days=114)
    now = datetime.now()

    current_time = now.replace(minute=0, second=0, microsecond=0)
    if latest_datetime:
        start_of_day = latest_datetime.replace(minute=0, second=0, microsecond=0)
        additional_hours = (interval_hours - start_of_day.hour % interval_hours) % interval_hours
        if additional_hours > 0:
            start_of_day += timedelta(hours=additional_hours)

    else:
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    top_keywords_summary = {}
    
    while start_of_day < current_time:
        end_of_interval = start_of_day + timedelta(hours=interval_hours)
        if end_of_interval > current_time:
            end_of_interval = current_time
            return start_bool, old_extracted_keywords
        print(start_of_day.strftime("%m/%d/%Y %H:%M:%S"))
        print(end_of_interval.strftime("%m/%d/%Y %H:%M:%S"))
        logging.info(f"Interval from {start_of_day.strftime('%m/%d/%Y %H:%M:%S')} to {end_of_interval.strftime('%m/%d/%Y %H:%M:%S')}")
        extracted_keywords = query_and_extract_keywords(es , start_of_day.strftime("%m/%d/%Y %H:%M:%S"), end_of_interval.strftime("%m/%d/%Y %H:%M:%S") , type )
        current_day_str = start_of_day.strftime("%m/%d/%Y")
        old_extracted_keywords = old_extracted_keywords + extracted_keywords
        top_keywords = calculate_top_keywords_with_topic_2_es(es_db , current_day_str, old_extracted_keywords, historical_data_index, type)
        sleep(1.6)
        top_keywords_trends = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index,type )
        start_of_day = end_of_interval
        if top_keywords["topic_ids"].get("all", {}).get("keywords_top") or top_keywords["topic_ids"].get("all", {}).get("hashtags_top"):
            start_bool = True
            load_data_to_elasticsearch_kw_a(es_db , top_keywords, historical_data_index)
            sleep(1.5)
            load_data_to_elasticsearch_kw_a(es_db , top_keywords_trends, historical_data_index_trends)
            
        
    return start_bool, old_extracted_keywords
                                                                      
        
def run_keyword_today():
    # current_day = datetime.now()-timedelta(days = 114)
    current_day = datetime.now()
    top_keywords_summary_fb = []
    top_keywords_summary_tik = []
    top_keywords_summary_ytb = []
    top_keywords_summary_voz = []
    top_keywords_summary_xamvn = []
    top_keywords_summary_oto = []
    top_keywords_summary_media = []

    global restart_needed  # Sử dụng biến toàn cục

    collections = ['facebook', 'tiktok', 'youtube', 'voz', 'xamvn', 'otofun', 'media']
    # query_data_files = [query_data_file_fb, query_data_file_tik, query_data_file_ytb, query_data_file_voz, query_data_file_xamvn, query_data_file_oto, query_data_file_media]
    top_keywords_summarys = [top_keywords_summary_fb, top_keywords_summary_tik, top_keywords_summary_ytb, top_keywords_summary_voz, top_keywords_summary_xamvn, top_keywords_summary_oto, top_keywords_summary_media]
    while True:
        # now = datetime.now() - timedelta(days=114)
        now = datetime.now()
        empty_collections_count = 0  # Biến đếm số lượng collections có len(top_keywords_summary) == 0

        if now.day != current_day.day:
            # top_keywords_summary = []

            for collection, top_keywords_summary in zip(collections, top_keywords_summarys):
                start_bool, old_extracted_keywords = summarize_keywords_in_intervals(collection, top_keywords_summary)
                top_keywords_summary = old_extracted_keywords
                
                if  not start_bool:
                    empty_collections_count += 1
                
                if empty_collections_count >= 3:
                    restart_needed = True

                    return   
            
            print("Đã sang ngày mới.")
            break
        
        if now.hour > interval_hours or now.hour == interval_hours:
            # for collection, query_data_file in zip(collections, query_data_files):
            #     start_bool = summarize_keywords_in_intervals(collection, query_data_file)
            #     print(f"Tóm tắt từ khóa cuối cùng của ngày từ {collection}:", start_bool)
            #     if not start_bool and now.hour >=8 :
            #         empty_collections_count += 1
            #         print(f"Không có từ khóa nào được tóm tắt từ {collection}.")
                
            #     if empty_collections_count >= 7:
            #         restart_needed = True

            #         print("Có 7 hoặc nhiều hơn collections không có từ khóa nào được tóm tắt. Thực hiện lại quá trình từ đầu.")
            #         return   
            for collection, top_keywords_summary in zip(collections, top_keywords_summarys):
                start_bool, old_extracted_keywords = summarize_keywords_in_intervals(collection, top_keywords_summary)
                top_keywords_summary = old_extracted_keywords
                
                if  not start_bool:
                    empty_collections_count += 1
                
                if empty_collections_count >= 3:
                    restart_needed = True

                    return   

        
        if now.hour % interval_hours == 0:
            sleep_hours = interval_hours
        else:
            sleep_hours = (interval_hours - (now.hour % interval_hours)) % interval_hours
        sleep_time = (sleep_hours * 3600) - (now.minute * 60) - now.second
        print(f"Chờ {sleep_time} giây cho đến khi thời gian hiện tại chia hết.")
        time.sleep(sleep_time)

        
def main_loop():
    global restart_needed  # Sử dụng biến toàn cục
    
    while True:
        run_keyword_all_day()
        print("Bắt đầu ngày hôm nay!")
        time.sleep(2)
        
        while not restart_needed:
            run_keyword_today()

        print("Không có từ khóa nào được tóm tắt, bắt đầu lại từ đầu.")
        restart_needed = False 
if __name__ == "__main__":
    # with open("/usr/app/src/run.log", "a") as f:
    #     f.write("run.py started\n")

    index_name_trend = "keyword_a_trend_test"

    mapping = {
        "mappings": {
            "properties": {
                "date": {
                    "type": "date",
                    "format": "MM_dd_yyyy"  # Định dạng ngày như bạn yêu cầu
                },
                "type": {
                    "type": "keyword"
                },
                "topic_ids": {
                    "type": "nested",  # Để lưu trữ danh sách các từ khóa trong topic_ids
                    "properties": {
                        "keyword": {
                            "type": "keyword"
                        },
                        "percentage": {
                            "type": "float"
                        },
                        "record": {
                            "type": "integer"
                        },
                        "score": {
                            "type": "integer"
                        },
                        "isTrend": {
                            "type": "boolean"
                        }
                    }
                }
            }
        }
    }

    # Kiểm tra nếu index đã tồn tại, nếu chưa thì tạo mới
    if not es_db.indices.exists(index=index_name_trend):
        es_db.indices.create(index=index_name_trend, body=mapping)
        print(f"Index {index_name_trend} created successfully")
    else:
        print(f"Index {index_name_trend} already exists")
    sleep(1)
    index_name = "keyword_a"

    # Định nghĩa mappings và settings cho chỉ mục
    index_settings = {
        "mappings": {
            "properties": {
                "date": {
                    "type": "date",
                    "format": "MM_dd_yyyy"
                },
                "type": {
                    "type": "keyword"
                },
                "topic_ids": {
                    "type": "object",
                    "properties": {
                        "keywords_top": {
                            "type": "nested",
                            "properties": {
                                "keyword": {
                                    "type": "keyword"
                                },
                                "percentage": {
                                    "type": "float"
                                },
                                "record": {
                                    "type": "integer"
                                }
                            }
                        },
                        "hashtags_top": {
                            "type": "nested",
                            "properties": {
                                "hashtag": {
                                    "type": "keyword"
                                },
                                "percentage": {
                                    "type": "float"
                                },
                                "record": {
                                    "type": "integer"
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    # Tạo chỉ mục nếu nó chưa tồn tại
    if not es_db.indices.exists(index=index_name):
        es_db.indices.create(index=index_name, body=index_settings)
        print(f"Index {index_name} created successfully")
    else:
        print(f"Index {index_name} already exists")

    main_loop()

