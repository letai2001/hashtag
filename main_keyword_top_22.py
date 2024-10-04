import json
from collections import defaultdict
from datetime import datetime , timedelta
# from langdetect import detect 
import numpy as np
import string
import re
# import matplotlib.pyplot as plt
from keyword_save_es import fetch_all_records , update_historical_data_to_es , load_data_to_elasticsearch_new_data, load_data_to_elasticsearch_kw_a
from elasticsearch import Elasticsearch
from time import sleep
from pymongo import MongoClient
from keyword_save_mongo import connect_to_mongodb , create_or_get_collection , load_data_to_mongodb_new_data
# Tạo cấu trúc dữ liệu cho thống kê
def CheckBig(arrcheck):
    min_value = min(arrcheck)
    if min_value < 1:
        return False
    if arrcheck[6] >min_value*3.2:
        return True
    else:
        return False
# def CheckBig(arrcheck):
#     for ar in arrcheck:
#         if ar < 1:
#             return False
#     if arrcheck[6] >2.5:
#         return True
#     else:
#         return False

def CheckSmall(arrcheck):
    for ar in arrcheck:
        if ar > 1:
            return False
    return True
def CheckPre(arrcheck):
    bRet = False
    for ar in arrcheck:
        if ar < 2:
            bRet = True
            break
    if bRet == True:
        if ((arrcheck[6]-min(arrcheck))>0.4) and (arrcheck[6]>2) and (arrcheck[6]>arrcheck[5]):
            return True
        else:
            return False          
    else:
        return False
def CheckOld(arrcheck):
    bRet = False
    for ar in arrcheck:
        if ar > 2:
            bRet = True
            break
    if bRet == True:
        if arrcheck[6]<2 :
            return True
        else:
            return False          
    else:
        return False
def CheckTrend(arrcheck):
    min_value = min(arrcheck)
    count_below_half = sum(1 for ar in arrcheck if ar <= 0.5)
    if count_below_half >= 4:
        if arrcheck[6] - min_value >= 1.1:
            return True

    elif count_below_half >= 3:
        if arrcheck[6] - min_value >= 1.4:
            return True

    elif count_below_half >= 2:
        if arrcheck[6] - min_value >= 1.6:
            return True
    elif count_below_half == 1:
        if arrcheck[6] - min_value >= 1.8:
            return True
    elif min_value < 1:
        if arrcheck[6] > 2:
            return True

    return False

def Check(keywordtop):
    categorized_keywords = {
        'BigTrend': [],
        'Trend': [],
        'PreTrend': [],
        'OldTrend': [],
        'Other': []
    }

    for arr in keywordtop:
        if CheckSmall(arr['percentage']):
            continue  
        if CheckBig(arr['percentage']):
            categorized_keywords['BigTrend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'] ,  'isTrend': True})
        elif CheckTrend(arr['percentage']):
            categorized_keywords['Trend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'], 'isTrend': True})
        elif CheckPre(arr['percentage']):
            categorized_keywords['PreTrend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'], 'isTrend': False})
        # elif CheckOld(arr['percentage']):
        #     categorized_keywords['OldTrend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] , 'isTrend': True})
        else:
            categorized_keywords['Other'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'], 'isTrend': False})

    for category in categorized_keywords:
        categorized_keywords[category].sort(key=lambda x: x['score'], reverse=True)
    
    sorted_keywords = []
    for category in ['BigTrend', 'Trend', 'PreTrend', 'Other']:
        sorted_keywords.extend(categorized_keywords[category])

    return sorted_keywords

def is_not_blackword(word):
    """
    Kiểm tra xem một từ có phải là từ không mong muốn (black word) hay không.
    Một từ được coi là black word nếu nó nằm trong danh sách black_words hoặc nếu nó chứa từ 'ảnh'.

    :param word: Từ cần kiểm tra.
    :param black_words: Danh sách các từ không mong muốn.
    :return: True nếu từ là black word, ngược lại là False.
    """
    with open('black_list.txt', 'r', encoding='utf-8') as f:
        black_words = f.read().splitlines()

    if word in black_words:
        return False
    
    if 'ảnh' in word :
        return False
    
    return True

# def is_keyword_selected(keyword, keyword_percentages,daily_keywords ,  check_date_str):
#     percentage_on_check_date = next((item['percentage'] for item in daily_keywords if item['keyword'] == keyword), 0)    
    
#     # Lấy phần trăm của từ trong các ngày khác
#     other_dates_percentages = [keyword_percentages[date][keyword] 
#                             for date in keyword_percentages 
#                             #    if date != check_date_str and keyword in keyword_percentages[date]
#                                     if  keyword in keyword_percentages[date]
#                             ]
    
#     # Đếm số ngày có phần trăm lớn hơn 0.6% và 0.8%
#     #trung bình số phần trăm của từ  top 20 mỗi ngày 
#     count_higher_09 = sum(perc >= 0.88 for perc in other_dates_percentages)
#     #trung bình số phần trăm của từ  top 10 mỗi ngày 
#     count_higher_11 = sum(perc >= 1.1 for perc in other_dates_percentages)
#     #trung bình số phần trăm của từ  top 6 mỗi ngày 
#     count_higher_14 = sum(perc >= 1.4 for perc in other_dates_percentages)

#     # Tiêu chí 1: Từ không được chọn nếu có tới 4 ngày lớn hơn 0.6% và 3 ngày lớn hơn 0.8%
#     if count_higher_09 >= 3 and count_higher_11 >= 2:
#         # Tiêu chí 2: Từ được chọn nếu phần trăm trong ngày check_date gấp 2.75 lần trung bình các ngày còn lại        
#         # avg_other_dates = sum(other_dates_percentages) / len(other_dates_percentages) if other_dates_percentages else 0
#         min_other_percentage = min(other_dates_percentages) if other_dates_percentages else 0
#         if percentage_on_check_date > 3 * min_other_percentage and count_higher_14 <=5:
#             return True
#         else:
#             return False
#     else:
#         # Từ được chọn theo tiêu chí 1
#         return True   
# def is_keyword_selected(keyword, historical_percentages, daily_keywords, check_date_str):
#     # Lấy phần trăm của từ khóa trong ngày đang xét
#     percentage_on_check_date = next((item['percentage'] for item in daily_keywords if item['keyword'] == keyword), 0)    
    
#     # Lấy phần trăm của từ khóa trong các ngày khác
#     other_dates_percentages = [
#         historical_percentages[keyword][i] 
#         for i in range(6)  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
#     ]
    
#     # Đếm số ngày có phần trăm lớn hơn các ngưỡng xác định
#     count_higher_09 = sum(perc >= 0.88 for perc in other_dates_percentages)
#     count_higher_11 = sum(perc >= 1.1 for perc in other_dates_percentages)
#     count_higher_14 = sum(perc >= 1.4 for perc in other_dates_percentages)

#     # Tiêu chí 1: Từ khóa không được chọn nếu có ít nhất 3 ngày lớn hơn 0.88% và ít nhất 2 ngày lớn hơn 1.1%
#     if count_higher_09 >= 3 and count_higher_11 >= 2:
#         # Tiêu chí 2: Từ khóa chỉ được chọn nếu phần trăm trong ngày hiện tại gấp 3 lần giá trị nhỏ nhất của các ngày khác và số ngày lớn hơn 1.4% không quá 5 ngày
#         min_other_percentage = min(other_dates_percentages) if other_dates_percentages else 0
#         if percentage_on_check_date > 3 * min_other_percentage and count_higher_14 <= 5:
#             return True
#         else:
#             return False
#     else:
#         # Nếu không đạt tiêu chí 1, từ khóa được chọn
#         return True   
def is_keyword_selected(keyword, historical_percentages, daily_keywords, check_date_str):
    # Lấy phần trăm của từ khóa trong ngày đang xét
    percentage_on_check_date = next((item['percentage'] for item in daily_keywords if item['keyword'] == keyword), 0)    
    
    # Lấy phần trăm của từ khóa trong các ngày khác
    # other_dates_percentages = [
    #     historical_percentages.get(i, 0)  # Ensure a default value of 0 if the topic key is missing
    #     for i in range(6)  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
    # ]
    for key, value in historical_percentages.items():
        if isinstance(value, list):
            other_dates_percentages = value[:6]  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
            break
    
    # Đảm bảo rằng nếu không có giá trị nào được tìm thấy, sẽ sử dụng danh sách rỗng hoặc danh sách mặc định
    if not other_dates_percentages:
        other_dates_percentages = [0] * 6

    # Đếm số ngày có phần trăm lớn hơn các ngưỡng xác định
    count_higher_09 = sum(perc >= 0.88 for perc in other_dates_percentages)
    count_higher_11 = sum(perc >= 1.1 for perc in other_dates_percentages)
    count_higher_14 = sum(perc >= 1.4 for perc in other_dates_percentages)

    # Tiêu chí 1: Từ khóa không được chọn nếu có ít nhất 3 ngày lớn hơn 0.88% và ít nhất 2 ngày lớn hơn 1.1%
    if count_higher_09 >= 3 and count_higher_11 >= 2:
        # Tiêu chí 2: Từ khóa chỉ được chọn nếu phần trăm trong ngày hiện tại gấp 3 lần giá trị nhỏ nhất của các ngày khác và số ngày lớn hơn 1.4% không quá 5 ngày
        min_other_percentage = min(other_dates_percentages) if other_dates_percentages else 0
        if percentage_on_check_date > 3 * min_other_percentage and count_higher_14 <= 5:
            return True
        else:
            return False
    else:
        # Nếu không đạt tiêu chí 1, từ khóa được chọn
        return True

# def is_subkeyword(keyword, other_keyword):
#     """
#     Check if any word in 'keyword' is present in 'other_keyword' after splitting both by underscores.
#     """
#     # Splitting the keywords into lists of words
#     keyword_words = keyword.lower().split('_')
#     other_keyword_words = other_keyword.lower().split('_')

#     # Checking if any word in keyword is present in other_keyword
#     return all(word in other_keyword_words for word in keyword_words) or all(word in keyword_words for word in other_keyword_words)
def is_subkeyword(keyword, other_keyword):
    """
    Check if any word in 'keyword' is present in 'other_keyword' after splitting both by underscores.
    Additionally, return true if there are at least four common words between the two keywords.
    """
    # Splitting the keywords into lists of words
    keyword_words = set(keyword.lower().split('_'))
    other_keyword_words = set(other_keyword.lower().split('_'))

    # Checking if any word in keyword is present in other_keyword
    basic_check = all(word in other_keyword_words for word in keyword_words) or all(word in keyword_words for word in other_keyword_words)

    # Checking for at least four common words
    common_words = keyword_words.intersection(other_keyword_words)
    four_common_words_check = len(common_words) >= 4

    return basic_check or four_common_words_check


def filter_keywords_all_words_no_sort(keyword_list):
    """
    Filter the keywords based on the all-words subkeyword relation without sorting.
    Each keyword in the list is a tuple of (keyword, percentage).
    """
    filtered_keywords = []
    
    for keyword, percentage in keyword_list:
        if(keyword =='thanh_hoá'):
            print(1)
        # Check if the current keyword is a subkeyword of any keyword in the filtered list
        if not any(is_subkeyword(keyword, existing_keyword) for existing_keyword, _ in filtered_keywords):
            filtered_keywords.append((keyword, percentage))

    return filtered_keywords

def calculate_top_keywords_with_topic(input_date, data, collection):
    # Đọc dữ liệu từ file JSON
    date_format = "%m/%d/%Y"
    date_counts = defaultdict(int)
    keyword_counts = defaultdict(lambda: defaultdict(int))
    hashtag_counts = defaultdict(lambda: defaultdict(int))
    keyword_topic_ids = defaultdict(lambda: defaultdict(set))
    hashtag_topic_ids = defaultdict(lambda: defaultdict(set))

    historical_data = list(collection.find({}, {'_id': False}))
    
    for item in data:
        keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
        hashtags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'
        # Lấy ngày tạo từ _source và chuyển đổi thành chuẩn định dạng ngày
        date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)

        date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        
        if date_str == input_date:
            date_counts[date_str] += 1
            for keyword in item['_source'].get(keywords_field, []):
                if len(keyword) > 2:
                    keyword_counts[date_str][keyword] += 1
                    keyword_topic_ids[date_str][keyword].update(item['_source'].get('topic_id', []))
            for hashtag in item['_source'].get(hashtags_field, []):
                if len(hashtag) > 2:
                    hashtag_counts[date_str][hashtag] += 1
                    hashtag_topic_ids[date_str][hashtag].update(item['_source'].get('topic_id', []))

    keyword_percentages = [
        {"keyword": keyword, "percentage": (count / date_counts[input_date]) * 100, "record": count, "topic_id": list(topic_ids)}
        for keyword, count, topic_ids in ((k, v, keyword_topic_ids[input_date][k]) for k, v in keyword_counts[input_date].items())
    ]
    keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)
    keyword_percentages = keyword_percentages[:4000]
    
    hastag_percentages = [
        {"hastag": hashtag, "percentage": (count / date_counts[input_date]) * 100, "record": count, "topic_id": list(topic_ids)}
        for hashtag, count, topic_ids in ((h, v, hashtag_topic_ids[input_date][h]) for h, v in hashtag_counts[input_date].items())
    ]
    hastag_percentages = sorted(hastag_percentages, key=lambda x: x['percentage'], reverse=True)
    hastag_percentages = hastag_percentages[:4000]

    if keyword_percentages or hastag_percentages:
        data = {
            "date": input_date,
            "keywords_top": keyword_percentages,
            "hastag_top": hastag_percentages
        }
        load_data_to_mongodb_new_data(data, collection)

    return {
        "date": input_date,
        "keywords_top": keyword_percentages,
        "hastag_top": hastag_percentages
    }


def calculate_top_keywords(input_date, data  , collection  ):
    # Đọc dữ liệu từ file JSON
    historical_data = []

    date_format = "%m/%d/%Y"
    date_counts = defaultdict(int)
    keyword_counts = defaultdict(lambda: defaultdict(int))
    hastag_counts = defaultdict(lambda: defaultdict(int))
    historical_data = list(collection.find({}, {'_id': False}))
    # historical_data = fetch_all_records(historical_data_index, es)

    # Duyệt qua từng mục trong dữ liệu
    # for item_id, item in data.items():
    #     date_str = datetime.strptime(item['created_time'], '%m/%d/%Y %H:%M:%S').strftime('%m/%d/%Y')

    #     if date_str == input_date:
    #         date_counts[date_str] += 1
    #         for keyword in item['keywords']:
    #             keyword_counts[date_str][keyword] += 1
    for item in data:
        # Xác định trường chứa từ khóa dựa trên _index
        keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
        hastags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'
        # Lấy ngày tạo từ _source và chuyển đổi thành chuẩn định dạng ngày
        date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        
        if date_str == input_date:
            date_counts[date_str] += 1
            for keyword in item['_source'].get(keywords_field, []):
                if len(keyword)>2:  # Kiểm tra để loại bỏ các chuỗi trống
                    keyword_counts[date_str][keyword] += 1
            for hastag in item['_source'].get(hastags_field, []):
                if len(hastag)>2:  # Kiểm tra để loại bỏ các chuỗi trống
                    hastag_counts[date_str][hastag] += 1

    # Kiểm tra số lượng bài viết cho ngày đó

    # Tính phần trăm xuất hiện của từng keyword
    keyword_percentages = [{"keyword": keyword, "percentage": (count / date_counts[input_date]) * 100 , "record":count }
                           for keyword, count in keyword_counts[input_date].items()
                           ]
    keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)
    # keyword_percentages = keyword_percentages[:3200]
    
    hastag_percentages = [{"hastag": hastag, "percentage": (count / date_counts[input_date]) * 100 , "record":count }
                           for hastag, count in hastag_counts[input_date].items()
                           ]
    hastag_percentages = sorted(hastag_percentages, key=lambda x: x['percentage'], reverse=True)
    # hastag_percentages = hastag_percentages[:3200]
    

    
    if keyword_percentages :
        data = {
                "date": input_date,
                "keywords_top": keyword_percentages,
                "hastag_top": hastag_percentages
                # "keywords": daily_keywords,
            }

        # historical_data.sort(key=lambda x: datetime.strptime(x['date'], "%m/%d/%Y"), reverse=False)
        # sleep(1.5)
        load_data_to_mongodb_new_data(data, collection)
        # load_data_to_elasticsearch_new_data(historical_data, historical_data_index , es)
        # sleep(1.5)
        


    # Trả về dữ liệu theo định dạng yêu cầu
    return {
        "date": input_date,
        "keywords_top": keyword_percentages,
        "hastag_top": hastag_percentages
        # "keywords": daily_keywords
    }
# def calculate_top_keywords_with_topic_2_es(es , input_date, data, index_name, platform):
#     # Định dạng ngày
#     date_format = "%m/%d/%Y"
#     # Chuyển input_date thành datetime object
#     date_obj = datetime.strptime(input_date, date_format)
#     date_str = date_obj.strftime("%m_%d_%Y")

#     # Khởi tạo các biến để lưu trữ số liệu thống kê
#     date_counts = defaultdict(int)
#     keyword_counts = defaultdict(lambda: defaultdict(int))
#     hashtag_counts = defaultdict(lambda: defaultdict(int))
#     keyword_topic_ids = defaultdict(lambda: defaultdict(set))
#     hashtag_topic_ids = defaultdict(lambda: defaultdict(set))
#     topic_ids_set = set()

#     # Duyệt qua từng item trong dữ liệu
#     for item in data:
#         keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
#         hashtags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'

#         # keywords_field = 'keyword'
#         # hashtags_field = 'hashtag'
        
#         # Lấy ngày tạo từ _source và chuyển đổi thành chuẩn định dạng ngày
#         item_date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        
#         # Nếu ngày tạo trùng khớp với input_date, tiến hành thống kê
#         if item_date_str == input_date:
#             date_counts[item_date_str] += 1
#             for keyword in item['_source'].get(keywords_field, []):
#                 if len(keyword) > 2:  # Lọc các từ khóa có độ dài lớn hơn 2
#                     keyword_counts[item_date_str][keyword] += 1
#                     for topic_id in item['_source'].get('topic_id', []):
#                         keyword_topic_ids[item_date_str][keyword].add(topic_id)
#                         topic_ids_set.add(topic_id)
#             for hashtag in item['_source'].get(hashtags_field, []):
#                 if len(hashtag) > 2:  # Lọc các hashtag có độ dài lớn hơn 2
#                     hashtag_counts[item_date_str][hashtag] += 1
#                     for topic_id in item['_source'].get('topic_id', []):
#                         hashtag_topic_ids[item_date_str][hashtag].add(topic_id)
#                         topic_ids_set.add(topic_id)

#     # Tính toán tỉ lệ phần trăm của các từ khóa
#     topic_data = {}
#     topic_ids = list(topic_ids_set)
#     for topic_id in topic_ids + ["all"]:
#         keyword_percentages = [
#             {"keyword": keyword, "percentage": (count / date_counts[input_date]) * 100, "record": count}
#             for keyword, count in keyword_counts[input_date].items()
#             if topic_id in keyword_topic_ids[input_date][keyword] or topic_id == "all"
#         ]
#         keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)
        
#         hashtag_percentages = [
#             {"hashtag": hashtag, "percentage": (count / date_counts[input_date]) * 100, "record": count}
#             for hashtag, count in hashtag_counts[input_date].items()
#             if topic_id in hashtag_topic_ids[input_date][hashtag] or topic_id == "all"
#         ]
#         hashtag_percentages = sorted(hashtag_percentages, key=lambda x: x['percentage'], reverse=True)

#         if keyword_percentages or hashtag_percentages:
#             topic_data[topic_id] = {
#                 "keywords_top": keyword_percentages,
#                 "hashtags_top": hashtag_percentages
#             }

#     # Lưu kết quả vào Elasticsearch nếu có dữ liệu
#     if topic_data:
#         data = {
#             "date": date_str,
#             "type": platform,
#             "topic_ids": topic_data
#         }
#         load_data_to_elasticsearch_kw_a(es , data, index_name)

#     # Trả về kết quả
#     return {
#         "date": date_str,
#         "type": platform,
#         "topic_ids": topic_data
#     }

def calculate_top_keywords_with_topic_2_es(es, input_date, data, index_name, platform):
    # Định dạng ngày
    date_format = "%m/%d/%Y"
    # Chuyển input_date thành datetime object
    date_obj = datetime.strptime(input_date, date_format)
    date_str = date_obj.strftime("%m_%d_%Y")

    # Khởi tạo các biến để lưu trữ số liệu thống kê
    date_counts = defaultdict(int)
    topic_article_counts = defaultdict(int)
    keyword_counts = defaultdict(lambda: defaultdict(int))
    hashtag_counts = defaultdict(lambda: defaultdict(int))
    topic_ids_set = set()

    # Duyệt qua từng item trong dữ liệu
    for item in data:
        keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
        hashtags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'

        # keywords_field = 'keyword'
        # hashtags_field = 'hashtag'

        # Lấy ngày tạo từ _source và chuyển đổi thành chuẩn định dạng ngày
        item_date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        
        # Nếu ngày tạo trùng khớp với input_date, tiến hành thống kê
        if item_date_str == input_date:
            date_counts[item_date_str] += 1
            for topic_id in item['_source'].get('topic_id', []):
                topic_article_counts[topic_id] += 1
                topic_ids_set.add(topic_id)
            topic_article_counts["all"] += 1  # Tăng tổng số bài viết cho "all"

            for keyword in item['_source'].get(keywords_field, []):
                if len(keyword) > 2:  # Lọc các từ khóa có độ dài lớn hơn 2
                    for topic_id in item['_source'].get('topic_id', []):
                        keyword_counts[topic_id][keyword] += 1
                    keyword_counts["all"][keyword] += 1

            for hashtag in item['_source'].get(hashtags_field, []):
                if len(hashtag) > 2:  # Lọc các hashtag có độ dài lớn hơn 2
                    for topic_id in item['_source'].get('topic_id', []):
                        hashtag_counts[topic_id][hashtag] += 1
                    hashtag_counts["all"][hashtag] += 1

    # Tính toán tỉ lệ phần trăm của các từ khóa
    topic_data = {}
    topic_ids = list(topic_ids_set) + ["all"]

    for topic_id in topic_ids:
        total_articles = topic_article_counts[topic_id]

        keyword_percentages = [
            {"keyword": keyword, "percentage": (count / total_articles) * 100, "record": count}
            for keyword, count in keyword_counts[topic_id].items()
        ]
        keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)

        hashtag_percentages = [
            {"hashtag": hashtag, "percentage": (count / total_articles) * 100, "record": count}
            for hashtag, count in hashtag_counts[topic_id].items()
        ]
        hashtag_percentages = sorted(hashtag_percentages, key=lambda x: x['percentage'], reverse=True)

        if keyword_percentages or hashtag_percentages or topic_id == "all":
            topic_data[topic_id] = {
                "keywords_top": keyword_percentages,
                "hashtags_top": hashtag_percentages
            }
    # Lưu kết quả vào Elasticsearch
    if topic_data.get("all", {}).get("keywords_top") or topic_data.get("all", {}).get("hashtags_top"):
        # Lưu kết quả vào Elasticsearch
        data = {
            "date": date_str,
            "type": platform,
            "topic_ids": topic_data
        }
        sleep(1.5)
        load_data_to_elasticsearch_kw_a(es, data, index_name)

    # Trả về kết quả
    return {
        "date": date_str,
        "type": platform,
        "topic_ids": topic_data
    }
# def calculate_top_keywords_with_trend_logic_topic(input_date, es, historical_data_index, platform):
#     input_datetime = datetime.strptime(input_date, "%m/%d/%Y")
#     input_date_str = input_datetime.strftime("%m_%d_%Y")
    
#     start_date_str = (input_datetime - timedelta(days=6)).strftime("%m_%d_%Y")
#     end_date_str = input_date_str
#     sleep(1.5)
#     daily_keywords = es.search(index=historical_data_index, body={
#         "query": {
#             "bool": {
#                 "filter": [
#                     {"term": {"type": platform}},
#                     {
#                         "range": {
#                             "date": {
#                                 "gte": start_date_str,
#                                 "lte": end_date_str,
#                                 "format": "MM_dd_yyyy"
#                             }
#                         }
#                     }
#                 ]
#             }
#         },
#         "_source": ["date", "topic_ids.all.keywords_top"],
#         "size": 6000 ,
#         "timeout": "60s"
#     })['hits']['hits']

#     current_day_keywords = [hit['_source'] for hit in daily_keywords if hit['_source']['date'] == input_date_str]
#     daily_keywords = [hit['_source'] for hit in daily_keywords]

#     if len(daily_keywords) == 7:
#         print('haha')
#         historical_percentages = {}
#         for record in daily_keywords:
#             for keyword_info in record['topic_ids']['all']['keywords_top'][:6000]:  # Chỉ lấy 6000 từ khóa đầu tiên
#                 keyword = keyword_info['keyword']
#                 if keyword not in historical_percentages:
#                     historical_percentages[keyword] = [0] * 7
#                 index = (input_datetime - datetime.strptime(record['date'], "%m_%d_%Y")).days
#                 historical_percentages[keyword][6 - index] = keyword_info['percentage']

#         keywordtop_for_check = [
#             {
#                 "keyword": keyword_info['keyword'],
#                 "percentage": historical_percentages.get(keyword_info['keyword'], [0]*7),
#                 "record": keyword_info["record"],
#                 "score": keyword_info.get("score", 0)  # Sử dụng giá trị mặc định nếu không có score
#             }
#             for keyword_info in current_day_keywords[0]['topic_ids']['all']['keywords_top'][:6000]
#         ]
        
#         sorted_keywords = Check(keywordtop_for_check)

#         top_keywords = []
#         for kw_dict in sorted_keywords:
#             if  is_not_blackword(kw_dict['keyword']) :
#                 if is_keyword_selected(kw_dict['keyword'], historical_percentages, sorted_keywords, input_date_str):
#                     if('_' in kw_dict['keyword']):
#                         top_keywords.append(kw_dict)

#         if top_keywords:
#             index_name = "keyword_a_trend"
#             sleep(2)
#             update_data = {
#                 "date": input_datetime.strftime("%m_%d_%Y"), 
#                 "type": platform,
#                 "keywords_top": top_keywords
               
#             }
#             es.index(index=index_name, id=f"{input_datetime.strftime('%m_%d_%Y')}_{platform}", body=update_data)

#         return top_keywords, len(top_keywords), current_day_keywords
#     else:
#         return [], 0, current_day_keywords
def calculate_top_keywords_with_trend_logic_topic(input_date, es, historical_data_index, platform):
    input_datetime = datetime.strptime(input_date, "%m/%d/%Y")
    input_date_str = input_datetime.strftime("%m_%d_%Y")
    
    start_date_str = (input_datetime - timedelta(days=6)).strftime("%m_%d_%Y")
    end_date_str = input_date_str
    sleep(1.5)
    
    daily_keywords = es.search(index=historical_data_index, body={
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": platform}},
                    {
                        "range": {
                            "date": {
                                "gte": start_date_str,
                                "lte": end_date_str,
                                "format": "MM_dd_yyyy"
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["date", "topic_ids"],
        "size": 6000,
        "timeout": "60s"
    })['hits']['hits']

    current_day_keywords = [hit['_source'] for hit in daily_keywords if hit['_source']['date'] == input_date_str]
    daily_keywords = [hit['_source'] for hit in daily_keywords]
    
    if len(daily_keywords) == 7:
        historical_percentages = {}

        for record in daily_keywords:
            for topic_id in current_day_keywords[0]['topic_ids'].keys():
                topic_data = record['topic_ids'].get(topic_id, {'keywords_top': []})
                for keyword_info in topic_data['keywords_top'][:6000]: 
                    keyword = keyword_info['keyword']
                    if keyword not in historical_percentages:
                        historical_percentages[keyword] = {tid: [0] * 7 for tid in current_day_keywords[0]['topic_ids'].keys()}
                    index = (input_datetime - datetime.strptime(record['date'], "%m_%d_%Y")).days
                    historical_percentages[keyword][topic_id][6 - index] = keyword_info['percentage']

        results_by_topic = {}
        for topic_id, topic_data in current_day_keywords[0]['topic_ids'].items():
            keywordtop_for_check = [
                {
                    "keyword": keyword_info['keyword'],
                    "percentage": historical_percentages.get(keyword_info['keyword'], {}).get(topic_id, [0] * 7),
                    "record": keyword_info["record"],
                    "score": keyword_info.get("score", 0)
                }
                for keyword_info in topic_data['keywords_top'][:16000]
            ]                    

            sorted_keywords = Check(keywordtop_for_check)

            top_keywords = []
            un_top_keywords = []
            un_top_keywords_2 = []

            black_keywords = []

            for kw_dict in sorted_keywords:
                if is_not_blackword(kw_dict['keyword']):
                    # Pass the correct topic-specific percentages to is_keyword_selected
                    topic_specific_percentages = historical_percentages[kw_dict['keyword']].get(topic_id, [0] * 7)
                    if is_keyword_selected(kw_dict['keyword'], {topic_id: topic_specific_percentages}, sorted_keywords, input_date_str):
                        if '_' in kw_dict['keyword']:
                            top_keywords.append(kw_dict)
                        else:
                            un_top_keywords.append(kw_dict)
                    else: 
                        un_top_keywords_2.append(kw_dict)
                else:
                    black_keywords.append(kw_dict)
            if len(top_keywords)>400:
                top_keywords = top_keywords[:400]+un_top_keywords+  un_top_keywords_2 + top_keywords[400:] + black_keywords
            else : 
                top_keywords = top_keywords+ un_top_keywords+ un_top_keywords_2 + black_keywords
            if top_keywords:
                results_by_topic[topic_id] = top_keywords

        # Save results for each topic in Elasticsearch
        if results_by_topic:
            index_name = "keyword_a_trend_test"
            sleep(3)
            update_data = {
                "date": input_datetime.strftime("%m_%d_%Y"),
                "type": platform,
                "topic_ids": results_by_topic
            }
            es.index(index=index_name, id=f"{input_datetime.strftime('%m_%d_%Y')}_{platform}", body=update_data)

        # return results_by_topic, {tid: len(kws) for tid, kws in results_by_topic.items()}, current_day_keywords
        return  {
                    "date": input_date_str,
                    "type": platform,
                    "topic_ids": results_by_topic
                }
    else:
        index_name = "keyword_a_trend_test"
        # sleep(1.5)
        
        # Chuẩn bị dữ liệu để lưu với định dạng tương tự results_by_topic
        formatted_topic_ids = {}
        if len(current_day_keywords)>0:
            for topic_id, topic_data in current_day_keywords[0]['topic_ids'].items():
                top_keywords = []
                for keyword_info in topic_data['keywords_top']:
                    top_keywords.append({
                        "keyword": keyword_info['keyword'],
                        "percentage": keyword_info['percentage'],
                        "record": keyword_info['record'],
                        "score": keyword_info.get("score", 0),
                        "isTrend": False  # Hoặc xác định giá trị phù hợp theo logic của bạn
                    })
                formatted_topic_ids[topic_id] = top_keywords
        
        update_data = {
            "date": input_datetime.strftime("%m_%d_%Y"),
            "type": platform,
            "topic_ids": formatted_topic_ids
        }
        
        es.index(index=index_name, id=f"{input_datetime.strftime('%m_%d_%Y')}_{platform}", body=update_data)

        # return {}, {}, current_day_keywords
        return {
            "date": input_date_str,
            "type": platform,
            "topic_ids": formatted_topic_ids
        }
if __name__ == "__main__":


    # client = MongoClient('localhost', 27017)
    # db_name = 'top_keyword_3'
    # collection_top_name = 'top_otofun'
    # # db = client[db_name]
    # # collection = db[collection_name]

    # collection = create_or_get_collection(client, db_name, collection_top_name)

    # es = Elasticsearch(["http://192.168.143.54:9200"])
    # historical_data_index = "top_hastag"
    es = Elasticsearch(["http://192.168.143.54:9200"])
    index_name = "keyword_a"
    

    today = datetime.today()
    input_day = datetime.today() - timedelta(days=88)

    input_day_str = input_day.strftime("%m/%d/%Y")
    historical_data = []

    # Xác định ngày trước input_day 7 ngày
    seven_days_before_input = input_day - timedelta(days=14)
    # historical_data = list(collection.find({}, {'_id': False}).sort('_id', -1).limit(1))
    # # ist(collection.find({}, {'_id': False}).sort('_id', -1).limit(1))
    # # historical_data.sort(key=lambda x: datetime.strptime(x['date'], "%m/%d/%Y"), reverse=False)
    # if historical_data:
    #         last_day_str = historical_data[0]['date']
    #         last_day = datetime.strptime(last_day_str, "%m/%d/%Y")
    #         last_day += timedelta(days=1)
    #         # Cập nhật last_day nếu nhỏ hơn ngày trước input_day 7 ngày
    #         if last_day <= seven_days_before_input:
    #             last_day = seven_days_before_input
    # else:
    last_day = seven_days_before_input
    input_day_str = input_day.strftime("%Y/%m/%d 23:59:59")
    current_day = last_day

    with open('content_test_newquery_filter_ytb.json', 'r', encoding='utf-8') as file:
            extracted_keywords = json.load(file)
    while current_day <= input_day:
        current_day_str = current_day.strftime("%m/%d/%Y")
        # Kiểm tra nếu ngày hiện tại không tồn tại trong dữ liệu lịch sử
        if not any(record['date'] == current_day_str for record in historical_data):
            # Thực hiện query và extract_keyword_title
            # Giả sử query_day và extract_keyword_title đã được định nghĩa
            # Thực hiện calculate_top_keywords
            top_keywords = calculate_top_keywords_with_topic_2_es(es, current_day_str, extracted_keywords , index_name, "youtube"  )
            top_keywords_trends = calculate_top_keywords_with_trend_logic_topic(current_day_str, es, index_name, "youtube")

            # Hiển thị kết quả
            print(f"Top Keywords for {current_day_str}: {top_keywords}")
            print(f"Top Keywords trends for {current_day_str}: {top_keywords_trends}")

        # Tăng ngày
        current_day += timedelta(days=1)

    
