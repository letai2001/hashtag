from pymongo import MongoClient, ASCENDING

# Kết nối tới MongoDB
client = MongoClient('172.168.200.200', 27017)
db = client['top_keyword_3']

# Giả sử các collections đã có
# valid_collections = ["top_facebook", "top_tiktok", "top_youtube", "top_voz", "top_xamvn", "top_otofun", "top_media"]

# # Tạo chỉ mục cho từng collection
# for collection_name in valid_collections:
#     collection = db[collection_name]
#     # Tạo chỉ mục trên trường `_id`
#     collection.create_index([("_id", ASCENDING)])
#     # Tạo chỉ mục trên trường `keywords_top.keyword`
#     collection.create_index([("keywords_top.keyword", ASCENDING)])
#     collection.create_index([("hastag_top.hastag", ASCENDING)])
# # collection = db['top_facebook']

# # Ví dụ truy vấn với explain()
# pipeline = [
#     {"$match": {"_id": {"$gte": "05_08_2024", "$lte": "05_16_2024"}}},
#     {"$unwind": "$keywords_top"},
#     {"$group": {"_id": "$keywords_top.keyword", "total_record": {"$sum": "$keywords_top.record"}}}
# ]



# explain_result = db.command('aggregate', 'top_facebook', pipeline=pipeline, explain=True)
# print(explain_result)
from datetime import datetime

def measure_query_performance(collections_to_query, start_date, end_date):
    # Kết nối tới MongoDB

    for collection_name in collections_to_query:
        # Chuẩn bị pipeline
        # pipeline = [
        #     {"$match": {"_id": {"$gte": start_date.strftime("%m_%d_%Y"), "$lte": end_date.strftime("%m_%d_%Y")}}},
        #     {"$unwind": "$keywords_top"},
        #     {"$group": {
        #         "_id": "$keywords_top.keyword",
        #         "total_records": {"$sum": "$keywords_top.record"}
        #     }}
        # ]
        pipeline = [
            {"$match": {"_id": {"$gte": start_date, "$lte": end_date}}},
            {"$unwind": "$keywords_top"},
            {"$group": {"_id": "$keywords_top.keyword", "total_record": {"$sum": "$keywords_top.record"}}}
        ]

#         pipeline = [
#     {"$match": {"_id": {"$gte": start_date.strftime("%m_%d_%Y"), "$lte": end_date.strftime("%m_%d_%Y")}}},  # Ensure these are datetime objects
#     {"$project": {
#         "keyword_totals": {
#             "$reduce": {
#                 "input": "$keywords_top",
#                 "initialValue": {},
#                 "in": {
#                     "$arrayToObject": {
#                         "$concatArrays": [
#                             {"$objectToArray": "$$value"},
#                             [{"k": "$$this.keyword", "v": {
#                                 "$cond": [
#                                     {"$eq": [{"$type": {"$objectToArray": "$$value"}}, "missing"]},
#                                     "$$this.record",
#                                     {"$add": ["$$this.record", {"$reduce": {
#                                         "input": {"$objectToArray": "$$value"},
#                                         "initialValue": 0,
#                                         "in": {"$cond": [
#                                             {"$eq": ["$$this.k", "$$value.keyword"]},
#                                             {"$add": ["$$value.v", "$$this.record"]},
#                                             "$$value.v"
#                                         ]}
#                                     }}]}
#                                 ]
#                             }}]
#                         ]
#                     }
#                 }
#             }
#         }
#     }}
# ]

        
        # Ghi nhận thời gian bắt đầu
        start_time = datetime.now()
        
        # Thực hiện truy vấn
        results = list(db[collection_name].aggregate(pipeline))
        # print(results)
        # Ghi nhận thời gian kết thúc
        end_time = datetime.now()
        
        # Tính toán thời gian thực thi
        duration = end_time - start_time
        
        # In ra thời gian thực thi
        print(f"Query time for collection {collection_name}: {duration.total_seconds()} seconds")

# Ví dụ sử dụng hàm
if __name__ == "__main__":
    # collections = ["top_facebook", "top_tiktok", "top_youtube"]
    # # start_date = "05/08/2024"
    # # end_date = "05/16/2024"
    # start_date = datetime.strptime("05/08/2024", "%m/%d/%Y")
    # end_date = datetime.strptime("05/16/2024", "%m/%d/%Y")
    # measure_query_performance(collections, start_date, end_date)


    collections = ["top_facebook", "top_tiktok", "top_youtube", "top_voz", "top_xamvn", "top_otofun", "top_media"]
    for collection in collections:
        db[collection].create_index([("hastag_top.topic_id", 1)])
