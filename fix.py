from pymongo import MongoClient
from datetime import datetime, timedelta
import difflib
from fuzzywuzzy import fuzz


def convert_id_to_datetime(db_name, collection_name):
    client = MongoClient('172.168.200.200', 27017)
    db = client[db_name]
    collection = db[collection_name]

    # Lặp qua mỗi document trong collection
    for doc in collection.find():
        try:
            # Chuyển đổi _id từ chuỗi sang datetime
            datetime_id = datetime.strptime(doc['_id'], "%m_%d_%Y")

            # Tạo document mới với _id đã được chuyển đổi
            new_doc = doc.copy()
            new_doc['_id'] = datetime_id

            # Chèn document mới
            collection.insert_one(new_doc)

            # Xóa document cũ
            collection.delete_one({'_id': doc['_id']})
            
            print(f"Updated document {doc['_id']} to {datetime_id}")

        except ValueError as e:
            print(f"Error converting {doc['_id']}: {e}")

# Gọi hàm chuyển đổi
# convert_id_to_datetime('top_keyword_2', 'top_youtube')
# input_day = datetime.today() - timedelta(days=114)
# print(input_day)
a = [0]
print(a[:4000])
# ratio = difflib.SequenceMatcher(None, "bac hồ", "bác_hồ").ratio()
# print(ratio)
# ratio = fuzz.ratio("bác hồ", "bác_hồ")
# print(ratio)

# a =   {
    
#     "date": "07_21_2024",
#     "type": "keyword",
#   "type_social": "facebook",
#   "name": "vietnam",
  
#   "topic_ids": {
#     "4b14366c-0455-4ed7-9f86-c3291f238c49": {
     
#           "percentage": 1.7391034347826086,
#           "record": 42
#         },

#     "651b8fdb-e6eb-4846-b7de-515e1bb4374f": {
      
#           "percentage": 1.7391034347826086,
#           "record": 42
#     },
#     "all": {
      
#           "percentage": 1.7391034347826086,
#           "record": 82
      
#       }
#     }
#   }

# a=  {
#   "date": "07_21_2024",
#   "type": "facebook",
#       "keywords_top": [
#         {
#           "keyword": "example_keyword",
#           "percentage": 1.7391034347826086,
#           "record": 42
#         },
#         {
#           "keyword": "example_keyword_2",
#           "percentage": 1.7391034347826086,
#           "record": 42
#         }
#       ],
     

   
#       }
    
