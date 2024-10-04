from pymongo import MongoClient
import json
from datetime import datetime, timedelta

def connect_to_mongodb():
    # Connect to the MongoDB server
    client = MongoClient('localhost', 27017)
    return client

def create_or_get_collection(client, db_name, collection_name):
    # Select the database and collection
    db = client[db_name]
    collection = db[collection_name]
    return collection

def insert_documents(collection, documents):
    # Insert multiple documents into the collection
    result = collection.insert_many(documents)
    return result.inserted_ids

def find_documents(collection, query):
    # Find documents matching the query
    results = collection.find(query)
    return list(results)  # Convert cursor to list

def update_documents(collection, query, new_values):
    # Update documents matching the query
    result = collection.update_many(query, new_values)
    return result.modified_count

def delete_documents(collection, query):
    # Delete documents matching the query
    result = collection.delete_many(query)
    return result.deleted_count
# def load_data_to_mongodb_new_data(data ,collection):
#     # Kết nối tới MongoDB

#     # Lặp qua dữ liệu và cập nhật vào MongoDB
#     # for record in data:
#         # Tạo ID dựa trên ngày, thay thế dấu '/' bằng '_'
#         document_id = data['date'].replace('/', '_')
#         # Sử dụng update_one với upsert=True để cập nhật hoặc thêm mới document
#         collection.update_one(
#             {'_id': document_id},  # Tìm document theo _id
#             {'$set': data},      # Cập nhật các trường từ record
#             upsert=True            # Tạo mới nếu không tìm thấy document
#         )

def load_data_to_mongodb_new_data(data, collection):
    # Chuyển đổi chuỗi ngày sang đối tượng datetime
    # Giả sử ngày đã được định dạng là "%m/%d/%Y" trong dữ liệu
    document_id = datetime.strptime(data['date'], '%m/%d/%Y')

    # Sử dụng update_one với upsert=True để cập nhật hoặc thêm mới document
    collection.update_one(
        {'_id': document_id},  # Tìm document theo _id
        {'$set': data},       # Cập nhật các trường từ record
        upsert=True            # Tạo mới nếu không tìm thấy document
    )

def delete_document_by_id(db_name, collection_name, document_id):
    # Kết nối tới MongoDB
    client = MongoClient('localhost', 27017)
    db = client[db_name]
    collection = db[collection_name]

    # Xóa document với _id được cung cấp
    result = collection.delete_one({'_id': document_id})

    # Kiểm tra và in ra kết quả của thao tác xóa
    if result.deleted_count > 0:
        print(f"Document with _id {document_id} was deleted successfully.")
    else:
        print(f"No document found with _id {document_id}.")
def query_tiktok_keywords(date_str , hour , minute):
    
    # start_date = datetime.strptime(date_str, "%m-%d-%Y")
    # start_date = datetime.strptime(date_str + " " + hour, "%Y-%m-%d %H")
    # end_date = start_date + timedelta(hours=1)

    # end_date = start_date + timedelta(days=1)
    start_date = datetime.strptime(f"{date_str} {hour}:{minute}", "%Y-%m-%d %H:%M")
    end_date = start_date + timedelta(minutes=5)

    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    client = MongoClient("mongodb://172.168.200.202:30000/osint")
    collection = client["osint"]["keywords"]
    
    query = {
        "created_time": {"$gte": start_timestamp, "$lt": end_timestamp},
        "type": {"$regex": "tiktok", "$options": "i"}  
    }
    
    results = collection.find(query, {'value': 1, 'type': 1 , 'created_time':1}).limit(100)

    results = list(results)
    formatted_results = []
    # for result in results:
    #     created_time = datetime.fromtimestamp(result["created_time"])
    #     result["created_time"] = created_time.strftime("%m/%d/%Y %H:%M:%S")
    #     formatted_results.append(result)
    
    with open('results.json', 'w' , encoding='utf-8') as f:
        json.dump(results, f, indent=4)
    
    return results
def count_keywords():
    # Kết nối đến MongoDB
    client = MongoClient("mongodb://172.168.200.202:30000/osint")
    collection = client["osint"]["keywords"]
    
    # Đếm số lượng documents trong collection 'keyword'
    count = collection.count_documents({})
    
    # Trả về kết quả
    return count

if __name__ == "__main__":  
    # results = query_tiktok_keywords("2024-04-26", "16" , "16")
    print(count_keywords())
# for result in results:
#     print(result)


# Define connection parameters
    # client = connect_to_mongodb()
    # collection = create_or_get_collection(client, 'top_taile', 'youtube_taile_hastag')
    # # historical_data = list(collection.find({}, {'_id': False}).sort('_id', -1).limit(1))
    
    # document_id = '04_19_2024'  # Giả sử đây là _id của document bạn muốn xóa

    # collection.delete_one({'_id': document_id})
    # print(historical_data)
# historical_data = list(collection.find({}, {'_id': False}).sort('_id', 1))
# print(historical_data)
# data = list(collection.find({}, {'_id': False}))
# print(data[0])
# Example documents to insert
# documents = [
#     {"name": "Alice", "age": 25},
#     {"name": "Bob", "age": 30}
# ]
# collection.delete_many({})

# # Insert documents
# inserted_ids = insert_documents(collection, documents)
# file_path ='keyword_percentages_main_title_noun_phase.json'

# # Đọc dữ liệu từ file JSON
# with open(file_path, 'r' , encoding='utf-8') as file:
#     data = json.load(file)
# # insert_documents(collection, data)
# with open('keyword_percentages_main_title_noun_phase_2.json', 'w', encoding='utf-8') as file:
#         json.dump(list(collection.find({}, {'_id': False})), file, ensure_ascii=False, indent=4)
# # Example query
# query = {"name": "Alice"}

# # Find documents
# found_documents = find_documents(collection, query)

# # Update documents
# new_values = {"$set": {"age": 26}}
# updated_count = update_documents(collection, query, new_values)

# # Delete documents
# deleted_count = delete_documents(collection, query)

# Uncomment the below lines to execute and see the results
# print("Inserted IDs:", inserted_ids)
# print("Found Documents:", found_documents)
# print("Updated Count:", updated_count)
# print("Deleted Count:", deleted_count)
