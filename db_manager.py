# -*- coding: utf-8 -*-
# db_manager.py
import os
import logging
from pymongo import MongoClient, ReturnDocument # Import ReturnDocument nếu cần dùng trong file này
import pymongo.errors # Import errors để bắt lỗi kết nối cụ thể
from dotenv import load_dotenv
from bson.objectid import ObjectId
import datetime
import time # Thêm time để có thể dùng sleep khi retry connect

# --- Load Environment Variables ---
load_dotenv(override=True)

# --- Logging Setup (Cơ bản) ---
# Worker/App chính nên cấu hình logging chi tiết hơn
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

# --- MongoDB Configuration ---
mongodb_uri = os.getenv("MONGO_URI", "mongodb://root:thevinh123@192.168.1.22:27017/")
db_name = os.getenv("MONGODB_DB_NAME", "content_db")
topics_collection_name = "Topics"
content_generations_collection_name = "ContentGenerations"
script_chunks_collection_name = "ScriptChunks"

# --- Biến toàn cục cho kết nối và collections ---
_client = None
db = None
topics_collection = None
content_generations_collection = None
script_chunks_collection = None
_last_connect_attempt = 0 # Theo dõi thời gian thử kết nối cuối

def connect_db(force_reconnect=False):
    """
    Thiết lập hoặc kiểm tra lại kết nối MongoDB.
    Gán các biến collection toàn cục.
    Ném ConnectionError nếu thất bại.
    """
    global _client, db, topics_collection, content_generations_collection, script_chunks_collection, _last_connect_attempt

    # Giới hạn tần suất thử lại kết nối nếu thất bại liên tục
    now = time.time()
    if not force_reconnect and db is not None and _client is not None:
        # Nếu đã kết nối, thử ping nhanh để kiểm tra
        try:
            _client.admin.command('ping')
            logging.debug("DB connection check (ping) successful.")
            return True # Kết nối vẫn tốt
        except (pymongo.errors.ConnectionFailure, pymongo.errors.ServerSelectionTimeoutError) as ping_err:
            logging.warning(f"DB ping failed: {ping_err}. Will attempt reconnect.")
            db = None # Đặt lại để buộc kết nối lại
            _client = None
        except Exception as generic_ping_err:
             logging.error(f"Unexpected error during DB ping: {generic_ping_err}")
             db = None # Đặt lại để buộc kết nối lại
             _client = None


    # Chỉ thử kết nối lại nếu chưa kết nối hoặc đã đủ thời gian chờ sau lần thử lỗi cuối
    if db is None and (now - _last_connect_attempt > 30): # Chờ 30s giữa các lần thử kết nối lỗi
        _last_connect_attempt = now # Ghi lại thời điểm thử
        try:
            logging.info(f"Attempting to connect to MongoDB: {mongodb_uri.split('@')[-1] if '@' in mongodb_uri else mongodb_uri} / DB: {db_name}")
            # Tăng timeout để xử lý mạng chậm
            _client = MongoClient(mongodb_uri,
                                  serverSelectionTimeoutMS=10000,
                                  connectTimeoutMS=10000,
                                  socketTimeoutMS=20000,
                                  retryWrites=True
                                 )
            # Kiểm tra kết nối bằng cách lấy DB (sẽ raise lỗi nếu thất bại)
            db = _client[db_name]
            topics_collection = db[topics_collection_name]
            content_generations_collection = db[content_generations_collection_name]
            script_chunks_collection = db[script_chunks_collection_name]
            logging.info(f"Successfully connected/reconnected to MongoDB.")

            # Đảm bảo Indices (nên chạy ở background)
            try:
                topics_collection.create_index([("status", 1)], background=True)
                content_generations_collection.create_index([("topic_id", 1)], background=True)
                content_generations_collection.create_index([("status", 1)], background=True)
                content_generations_collection.create_index([("priority", 1), ("created_at", 1)], background=True)
                if script_chunks_collection is not None: # <<< SỬA: Dùng is not None
                     script_chunks_collection.create_index([("generation_id", 1), ("section_index", 1)], background=True)
                logging.info("MongoDB index creation requests sent.")
            except Exception as index_err:
                 logging.warning(f"Could not ensure indices: {index_err}")
            return True # Kết nối thành công

        except Exception as e:
            logging.critical(f"CRITICAL: Failed to connect to MongoDB: {e}")
            db = None; _client = None # Đặt lại các biến
            topics_collection = None; content_generations_collection = None; script_chunks_collection = None
            raise ConnectionError(f"Failed to connect to MongoDB: {e}") from e

    elif db is None: # Nếu db là None nhưng chưa đủ thời gian chờ
         logging.warning("DB connection failed recently, waiting before retry...")
         raise ConnectionError("DB connection unavailable, waiting period.")
    else: # DB is not None, connection assumed ok from previous check or ping
        return True


def get_db():
    """Trả về đối tượng database, kết nối nếu cần. Raise ConnectionError nếu thất bại."""
    connect_db() # Hàm này sẽ raise ConnectionError nếu không kết nối được
    # Sau khi gọi connect_db mà không có lỗi, db chắc chắn không phải None
    return db

def get_topics_collection():
    """Trả về đối tượng collection 'Topics'. Raise ConnectionError nếu thất bại."""
    connect_db()
    if topics_collection is None: # <<< SỬA: Dùng is None
         raise ConnectionError("Topics collection is unavailable.")
    return topics_collection

def get_content_generations_collection():
    """Trả về đối tượng collection 'ContentGenerations'. Raise ConnectionError nếu thất bại."""
    connect_db()
    if content_generations_collection is None: # <<< SỬA: Dùng is None
         raise ConnectionError("ContentGenerations collection is unavailable.")
    return content_generations_collection

def get_script_chunks_collection():
    """Trả về đối tượng collection 'ScriptChunks'. Raise ConnectionError nếu thất bại."""
    connect_db()
    if script_chunks_collection is None: # <<< SỬA: Dùng is None
         raise ConnectionError("ScriptChunks collection is unavailable.")
    return script_chunks_collection

# --- Hàm Lưu Chunk (Sửa lại để dùng getter và xử lý lỗi tốt hơn) ---
def save_chunk_to_db(generation_id, script_name, section_index, section_title, text_content, level, item_type=None, audio_file_path=None):
    """Lưu hoặc cập nhật script chunk."""
    try:
        collection = get_script_chunks_collection() # Lấy collection qua getter
    except ConnectionError as e:
        logging.error(f"Cannot save chunk {section_index} for gen:{generation_id}. DB Error: {e}")
        return None

    if not isinstance(generation_id, ObjectId):
        try: generation_id = ObjectId(generation_id)
        except Exception as e: logging.error(f"Invalid generation_id format: {generation_id}. Error: {e}"); return None

    doc = { # Tạo document mới hoặc các trường cần $set
        "generation_id": generation_id, "script_name": script_name, "section_index": section_index,
        "section_title": str(section_title or ""), "text_content": str(text_content or ""), "level": level,
        "item_type": item_type, "audio_file_path": audio_file_path,
        # Các trường này sẽ không ghi đè nếu dùng $set và $setOnInsert đúng cách
        # "audio_created": False, "audio_error": None,
    }
    try:
        result = collection.update_one(
             {"generation_id": generation_id, "section_index": section_index},
             {
                 "$set": { # Luôn cập nhật các trường này
                     "section_title": doc["section_title"], "text_content": doc["text_content"],
                     "level": doc["level"], "item_type": doc["item_type"], "script_name": doc["script_name"],
                     "audio_file_path": doc["audio_file_path"], # Cập nhật cả cái này nếu chunk được tạo lại
                     "updated_at": datetime.datetime.now(datetime.timezone.utc)
                 },
                 "$setOnInsert": { # Chỉ đặt khi insert mới
                     "created_at": datetime.datetime.now(datetime.timezone.utc),
                     "generation_id": doc["generation_id"],
                     "section_index": doc["section_index"],
                     "audio_created": False, # Trạng thái audio ban đầu
                     "audio_error": None
                 }
             },
             upsert=True
        )
        if result.upserted_id:
            logging.info(f"Inserted chunk idx:{section_index} ('{doc['section_title'][:50]}...') doc_id:{result.upserted_id}")
            return result.upserted_id
        elif result.modified_count > 0 or result.matched_count > 0: # Nếu update hoặc match (không đổi)
             logging.info(f"Updated/Matched chunk idx:{section_index} ('{doc['section_title'][:50]}...')")
             existing = collection.find_one({"generation_id": generation_id, "section_index": section_index}, {"_id": 1})
             return existing['_id'] if existing else None
        else: # Upsert không thành công cũng không match? -> Lạ
             logging.error(f"Chunk upsert failed unexpectedly for idx:{section_index}, gen:{generation_id}")
             return None

    except pymongo.errors.PyMongoError as e: # Bắt lỗi cụ thể của pymongo
        logging.error(f"PyMongoError saving chunk {section_index} for gen:{generation_id}: {e}")
        return None
    except Exception as e: # Bắt lỗi khác
        logging.exception(f"Unexpected error saving chunk {section_index} for gen:{generation_id}")
        return None

# --- Hàm Lấy Text (Sửa lại để dùng getter và xử lý lỗi tốt hơn) ---
def get_text_from_db(generation_id):
    """Fetches and concatenates text content for a generation ID."""
    try:
        collection = get_script_chunks_collection() # Lấy collection qua getter
    except ConnectionError as e:
        logging.error(f"Cannot get text for gen {generation_id}. DB Error: {e}")
        return "" # Trả về rỗng nếu lỗi DB

    if not isinstance(generation_id, ObjectId):
        try: generation_id = ObjectId(generation_id)
        except Exception: logging.error(f"Invalid ID format: {generation_id}"); return ""

    try:
        documents = collection.find({"generation_id": generation_id}).sort("section_index", 1)
        # Dùng list comprehension và join để hiệu quả hơn
        all_text = "\n\n".join([doc.get("text_content", "") for doc in documents])
        return all_text.strip()
    except pymongo.errors.PyMongoError as e:
        logging.error(f"PyMongoError fetching text for gen {generation_id}: {e}")
        return ""
    except Exception as e:
        logging.exception(f"Unexpected error fetching text for gen {generation_id}")
        return ""

# --- Có thể thêm các hàm DB helper khác ở đây ---