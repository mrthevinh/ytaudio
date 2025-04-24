# check_word_count.py
import os
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from dotenv import load_dotenv
import argparse
import jieba # <<< THÊM IMPORT JIEBA

# --- (Setup logging, load dotenv, MongoDB config như trước) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv(override=True)
mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
db_name = os.getenv("MONGODB_DB_NAME", "content_db")
script_chunks_collection_name = "ScriptChunks"
# Thêm collection để lấy ngôn ngữ
content_generations_collection_name = "ContentGenerations"

db = None
script_chunks_collection = None
content_generations_collection = None

def connect_db_checker():
    global client, db, script_chunks_collection, content_generations_collection # Thêm collection mới
    if db is None:
        try:
            logging.info(f"Checker connecting to MongoDB: {db_name}")
            client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client[db_name]
            script_chunks_collection = db[script_chunks_collection_name]
            content_generations_collection = db[content_generations_collection_name] # Gán collection
            logging.info(f"Checker connected successfully.")
            return True
        except Exception as e:
            logging.critical(f"Checker CRITICAL: Failed to connect to MongoDB: {e}")
            db = None
            script_chunks_collection = None
            content_generations_collection = None
            return False
    return True

def count_content_length(generation_id_str): # Đổi tên hàm
    """
    Đếm tổng số "từ" (dùng Jieba cho tiếng Trung) hoặc ký tự (ngôn ngữ khác)
    của tất cả các chunk thuộc về một generation_id.
    """
    if not connect_db_checker() or script_chunks_collection is None or content_generations_collection is None:
        return -1, "Database connection failed.", "N/A"

    try:
        gen_object_id = ObjectId(generation_id_str)
    except Exception:
        logging.error(f"Invalid Generation ID format provided: {generation_id_str}")
        return -1, "Invalid Generation ID format.", "N/A"

    # Lấy ngôn ngữ từ ContentGenerations document
    generation_doc = content_generations_collection.find_one({"_id": gen_object_id}, {"language": 1})
    if not generation_doc:
        return -1, f"Generation document not found for ID: {generation_id_str}", "N/A"
    language = generation_doc.get("language", "Unknown") # Lấy ngôn ngữ

    total_count = 0
    chunk_count = 0
    error_occurred = False
    error_msg = ""
    count_unit = "characters" # Mặc định

    try:
        chunks_cursor = script_chunks_collection.find({"generation_id": gen_object_id})

        # Kiểm tra nếu là tiếng Trung
        is_chinese = language and ("Chinese" in language.lower() or "trung" in language.lower())
        if is_chinese:
            count_unit = "words (Jieba)"

        for chunk in chunks_cursor:
            chunk_count += 1
            text_content = chunk.get("text_content")
            if isinstance(text_content, str):
                # ***** SỬA LẠI CÁCH ĐẾM *****
                if is_chinese:
                    words = list(jieba.cut(text_content))
                    # words = [w for w in words if w.strip()] # Lọc khoảng trắng nếu cần
                    total_count += len(words)
                else:
                    total_count += len(text_content) # Đếm ký tự
                # ****************************
            else:
                logging.warning(f"Chunk {chunk.get('_id')} has invalid 'text_content'.")

        if chunk_count == 0:
             logging.warning(f"No chunks found for Generation ID: {generation_id_str}")
             error_msg = "No content chunks found."
             # Không coi là lỗi nếu chưa có content

    except Exception as e:
        logging.exception(f"Error querying DB for generation {generation_id_str}: {e}")
        error_msg = f"Database query error: {e}"
        error_occurred = True
        total_count = -1

    if error_occurred:
        return -1, error_msg, language
    else:
        return total_count, f"Processed {chunk_count} chunks.", language, count_unit # Trả về cả unit

# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check content length (words for Chinese, chars otherwise).") # Sửa mô tả
    parser.add_argument("generation_id", type=str, help="The Generation ID (ObjectId string) to check.")
    args = parser.parse_args()

    generation_id_to_check = args.generation_id

    logging.info(f"Checking content length for Generation ID: {generation_id_to_check}")

    count_value, message, lang, unit = count_content_length(generation_id_to_check) # Lấy thêm lang, unit

    if count_value >= 0:
        print("-" * 30)
        print(f"Generation ID: {generation_id_to_check}")
        print(f"Language: {lang}")
        print(f"Total Count ({unit}): {count_value}") # Hiển thị đơn vị đếm
        print(f"Details: {message}")
        print("-" * 30)
    else:
        print("-" * 30)
        print(f"Generation ID: {generation_id_to_check}")
        print(f"Error calculating content length.") # Sửa thông báo
        print(f"Details: {message}")
        print("-" * 30)