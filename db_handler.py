# -*- coding: utf-8 -*-
"""
Module xử lý tương tác với MongoDB cho ứng dụng tạo video.
"""

import os
import datetime
import traceback
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId
from dotenv import load_dotenv

# Tải biến môi trường (có thể gọi lại nếu chạy độc lập)
load_dotenv()

# Lấy thông tin kết nối từ biến môi trường
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# Biến toàn cục để giữ kết nối (Singleton Pattern đơn giản)
_client = None
_db = None
_collection = None

def connect_db(mongo_uri=MONGO_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME):
    """
    Thiết lập hoặc trả về kết nối MongoDB đang hoạt động.

    Args:
        mongo_uri (str, optional): Chuỗi kết nối MongoDB.
        db_name (str, optional): Tên database.
        collection_name (str, optional): Tên collection.

    Raises:
        ValueError: Nếu thiếu thông tin kết nối.
        ConnectionFailure: Nếu không kết nối được DB.
        Exception: Cho các lỗi khác.

    Returns:
        pymongo.collection.Collection: Đối tượng collection đã kết nối.
    """
    global _client, _db, _collection

    # Nếu đã có kết nối, kiểm tra xem còn sống không
    if _collection is not None:
        if _client:
            try:
                _client.admin.command('ismaster') # Lệnh kiểm tra nhẹ nhàng
                return _collection
            except ConnectionFailure:
                print("Cảnh báo: Mất kết nối MongoDB. Đang kết nối lại...")
            except Exception as ping_err:
                 print(f"Cảnh báo: Lỗi ping MongoDB ({ping_err}). Đang kết nối lại...")
        # Nếu kiểm tra lỗi hoặc chưa có client, reset để kết nối lại
        _client = _db = _collection = None

    # Kiểm tra thông tin kết nối
    if not mongo_uri or not db_name or not collection_name:
        raise ValueError("Thiếu thông tin kết nối MongoDB (MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME)")

    # Kết nối mới
    try:
        print(f"Đang kết nối MongoDB: {mongo_uri} -> DB: {db_name} -> Col: {collection_name}")
        _client = MongoClient(mongo_uri,
                              serverSelectionTimeoutMS=5000, # Timeout 5 giây
                              connectTimeoutMS=5000,
                              socketTimeoutMS=10000) # Tăng socket timeout
        _client.admin.command('ismaster') # Xác nhận kết nối thành công
        _db = _client[db_name]
        _collection = _db[collection_name]
        print("-> Kết nối MongoDB thành công.")
        return _collection
    except ConnectionFailure as e:
        print(f"!!! Lỗi kết nối MongoDB: {e}")
        _client = _db = _collection = None
        raise # Ném lại lỗi để hàm gọi biết
    except Exception as e:
        print(f"!!! Lỗi MongoDB khác khi kết nối: {e}")
        _client = _db = _collection = None
        raise

def get_next_pending_task():
    """
    Tìm một task đang chờ xử lý ('generated'), cập nhật trạng thái thành
    'rendering' một cách nguyên tử và trả về document đó.

    Returns:
        dict or None: Document đã được cập nhật hoặc None nếu không có task/lỗi.
    """
    try:
        collection = connect_db() # Đảm bảo có kết nối
        if collection is None:
             print("Lỗi: Không thể lấy collection MongoDB.")
             return None

        query = {
            "thumbnail_status": "generated",
            "$or": [
                # Điều kiện 1: Trường video_render_status không tồn tại
                {"video_render_status": {"$exists": False}},
                # Điều kiện 2: Trường video_render_status tồn tại nhưng giá trị không nằm trong danh sách loại trừ
                {"video_render_status": {"$nin": ["rendering", "finish", "skipped"]}}
            ]
        }
        update = {
            "$set": {
                "video_render_status": "rendering",
                "video_render_start_time": datetime.datetime.utcnow(),
                "render_error": None # Xóa lỗi cũ khi bắt đầu render lại
            }
        }
        # Sắp xếp để lấy task cũ hơn trước (tùy chọn)
        # sort_order = [('creation_timestamp', 1)] # Giả sử có trường timestamp

        # Tìm và cập nhật, trả về document SAU KHI cập nhật
        doc = collection.find_one_and_update(
            query,
            update,
            # sort=sort_order, # Bỏ comment nếu dùng sort
            return_document=ReturnDocument.AFTER
        )

        if doc:
             print(f"-> Đã khóa task cho document ID: {doc['_id']}")
             return doc
        else:
             # print("DEBUG: Không tìm thấy task nào đang chờ.") # Debug
             return None # Không còn task nào

    except OperationFailure as e:
        # Lỗi cụ thể từ MongoDB (vd: hết quyền, sai query)
        print(f"Lỗi thao tác MongoDB khi lấy task: {e}")
        return None
    except ConnectionFailure: # Xử lý lỗi kết nối lại nếu có thể
        print(f"Lỗi kết nối MongoDB khi lấy task. Thử lại sau.")
        # Không raise ở đây để vòng lặp chính có thể tiếp tục hoặc chờ
        return None
    except Exception as e:
        print(f"Lỗi không mong muốn khi lấy task: {e}")
        traceback.print_exc()
        return None

def update_task_status(doc_id, status, output_path=None, error_message=None):
    """
    Cập nhật trạng thái và thông tin liên quan cho một task document.

    Args:
        doc_id (str or ObjectId): ID của document cần cập nhật.
        status (str): Trạng thái mới ('finish', 'failed', 'skipped', etc.).
        output_path (str, optional): Đường dẫn file video cuối cùng nếu thành công.
        error_message (str, optional): Thông báo lỗi nếu thất bại.

    Returns:
        bool: True nếu cập nhật thành công, False nếu lỗi.
    """
    if not isinstance(doc_id, ObjectId):
        try:
            doc_id = ObjectId(str(doc_id)) # Chuyển đổi string ID sang ObjectId
        except Exception:
            print(f"!!! Lỗi: Định dạng document ID không hợp lệ: {doc_id}")
            return False

    try:
        collection = connect_db()
        if collection is None:
             print(f"Lỗi: Không thể cập nhật status cho {doc_id} do mất kết nối DB.")
             return False

        update_fields = {
            "video_render_status": status,
            "video_render_end_time": datetime.datetime.utcnow(),
        }
        unset_fields = {} # Các trường cần xóa

        if status == "finish":
            if output_path: update_fields["final_video_path"] = output_path
            unset_fields["render_error"] = "" # Xóa lỗi cũ nếu thành công
        elif status in ["failed", "skipped"]:
            update_fields["final_video_path"] = None # Xóa đường dẫn nếu lỗi
            if error_message: update_fields["render_error"] = str(error_message)[:1000] # Giới hạn lỗi

        # Xây dựng lệnh update cuối cùng
        update_command = {"$set": update_fields}
        if unset_fields:
             update_command["$unset"] = unset_fields

        print(f"-> Chuẩn bị cập nhật doc ID {doc_id} thành status: {status}...")
        result = collection.update_one({"_id": doc_id}, update_command)

        if result.modified_count == 1:
            print(f"-> Cập nhật status thành công.")
            return True
        elif result.matched_count == 1:
             print(f"-> Document {doc_id} được tìm thấy nhưng không cần cập nhật (có thể status đã đúng?).")
             return True # Vẫn coi là thành công về mặt thao tác DB
        else:
            print(f"!!! Cảnh báo: Không tìm thấy document ID {doc_id} để cập nhật.")
            return False

    except OperationFailure as e:
        print(f"!!! Lỗi thao tác MongoDB khi cập nhật status cho {doc_id}: {e}")
        return False
    except ConnectionFailure:
         print(f"Lỗi kết nối MongoDB khi cập nhật status cho {doc_id}.")
         return False
    except Exception as e:
        print(f"!!! Lỗi không mong muốn khi cập nhật status cho {doc_id}: {e}")
        traceback.print_exc()
        return False

def close_db_connection():
    """Đóng kết nối MongoDB nếu đang mở."""
    global _client
    if _client:
        try:
            _client.close()
            print("-> Đã đóng kết nối MongoDB.")
        except Exception as e:
            print(f"Lỗi khi đóng kết nối MongoDB: {e}")
        finally:
             _client = None # Đảm bảo reset biến client
             _db = None
             _collection = None