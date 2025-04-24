from pymongo import MongoClient
import os

# Thông tin kết nối MongoDB bằng URL
MONGO_URI = "mongodb://root:thevinh123@192.168.1.22:27017/"  # Thay đổi thành MongoDB Connection URL của bạn

# Tên database và collection
MONGO_DB = "content_db"  # Thay đổi thành tên database của bạn
MONGO_COLLECTION = "ContentGenerations"  # Thay đổi thành tên collection chứa thông tin video

# Tên trường chứa đường dẫn file MP4 trong document MongoDB
VIDEO_PATH_FIELD = "final_video_path"  # Thay đổi nếu tên trường khác

def check_and_update_video_status():
    """
    Kết nối MongoDB bằng URL, kiểm tra sự tồn tại của file MP4 và cập nhật trạng thái nếu không tồn tại.
    """
    client = None
    try:
        # Kết nối MongoDB bằng URL
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        # Lấy tất cả các document trong collection (bạn có thể thêm điều kiện lọc nếu cần)
        documents = collection.find()

        for doc in documents:
            video_path = doc.get(VIDEO_PATH_FIELD)

            # Kiểm tra nếu đường dẫn file MP4 tồn tại trong document
            if video_path:
                # Kiểm tra xem file MP4 có tồn tại trên hệ thống không
                if not os.path.exists(video_path):
                    # Nếu file không tồn tại, cập nhật trạng thái trong MongoDB
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"video_render_status": "rerender", "thumbnail_status": "recreate"}}
                    )
                    print(f"Đã cập nhật trạng thái cho video có ID: {doc['_id']}")
                else:
                    print(f"File MP4 tồn tại cho video có ID: {doc['_id']}")
            else:
                print(f"Không tìm thấy đường dẫn file MP4 cho video có ID: {doc['_id']}")

        print("Hoàn thành kiểm tra và cập nhật trạng thái video.")

    except ConnectionError as e:
        print(f"Lỗi kết nối MongoDB: {e}")
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")
    finally:
        if client:
            client.close()
            
def file_exists(file_path):
    return os.path.exists(file_path)

# Hàm chính để xóa document
def delete_completed_videos_without_files():
    # Kết nối đến MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Tìm tất cả document có status là "completed"
    completed_videos = collection.find({"video_render_status": "finish"})

    for video in completed_videos:
        video_id = video["_id"]
        final_video_path = video.get("final_video_path")

        # Kiểm tra xem trường final_video_path có tồn tại và file có tồn tại không
        if final_video_path and not file_exists(final_video_path):
            print(f"File không tồn tại, xóa document với _id: {video_id}")
            # Xóa document
            collection.delete_one({"_id": video_id})
        elif not final_video_path:
            print(f"Document với _id: {video_id} không có trường final_video_path")
        else:
            print(f"File tồn tại, giữ lại document với _id: {video_id}")

    print("Hoàn thành quá trình xóa document.")

if __name__ == "__main__":
    # check_and_update_video_status()
    delete_completed_videos_without_files()