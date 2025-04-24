# -*- coding: utf-8 -*-
# create_audio_vietnamese.py

import logging
import time
import datetime
from bson.objectid import ObjectId
from pymongo import ReturnDocument
import schedule
import os
from dotenv import load_dotenv

# Import từ các module khác
try:
    # Kết nối DB và lấy collections
    from db_manager import (
        connect_db,
        get_content_generations_collection,
        get_script_chunks_collection
    )
    # Các hàm xử lý audio từ tts_utils
    from tts_utils import (
        get_voice_settings,
        create_audio_for_chunk,
        combine_audio_from_db,
        VOICE_CONFIG # Lấy config giọng đọc đã load sẵn
    )
except ImportError as e:
    logging.critical(f"CRITICAL: Failed to import required modules: {e}. Worker cannot start.")
    exit(1)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
log_file_vi = 'create_audio_vietnamese.log'
file_handler_vi = logging.FileHandler(log_file_vi, mode='a', encoding='utf-8')
file_handler_vi.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))
logger_vi = logging.getLogger("VietnameseAudioWorker")
if logger_vi.hasHandlers(): logger_vi.handlers.clear()
logger_vi.addHandler(file_handler_vi)
logger_vi.addHandler(logging.StreamHandler())
logger_vi.setLevel(logging.INFO)

# --- Kết nối DB ---
try:
    connect_db() # Khởi tạo kết nối và các biến collection trong db_manager
    content_generations_collection = get_content_generations_collection()
    script_chunks_collection = get_script_chunks_collection()
    if content_generations_collection is None or script_chunks_collection is None:
        raise ConnectionError("Failed to get necessary DB collections.")
except ConnectionError as e:
    logger_vi.critical(f"Vietnamese Worker cannot start due to DB error: {e}")
    exit(1)
except Exception as e:
    logger_vi.critical(f"Unexpected error during DB initialization: {e}", exc_info=True)
    exit(1)

# --- Hàm xử lý một task Tiếng Việt (Tuần tự) ---
def process_audio_task_single_thread(generation_doc):
    """Xử lý tạo audio TUẦN TỰ cho một generation task Tiếng Việt."""
    generation_id_obj = generation_doc["_id"]
    generation_id = str(generation_id_obj)
    script_name = generation_doc.get("script_name")
    language = generation_doc.get("language")

    # Chỉ xử lý Tiếng Việt
    if language != "Vietnamese":
        logger_vi.error(f"Task {generation_id} is not Vietnamese, skipping in Vietnamese worker.")
        # Unlock task nếu bị lock nhầm
        content_generations_collection.update_one(
            {"_id": generation_id_obj, "status": "audio_processing_lock"},
            {"$set": {"status": "content_ready"}} # Trả về content_ready để worker khác xử lý
        )
        return False

    if not script_name:
        err_msg = f"Missing script_name in generation doc {generation_id}"
        logger_vi.error(f"Cannot process audio: {err_msg}")
        content_generations_collection.update_one(
             {"_id": generation_id_obj},
             {"$set": {"status": "audio_failed", "error_details": {"stage": "audio_setup", "message": err_msg}, "updated_at": datetime.datetime.now(datetime.timezone.utc)}}
         )
        return False

    logger_vi.info(f"--- Processing Vietnamese Audio Task Start: {generation_id} ---")

    # Lấy voice settings cho Tiếng Việt
    voice_settings = get_voice_settings("Vietnamese", VOICE_CONFIG)

    # Cập nhật trạng thái bắt đầu
    content_generations_collection.update_one(
        {"_id": generation_id_obj},
        {"$set": {"status": "audio_generating", "updated_at": datetime.datetime.now(datetime.timezone.utc)}}
    )

    # Tìm các chunk cần tạo audio
    try:
        pending_chunks = list(script_chunks_collection.find({
            "generation_id": generation_id_obj,
            "$or": [{"audio_created": False}, {"audio_error": {"$ne": None}}] # Lấy cả chunk lỗi để thử lại
        }).sort("section_index", 1))
    except Exception as e:
        logger_vi.error(f"Error finding chunks for gen {generation_id}: {e}")
        content_generations_collection.update_one(
            {"_id": generation_id_obj},
            {"$set": {"status": "audio_failed", "error_details": {"stage":"find_chunks", "message":str(e)}}}
        )
        return False

    logger_vi.info(f"Found {len(pending_chunks)} Vietnamese chunks needing audio for {generation_id}.")

    # --- TẠO AUDIO TUẦN TỰ ---
    any_chunk_failed = False
    for chunk in pending_chunks:
        chunk_id = chunk["_id"]
        logger_vi.info(f"Processing chunk {chunk_id} (Index: {chunk.get('section_index')})...")
        try:
            # Gọi hàm tạo audio (đã có retry bên trong tts_utils)
            _cid, success, _fpath = create_audio_for_chunk(chunk_id, script_name, voice_settings)
            if not success:
                 any_chunk_failed = True
                 logger_vi.error(f"Audio generation FAILED for chunk {chunk_id}.")
                 # Có thể dừng sớm nếu muốn
                 # break
        except Exception as e:
            # Lỗi này thường là tenacity đã raise sau khi hết retry
            logger_vi.error(f"Audio generation FAILED for chunk {chunk_id} after retries: {e}")
            any_chunk_failed = True
            # Có thể dừng sớm nếu muốn
            # break

        # KHÔNG CẦN sleep ở đây vì đang chạy tuần tự, hàm TTS đã block

    # --- Kiểm tra lại trạng thái chunks và cập nhật status ---
    try:
        all_chunks_done = script_chunks_collection.count_documents({"generation_id": generation_id_obj, "audio_created": True, "audio_error": None})
        total_chunks = script_chunks_collection.count_documents({"generation_id": generation_id_obj})
        error_chunks_count = script_chunks_collection.count_documents({"generation_id": generation_id_obj, "audio_error": {"$ne": None}})
    except Exception as e:
        logger_vi.error(f"Error counting chunks for gen {generation_id}: {e}")
        content_generations_collection.update_one(
            {"_id": generation_id_obj},
            {"$set": {"status": "audio_failed", "error_details": {"stage":"count_chunks", "message":str(e)}}}
        )
        return False

    logger_vi.info(f"Audio check for {generation_id}: Total={total_chunks}, Done={all_chunks_done}, Errors={error_chunks_count}")

    final_status = "unknown"
    final_audio_path = generation_doc.get("final_audio_path")
    error_details_update = None
    now = datetime.datetime.now(datetime.timezone.utc)

    if error_chunks_count > 0: # Nếu còn lỗi sau khi chạy
        final_status = "audio_failed"
        error_details_update = {"stage": "audio_chunk_generation", "message": f"{error_chunks_count}/{total_chunks} chunks failed.", "timestamp": now}
    elif all_chunks_done == total_chunks and total_chunks > 0:
        logger_vi.info(f"All chunks ready for {generation_id}. Combining...")
        try:
            # Gọi hàm ghép file (đường dẫn cục bộ)
            combined_path_local = combine_audio_from_db(generation_id_obj, script_name)
            if combined_path_local:
                final_status = "completed"
                final_audio_path = combined_path_local # Lưu đường dẫn cục bộ
            else:
                final_status = "audio_failed"; error_details_update = {"stage": "audio_concatenation", "message": "Combine failed.", "timestamp": now}
        except Exception as concat_err:
             final_status = "audio_failed"; error_details_update = {"stage": "audio_concatenation", "message": f"Exception: {concat_err}", "timestamp": now}
             logger_vi.exception(f"Concatenation exception for {generation_id}")
    elif all_chunks_done < total_chunks and total_chunks > 0:
         # Vẫn còn chunk chưa xử lý? Lạ. Đặt lại để chạy tiếp.
         final_status = "content_ready" # Hoặc audio_generating? -> content_ready an toàn hơn
         logger_vi.warning(f"Generation {generation_id}: Not all chunks done unexpectedly. Status set back to {final_status}.")
    elif total_chunks == 0: # Không có chunk nào
         final_status = "content_ready"; logger_vi.warning(f"Generation {generation_id} has no chunks.")

    # Cập nhật DB
    update_data = {"status": final_status, "updated_at": now}
    if final_audio_path: update_data["final_audio_path"] = final_audio_path
    if error_details_update: update_data["error_details"] = error_details_update
    elif final_status == 'completed': update_data["error_details"] = None

    try:
        content_generations_collection.update_one({"_id": generation_id_obj}, {"$set": update_data})
        logger_vi.info(f"--- Processing Vietnamese Audio Task Finished: {generation_id} with Status: {final_status} ---")
        return final_status not in ["audio_failed", "unknown"] # Trả về thành công nếu không lỗi
    except Exception as e:
         logger_vi.error(f"Failed to update final status for gen {generation_id}: {e}")
         return False # Báo lỗi xử lý


# --- Hàm Job chạy định kỳ ---
def job_vietnamese():
    """Tìm và xử lý các task Tiếng Việt."""
    logger_vi.info("Starting Vietnamese audio creation job run...")
    try:
         # Lấy collection mỗi lần chạy job để đảm bảo kết nối mới nhất
         content_generations_coll_job = get_content_generations_collection()
         if content_generations_coll_job is None: raise ConnectionError("DB not available")
    except ConnectionError as e:
         logger_vi.error(f"Vietnamese job cannot run: {e}"); return

    # Tìm các task Tiếng Việt cần xử lý
    try:
        tasks_to_process_cursor = content_generations_coll_job.find({
            "language": "Vietnamese", # <<< CHỈ LẤY TIẾNG VIỆT
            "$or": [{"status": "content_ready"}, {"status": "audio_failed"}]
        }).sort([("priority", -1), ("created_at", 1)]).limit(10) # Giới hạn số task/lần

        processed_count = 0
        # Xử lý tuần tự từng task
        for task_doc in tasks_to_process_cursor:
            gen_id = task_doc['_id']
            logger_vi.info(f"Found Vietnamese task {gen_id}, attempting to lock...")
            # Lock task
            result = content_generations_coll_job.find_one_and_update(
                 {"_id": gen_id, "status": {"$in": ["content_ready", "audio_failed"]}},
                 {"$set": {"status": "audio_processing_lock", "updated_at": datetime.datetime.now(datetime.timezone.utc)}},
                 return_document=ReturnDocument.AFTER
            )
            if result:
                try:
                     process_audio_task_single_thread(result) # Xử lý task đã lock
                     processed_count += 1
                except Exception as e:
                     logger_vi.exception(f"CRITICAL failure processing task {gen_id}: {e}")
                     try: content_generations_coll_job.update_one({"_id": gen_id}, {"$set": {"status": "audio_failed", "error_details": {"message": f"Worker error: {e}"}}})
                     except Exception as db_err: logger_vi.error(f"Failed to set audio_failed status for {gen_id}: {db_err}")
            else:
                 logger_vi.info(f"Vietnamese task {gen_id} lock failed or status changed. Skipping.")

        logger_vi.info(f"Vietnamese audio job finished run. Processed {processed_count} tasks.")

    except Exception as e:
         logger_vi.exception("Error during Vietnamese audio job execution")


# --- Main Execution ---
if __name__ == "__main__":
    RUN_INTERVAL_MINUTES = int(os.getenv("VI_AUDIO_INTERVAL_MINUTES", 5)) # Lấy từ env, mặc định 5 phút
    logging.info(f"Starting Vietnamese Audio Worker (Interval: {RUN_INTERVAL_MINUTES} minutes)...")

    # Chạy ngay lần đầu
    job_vietnamese()

    # Lên lịch
    schedule.every(RUN_INTERVAL_MINUTES).minutes.do(job_vietnamese)
    while True:
        schedule.run_pending()
        time.sleep(1)