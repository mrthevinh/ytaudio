# -*- coding: utf-8 -*-
# create_audio_other.py

import logging
import time
import datetime
from bson.objectid import ObjectId
from pymongo import ReturnDocument
import schedule
import concurrent.futures # <<< Giữ lại đa luồng
import os
from dotenv import load_dotenv

# Import các thành phần cần thiết từ các module khác
try:
    from db_manager import (
        connect_db,
        get_content_generations_collection,
        get_script_chunks_collection
    )
    from tts_utils import ( # Import từ tts_utils
        get_voice_settings,
        create_audio_for_chunk,
        combine_audio_from_db,
        VOICE_CONFIG # Lấy config đã load
    )
except ImportError as e:
    logging.critical(f"CRITICAL: Failed to import required modules: {e}. Worker cannot start.")
    exit(1)

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
log_file_other = 'create_audio_other.log'
file_handler_other = logging.FileHandler(log_file_other, mode='a', encoding='utf-8')
file_handler_other.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))
logger_other = logging.getLogger("OtherLangAudioWorker")
if logger_other.hasHandlers(): logger_other.handlers.clear()
logger_other.addHandler(file_handler_other)
logger_other.addHandler(logging.StreamHandler())
logger_other.setLevel(logging.INFO)

# --- Kết nối DB ---
try:
    connect_db()
    content_generations_collection = get_content_generations_collection()
    script_chunks_collection = get_script_chunks_collection()
    if content_generations_collection is None or script_chunks_collection is None:
         raise ConnectionError("Failed to get necessary DB collections.")
except ConnectionError as e:
    logger_other.critical(f"Other Lang Worker cannot start due to DB error: {e}")
    exit(1)
except Exception as e:
     logger_other.critical(f"Unexpected error during DB initialization: {e}", exc_info=True)
     exit(1)

# --- Hàm xử lý một task Ngôn ngữ khác (Đa luồng tạo Chunk) ---
def process_audio_task_multi_thread(generation_doc):
    """Xử lý tạo audio ĐA LUỒNG cho một generation task."""
    generation_id_obj = generation_doc["_id"]
    generation_id = str(generation_id_obj)
    script_name = generation_doc.get("script_name")
    language = generation_doc.get("language")

    # Kiểm tra phòng ngừa (dù query đã lọc)
    if not script_name or not language or language == "Vietnamese":
        err_msg = f"Invalid task for Other Languages worker: ScriptName={script_name}, Lang={language}"
        logger_other.error(err_msg)
        # Trả lại trạng thái cũ để task không bị kẹt lock
        content_generations_collection.update_one(
             {"_id": generation_id_obj, "status": "audio_processing_lock"},
             {"$set": {"status": generation_doc.get("status_before_lock", "content_ready")}} # Giả sử có lưu status trước đó, nếu không trả về content_ready
        )
        return False

    logger_other.info(f"--- Processing Other Lang Audio Task Start: {generation_id} (Lang: {language}) ---")

    # Lấy voice settings
    voice_settings = get_voice_settings(language, VOICE_CONFIG)

    # Cập nhật trạng thái
    content_generations_collection.update_one(
        {"_id": generation_id_obj},
        {"$set": {"status": "audio_generating", "updated_at": datetime.datetime.now(datetime.timezone.utc)}}
    )

    # Tìm chunks cần xử lý
    try:
        pending_chunks = list(script_chunks_collection.find({
            "generation_id": generation_id_obj,
            "$or": [{"audio_created": False}, {"audio_error": {"$ne": None}}]
        }).sort("section_index", 1))
    except Exception as e:
         logger_other.error(f"Error finding chunks for gen {generation_id}: {e}")
         content_generations_collection.update_one({"_id": generation_id_obj}, {"$set": {"status": "audio_failed", "error_details": {"stage":"find_chunks", "message":str(e)}}})
         return False


    logger_other.info(f"Found {len(pending_chunks)} '{language}' chunks needing audio for {generation_id}.")

    # --- TẠO AUDIO ĐA LUỒNG (CHO CHUNKS) ---
    if pending_chunks:
        # Số luồng tối đa để tạo chunk audio cùng lúc
        max_chunk_workers = int(os.getenv("AUDIO_MAX_CONCURRENT_CHUNKS", 4))
        any_chunk_failed_this_run = False # Cờ theo dõi lỗi trong đợt chạy này
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_chunk_workers) as executor:
            # Truyền voice_settings vào create_audio_for_chunk
            futures = [executor.submit(create_audio_for_chunk, chunk["_id"], script_name, voice_settings)
                       for chunk in pending_chunks]

            processed_count = 0
            for future in concurrent.futures.as_completed(futures):
                processed_count += 1
                try:
                    chunk_id, success, _ = future.result()
                    if not success:
                         any_chunk_failed_this_run = True
                         logger_other.error(f"Audio gen FAILED permanently for chunk {chunk_id}.")
                except Exception as e:
                     # Lỗi này là do tenacity raise sau khi hết retry
                     any_chunk_failed_this_run = True
                     logger_other.error(f"Future for audio chunk failed after retries: {e}") # Không cần exc_info vì tenacity đã log rồi
            logger_other.info(f"Finished processing {processed_count} audio futures for {generation_id}.")
    else:
         any_chunk_failed_this_run = False # Không có chunk nào cần chạy


    # --- Kiểm tra lại trạng thái chunks và cập nhật status ---
    try:
        all_chunks_done = script_chunks_collection.count_documents({"generation_id": generation_id_obj, "audio_created": True, "audio_error": None})
        total_chunks = script_chunks_collection.count_documents({"generation_id": generation_id_obj})
        error_chunks_count = script_chunks_collection.count_documents({"generation_id": generation_id_obj, "audio_error": {"$ne": None}})
    except Exception as e:
         logger_other.error(f"Error counting chunks for gen {generation_id}: {e}")
         content_generations_collection.update_one({"_id": generation_id_obj}, {"$set": {"status": "audio_failed", "error_details": {"stage":"count_chunks", "message":str(e)}}})
         return False

    logger_other.info(f"Audio check for {generation_id}: Total={total_chunks}, Done={all_chunks_done}, Errors={error_chunks_count}")

    final_status = "unknown"; final_audio_path = generation_doc.get("final_audio_path"); error_details_update = None
    now = datetime.datetime.now(datetime.timezone.utc)

    # Logic xác định final_status (giống worker tiếng Việt)
    if error_chunks_count > 0: # Nếu còn lỗi tồn đọng
        final_status = "audio_failed"; error_details_update = {"stage": "audio_chunk", "message": f"{error_chunks_count} chunks failed.", "timestamp": now}
    elif all_chunks_done == total_chunks and total_chunks > 0:
        logger_other.info(f"All chunks ready for {generation_id}. Combining...")
        try:
            combined_path_local = combine_audio_from_db(generation_id_obj, script_name)
            if combined_path_local: final_status = "completed"; final_audio_path = combined_path_local
            else: final_status = "audio_failed"; error_details_update = {"stage": "audio_combine", "message": "Combine failed.", "timestamp": now}
        except Exception as concat_err: final_status = "audio_failed"; error_details_update = {"stage": "audio_combine", "message": f"Exception: {concat_err}", "timestamp": now}; logger_other.exception(f"Concatenation exception")
    elif all_chunks_done < total_chunks and total_chunks > 0:
         final_status = "audio_generating" # Thử lại lần sau
         logger_other.warning(f"Generation {generation_id}: Not all chunks done. Status -> {final_status}.")
    else: # total_chunks == 0
         final_status = "content_ready"; logger_other.warning(f"Generation {generation_id} has no chunks. Status -> {final_status}.")

    # Cập nhật DB
    update_data = {"status": final_status, "updated_at": now}
    if final_audio_path: update_data["final_audio_path"] = final_audio_path
    if error_details_update: update_data["error_details"] = error_details_update
    elif final_status == 'completed': update_data["error_details"] = None

    try:
        content_generations_collection.update_one({"_id": generation_id_obj}, {"$set": update_data})
        logger_other.info(f"--- Processing Other Lang Audio Task Finished: {generation_id} with Status: {final_status} ---")
        return final_status not in ["audio_failed", "unknown"] # Thành công nếu không lỗi
    except Exception as e:
        logger_other.error(f"Failed to update final status for gen {generation_id}: {e}")
        return False # Báo lỗi xử lý

# --- Hàm Job chạy định kỳ ---
def job_other():
    """Tìm và xử lý các task KHÔNG phải Tiếng Việt."""
    logger_other.info("Starting Other Languages audio creation job run...")
    try:
         content_generations_coll_job = get_content_generations_collection()
         if content_generations_coll_job is None: raise ConnectionError("DB not available")
    except ConnectionError as e: logger_other.error(f"Other Lang job cannot run: {e}"); return

    try:
        tasks_to_process_cursor = content_generations_coll_job.find({
            "language": {"$ne": "Vietnamese"}, # <<< KHÔNG LẤY TIẾNG VIỆT
            "$or": [{"status": "content_ready"}, {"status": "audio_failed"}]
        }).sort([("priority", -1), ("created_at", 1)]).limit(10) # Giới hạn số task/lần

        processed_count = 0
        # Có thể xử lý song song các task ở mức job nếu muốn, nhưng hiện tại xử lý tuần tự
        for task_doc in tasks_to_process_cursor:
            gen_id = task_doc['_id']
            logger_other.info(f"Found other lang task {gen_id}, attempting to lock...")
            # Lock task
            result = content_generations_coll_job.find_one_and_update(
                 {"_id": gen_id, "status": {"$in": ["content_ready", "audio_failed"]}},
                 {"$set": {"status": "audio_processing_lock", "updated_at": datetime.datetime.now(datetime.timezone.utc)}},
                 return_document=ReturnDocument.AFTER
            )
            if result:
                try:
                     process_audio_task_multi_thread(result) # Xử lý task đã lock
                     processed_count += 1
                except Exception as e:
                     logger_other.exception(f"CRITICAL failure processing task {gen_id}: {e}")
                     try: content_generations_coll_job.update_one({"_id": gen_id}, {"$set": {"status": "audio_failed", "error_details": {"message": f"Worker error: {e}"}}})
                     except Exception as db_err: logger_other.error(f"Failed to set audio_failed status for {gen_id}: {db_err}")
            else:
                 logger_other.info(f"Task {gen_id} lock failed or status changed. Skipping.")

        logger_other.info(f"Other Languages audio job run finished. Processed {processed_count} tasks.")
    except Exception as e:
        logger_other.exception("Error during Other Languages audio job execution")


# --- Main Execution ---
if __name__ == "__main__":
    RUN_INTERVAL_MINUTES = int(os.getenv("OTHER_AUDIO_INTERVAL_MINUTES", 10)) # Lấy từ env, mặc định 10 phút
    logging.info(f"Starting Other Languages Audio Worker (Interval: {RUN_INTERVAL_MINUTES} minutes)...")

    job_other() # Chạy ngay lần đầu

    schedule.every(RUN_INTERVAL_MINUTES).minutes.do(job_other)
    while True:
        schedule.run_pending()
        time.sleep(1)