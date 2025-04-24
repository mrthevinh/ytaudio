# -*- coding: utf-8 -*-
# main_worker.py (Hoàn chỉnh)

import os
import datetime
import logging
import time
import uuid
import concurrent.futures
from bson.objectid import ObjectId
from dotenv import load_dotenv
import pymongo
import pymongo.errors # Import errors để bắt lỗi cụ thể
from pymongo import ReturnDocument # Import ReturnDocument

# Import các thành phần từ các module khác
try:
    from db_manager import (
        connect_db,
        get_topics_collection,
        get_content_generations_collection,
        get_script_chunks_collection,
        save_chunk_to_db
    )
    from outline_parser import (
        parse_outline_markdown as parse_outline, # Dùng parser Markdown
        flatten_outline
    )
    from content_generator import (
        generate_outline_markdown,      # Tạo outline từ topic
        generate_outline_from_script,   # Tạo outline từ script
        generate_seo_title, 
        generate_long_text,             # Tạo content từ outline
        rewrite_entire_script,          # Tạo content từ script gốc (rewrite)
        translate_text                  # Import hàm dịch
        # add_new_quote_or_story không cần import vì được gọi bên trong generate_long_text
    )
    from utils import estimate_num_quotes_stories, count_tokens, split_script_into_chunks
except ImportError as e:
     logging.critical(f"Failed to import necessary modules: {e}. Worker cannot start.", exc_info=True)
     exit(1)


# --- Logging configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
log_file = 'daluong.log'
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))
logger = logging.getLogger()
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO) # Đặt INFO, có thể đổi DEBUG khi cần

# --- Load Environment Variables ---
load_dotenv(override=True)

# --- MongoDB Connection ---
topics_collection = None
content_generations_collection = None
script_chunks_collection = None
db = None # Giữ biến db để check kết nối ban đầu
try:
    connect_db() # Gọi hàm kết nối từ db_manager
    topics_collection = get_topics_collection()
    content_generations_collection = get_content_generations_collection()
    script_chunks_collection = get_script_chunks_collection()
    if None in [topics_collection, content_generations_collection, script_chunks_collection]:
         raise ConnectionError("One or more DB collections are None after connection attempt.")
except ConnectionError as e:
    logging.critical(f"Worker cannot start due to DB connection error: {e}")
    exit(1)
except Exception as e:
     logging.critical(f"Unexpected error during DB initialization: {e}", exc_info=True)
     exit(1)

# --- Main Processing Function ---
def process_generation_task(generation_doc):
    """Xử lý một yêu cầu tạo nội dung từ ContentGenerations."""
    generation_id_obj = generation_doc["_id"]; generation_id = str(generation_id_obj)
    task_type = generation_doc.get("task_type", "from_topic"); language = generation_doc.get("language", "Vietnamese")
    topic_id = generation_doc.get("topic_id"); script_name = generation_doc.get("script_name")
    if not script_name: script_name = str(uuid.uuid4()); content_generations_collection.update_one({"_id": generation_id_obj}, {"$set": {"script_name": script_name}})

    logging.info(f"--- Processing Task Start: {generation_id} (Type: {task_type}) ---"); logging.info(f"Lang: {language}")

    content_generations_coll = get_content_generations_collection()
    if content_generations_coll is None: logging.error(f"DB lost {generation_id}. Abort."); return

    try:
        # --- Status Handling ---
        current_status = generation_doc.get("status"); next_status = current_status
        needs_outline = (task_type == "from_topic" and not generation_doc.get("outline")) or \
                        (task_type == "rewrite_script" and not generation_doc.get("derived_outline"))
        if current_status in ["pending", "processing_lock", "outline_failed"]: next_status = "generating_outline" if needs_outline else "content_generating"
        elif current_status == "content_failed": next_status = "content_generating"
        elif current_status not in ["generating_outline", "content_generating"]: logging.warning(f"Task {generation_id} unexpected status '{current_status}'. Resetting."); content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": {"status": "pending"}}); return
        if next_status != current_status: logging.info(f"Update status {generation_id}: '{current_status}' -> '{next_status}'"); content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": {"status": next_status, "updated_at": datetime.datetime.now(datetime.timezone.utc)}})

        # --- Config/Estimation ---
        model = generation_doc.get("model", "gpt-4o")
        duration_minutes = generation_doc.get("target_duration_minutes") # Lấy duration đã lưu
        # <<< Sửa: Ước lượng target_chars và num_quotes/stories dựa trên ngôn ngữ >>>
        num_quotes, num_stories, target_chars = estimate_num_quotes_stories(duration_minutes, language) # Gọi hàm đã sửa trong utils
        # Lưu lại số lượng quote/story ước tính vào DB (nếu chưa có)
        db_num_quotes = generation_doc.get("num_quotes")
        db_num_stories = generation_doc.get("num_stories")
        db_target_chars = generation_doc.get("target_chars") # Lưu target_chars thay target_words
        update_est_fields = {}
        if db_num_quotes is None: update_est_fields["num_quotes"] = num_quotes
        if db_num_stories is None: update_est_fields["num_stories"] = num_stories
        if db_target_chars is None: update_est_fields["target_chars"] = target_chars
        if update_est_fields:
             logging.info(f"Task {generation_id}: Saving estimated params: {update_est_fields}")
             content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": update_est_fields})
        # Sử dụng giá trị từ DB nếu có, nếu không dùng giá trị vừa ước lượng
        num_quotes = db_num_quotes if db_num_quotes is not None else num_quotes
        num_stories = db_num_stories if db_num_stories is not None else num_stories
        target_chars = db_target_chars if db_target_chars is not None else target_chars
        # -------------------------------------------------------------------
        min_chars = max(3000, int(target_chars * 0.9)) # 90% target, tối thiểu 3000 ký tự
        logging.info(f"Config: Q={num_quotes}, S={num_stories}, TargetChars={target_chars}, MinChars={min_chars}, Model={model}")

        # --- XỬ LÝ TASK ---
        generation_success = False
        topic_input = generation_doc.get("title", "Default Topic") # Dùng cho metadata
        now = datetime.datetime.now(datetime.timezone.utc)

        # --- Tạo Metadata (Chạy cho cả 2 loại task) ---
        # ... (Code tạo metadata như trước, dùng topic_input, language, model. Nhớ gọi translate_text nếu cần) ...
        logging.info(f"Generating metadata for task {generation_id}...")
        updates_for_metadata = {}; updates_for_topic = {}
        doc_for_check = content_generations_coll.find_one({"_id": generation_id_obj}, {"seo_title": 1, "thumbnail_titles": 1, "image_prompt": 1, "title": 1, "title_vi": 1})
        current_topic_doc = topics_collection.find_one({"_id": topic_id}, {"title": 1, "title_vi": 1}) if topic_id else {}
        input_for_seo = generation_doc.get("source_script", topic_input)[:1000] if task_type=="rewrite_script" else topic_input # Snippet hoặc topic
        if not doc_for_check or not doc_for_check.get("seo_title"):
            try: new_seo_title = generate_seo_title(input_for_seo, language, model); updates_for_metadata["seo_title"]=new_seo_title; updates_for_metadata["title"]=new_seo_title
            except Exception as e: logging.error(f"Fail gen SEO title: {e}")
            if topic_id and new_seo_title and new_seo_title != current_topic_doc.get("title"):
                updates_for_topic["title"]=new_seo_title; new_title_vi=new_seo_title if language=="Vietnamese" else translate_text(new_seo_title,source_language=language)
                if not new_title_vi.startswith("[Loi"): updates_for_topic["title_vi"]=new_title_vi; updates_for_metadata["title_vi"]=new_title_vi
        title_for_others = updates_for_metadata.get("title", topic_input)
        # if not doc_for_check or not doc_for_check.get("thumbnail_titles"):
        #      try: updates_for_metadata["thumbnail_titles"] = generate_thumbnail_titles(title_for_others, language, model)
        #      except Exception as e: logging.error(f"Fail gen Thumb titles: {e}")
        # if not doc_for_check or not doc_for_check.get("image_prompt"):
        #      try: updates_for_metadata["image_prompt"] = generate_image_prompt(title_for_others, model)
        #      except Exception as e: logging.error(f"Fail gen Image prompt: {e}")
        final_meta_updates_gen = {k: v for k,v in updates_for_metadata.items() if v and v != doc_for_check.get(k)}
        if final_meta_updates_gen: # Chỉ update nếu có thay đổi thực sự
            logging.debug(f"Updating ContentGenerations Filter: {{'_id': {generation_id_obj}}}, Update: {{'$set': {final_meta_updates_gen}}}") # Thêm log để kiểm tra
            # ---> DÒNG 153 (hoặc gần đó) GÂY LỖI? <---
            content_generations_coll.update_one(
                {"_id": generation_id_obj}, # Tham số 1: filter (OK)
                {"$set": final_meta_updates_gen} # Tham số 2: update (OK)
            )
            # ------------------------------------
            logging.info(f"Updated Gen metadata fields: {list(final_meta_updates_gen.keys())}")
        if updates_for_topic: updates_for_topic["updated_at"]=now; topics_collection.update_one({"_id": topic_id}, {"$set": updates_for_topic}); logging.info(f"Updated Topic meta: {list(updates_for_topic.keys())}")
        topic_input = updates_for_metadata.get("title", topic_input) # Cập nhật lại

        # --- Logic chính theo Task Type ---
        if task_type == "rewrite_script":
            source_script = generation_doc.get("source_script"); assert source_script, "Source script missing."
            outline_markdown = generation_doc.get("derived_outline")
            if next_status == "generating_outline":
                outline_markdown = generate_outline_from_script(source_script, language, model); assert outline_markdown, "Fail gen outline from script."
                content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": {"derived_outline": outline_markdown, "status": "rewriting_script"}})
                next_status = "rewriting_script"
            else: logging.info("Using existing derived outline."); content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": {"status": "rewriting_script"}})

            logging.info("Starting full script rewrite...")
            final_script = rewrite_entire_script(source_script, outline_markdown, language, model, target_chars)
            assert final_script, "Failed rewrite (LLM empty)."
            generation_success = True
            script_chunks_collection.delete_many({"generation_id": generation_id_obj})
            logging.info("Splitting rewritten script...")
            max_chars_tts = 3500
            chunks = split_script_into_chunks(final_script, max_chars_tts, language)
            assert chunks, "Failed to split rewritten script."
            logging.info(f"Saving {len(chunks)} rewritten chunks...")
            for idx, chunk_txt in enumerate(chunks): save_chunk_to_db(generation_id_obj, script_name, idx, f"Rewrite Pt.{idx+1}", chunk_txt, 1, "rewrite_chunk")

        elif task_type == "from_topic":
            outline_markdown = generation_doc.get("outline")
            if next_status == "generating_outline":
                 outline_markdown = generate_outline_markdown(topic_input, language, model, num_quotes, num_stories); assert outline_markdown, "Fail gen outline."
                 content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": {"outline": outline_markdown, "status": "content_generating"}})
                 next_status = "content_generating"
            else: logging.info("Using existing outline."); content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": {"status": "content_generating"}})

            parsed = parse_outline(outline_markdown); assert parsed, "Fail parse outline."
            flat_outline = flatten_outline(parsed); assert flat_outline, "Flattened outline empty."
            logging.info(f"Outline processed: {len(flat_outline)} items.")

            logging.info("Starting detailed content generation...")
            chunk_words = 300 # Ước tính từ cho mỗi mục outline
            generation_success = generate_long_text(flat_outline, topic_input, language, script_name, generation_id_obj, num_quotes, num_stories, min_chars, chunk_words, model)

        else: raise ValueError(f"Unknown task_type: {task_type}")

        # --- Cập nhật status cuối ---
        final_status = "content_ready" if generation_success else "content_failed"
        final_update = {"status": final_status, "updated_at": datetime.datetime.now(datetime.timezone.utc)}
        if not generation_success: final_update["error_details"] = {"stage": f"task_{task_type}", "message": "Process finished with errors.", "timestamp": datetime.datetime.now(datetime.timezone.utc)}
        content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": final_update})
        log_func = logging.info if generation_success else logging.error
        log_func(f"--- Task {generation_id} ({task_type}) FINISHED with Status: {final_status} ---")

    except Exception as e:
        logging.exception(f"CRITICAL error processing task {generation_id}: {e}")
        try: content_generations_coll.update_one({"_id": generation_id_obj}, {"$set": {"status": "content_failed", "error_details": {"stage": "main_exception", "message": str(e)[:500]}, "updated_at": datetime.datetime.now(datetime.timezone.utc)}})
        except Exception as db_err: logging.error(f"Failed update CRITICAL error status {generation_id}: {db_err}")


# --- Main Worker Loop ---
def main():
    logging.info("=== Content Generation Worker started ===")
    try:
        # Log DB info using imported collection object
        logging.info(f"Monitoring MongoDB: DB: {content_generations_collection.database.name}, Collection: {content_generations_collection.name}")
        logging.info(f"Monitoring MongoDB: DB: {content_generations_collection.database.name}, Collection: {content_generations_collection.name}")
    except Exception as e: logging.error(f"Could not log MongoDB info (DB might be down): {e}")

    CHECK_INTERVAL_SECONDS = 15 # Check more frequently
    MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", 2)) # Default to 1 for stability
    logging.info(f"Checking interval: {CHECK_INTERVAL_SECONDS}s. Max concurrent tasks: {MAX_CONCURRENT_TASKS}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TASKS) as executor:
        active_tasks = set() # generation_id currently being processed

        while True:
            processed_in_cycle = False
            # Check and unlock potentially stuck tasks first
            try:
                one_hour_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
                stuck_tasks_result = content_generations_collection.update_many(
                    {"status": "processing_lock", "updated_at": {"$lt": one_hour_ago}},
                    {"$set": {"status": "pending", "updated_at": datetime.datetime.now(datetime.timezone.utc), "error_details": {"message": "Reset from stuck processing_lock"}}} )
                if stuck_tasks_result.modified_count > 0:
                    logging.warning(f"Reset {stuck_tasks_result.modified_count} tasks stuck in 'processing_lock' state.")
                    # We don't know exactly which tasks were reset without another query,
                    # so clearing active_tasks might be inaccurate if some finish correctly.
                    # A safer approach might be needed in production. For now, log it.
            except Exception as unlock_err:
                 logging.error(f"Error trying to unlock stuck tasks: {unlock_err}")

            try:
                # Attempt to fetch a new task only if below concurrency limit
                if len(active_tasks) < MAX_CONCURRENT_TASKS:
                    logging.debug(f"Active: {len(active_tasks)}/{MAX_CONCURRENT_TASKS}. Checking for tasks...")

                    # Find a task to process (prioritize failed, then pending)
                    task_to_process = content_generations_collection.find_one_and_update(
                       {"status": {"$in": ["pending", "content_failed", "outline_failed"]}}, # States to pick up
                       {"$set": {"status": "processing_lock", "updated_at": datetime.datetime.now(datetime.timezone.utc)}},
                       sort=[("status", 1),("priority", -1), ("created_at", 1)], # Sort: failed first, then high prio, then oldest
                       return_document=ReturnDocument.AFTER # Use imported constant
                    )

                    if task_to_process:
                        gen_id = task_to_process['_id']
                        logging.info(f"Picked up task: {gen_id} (Type: {task_to_process.get('task_type')}, Status: {task_to_process.get('status')})")
                        active_tasks.add(gen_id) # Add to our tracked set
                        processed_in_cycle = True

                        # Submit to thread pool
                        future = executor.submit(process_generation_task, task_to_process)

                        # Define callback to run when future completes
                        def task_done_callback(task_id):
                            def callback(fut):
                                try:
                                    fut.result() # Raise exception if task failed internally
                                    logging.info(f"Task {task_id} thread finished.")
                                except Exception as e:
                                    # Log error from the thread execution
                                    logging.error(f"Task {task_id} thread finished with exception: {e}", exc_info=True) # Include traceback from thread
                                finally:
                                    # Always remove from active set when thread finishes
                                    if task_id in active_tasks: active_tasks.remove(task_id)
                                    logging.debug(f"Task {task_id} removed from active set. Current: {len(active_tasks)}")
                            return callback

                        future.add_done_callback(task_done_callback(gen_id)) # Pass gen_id to callback correctly
                    else:
                        logging.debug("No suitable tasks found in this cycle.")
                        # processed_in_cycle remains False

                # --- Sleep logic ---
                if not processed_in_cycle and len(active_tasks) == 0:
                    # No new tasks picked up AND no tasks running -> Long sleep
                    logging.info(f"No tasks found or running. Waiting {CHECK_INTERVAL_SECONDS}s...")
                    time.sleep(CHECK_INTERVAL_SECONDS)
                elif len(active_tasks) >= MAX_CONCURRENT_TASKS:
                    # Max tasks running -> Short sleep, check again soon
                    logging.debug(f"Max concurrent tasks ({len(active_tasks)}) reached. Waiting {CHECK_INTERVAL_SECONDS / 3:.1f}s...")
                    time.sleep(CHECK_INTERVAL_SECONDS / 3)
                else:
                    # Tasks are running but slots available, OR just processed one -> Very short sleep
                    time.sleep(3)

            except pymongo.errors.ConnectionFailure as conn_err:
                 logging.error(f"MongoDB Connection Failure in main loop: {conn_err}. Resetting connection flag and waiting...")
                 db = None # Reset global connection flag in db_manager implicitly next time connect_db is called
                 time.sleep(60) # Wait longer after connection failure
                 connect_db() # Attempt to reconnect immediately
            except Exception as loop_error:
                logging.exception("Unexpected error in main worker loop:")
                # Attempt to unlock tasks currently marked as active in this worker instance
                if active_tasks:
                     try:
                          ids_to_unlock = list(active_tasks)
                          logging.warning(f"Attempting to unlock tasks {ids_to_unlock} due to loop error.")
                          unlock_update = content_generations_collection.update_many(
                               {"_id": {"$in": ids_to_unlock}, "status": "processing_lock"},
                               {"$set": {"status": "pending", "updated_at": datetime.datetime.now(datetime.timezone.utc)}})
                          logging.warning(f"Unlocked {unlock_update.modified_count} tasks.")
                          active_tasks.clear() # Clear local tracking
                     except Exception as unlock_err:
                          logging.error(f"Failed to unlock tasks after main loop error: {unlock_err}")
                time.sleep(CHECK_INTERVAL_SECONDS * 2) # Wait longer after unexpected error

if __name__ == "__main__":
    # Initial DB connection check before starting main loop
    
    main()