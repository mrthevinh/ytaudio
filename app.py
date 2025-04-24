# -*- coding: utf-8 -*-
# app.py (Flask + HTMX - Hỗ trợ cả 2 luồng)

import os
import datetime
import logging
import json
import re
import concurrent.futures
import time
from flask import (Flask, render_template, request, redirect,
                   url_for, flash, jsonify, make_response, abort)
from pymongo import MongoClient, ReturnDocument
from bson.objectid import ObjectId
from dotenv import load_dotenv
import openai
from werkzeug.exceptions import NotFound, InternalServerError, BadRequest
import traceback

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
log_file = 'webform.log'
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))
logger = logging.getLogger()
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# --- Load Environment Variables ---
load_dotenv(override=True)

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key-must-change")
# from flask_cors import CORS # Bỏ comment nếu cần CORS
# CORS(app)

# --- MongoDB Connection ---
mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
db_name = os.getenv("MONGODB_DB_NAME", "content_db")
topics_collection_name = "Topics"
content_generations_collection_name = "ContentGenerations"
script_chunks_collection_name = "ScriptChunks"

db = None; topics_collection = None; content_generations_collection = None; script_chunks_collection = None
client = None # Giữ lại client để kiểm tra ping

def connect_db():
    """Establishes MongoDB connection."""
    global client, db, topics_collection, content_generations_collection, script_chunks_collection
    try:
        # Đóng kết nối cũ nếu còn tồn tại (để đảm bảo kết nối mới)
        if client:
             try: client.close()
             except Exception as close_err: logging.warning(f"Error closing previous MongoDB client: {close_err}")

        logger.info(f"Connecting to MongoDB: {db_name}")
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        client.admin.command('ping')
        db = client[db_name]
        topics_collection = db[topics_collection_name]
        content_generations_collection = db[content_generations_collection_name]
        script_chunks_collection = db[script_chunks_collection_name]
        logger.info(f"Successfully connected to MongoDB.")
        # Ensure indices silently
        try:
            topics_collection.create_index([("status", 1)], background=True)
            # ... (Thêm các create_index khác nếu cần) ...
            logging.info("MongoDB index creation requests sent.")
        except Exception as index_err: logging.warning(f"Could not ensure indices: {index_err}")
        return True # Báo kết nối thành công
    except Exception as e:
        logging.critical(f"CRITICAL: Failed to connect to MongoDB: {e}")
        db = None; topics_collection = None; content_generations_collection = None; script_chunks_collection = None; client = None
        return False # Báo kết nối thất bại

connect_db() # Connect on startup

# --- OpenAI API setup ---
oai_client = None
try:
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key: logger.error("CRITICAL: OPENAI_API_KEY not set.")
    else:
        openai_base_url = os.getenv("OPENAI_BASE_URL")
        oai_client = openai.OpenAI(api_key=openai_api_key, base_url=openai_base_url) if openai_base_url else openai.OpenAI(api_key=openai_api_key)
        logging.info("OpenAI client initialized.")
except Exception as e: logger.critical(f"CRITICAL: Failed to initialize OpenAI client: {e}"); oai_client = None

# --- Helper Functions ---

def check_db_available(use_abort=True):
     """Check DB and abort/flash if unavailable."""
     global db # Kiểm tra biến toàn cục
     # Thử ping để kiểm tra kết nối hiện tại
     if db is None or client is None:
          connected = connect_db() # Thử kết nối lại
          if not connected:
               error_message = "Loi Ket Noi Database Nghiêm Trong."
               if use_abort: abort(make_response(f"<div class='error-message flash flash-error'>{error_message}</div>", 503))
               else: flash(error_message + " Vui long kiem tra logs.", "error"); return False
     else:
          try:
               client.admin.command('ping') # Kiểm tra kết nối hiện tại
          except Exception as ping_err:
               logging.error(f"DB ping failed: {ping_err}. Attempting reconnect...")
               connected = connect_db() # Thử kết nối lại
               if not connected:
                    error_message = "Mat Ket Noi Database. Dang thu ket noi lai..."
                    if use_abort: abort(make_response(f"<div class='error-message flash flash-error'>{error_message}</div>", 503))
                    else: flash(error_message, "error"); return False
     return True


def check_openai_available(use_abort=True):
     """Check OpenAI client and abort/flash if unavailable."""
     if oai_client is None:
          error_message = "Loi: OpenAI client chua duoc cau hinh hoac khong san sang."
          logger.error(error_message)
          if use_abort: abort(make_response(f"<div class='error-message flash flash-error'>{error_message}</div>", 503))
          else: flash(error_message, "error"); return False
     return True

# Hàm translate_text (sử dụng oai_client)
def translate_text(text, target_language="Vietnamese", source_language="auto", model="gpt-4o-mini", max_retries=2):
    if not check_openai_available(use_abort=False): return "[Loi dich: Client]"
    if not text: return ""
    prompt = f"Translate to {target_language}. Output ONLY translated text:\n\n{text}"
    if source_language != "auto": prompt = f"Translate from {source_language} to {target_language}. Output ONLY translated text:\n\n{text}"
    messages = [{"role": "system", "content": f"Translate accurately to {target_language}. ONLY output translation."}, {"role": "user", "content": prompt}]
    attempts = 0
    while attempts <= max_retries:
        try:
            response = oai_client.chat.completions.create(model=model, messages=messages, max_tokens=int(len(text.split())*3.5 + 80), temperature=0.2)
            translation_raw = response.choices[0].message.content.strip()
            translation_clean = re.sub(r'^["\'“‘\[\(\{*\-\s]+|["\'”’\]\)\}\*\-\s]+$', '', translation_raw)
            if len(translation_clean) < 3 and len(text) > 10: raise openai.APIError("Short translation", request=None, code=None)
            return translation_clean
        except openai.RateLimitError as e: wait_time = 5*(attempts+1); logging.warning(f"Translate Rate Limit (Attempt {attempts+1}): {e}. Retrying..."); time.sleep(wait_time)
        except (openai.APIError, openai.APIConnectionError, openai.Timeout) as e: wait_time = 2*(attempts+1); logging.warning(f"Translate API Error (Attempt {attempts+1}): {e}. Retrying..."); time.sleep(wait_time)
        except Exception as e: logging.error(f"Unexpected translation error: {e}", exc_info=True); return f"[Dich loi: Exception]"
        attempts += 1
    return f"[Dich loi: Failed]"

# Hàm render partial topic item
def render_topic_item(topic_id):
    """Fetches data and renders the HTML for a single topic item using _topic_item.html."""
    if not check_db_available(use_abort=False): return "<p class='error-message'>DB error</p>"
    try:
        if not isinstance(topic_id, ObjectId): topic_id = ObjectId(topic_id)
        topic = topics_collection.find_one({"_id": topic_id})
        if not topic: return f"<div id='topic-item-{topic_id}' class='deleted-item'>Topic deleted.</div>"
        generation = None
        gen_id = topic.get("generation_id")
        if gen_id:
            try:
                if not isinstance(gen_id, ObjectId): gen_id = ObjectId(gen_id)
                generation = content_generations_collection.find_one({"_id": gen_id})
            except Exception: gen_id=None; generation=None
            if not generation and topic.get("status") != "suggested": logging.warning(f"Generation {gen_id} for topic {topic_id} not found.")
        return render_template('_topic_item.html', topic=topic, generation=generation)
    except Exception as e:
        logging.exception(f"Exception rendering topic item {topic_id}")
        return f"<div id='topic-item-{topic_id}' class='render-error error-message'>Render error.</div>"

# Hàm tạo header HX-Trigger (ASCII safe)
def trigger_flash(level="info", message=""):
    try:
        safe_message = message.encode('ascii', 'replace').decode('ascii')
        trigger_data = json.dumps({"showMessage": {"level": level, "message": safe_message}})
        return {"HX-Trigger": trigger_data}
    except Exception as e:
        logging.error(f"Error creating HX-Trigger: {e}")
        return {"HX-Trigger": json.dumps({"showMessage": {"level": "error", "message": "Loi xu ly thong bao."}})}

# --- Flask Routes ---

@app.route('/')
def index():
    """Displays the main dashboard."""
    if not check_db_available(use_abort=False):
        return render_template('index.html', topics=[], generations={})
    try:
        topics_cursor = topics_collection.find({"status": {"$ne": "deleted"}}).sort("updated_at", -1).limit(100)
        topics = list(topics_cursor)
        generation_ids = [t.get('generation_id') for t in topics if t.get('generation_id')]
        generations_map = {}
        if generation_ids:
            generations_cursor = content_generations_collection.find({"_id": {"$in": generation_ids}})
            generations_map = {gen['_id']: gen for gen in generations_cursor}
        return render_template('index.html', topics=topics, generations=generations_map)
    except Exception as e:
        logging.exception("Error loading index page")
        flash(f"Loi tai du lieu: {e}", "error")
        return render_template('index.html', topics=[], generations={})

# --- Route xử lý yêu cầu ban đầu (Gợi ý hoặc Submit Rewrite) ---
@app.route('/handle_initial_submission', methods=['POST'])
def handle_initial_submission():
    """Handles main form: suggests topics or directly queues a rewrite task."""
    check_db_available(use_abort=True); # Abort nếu DB lỗi

    task_type = request.form.get('task_type', 'from_topic')
    language = request.form.get('language')
    seed_topic = request.form.get('seed_topic', '').strip()
    source_script = request.form.get('source_script', '').strip()
    # Các tùy chọn chung cũng được gửi từ form này
    target_duration_str = request.form.get('target_duration')
    priority_str = request.form.get('priority', 'medium').lower()
    model = request.form.get('model', 'gpt-4o')

    logging.info(f"Handling initial submission: Type={task_type}, Lang={language}, Prio={priority_str}, Model={model}")

    # --- Input Validation ---
    if not language: return make_response("<p class='flash flash-error'>Vui long chon Ngon ngu.</p>", 400)

    try:
        if task_type == 'rewrite_script':
            # --- Directly Queue Rewrite Task ---
            if not source_script: return make_response("<p class='flash flash-error'>Vui long nhap Script Goc.</p>", 400)
            check_openai_available(use_abort=True) # Cần để dịch title nếu cần

            topic_title = f"Rewrite Task ({language}) - {source_script[:40]}..."
            vietnamese_title = topic_title if language == "Vietnamese" else translate_text(topic_title, source_language=language)
            now = datetime.datetime.now(datetime.timezone.utc)
            target_duration = None
            if target_duration_str and target_duration_str.isdigit(): td = int(target_duration_str); target_duration = td if 1 <= td <= 180 else None
            priority_map = {"low": 3, "medium": 2, "high": 1}; priority = priority_map.get(priority_str, 2)

            # Upsert Topic
            topic_doc = topics_collection.find_one_and_update(
                {"initial_content_snippet": source_script[:200], "language": language},
                {"$setOnInsert": {"seed_topic": topic_title, "language": language, "title": topic_title, "title_vi": vietnamese_title, "status": "task_created", "created_at": now}, "$set": { "updated_at": now }},
                projection={"_id": 1}, upsert=True, return_document=ReturnDocument.AFTER
            )
            topic_id = topic_doc['_id']

            # Create Generation Task
            generation_data = { "topic_id": topic_id, "language": language, "title": topic_title, "title_vi": vietnamese_title, "task_type": task_type, "status": "pending", "created_at": now, "updated_at": now, "priority": priority, "model": model, **({"target_duration_minutes": target_duration} if target_duration is not None else {}), "source_script": source_script }
            gen_result = content_generations_collection.insert_one(generation_data)
            generation_id = gen_result.inserted_id
            topics_collection.update_one({"_id": topic_id}, {"$set": {"generation_id": generation_id, "status": "generation_requested", "updated_at": now}})
            logging.info(f"Created REWRITE generation task {generation_id} for topic {topic_id}.")

            # Trả về thông báo thành công và trigger refresh
            response = make_response(f"<p class='flash flash-success'>Da gui yeu cau viet lai script (ID: {generation_id}).</p>")
            response.headers.update(trigger_flash("success", f"Da gui yeu cau rewrite {generation_id}."))
            response.headers['HX-Refresh'] = 'true' # Bảo HTMX tải lại toàn bộ trang
            return response

        elif task_type == 'from_topic':
            # --- Generate Suggestions and return HTML fragment ---
            if not seed_topic: return make_response("<p class='flash flash-error'>Vui long nhap Seed Topic.</p>", 400)
            check_openai_available(use_abort=True)

            logging.info(f"Generating suggestions for seed: '{seed_topic}', Lang: {language}")
            num_suggestions = 5
            # ... (Logic gọi AI tạo gợi ý - suggested_topics_original) ...
            prompt_generate = f"""Suggest {num_suggestions} YouTube titles for "{seed_topic}". Req: SEO, concise, keywords. Lang: {language}. Output ONLY titles, 1 per line."""
            messages = [{"role": "system", "content": f"Expert YouTube title creator in {language}."}, {"role": "user", "content": prompt_generate}]
            response_ai = oai_client.chat.completions.create(model="gpt-4o-mini", messages=messages, max_tokens=500, temperature=0.75)
            suggested_topics_original = [t.strip().strip('"\'()[]{}.-* ') for t in response_ai.choices[0].message.content.strip().split('\n') if t.strip() and len(t.strip()) > 3][:num_suggestions]

            if not suggested_topics_original: return "<p class='flash flash-info'>AI khong tao duoc goi y.</p>", 200

            # Translate
            topics_data = []
            if language != "Vietnamese":
                with concurrent.futures.ThreadPoolExecutor(max_workers=num_suggestions) as executor:
                    future_map = {executor.submit(translate_text, orig, source_language=language): orig for orig in suggested_topics_original}
                    for future in concurrent.futures.as_completed(future_map):
                        orig = future_map[future]; trans = future.result()
                        topics_data.append({"original": orig, "translation_vi": orig if trans.startswith("[Loi") else trans})
            else:
                topics_data = [{"original": orig, "translation_vi": orig} for orig in suggested_topics_original]

            # Render HTML fragment chứa form chọn gợi ý và các tùy chọn đã nhập từ form chính
            return render_template('_suggestion_list.html',
                                   suggestions=topics_data,
                                   language=language,
                                   # Truyền các tùy chọn đã nhập để hiển thị lại trên form thứ 2
                                   target_duration=target_duration_str,
                                   priority=priority_str,
                                   selected_model=model)
        else:
             return make_response("<p class='flash flash-error'>Loai task khong hop le.</p>", 400)

    except Exception as e:
        logging.exception("Error in /handle_initial_submission")
        return make_response(f"<div class='error-message flash flash-error'>Loi server: {e}</div>"), 500

@app.route('/submit_selected_for_generation', methods=['POST'])
def submit_selected_for_generation():
    """Handles submission of selected suggestions from the fragment."""
    check_db_available()
    try:
        selected_combined = request.form.getlist('selected_suggestion')
        language = request.form.get('language_for_generation')
        # Lấy tùy chọn từ form thứ hai này
        target_duration_str = request.form.get('target_duration_submit')
        priority_str = request.form.get('priority_submit', 'medium').lower()
        model = request.form.get('model_submit', 'gpt-4o')

        if not language: return make_response("", 400, trigger_flash("error", "Loi: Thieu ngon ngu."))
        logging.info(f"Submitting {len(selected_combined)} selected topics. Lang:'{language}', Prio:{priority_str}, Model:{model}")
        if not selected_combined: return make_response("<div id='suggestions-display'></div>", 200, trigger_flash("warning", "Vui long chon goi y.")) # Xóa form gợi ý

        target_duration = None
        if target_duration_str and target_duration_str.isdigit(): td = int(target_duration_str); target_duration = td if 1 <= td <= 180 else None
        priority_map = {"low": 3, "medium": 2, "high": 1}; priority = priority_map.get(priority_str, 2)
        now = datetime.datetime.now(datetime.timezone.utc)

        processed_topic_ids = []; created_gen_count = 0; skipped_count = 0; error_count = 0

        for combined in selected_combined:
            try:
                parts = combined.split('||', 1); original_title = parts[0].strip()
                vietnamese_title = parts[1].strip() if len(parts) > 1 else original_title
                if not original_title: continue

                topic = topics_collection.find_one_and_update(
                    {"title": original_title, "language": language},
                    {"$setOnInsert": {"seed_topic": original_title, "language": language, "title": original_title, "title_vi": vietnamese_title, "status": "suggested", "created_at": now}, "$set": { "updated_at": now }},
                    projection={"_id": 1, "generation_id": 1, "status": 1}, upsert=True, return_document=ReturnDocument.AFTER
                )
                topic_id = topic['_id']

                existing_gen = content_generations_collection.find_one({"topic_id": topic_id, "status": {"$nin": ["content_failed", "audio_failed", "deleted", "reset"]}})
                if existing_gen: skipped_count += 1; processed_topic_ids.append(topic_id); continue

                gen_data = { "topic_id": topic_id, "language": language, "title": original_title, "title_vi": vietnamese_title, "seed_topic": original_title, "status": "pending", "created_at": now, "updated_at": now, "priority": priority, "model": model, **({"target_duration_minutes": target_duration} if target_duration is not None else {}), "task_type": "from_topic" }
                gen_result = content_generations_collection.insert_one(gen_data)
                created_gen_count += 1
                topics_collection.update_one({"_id": topic_id}, {"$set": {"generation_id": gen_result.inserted_id, "status": "generation_requested", "updated_at": now}})
                processed_topic_ids.append(topic_id)
            except Exception as item_err: error_count += 1; logging.error(f"Error processing item '{combined}': {item_err}", exc_info=True)

        msg = ""; lvl = "info"
        if created_gen_count > 0: msg += f"Da gui {created_gen_count} yeu cau moi. "; lvl = "success"
        if skipped_count > 0: msg += f"Bo qua {skipped_count} topic da co. "
        if error_count > 0: msg += f"Loi xu ly {error_count} topic. "; lvl = "warning" if created_gen_count > 0 else "error"
        if not msg: msg = "Khong co hanh dong."

        updated_items_html = "".join([render_topic_item(tid) for tid in processed_topic_ids])
        response = make_response(updated_items_html)
        response.headers['HX-Reswap'] = 'afterbegin' # Chèn các item mới lên đầu list #topic-list-dynamic
        response.headers['HX-Retarget'] = '#topic-list-dynamic' # Chỉ định rõ target cho HTML trả về
        response.headers.update(trigger_flash(lvl, msg.strip()))
        # Trigger JS để xóa div gợi ý
        trigger_payload = json.loads(response.headers['HX-Trigger'])
        trigger_payload['clearSuggestionDisplay'] = True
        response.headers['HX-Trigger'] = json.dumps(trigger_payload)
        return response

    except Exception as e:
        logging.error(f"Error in /submit_selected_topics: {e}", exc_info=True)
        return make_response("", 500, trigger_flash("error", "Loi server khi gui yeu cau."))

# --- Các Route Actions (/delete_topic, /delete_generation, /reset_generation, /reset_topic_link) ---
@app.route('/delete_topic/<topic_id>', methods=['DELETE'])
def delete_topic(topic_id):
    check_db_available()
    try: oid = ObjectId(topic_id)
    except Exception: return "ID khong hop le", 400
    # Chỉ xóa nếu là suggested và không có generation_id
    delete_result = topics_collection.delete_one({"_id": oid, "status": "suggested", "generation_id": None})
    if delete_result.deleted_count > 0: return "", 200 # OK, HTMX sẽ xóa
    else:
        topic = topics_collection.find_one({"_id": oid})
        msg = "Khong the xoa topic (da xu ly?)" if topic else "Topic khong tim thay."
        resp = make_response("", 409 if topic else 404)
        resp.headers.update(trigger_flash("warning", msg))
        return resp

@app.route('/delete_generation/<generation_id>', methods=['DELETE'])
def delete_generation(generation_id):
    check_db_available()
    try: oid = ObjectId(generation_id)
    except Exception: return "", 400
    generation = content_generations_collection.find_one({"_id": oid}, {"topic_id": 1, "script_name": 1})
    if not generation: return "", 404
    topic_id = generation.get("topic_id"); script_name = generation.get("script_name")
    now = datetime.datetime.now(datetime.timezone.utc)
    try:
        script_chunks_collection.delete_many({"generation_id": oid})
        content_generations_collection.delete_one({"_id": oid})
        # Placeholder: Xóa file vật lý
        if script_name:
            audio_folder = os.path.join(os.getenv("LOCAL_AUDIO_OUTPUT_PATH", "/mnt/NewVolume/Audio"), script_name)
            # import shutil; if os.path.isdir(audio_folder): shutil.rmtree(audio_folder) # Cẩn thận khi dùng rmtree
            logging.info(f"Placeholder: Deleted files for {script_name}")
        logging.info(f"Deleted generation {oid}.")
    except Exception as e: logging.error(f"Error deleting data for gen {oid}: {e}"); abort(500)
    if topic_id:
        topics_collection.update_one({"_id": topic_id, "generation_id": oid},{"$set": {"generation_id": None, "status": "generation_reset", "updated_at": now}})
        return render_topic_item(topic_id)
    else: return "", 200

@app.route('/reset_generation/<generation_id>', methods=['POST'])
def reset_generation(generation_id):
    check_db_available()
    try: oid = ObjectId(generation_id)
    except Exception: return make_response("", 400, trigger_flash("error", "ID khong hop le."))
    now = datetime.datetime.now(datetime.timezone.utc)
    generation = content_generations_collection.find_one({"_id": oid}, {"topic_id": 1})
    if not generation: return make_response("", 404, trigger_flash("error", "Generation khong tim thay."))
    topic_id = generation.get("topic_id")
    try: # Xóa chunks cũ
        script_chunks_collection.delete_many({"generation_id": oid})
    except Exception as e: logging.error(f"Error deleting chunks for reset {generation_id}: {e}")
    # Reset generation status
    update_gen = content_generations_collection.update_one({"_id": oid}, {"$set": {"status": "pending", "updated_at": now, "error_details": None, "outline": None, "derived_outline": None, "final_audio_path": None}})
    logging.info(f"Reset generation {generation_id} to pending.")
    if topic_id: # Reset topic status
        topics_collection.update_one({"_id": topic_id}, {"$set": {"status": "generation_pending", "updated_at": now}})
        return make_response(render_topic_item(topic_id), 200, trigger_flash("success", f"Da reset generation {generation_id}."))
    else: return make_response("", 200, trigger_flash("info", f"Gen {generation_id} reset, khong co topic."))

@app.route('/reset_topic_link/<topic_id>', methods=['POST'])
def reset_topic_link(topic_id):
    check_db_available()
    try: oid = ObjectId(topic_id)
    except Exception: return make_response("", 400, trigger_flash("error", "Topic ID khong hop le."))
    topic = topics_collection.find_one({"_id": oid}, {"generation_id": 1})
    if not topic: return make_response("", 404, trigger_flash("error", "Topic khong ton tai."))
    gen_id = topic.get("generation_id")
    if not gen_id: return make_response(render_topic_item(oid), 200, trigger_flash("info", "Topic khong co lien ket."))
    gen_exists = content_generations_collection.find_one({"_id": gen_id}, {"_id": 1})
    if gen_exists: return make_response(render_topic_item(oid), 200, trigger_flash("warning", "Lien ket generation hop le."))
    # Unlink
    now = datetime.datetime.now(datetime.timezone.utc)
    topics_collection.update_one({"_id": oid}, {"$set": {"generation_id": None, "status": "suggested", "updated_at": now}})
    logging.info(f"Unlinked missing gen {gen_id} from topic {oid}.")
    return make_response(render_topic_item(oid), 200, trigger_flash("success", "Da go lien ket loi."))

# --- Route xem chi tiết ---
@app.route('/view_generation/<generation_id>')
def view_generation(generation_id):
    check_db_available()
    try: oid = ObjectId(generation_id)
    except Exception: abort(404, description="Generation ID không hợp lệ.")
    generation = content_generations_collection.find_one({"_id": oid})
    if not generation: abort(404, description="Không tìm thấy Generation.")
    chunks = list(script_chunks_collection.find({"generation_id": oid}).sort("section_index", 1))
    # Render template xem chi tiết (cần tạo file này)
    return render_template('view_content.html', generation=generation, chunks=chunks)

# --- API Status ---
@app.route('/api/generation_status/<generation_id>')
def api_generation_status(generation_id):
    check_db_available()
    try: oid = ObjectId(generation_id); gen = content_generations_collection.find_one({"_id": oid},{"status": 1, "error_details": 1, "updated_at": 1, "_id": 0})
    except Exception: return jsonify({"error": f"Invalid ID: {generation_id}"}), 400
    if gen:
        if gen.get('updated_at'): gen['updated_at'] = gen['updated_at'].isoformat() + 'Z'
        if gen.get('error_details') and gen['error_details'].get('timestamp'): gen['error_details']['timestamp'] = gen['error_details']['timestamp'].isoformat() + 'Z'
        return jsonify(gen)
    else: return jsonify({"error": "Generation not found"}), 404

# --- Jinja Filter for Audio Paths ---
@app.template_filter('network_to_static_url')
def network_path_to_static_url(network_or_local_path):
    """Chuyển đổi đường dẫn UNC hoặc Local Linux thành URL static tương đối."""
    if not network_or_local_path or not isinstance(network_or_local_path, str):
        return None
    try:
        # Đường dẫn cơ sở vật lý trên Linux mà thư mục static/audio_output trỏ tới
        audio_base_physical = os.path.abspath(os.getenv("LOCAL_AUDIO_OUTPUT_PATH", "/mnt/NewVolume/Audio"))
        # Đường dẫn tương đối trong thư mục static
        static_audio_rel_path = "audio_output"

        # Chuẩn hóa path nhận được
        normalized_path = os.path.normpath(network_or_local_path.replace('\\', '/')) # Ưu tiên dùng /

        # Nếu là đường dẫn UNC, cố gắng chuyển đổi
        unc_base = os.path.normpath(fr"\\{os.getenv('LINUX_SERVER_IP', '0.0.0.0')}\{os.getenv('SAMBA_SHARE_NAME', 'AudioOutput')}".replace('\\','/'))
        if normalized_path.lower().startswith(unc_base.lower()):
             relative_path = normalized_path[len(unc_base):].lstrip('/')
             static_path = f"{static_audio_rel_path}/{relative_path}".replace("\\", "/")
             logging.debug(f"Converted UNC '{network_or_local_path}' to Static '{static_path}'")
             return static_path

        # Nếu là đường dẫn cục bộ Linux, kiểm tra xem có nằm trong thư mục audio không
        normalized_local_base = os.path.normpath(audio_base_physical)
        if normalized_path.startswith(normalized_local_base):
             relative_path = normalized_path[len(normalized_local_base):].lstrip('/')
             static_path = f"{static_audio_rel_path}/{relative_path}".replace("\\", "/")
             logging.debug(f"Converted Local '{network_or_local_path}' to Static '{static_path}'")
             return static_path

        # Nếu không khớp, có thể là đường dẫn lỗi hoặc cấu hình sai
        logging.warning(f"Path '{network_or_local_path}' could not be converted to static URL.")
        return None # Trả về None nếu không chuyển đổi được
    except Exception as e:
         logging.error(f"Error in network_to_static_url filter for path '{network_or_local_path}': {e}")
         return None


# --- Error Handlers ---
@app.errorhandler(NotFound) # 404
def not_found_error(error):
    logging.warning(f"404 Not Found: {request.url} - {error.description}")
    if request.headers.get('HX-Request') == 'true':
        resp = make_response(f"<div class='error-message flash flash-error'>Loi 404: Khong tim thay ({request.path}).</div>", 404)
        resp.headers.update(trigger_flash("error", f"Khong tim thay: {request.path}"))
        return resp
    # Đảm bảo bạn có file 404.html
    return render_template('404.html', error=error), 404

@app.errorhandler(InternalServerError) # 500
@app.errorhandler(Exception) # Catch others
def internal_error(error):
    status_code = getattr(error, 'code', 500); status_code = status_code if isinstance(status_code, int) else 500
    # Chỉ log traceback cho lỗi 500 không mong muốn
    if status_code == 500 and not isinstance(error, (NotFound, BadRequest)):
        logging.error(f"Server Error: {request.url}", exc_info=error)
    else: # Log lỗi 4xx hoặc 500 có chủ đích (từ abort) ở mức WARNING
         logging.warning(f"Handled Error {status_code}: {request.url} - {error}")

    is_htmx = request.headers.get('HX-Request') == 'true'
    error_desc = getattr(error, 'description', str(error))

    if is_htmx:
        message = f"Loi Server {status_code}."
        if error_desc and status_code != 500: message = f"Loi {status_code}: {error_desc}"
        resp = make_response(f"<div class='error-message flash flash-error'>{message}</div>", status_code)
        resp.headers.update(trigger_flash("error", f"Loi Server {status_code}."))
        return resp
    else:
        # Đảm bảo bạn có file 500.html
        return render_template('500.html', error=error), status_code

# --- Main Execution ---
if __name__ == '__main__':
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    try: port = int(os.getenv("FLASK_PORT", "5001"))
    except ValueError: port = 5001; logger.warning(f"Invalid FLASK_PORT, using {port}.")
    debug_mode = os.getenv("FLASK_DEBUG", "False").lower() in ["true", "1", "yes", "on"]
    if debug_mode: logger.setLevel(logging.DEBUG); logger.info("Flask DEBUG mode is ON.")
    else: logger.info("Flask DEBUG mode is OFF.")

    logger.info(f"Starting Flask server on http://{host}:{port}")
    app.run(debug=debug_mode, host=host, port=port, threaded=True, use_reloader=debug_mode)