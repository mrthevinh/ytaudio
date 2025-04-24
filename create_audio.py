from pathlib import Path
import os
import datetime
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from pydub import AudioSegment
import concurrent.futures
from bson.objectid import ObjectId
from openai import OpenAI
import tenacity
import schedule
import time

load_dotenv(override=True)

# --- Logging configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log_file = 'create_audio.log'
file_handler = logging.FileHandler(log_file, mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# --- TTS Local Configuration ---
tts_api_key = os.getenv("TTS_API_KEY", "trmeczqbMwJYJz4PXbPBdzgjos2RF6z6")
tts_base_url = os.getenv("TTS_BASE_URL", "https://api.lemonfox.ai/v1")
client_tts = OpenAI(api_key=tts_api_key, base_url=tts_base_url)

# --- MongoDB Configuration ---
mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
db_name = "content_db"
content_generations_collection_name = "ContentGenerations"
script_chunks_collection_name = "ScriptChunks"

# Kết nối MongoDB (một lần)
try:
    mongo_client = MongoClient(mongodb_uri)
    db = mongo_client[db_name]
    content_generations_collection = db[content_generations_collection_name]
    script_chunks_collection = db[script_chunks_collection_name]
    logging.info("Connected to MongoDB.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(2),
    reraise=True
)
def create_audio_for_chunk(chunk_doc_id, generation_id, script_name, voice="onyx", speed=1.0):
    """Tạo audio cho một chunk (lấy từ MongoDB), có retry."""
    chunk_doc = script_chunks_collection.find_one({"_id": chunk_doc_id}) # Tìm bằng _id của chunk
    if not chunk_doc:
        logging.error(f"Chunk document with id {chunk_doc_id} not found.")
        return chunk_doc_id, False, None

    text_content = chunk_doc["text_content"]
    section_index = chunk_doc["section_index"]

    script_folder_name = Path(script_name).stem # script_name dùng cho folder
    # output_dir = Path(__file__).parent / "audio_output" / script_folder_name
    output_dir = Path("/mnt/NewVolume/Audio") / script_folder_name
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    audio_file_name = f"section_{section_index}_{timestamp}.mp3"
    audio_file_path = str(output_dir / audio_file_name)

    try:
        with client_tts.audio.speech.with_streaming_response.create(
            model='tts-1',
            voice=voice,
            input=text_content,
            speed=speed,
            response_format='mp3'
        ) as response:
            response.stream_to_file(audio_file_path)

        # Cập nhật thành công trong ScriptChunks
        script_chunks_collection.update_one(
            {"_id": chunk_doc_id},
            {"$set": {"audio_file_path": audio_file_path, "audio_created": True, "audio_error": None}}
        )
        logging.info(f"Audio created for chunk {chunk_doc_id} saved to {audio_file_path}")
        return chunk_doc_id, True, audio_file_path

    except Exception as e:
        logging.error(f"Error creating audio for chunk {chunk_doc_id}: {e}")
        # Cập nhật trạng thái lỗi trong ScriptChunks
        script_chunks_collection.update_one(
            {"_id": chunk_doc_id},
            {"$set": {"audio_created": False, "audio_error": str(e)}}
        )
        raise

def process_audio_task(generation_doc, voice="onyx", speed=1.0):
    """Xử lý tạo audio cho một generation task."""
    generation_id = str(generation_doc["_id"])
    script_name = generation_doc["script_name"] # Lấy script_name từ generation doc

    logging.info(f"Processing audio for generation task: {generation_id}")

    # Cập nhật trạng thái thành audio_generating
    content_generations_collection.update_one(
        {"_id": ObjectId(generation_id)},
        {"$set": {"status": "audio_generating", "updated_at": datetime.datetime.now(datetime.timezone.utc)}}
    )

    # Tìm các chunk cần tạo audio
    pending_chunks = script_chunks_collection.find({
        "generation_id": ObjectId(generation_id), # Lọc theo generation_id
        "$or": [
            {"audio_file_path": None},
            {"audio_created": False},
            {"audio_error": {"$ne": None}}
        ]
    }).sort("section_index", 1)

    # Tạo audio (đa luồng)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(create_audio_for_chunk, chunk["_id"], generation_id, script_name, voice, speed)
                   for chunk in pending_chunks]

        for future in concurrent.futures.as_completed(futures):
            chunk_id, success, _ = future.result()
            # Có thể log thêm ở đây nếu muốn

    # Kiểm tra lại trạng thái sau khi các thread hoàn thành
    all_chunks_done = script_chunks_collection.count_documents({
        "generation_id": ObjectId(generation_id),
        "audio_created": True,
        "audio_error": None
    })
    total_chunks = script_chunks_collection.count_documents({"generation_id": ObjectId(generation_id)})
    any_errors = script_chunks_collection.count_documents({"generation_id": ObjectId(generation_id), "audio_error": {"$ne": None}}) > 0

    # Cập nhật trạng thái cuối cùng trong ContentGenerations
    final_status = "completed"
    if any_errors:
        final_status = "audio_failed"
    elif all_chunks_done < total_chunks:
        final_status = "content_ready" # Về lại content_ready để lần sau quét tiếp

    update_result = {"status": final_status, "updated_at": datetime.datetime.now(datetime.timezone.utc)}

    # Ghép audio nếu không có lỗi và đã tạo đủ
    if final_status == "completed":
        combined_audio_path = combine_audio_from_db(generation_id, script_name) # Cần script_name
        if combined_audio_path:
            update_result["final_audio_path"] = combined_audio_path
        else:
            final_status = "audio_failed" # Không ghép được cũng là lỗi
            update_result["status"] = final_status
            update_result["error_details"] = {"stage": "audio_concatenation", "message": "Failed to concatenate audio", "timestamp": datetime.datetime.now(datetime.timezone.utc)}

    content_generations_collection.update_one(
        {"_id": ObjectId(generation_id)},
        {"$set": update_result}
    )
    logging.info(f"Audio processing finished for task {generation_id} with status: {final_status}")

def concatenate_audio(audio_file_paths, output_file_path):
    """Nối các file audio (sử dụng pydub)."""
    if not audio_file_paths:
        logging.info("Không có file audio nào để ghép.")
        return

    try:
        combined = AudioSegment.empty()
        for path in audio_file_paths:
            #Thêm kiểm tra file tồn tại.
            if Path(path).is_file():
              sound = AudioSegment.from_file(path)
              combined += sound
            else:
                logging.warning(f"Audio file not found, skipping: {path}")
        if len(combined) > 0:
            combined.export(output_file_path, format="mp3")
            logging.info(f"Combined audio saved to {output_file_path}")
        else:
            logging.warning("No valid audio segments to combine.")


    except Exception as e:
        logging.error(f"Error during audio concatenation: {e}")
def combine_audio_from_db(generation_id, script_name):
    """Lấy đường dẫn audio từ ScriptChunks, ghép lại (nếu có)."""
    script_folder_name = Path(script_name).stem
    output_dir = Path("/mnt/NewVolume/Audio") / script_folder_name
    # output_dir = Path(__file__).parent / "audio_output" / script_folder_name
    output_audio_file = output_dir / f"{script_folder_name}_combined_{generation_id}.mp3" # Thêm generation_id vào tên file

    # Chỉ tìm các file đã tạo thành công cho generation_id này
    audio_paths = [doc['audio_file_path'] for doc in script_chunks_collection.find({
        "generation_id": ObjectId(generation_id),
        "audio_created": True,
        "audio_error": None # Không có lỗi
        }).sort("section_index", 1)]

    if audio_paths:
        concatenate_audio(audio_paths, str(output_audio_file))
        return str(output_audio_file)
    else:
        logging.info(f"No successfully created audio files found for generation {generation_id} to combine.")
        return None
def job():
    """Hàm này sẽ được chạy định kỳ."""
    logging.info("Starting audio creation job...")
    # Tìm các task có status 'content_ready' hoặc 'audio_failed'
    tasks_to_process = content_generations_collection.find({
        "$or": [
            {"status": "content_ready"},
            {"status": "audio_failed"}
        ]
    }).sort("created_at", 1)

    for task_doc in tasks_to_process:
         # Thêm lock để tránh nhiều worker cùng xử lý 1 task (nếu chạy nhiều instance create_audio)
        result = content_generations_collection.find_one_and_update(
             {"_id": task_doc["_id"], "status": {"$in": ["content_ready", "audio_failed"]}}, # Chỉ update nếu status còn là content_ready/audio_failed
             {"$set": {"status": "audio_processing_lock", "updated_at": datetime.datetime.now(datetime.timezone.utc)}},
             return_document=True # Trả về document đã update
        )
        if result: # Nếu update thành công (giành được lock)
            process_audio_task(result) # Truyền document đã update
        else:
             logging.info(f"Task {task_doc['_id']} is being processed by another worker. Skipping.")


    logging.info("Audio creation job finished.")

if __name__ == "__main__":
    job() # <<< CHẠY NGAY LẦN ĐẦU TIÊN KHI KHỞI ĐỘNG
    # Lên lịch chạy mỗi 10 phút cho các lần sau
    schedule.every(5).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
    # job()