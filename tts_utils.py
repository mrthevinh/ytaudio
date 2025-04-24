# tts_utils.py
import datetime
import json
import logging
import os
import shutil
import tempfile
# import time # Có thể bỏ nếu không dùng sleep hoặc logic thời gian khác
import urllib.parse
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any

import openai # Import module để dùng exception types
import pymongo # Import để dùng sort direction và errors
import pymongo.errors
import requests
import tenacity # Import tenacity đầy đủ
from bson.objectid import ObjectId
from dotenv import load_dotenv
from openai import OpenAI
# Import cụ thể các thành phần tenacity cần dùng
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from typing import Dict, Optional, Tuple, List, Any, Literal, cast

# Gần phần Constants
VALID_OPENAI_VOICES: set[str] = {'alloy', 'ash', 'coral', 'echo', 'fable', 'onyx', 'nova', 'sage', 'shimmer'}
OpenAIVoice = Literal['alloy', 'ash', 'coral', 'echo', 'fable', 'onyx', 'nova', 'sage', 'shimmer']
# --- Local Module Imports ---
try:
    from db_manager import get_script_chunks_collection
    # Cần hàm chia chunk từ utils
    from utils import split_script_into_chunks
except ImportError as e:
    logging.critical(f"tts_utils: Failed critical imports (db_manager, utils): {e}. Exiting.")
    exit(1) # Thoát nếu import cốt lõi thất bại

# --- Pydub / FFmpeg Setup ---
try:
    from pydub import AudioSegment
    FFMPEG_PATH_ENV = os.getenv("FFMPEG_PATH")
    # Sử dụng Path object để kiểm tra
    ffmpeg_path_obj = Path(FFMPEG_PATH_ENV) if FFMPEG_PATH_ENV else None
    if ffmpeg_path_obj and ffmpeg_path_obj.is_file():
        AudioSegment.converter = str(ffmpeg_path_obj) # Pydub cần string path
        logging.info(f"Using ffmpeg from FFMPEG_PATH: {AudioSegment.converter}")
    else:
        # Thử tìm ffmpeg trong PATH hệ thống (pydub sẽ tự làm điều này)
        logging.info("FFMPEG_PATH not set or invalid. Using ffmpeg from system PATH (ensure it's installed and includes libmp3lame).")
except ImportError:
    logging.critical("Pydub is not installed (pip install pydub). Audio processing/concatenation disabled.")
    AudioSegment = None # Gán lại để kiểm tra sau này
except Exception as e:
    # Lỗi khác khi cấu hình pydub
    logging.warning(f"Pydub/FFmpeg configuration warning: {e}. Audio processing might fail.", exc_info=True)
    # Không gán lại AudioSegment = None ở đây, vì pydub có thể vẫn import được

# --- Load Environment Variables ---
load_dotenv(override=True)

# --- Logging Setup ---
# Cấu hình logging cơ bản nếu chưa được cấu hình ở tầng cao hơn
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Sử dụng logger theo tên module

# --- Configuration & Constants ---
VOICE_CONFIG_FILE = Path(os.getenv("VOICE_CONFIG_FILE", 'voice_config.json'))
# Sử dụng Path object cho đường dẫn
LOCAL_AUDIO_BASE_PATH_STR = os.getenv("LOCAL_AUDIO_OUTPUT_PATH", "/mnt/NewVolume/Audio") # Thay đổi path mặc định nếu cần
LOCAL_AUDIO_BASE_PATH = Path(LOCAL_AUDIO_BASE_PATH_STR)
TTS_API_CHAR_LIMIT = int(os.getenv("TTS_CHUNK_CHAR_LIMIT", 500)) # Giới hạn ký tự cho TTS API
MIN_AUDIO_FILE_SIZE_BYTES = 100 # File audio hợp lệ phải lớn hơn ngưỡng này
POLLINATIONS_API_URL_BASE = "https://text.pollinations.ai/"
POLLINATIONS_URL_CHAR_LIMIT = 4000 # Giới hạn URL của Pollinations (ước tính)
# Hằng số cho retry chung
RETRY_ATTEMPTS = 3 # Số lần retry cho hàm chính
RETRY_WAIT_SECONDS = 5 # Thời gian chờ giữa các lần retry chính
# Hằng số cho retry của Pollinations
POLLINATIONS_RETRY_ATTEMPTS = 3
POLLINATIONS_RETRY_WAIT_SECONDS = 3
# Timeout chung cho các request API
API_TIMEOUT_SECONDS = 120

# --- Đảm bảo thư mục audio tồn tại ---
try:
    LOCAL_AUDIO_BASE_PATH.mkdir(parents=True, exist_ok=True)
    # Kiểm tra quyền ghi và thực thi (cần thiết để tạo file và thư mục con)
    if not os.access(LOCAL_AUDIO_BASE_PATH, os.W_OK | os.X_OK):
        raise OSError(f"No write/execute access to '{LOCAL_AUDIO_BASE_PATH}'. Check permissions.")
    logging.info(f"Using local audio base path: {LOCAL_AUDIO_BASE_PATH}")
except OSError as e:
    logging.critical(f"CRITICAL: Error with audio path '{LOCAL_AUDIO_BASE_PATH}': {e}", exc_info=True)
    exit(1) # Thoát nếu không thể ghi vào thư mục audio

# --- TTS Client Initialization ---
# Client cho các ngôn ngữ khác (OpenAI hoặc local TTS server tương thích OpenAI)
tts_api_key_other = os.getenv("TTS_API_KEY")
tts_base_url_other = os.getenv("TTS_BASE_URL") # Optional, nếu dùng local server
client_tts_other: Optional[OpenAI] = None
if tts_api_key_other:
    try:
        client_tts_other = OpenAI(api_key=tts_api_key_other, base_url=tts_base_url_other)
        base_url_log = f"base_url={tts_base_url_other}" if tts_base_url_other else "Default OpenAI URL"
        logging.info(f"Initialized OTHER TTS client ({base_url_log})")
    except Exception as e:
        logging.error(f"Failed to initialize OTHER TTS client: {e}", exc_info=True)
        client_tts_other = None # Đảm bảo client là None nếu init lỗi
else:
    logging.warning("TTS_API_KEY (for OpenAI/Local TTS) not set. These providers will be unavailable.")

# --- Custom Exceptions ---
class TTSProviderError(Exception):
    """Lỗi chung liên quan đến provider TTS."""
    pass

class PollinationsError(TTSProviderError):
    """Exception tùy chỉnh cho các lỗi logic của Pollinations."""
    pass

class ConfigurationError(Exception):
    """Lỗi liên quan đến cấu hình."""
    pass

# --- Helper Functions ---

def load_voice_config(config_path: Path = VOICE_CONFIG_FILE) -> Dict[str, Any]:
    """Tải cấu hình giọng đọc từ file JSON."""
    try:
        # Thử đường dẫn tuyệt đối hoặc tương đối so với script
        if not config_path.is_absolute():
            script_dir = Path(__file__).parent
            absolute_config_path = script_dir / config_path
        else:
            absolute_config_path = config_path

        if not absolute_config_path.exists():
            # Thử đường dẫn tương đối so với CWD nếu không tìm thấy gần script
            absolute_config_path = Path.cwd() / config_path
            if not absolute_config_path.exists():
                logging.error(f"Voice config file not found at specified/script/CWD path: {config_path}")
                return {}

        with absolute_config_path.open('r', encoding='utf-8') as f:
            config = json.load(f)
        logging.info(f"Loaded voice configuration from {absolute_config_path}")
        return config
    except json.JSONDecodeError as json_e:
        logging.error(f"Error decoding JSON from voice config file {config_path}: {json_e}", exc_info=True)
        return {}
    except Exception as e:
        logging.error(f"Error loading voice config from {config_path}: {e}", exc_info=True)
        return {}

# Load config toàn cục một lần
VOICE_CONFIG = load_voice_config()
if not VOICE_CONFIG:
    logging.warning("VOICE_CONFIG is empty or failed to load. Using hardcoded default voice settings only.")

def get_voice_settings(language: str, voice_config: Dict[str, Any]) -> Dict[str, Any]:
    """Lấy thông tin cài đặt giọng đọc cho một ngôn ngữ."""
    # Cấu hình mặc định cuối cùng nếu mọi thứ thất bại
    ultimate_default = {
        "provider": "openai",
        "voice_name": "onyx",
        "language_code": "en-US", # Giữ lại để tham khảo, nhưng provider quyết định
        "speaking_rate": 1.0
    }

    if not voice_config:
        logging.warning("Voice config is empty, using ultimate default settings.")
        return ultimate_default.copy() # Trả về bản copy

    # Lấy cấu hình default từ file, hoặc dùng ultimate default
    default_settings = voice_config.get("__DEFAULT__", ultimate_default).copy()

    settings = None
    lang_lower = language.lower()

    # 1. Thử khớp chính xác (không phân biệt hoa thường)
    for key, value in voice_config.items():
        if key != "__DEFAULT__" and key.lower() == lang_lower:
            settings = value.copy() # Lấy bản copy để tránh thay đổi config gốc
            logging.info(f"Found exact match for language '{language}' in config.")
            break

    # 2. Nếu không khớp chính xác, thử khớp một phần
    if settings is None:
        for key, value in voice_config.items():
            if key != "__DEFAULT__" and lang_lower in key.lower():
                settings = value.copy()
                logging.warning(f"Used partial match config key '{key}' for requested language '{language}'.")
                break

    # 3. Nếu vẫn không tìm thấy, dùng default
    if settings is None:
        logging.warning(f"No specific or partial match found for language '{language}'. Using default settings.")
        settings = default_settings

    # Đảm bảo các trường thiết yếu tồn tại, lấy từ default nếu thiếu
    settings.setdefault("provider", default_settings.get("provider", "openai"))
    settings.setdefault("voice_name", default_settings.get("voice_name", "onyx"))
    settings.setdefault("speaking_rate", float(default_settings.get("speaking_rate", 1.0)))
    # language_code thường đi kèm voice, không cần setdefault cứng nhắc ở đây

    logging.info(f"Final settings for '{language}': Provider='{settings['provider']}', Voice='{settings['voice_name']}', Rate={settings['speaking_rate']:.2f}")
    return settings

# --- Provider-Specific TTS Callers ---

@retry(
    stop=stop_after_attempt(POLLINATIONS_RETRY_ATTEMPTS),
    wait=wait_fixed(POLLINATIONS_RETRY_WAIT_SECONDS),
    retry=retry_if_exception_type((requests.exceptions.RequestException, PollinationsError)), # Retry lỗi mạng và lỗi logic của Pollinations
    reraise=True, # Quan trọng: Ném lại lỗi cuối cùng để tầng gọi ngoài biết
     before_sleep=lambda retry_state: logger.warning(
        (
            f"Retrying create_audio_for_chunk (Attempt #{retry_state.attempt_number}) "
            f"due to {type(retry_state.outcome.exception()).__name__ if retry_state.outcome and retry_state.outcome.exception() else 'UnknownOutcome'}. " # <<< SỬA Ở ĐÂY
            f"Waiting {RETRY_WAIT_SECONDS}s..."
        )
    )
)
def _call_pollinations_tts(text: str, voice_name: str, output_filename: Path) -> bool:
    """
    Gọi API Pollinations TTS, lưu file audio, và có cơ chế thử lại riêng.

    Args:
        text: Văn bản cần chuyển đổi.
        voice_name: Tên giọng đọc.
        output_filename: Path object đến file audio đích.

    Returns:
        True nếu thành công.

    Raises:
        requests.exceptions.RequestException: Nếu lỗi mạng/HTTP xảy ra sau các lần thử lại.
        PollinationsError: Nếu lỗi logic Pollinations xảy ra sau các lần thử lại.
        TTSProviderError: Cho các lỗi không mong muốn khác trong quá trình gọi.
    """
    
    processed_text = text.replace("chết", "chít") # Ví dụ thay thế từ
    logger.debug(f"[Pollinations] Calling API for voice '{processed_text}' -> {output_filename} (Attempt info managed by tenacity)")
    logger.debug(f"[Pollinations] Calling API for vprocessed_text '{processed_text}' -> {output_filename} (Attempt info managed by tenacity)")

    encoded_text = urllib.parse.quote(processed_text)
    api_url = f"{POLLINATIONS_API_URL_BASE}{encoded_text}"
    params = {"model": "openai-audio", "voice": voice_name}

    if len(api_url) > POLLINATIONS_URL_CHAR_LIMIT:
        logger.warning(f"[Pollinations] Request URL ({len(api_url)} chars) might exceed limit ({POLLINATIONS_URL_CHAR_LIMIT}).")

    try:
        with requests.get(api_url, params=params, timeout=API_TIMEOUT_SECONDS, stream=True) as response:
            response.raise_for_status() # Vẫn check HTTP errors -> raise RequestException (HTTPError)
            content_type = response.headers.get('Content-Type', '')

            if 'audio/mpeg' not in content_type:
                error_msg = f"Expected 'audio/mpeg', got '{content_type}'"
                try:
                    error_details = response.text[:500]
                    error_msg += f". Response: {error_details}"
                except Exception: pass
                raise PollinationsError(error_msg) # Decorator sẽ bắt lỗi này

            # Ghi file audio
            with output_filename.open('wb') as f:
                bytes_written = 0
                for data in response.iter_content(chunk_size=1024*10):
                    if data:
                        f.write(data)
                        bytes_written += len(data)

            # Kiểm tra file sau khi ghi
            if output_filename.exists() and bytes_written > MIN_AUDIO_FILE_SIZE_BYTES:
                logger.debug(f"[Pollinations] Successfully saved audio: {output_filename} ({bytes_written} bytes)")
                return True # Thành công nếu không có exception
            else:
                file_size = bytes_written if output_filename.exists() else 0
                if output_filename.exists():
                    try:
                        output_filename.unlink() # Xóa file lỗi
                    except OSError as e_del:
                         logger.warning(f"[Pollinations] Could not delete empty/small file {output_filename}: {e_del}")
                raise PollinationsError(f"Saved audio file empty or too small ({file_size} bytes).") # Decorator sẽ bắt lỗi này

    except (requests.exceptions.RequestException, PollinationsError) as handled_exception:
         # Decorator với reraise=True sẽ tự động ném lại lỗi này nếu hết lần thử.
         # Không cần làm gì thêm ở đây, chỉ để lỗi được ném ra.
         raise handled_exception
    except Exception as e:
        # Bắt các lỗi không mong muốn khác không nằm trong diện retry
        logger.error(f"[Pollinations] Unexpected non-retryable error: {e}", exc_info=True)
        # Bọc lỗi này bằng TTSProviderError để rõ ràng hơn
        raise TTSProviderError(f"Unexpected error during Pollinations call: {e}") from e


def _call_openai_tts(text: str, voice_name: str, speed: float, output_filename: Path) -> bool:
    """
    Gọi API OpenAI TTS (hoặc local tương thích) và lưu file audio.
    Hàm này KHÔNG có cơ chế retry riêng, dựa vào retry của hàm gọi nó.

    Args:
        text: Văn bản cần chuyển đổi.
        voice_name: Tên giọng đọc (vd: 'alloy').
        speed: Tốc độ đọc (vd: 1.0).
        output_filename: Path object đến file audio đích.

    Returns:
        True nếu thành công.

    Raises:
        ConfigurationError: Nếu TTS client chưa được khởi tạo.
        openai.* Exceptions: Các lỗi từ thư viện OpenAI (APIConnectionError, RateLimitError, etc.).
        TTSProviderError: Cho các lỗi logic (file rỗng) hoặc lỗi không mong muốn khác.
    """
    logger.debug(f"[OpenAI/Local] Calling API for voice '{voice_name}', speed={speed:.2f} -> {output_filename}")
    if client_tts_other is None:
        raise ConfigurationError("OpenAI/Local TTS client is not initialized (check TTS_API_KEY).")

    try:
        # Sử dụng streaming response để ghi trực tiếp vào file
        with client_tts_other.audio.speech.with_streaming_response.create(
            model='tts-1', # Hoặc model khác nếu server local hỗ trợ
            voice=cast(OpenAIVoice, voice_name),
            input=text,
            speed=speed,
            response_format='mp3'
        ) as response:
            # Ghi vào file, stream_to_file xử lý việc mở/đóng file
            response.stream_to_file(str(output_filename))

        # Kiểm tra file sau khi ghi
        if output_filename.exists() and output_filename.stat().st_size > MIN_AUDIO_FILE_SIZE_BYTES:
            logger.debug(f"[OpenAI/Local] Successfully saved audio: {output_filename} ({output_filename.stat().st_size} bytes)")
            return True # Thành công
        else:
            file_size = output_filename.stat().st_size if output_filename.exists() else 0
            if output_filename.exists():
                 try:
                     output_filename.unlink() # Xóa file lỗi
                 except OSError as e_del:
                     logger.warning(f"[OpenAI/Local] Could not delete empty/small file {output_filename}: {e_del}")
            # Ném lỗi logic nếu file không hợp lệ
            raise TTSProviderError(f"Saved audio file empty or too small ({file_size} bytes).")

    except (openai.APIConnectionError, openai.RateLimitError, openai.APITimeoutError,
            openai.APIStatusError, openai.AuthenticationError) as openai_error:
        # Các lỗi này cần được ném ra để retry decorator chính (nếu có) hoặc hàm gọi xử lý
        logger.error(f"[OpenAI/Local] OpenAI API Error: {type(openai_error).__name__} - {openai_error}", exc_info=False) # Giảm độ dài log cho lỗi API
        raise openai_error # Ném lại lỗi gốc
    except Exception as e:
        # Bắt các lỗi không mong muốn khác
        logger.error(f"[OpenAI/Local] Unexpected error: {e}", exc_info=True)
        # Bọc lỗi không mong muốn bằng TTSProviderError
        raise TTSProviderError(f"Unexpected error during OpenAI/Local TTS call: {e}") from e


# --- Core Audio Generation Function ---

# Các Exceptions cần retry cho hàm chính:
# Lỗi mạng/API (OpenAI), Lỗi kết nối DB, Lỗi logic Pollinations (có thể tạm thời)
# Có thể thêm OSError nếu lỗi file system đáng retry.
RETRYABLE_EXCEPTIONS_MAIN = (
    requests.exceptions.RequestException, # Lỗi mạng chung (có thể từ Pollinations)
    openai.APIConnectionError,
    openai.RateLimitError,
    openai.APITimeoutError,
    openai.InternalServerError, # Lỗi 5xx từ OpenAI
    PollinationsError, # Retry cả tiến trình nếu Pollinations lỗi hẳn sau retry nội bộ
    ConnectionError,   # Lỗi kết nối chung (bao gồm DB)
    TimeoutError,      # Lỗi timeout chung
    pymongo.errors.NetworkTimeout, # Lỗi timeout kết nối DB
    pymongo.errors.ConnectionFailure # Lỗi mất kết nối DB
    # OSError, # Cân nhắc kỹ, có thể không nên retry lỗi file system
)

@tenacity.retry(
    stop=tenacity.stop_after_attempt(RETRY_ATTEMPTS), # Dùng hằng số retry chính
    wait=tenacity.wait_fixed(RETRY_WAIT_SECONDS),   # Dùng hằng số wait chính
    retry=tenacity.retry_if_exception_type(RETRYABLE_EXCEPTIONS_MAIN),
    reraise=True, # Ném lại exception cuối cùng nếu hết lần retry
    before_sleep=lambda retry_state: logger.warning(
        (
            f"Retrying create_audio_for_chunk (Attempt #{retry_state.attempt_number}) "
            f"due to {type(retry_state.outcome.exception()).__name__ if retry_state.outcome and retry_state.outcome.exception() else 'UnknownOutcome'}. " # <<< SỬA Ở ĐÂY
            f"Waiting {RETRY_WAIT_SECONDS}s..."
        )
    )
)
def create_audio_for_chunk(chunk_doc_id_str: str, script_name: str, voice_settings: Dict[str, Any]) -> Tuple[str, bool, Optional[str]]:
    """
    Tạo file audio cho một chunk văn bản từ DB, tự động chia nhỏ nếu cần.
    Lưu file vào đường dẫn LOCAL và cập nhật đường dẫn này vào DB.
    Hàm này có cơ chế retry cho các lỗi mạng, DB và API có thể phục hồi.

    Args:
        chunk_doc_id_str: ID (dạng string) của document chunk trong DB.
        script_name: Tên kịch bản (dùng để tạo thư mục).
        voice_settings: Dict chứa thông tin provider, voice_name, speaking_rate.

    Returns:
        Tuple: (chunk_doc_id_str, success_flag, local_audio_path_str or None)
               success_flag là True nếu audio được tạo thành công.

    Raises:
        Các exception được liệt kê trong RETRYABLE_EXCEPTIONS_MAIN (nếu retry thất bại).
        ConnectionError: Nếu không kết nối được DB ban đầu.
        FileNotFoundError: Nếu chunk không tồn tại trong DB ban đầu.
        ConfigurationError: Nếu thiếu cấu hình cần thiết (vd: TTS client).
        ValueError: Nếu đầu vào không hợp lệ (vd: thiếu voice_name, ID sai định dạng).
        RuntimeError: Nếu Pydub không được load và cần ghép file.
        Exception: Các lỗi không mong muốn khác không được retry.
    """
    script_chunks_coll = get_script_chunks_collection()
    if script_chunks_coll is None:
        # Lỗi này không nên retry, là lỗi cấu hình/kết nối ban đầu
        raise ConnectionError("Database connection error: ScriptChunks collection unavailable.")

    # --- Lấy dữ liệu Chunk từ DB ---
    try:
        chunk_doc_id = ObjectId(chunk_doc_id_str)
        # Thêm logic retry riêng cho việc đọc DB nếu cần, hoặc dựa vào retry chính
        chunk_doc = script_chunks_coll.find_one({"_id": chunk_doc_id})
    except pymongo.errors.PyMongoError as db_find_err:
        # Lỗi DB có thể retry bởi decorator chính
        raise ConnectionError(f"Database error finding chunk {chunk_doc_id_str}: {db_find_err}") from db_find_err
    except Exception as e_oid: # Lỗi khi convert ObjectId -> Lỗi đầu vào, không retry
        raise ValueError(f"Invalid chunk document ID format: {chunk_doc_id_str}") from e_oid

    if not chunk_doc:
        # Không tìm thấy chunk -> lỗi nghiêm trọng, không retry
        raise FileNotFoundError(f"Chunk document with ID {chunk_doc_id_str} not found in the database.")

    generation_id_obj = chunk_doc.get("generation_id")
    if not generation_id_obj:
        # Thiếu thông tin quan trọng -> lỗi dữ liệu, không retry
        raise ValueError(f"Chunk {chunk_doc_id_str} is missing the 'generation_id' field.")
    generation_id_str_for_path = str(generation_id_obj)

    text_content = chunk_doc.get("text_content", "").strip()
    section_index = chunk_doc.get("section_index", "unknown") # Để định danh file/log

    if not text_content:
        logging.warning(f"Chunk {chunk_doc_id_str} (Index: {section_index}) has no text content. Skipping TTS.")
        try:
            # Cập nhật trạng thái lỗi vào DB (không nên retry nếu lỗi này xảy ra)
            script_chunks_coll.update_one(
                {"_id": chunk_doc_id},
                {"$set": {"audio_created": False, "audio_error": "No text content"}}
            )
        except pymongo.errors.PyMongoError as db_err:
            logging.error(f"Database error updating empty chunk {chunk_doc_id_str}: {db_err}")
            # Không ném lại lỗi ở đây, chỉ log và trả về thất bại
        return chunk_doc_id_str, False, None # Trả về thất bại

    # --- Chuẩn bị đường dẫn và thông số ---
    script_folder_path = LOCAL_AUDIO_BASE_PATH / script_name
    try:
        script_folder_path.mkdir(parents=True, exist_ok=True) # Đảm bảo thư mục tồn tại
    except OSError as e_mkdir:
        logging.error(f"Failed to create script directory '{script_folder_path}': {e_mkdir}")
        # Lỗi OS có thể retry hoặc không tùy cấu hình RETRYABLE_EXCEPTIONS_MAIN
        raise # Ném lại lỗi

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    lang_code_suffix = voice_settings.get("language_code", "unk") # Thêm mã ngôn ngữ vào tên file
    # Đảm bảo tên file hợp lệ trên các HĐH
    safe_script_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in script_name)
    audio_file_name = f"{safe_script_name}_section_{section_index}_{timestamp}_{lang_code_suffix}.mp3"
    audio_file_path_local = script_folder_path / audio_file_name

    provider = voice_settings.get("provider", "openai").lower()
    voice_name = voice_settings.get("voice_name")
    speed = float(voice_settings.get("speaking_rate", 1.0))

    if not voice_name:
        # Lỗi cấu hình, không retry
        raise ValueError(f"Missing 'voice_name' in voice settings for chunk {chunk_doc_id_str}.")

    logging.info(f"Processing chunk {chunk_doc_id_str} (Idx:{section_index}, Prov:{provider}, Voice:{voice_name}) -> {audio_file_path_local}")

    temp_dir_obj: Optional[Path] = None # Để dọn dẹp
    final_success = False
    final_error_message = None

    try:
        # --- Logic chính: Chia nhỏ hoặc xử lý trực tiếp ---
        if len(text_content) > TTS_API_CHAR_LIMIT:
            # --- Xử lý Chunk dài (Chia nhỏ -> TTS từng phần -> Ghép) ---
            if not AudioSegment: # Kiểm tra Pydub trước khi bắt đầu ghép nối
                 raise RuntimeError("Pydub library not loaded. Cannot process long chunk requiring concatenation.")

            logging.warning(f"Chunk {chunk_doc_id_str} text ({len(text_content)} chars) > limit ({TTS_API_CHAR_LIMIT}). Splitting...")

            lang_code_for_split = voice_settings.get("language_code", "en-US")
            # Tìm tên ngôn ngữ tương ứng với language_code từ VOICE_CONFIG để dùng cho NLTK
            lang_name_for_split = next((k for k, v in VOICE_CONFIG.items() if k != "__DEFAULT__" and v.get("language_code") == lang_code_for_split), 'english').lower()

            sub_chunks_text = split_script_into_chunks(text_content, TTS_API_CHAR_LIMIT, language=lang_name_for_split)
            if not sub_chunks_text:
                 # Lỗi không chia được chunk -> Lỗi logic, không retry
                raise ValueError(f"Failed to split long text content for chunk {chunk_doc_id_str}.")

            logging.info(f"Split into {len(sub_chunks_text)} sub-chunks. Generating audio for each...")
            # Tạo thư mục tạm duy nhất cho generation ID và chunk index
            temp_dir = tempfile.mkdtemp(prefix=f"tts_{generation_id_str_for_path}_chunk{section_index}_")
            temp_dir_obj = Path(temp_dir)
            temp_audio_files: List[Path] = []
            all_sub_chunks_ok = True

            for sub_idx, sub_text in enumerate(sub_chunks_text):
                sub_text = sub_text.strip()
                print(sub_text)
                if not sub_text: continue

                temp_filename = temp_dir_obj / f"sub_{section_index}_{sub_idx}.mp3"
                logging.debug(f"Generating sub-chunk {sub_idx+1}/{len(sub_chunks_text)} (Prov:{provider}) -> {temp_filename}")

                try:
                    # Gọi hàm helper tương ứng
                    if provider == "pollinations":
                        # Hàm này có retry riêng
                        _call_pollinations_tts(sub_text, voice_name, temp_filename)
                    elif provider in ["openai", "local_tts"]:
                        # Hàm này không có retry riêng, lỗi sẽ được retry bởi decorator chính
                        _call_openai_tts(sub_text, voice_name, speed, temp_filename)
                    # --- Thêm các provider khác ở đây ---
                    # elif provider == "google":
                    #    _call_google_tts(...)
                    else:
                        # Provider không hỗ trợ -> Lỗi cấu hình, không retry
                        raise ConfigurationError(f"Unsupported TTS provider specified: '{provider}'")

                    # Nếu không có exception tức là thành công
                    temp_audio_files.append(temp_filename)
                    logging.debug(f"Sub-chunk {sub_idx+1} generated successfully.")

                except Exception as sub_e:
                    # Bắt lỗi từ các hàm _call_... sau khi chúng đã retry (nếu có)
                    all_sub_chunks_ok = False
                    # Giữ lại lỗi đầu tiên gặp phải
                    if not final_error_message:
                        final_error_message = f"Failed generating sub-chunk {sub_idx+1} ({type(sub_e).__name__}): {str(sub_e)[:200]}"
                    logging.error(f"Error on sub-chunk {sub_idx+1}: {final_error_message}", exc_info=False) # Không cần traceback đầy đủ ở đây
                    break # Dừng xử lý các sub-chunk còn lại nếu có lỗi

            # --- Kết thúc vòng lặp sub-chunk ---
            if all_sub_chunks_ok and temp_audio_files:
                logging.info(f"All {len(temp_audio_files)} sub-chunks generated. Concatenating...")
                try:
                    # Gọi hàm ghép file
                    if concatenate_audio(temp_audio_files, audio_file_path_local):
                        final_success = True
                        logging.info(f"Successfully concatenated sub-chunks to {audio_file_path_local}")
                    else:
                        # Lỗi logic trong hàm ghép file
                        final_error_message = final_error_message or "Audio concatenation function reported failure."
                        logging.error(final_error_message)
                except Exception as concat_e:
                    final_error_message = f"Error during audio concatenation: {concat_e}"
                    logging.error(final_error_message, exc_info=True)
            elif not all_sub_chunks_ok:
                # Lỗi đã được ghi log và lưu vào final_error_message bên trong vòng lặp
                 pass
            else: # all_sub_chunks_ok is True but temp_audio_files is empty (do tất cả sub-text rỗng?)
                final_error_message = "Sub-chunk processing resulted in no audio files (check if sub-texts were empty)."
                logging.error(final_error_message)

        else:
            # --- Xử lý Chunk ngắn (Gọi TTS trực tiếp) ---
            logging.debug(f"Chunk {chunk_doc_id_str} text length OK. Calling TTS directly...")
            logging.debug(f"Text {text_content} text length OK. Calling TTS directly...")
            try:
                if provider == "pollinations":
                    print(text_content)
                    final_success = _call_pollinations_tts(text_content, voice_name, audio_file_path_local)
                elif provider in ["openai", "local_tts"]:
                    final_success = _call_openai_tts(text_content, voice_name, speed, audio_file_path_local)
                # --- Thêm các provider khác ---
                # elif provider == "google":
                #    final_success = _call_google_tts(...)
                else:
                     # Provider không hỗ trợ -> Lỗi cấu hình, không retry
                    raise ConfigurationError(f"Unsupported TTS provider specified: '{provider}'")

                if not final_success and not final_error_message:
                     # Trường hợp hàm helper trả về False (không nên xảy ra nếu dùng exception)
                     final_error_message = f"Direct TTS call for provider '{provider}' failed unexpectedly."
                     logging.error(final_error_message)

            except Exception as direct_tts_e:
                 # Bắt lỗi trực tiếp từ các hàm _call_...
                 final_error_message = f"Direct TTS call failed ({type(direct_tts_e).__name__}): {str(direct_tts_e)[:200]}"
                 logging.error(final_error_message, exc_info=False)
                 # Lỗi này sẽ được decorator chính retry nếu nằm trong danh sách

        # --- Cập nhật DB (Thành công hoặc Thất bại) ---
        # Khối này nằm ngoài try...except chính của TTS để đảm bảo luôn được thực thi
        # trước khi trả về hoặc raise lỗi cuối cùng (nếu retry thất bại)
        update_data = {}
        db_update_successful = False
        if final_success:
            logging.info(f"Successfully generated audio for chunk {chunk_doc_id_str} at {audio_file_path_local}")
            update_data = {
                "audio_file_path": str(audio_file_path_local), # Lưu string path vào DB
                "audio_created": True,
                "audio_error": None
            }
        else:
            final_error_message = final_error_message or "Unknown error during audio generation."
            logging.error(f"Failed to generate audio for chunk {chunk_doc_id_str}: {final_error_message}")
            update_data = {
                "audio_created": False,
                "audio_error": str(final_error_message)[:500] # Giới hạn độ dài lỗi lưu vào DB
            }
            # Nếu lỗi xảy ra, đảm bảo file đích (nếu có) bị xóa
            if audio_file_path_local.exists():
                try:
                    audio_file_path_local.unlink()
                    logging.debug(f"Removed incomplete/failed output file: {audio_file_path_local}")
                except OSError as e_del:
                    logging.warning(f"Could not remove failed output file {audio_file_path_local}: {e_del}")

        try:
            logging.debug(f"Updating DB for chunk {chunk_doc_id_str} with status: audio_created={final_success}")
            # Có thể thêm retry cho việc update DB ở đây nếu muốn
            update_result = script_chunks_coll.update_one(
                {"_id": chunk_doc_id},
                {"$set": update_data}
            )
            if update_result.matched_count == 0:
                logging.warning(f"DB update failed: Chunk {chunk_doc_id_str} not found during final update.")
            elif update_result.modified_count == 0 and update_result.matched_count > 0:
                 logging.debug(f"DB status for chunk {chunk_doc_id_str} was already up-to-date.")
            else: # matched_count > 0 and modified_count > 0
                 db_update_successful = True
                 logging.debug(f"DB status updated successfully for chunk {chunk_doc_id_str}.")


        except pymongo.errors.PyMongoError as db_upd_err:
            # Log lỗi nghiêm trọng nếu không thể cập nhật trạng thái DB
            # Lỗi này có thể được retry bởi decorator chính nếu nằm trong danh sách
            logging.error(f"CRITICAL: Failed to update final DB status for chunk {chunk_doc_id_str}: {db_upd_err}", exc_info=True)
            # Ném lại lỗi để decorator chính xử lý
            raise ConnectionError("Failed to update DB status") from db_upd_err


        # Trả về kết quả cuối cùng chỉ sau khi đã cố gắng cập nhật DB
        return chunk_doc_id_str, final_success, str(audio_file_path_local) if final_success else None

    # Block except Exception as main_e này có thể không cần thiết nữa
    # vì các lỗi cụ thể hơn đã được xử lý và decorator retry sẽ ném lại lỗi cuối cùng.
    # Giữ lại để bắt những lỗi hoàn toàn không lường trước.
    except Exception as main_e:
        final_error_msg = f"Unhandled exception in create_audio_for_chunk {chunk_doc_id_str}: {type(main_e).__name__} - {str(main_e)[:250]}"
        logging.critical(final_error_msg, exc_info=True) # Lỗi nghiêm trọng
        # Cố gắng cập nhật lỗi vào DB lần cuối
        try:
            script_chunks_coll.update_one(
                {"_id": chunk_doc_id},
                {"$set": {"audio_created": False, "audio_error": final_error_msg}}
            )
        except Exception as db_final_err:
             logging.error(f"Failed to update DB with final unhandled error for chunk {chunk_doc_id_str}: {db_final_err}")
        # Ném lại lỗi gốc để dừng tiến trình hoặc cho tầng gọi cao hơn biết
        raise main_e

    finally:
        # --- Dọn dẹp thư mục tạm ---
        if temp_dir_obj and temp_dir_obj.is_dir():
            try:
                shutil.rmtree(temp_dir_obj)
                logging.debug(f"Successfully removed temporary directory: {temp_dir_obj}")
            except Exception as e_clean:
                # Ghi log cảnh báo nhưng không nên làm dừng chương trình
                logging.warning(f"Could not remove temporary directory {temp_dir_obj}: {e_clean}")


# --- Audio Concatenation ---

def concatenate_audio(audio_file_paths: List[Path], output_file_path: Path) -> bool:
    """
    Nối các file audio MP3 từ danh sách đường dẫn Path object.

    Args:
        audio_file_paths: List các đối tượng Path trỏ đến file audio nguồn.
        output_file_path: Đối tượng Path cho file audio đích.

    Returns:
        True nếu ghép nối thành công, False nếu thất bại.

    Raises:
        RuntimeError: Nếu Pydub không khả dụng.
        Exception: Các lỗi không mong muốn khác trong quá trình xử lý file.
    """
    if not AudioSegment:
        # Lỗi này nên được raise thay vì chỉ log và trả về False
        raise RuntimeError("Pydub library not loaded or failed to initialize. Cannot concatenate audio.")
    if not audio_file_paths:
        logging.warning("No audio file paths provided for concatenation.")
        return False

    combined = AudioSegment.empty()
    loaded_count = 0
    logging.info(f"Attempting to combine {len(audio_file_paths)} audio segments into {output_file_path}...")

    valid_paths: List[Path] = []
    for p in audio_file_paths:
        try:
            # Kiểm tra file tồn tại và có kích thước hợp lệ
            if p.is_file() and p.stat().st_size > MIN_AUDIO_FILE_SIZE_BYTES:
                valid_paths.append(p)
            else:
                size_info = f"Size: {p.stat().st_size}" if p.exists() else "Does not exist"
                logging.warning(f"Skipping invalid/empty/small file: {p} ({size_info})")
        except Exception as e_stat:
             logging.warning(f"Error checking file {p}: {e_stat}. Skipping.")

    if not valid_paths:
        logging.error("No valid audio segments found to combine after checking.")
        return False # Không có gì để ghép

    error_occurred_loading = False
    for i, path in enumerate(valid_paths):
        try:
            # Chỉ định format để chắc chắn
            sound = AudioSegment.from_file(str(path), format="mp3")
            combined += sound
            loaded_count += 1
            logging.debug(f"Appended segment {i+1}/{len(valid_paths)}: {path.name} ({len(sound)/1000.0:.2f}s)")
        except Exception as e_load:
            # Ghi log lỗi chi tiết và đánh dấu có lỗi xảy ra
            logging.error(f"Error loading audio segment from {path}: {e_load}. Skipping this segment.", exc_info=True)
            error_occurred_loading = True
            # Không dừng hẳn, cố gắng ghép những file còn lại

    if loaded_count > 0:
        try:
            logging.info(f"Successfully loaded {loaded_count} valid audio segments. Total duration: {len(combined)/1000.0:.2f}s. Exporting...")
            # Đảm bảo thư mục đích tồn tại
            output_file_path.parent.mkdir(parents=True, exist_ok=True)
            # Xuất file MP3, chỉ định codec để đảm bảo chất lượng/tương thích
            combined.export(str(output_file_path), format="mp3", codec='libmp3lame') # Hoặc codec khác nếu cần
            logging.info(f"Successfully exported combined audio to: {output_file_path}")
            # Trả về True ngay cả khi có lỗi load file trước đó, vì đã tạo ra file output
            if error_occurred_loading:
                 logging.warning(f"Combined audio generated at {output_file_path}, but some segments were skipped due to loading errors.")
            return True
        except Exception as e_export:
            # Lỗi nghiêm trọng khi export file cuối cùng
            logging.error(f"Failed to export combined audio to {output_file_path}: {e_export}", exc_info=True)
            # Ném lại lỗi để hàm gọi biết việc export thất bại
            raise RuntimeError(f"Failed to export combined audio: {e_export}") from e_export
    else:
        # Không load được file nào hợp lệ
        logging.error("No audio segments were successfully loaded for concatenation.")
        return False


# --- Combine from DB Function ---

def combine_audio_from_db(generation_id_str: str, script_name: str) -> Optional[str]:
    """
    Lấy đường dẫn file audio LOCAL từ DB, ghép chúng lại, lưu file tổng hợp LOCAL,
    và trả về đường dẫn LOCAL (dạng string) của file tổng hợp.

    Args:
        generation_id_str: ID (dạng string) của generation.
        script_name: Tên kịch bản (đảm bảo an toàn cho tên file/thư mục).

    Returns:
        Đường dẫn string đến file audio tổng hợp LOCAL nếu thành công, None nếu thất bại.
    """
    script_chunks_coll = get_script_chunks_collection()
    if script_chunks_coll is None:
        logging.error("Combine from DB: Cannot get script chunks collection.")
        return None

    try:
        generation_id = ObjectId(generation_id_str)
    except Exception:
        logging.error(f"Combine from DB: Invalid generation ID format: {generation_id_str}")
        return None

    # Tạo tên file/thư mục an toàn
    safe_script_name = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in script_name)
    script_folder_path = LOCAL_AUDIO_BASE_PATH / safe_script_name
    output_audio_file_local = script_folder_path / f"{safe_script_name}_combined_{generation_id_str}.mp3"

    try:
        # Lấy các document chunk đã tạo audio thành công, sắp xếp theo section_index
        cursor = script_chunks_coll.find(
            {
                "generation_id": generation_id,
                "audio_created": True,
                "audio_error": None,
                "audio_file_path": {"$ne": None, "$exists": True, "$type": "string"} # Đảm bảo path tồn tại, không null và là string
            },
            {"audio_file_path": 1, "section_index": 1} # Chỉ lấy các trường cần thiết
        ).sort("section_index", pymongo.ASCENDING) # Sắp xếp tăng dần

        # Lấy danh sách đường dẫn Path object
        audio_paths_local: List[Path] = []
        for doc in cursor:
            path_str = doc.get('audio_file_path')
            if path_str: # Đã kiểm tra $type: "string" trong query
                path_obj = Path(path_str)
                # Kiểm tra lại xem file thực sự tồn tại trên hệ thống file không
                if path_obj.is_file():
                    audio_paths_local.append(path_obj)
                else:
                    logging.warning(f"Combine from DB: Audio file path found in DB but file does not exist: {path_str} (for gen {generation_id_str}, section {doc.get('section_index')})")
            else:
                 logging.warning(f"Combine from DB: Null or missing audio_file_path in doc for gen {generation_id_str}, section {doc.get('section_index')}")

        if not audio_paths_local:
            logging.info(f"Combine from DB: No valid & existing audio chunk file paths found for generation {generation_id_str} to combine.")
            return None

        logging.info(f"Combine from DB: Found {len(audio_paths_local)} existing chunk files to combine for generation {generation_id_str}.")

        # Gọi hàm ghép file
        if concatenate_audio(audio_paths_local, output_audio_file_local):
            return str(output_audio_file_local) # Trả về string path nếu thành công
        else:
            # Lỗi xảy ra trong quá trình ghép (đã được log bên trong concatenate_audio)
            logging.error(f"Combine from DB: Concatenation reported failure for generation {generation_id_str}.")
            return None

    except pymongo.errors.PyMongoError as db_err:
        logging.error(f"Combine from DB: Database error fetching chunks for gen {generation_id_str}: {db_err}", exc_info=True)
        return None
    except RuntimeError as pydub_err: # Bắt lỗi Pydub từ concatenate_audio
        logging.error(f"Combine from DB: Pydub/Runtime error during concatenation for gen {generation_id_str}: {pydub_err}", exc_info=True)
        return None
    except Exception as e:
        # Bắt các lỗi không mong muốn khác
        logging.error(f"Combine from DB: Unexpected error during combination for gen {generation_id_str}: {e}", exc_info=True)
        return None


# --- Main Execution Guard (Optional Example) ---
if __name__ == "__main__":
    # Cấu hình logging để xem output khi chạy trực tiếp
    logging.basicConfig(
        level=logging.DEBUG, # Đặt DEBUG để xem log chi tiết
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()] # Xuất ra console
    )
    logger.info("tts_utils.py executed directly (for testing).")

    # --- Test Cases ---
    # Lưu ý: Các test case này cần dữ liệu giả hoặc kết nối thực tế để chạy.

    # 1. Test get voice settings
    print("\n--- Testing get_voice_settings ---")
    test_langs = ["Vietnamese", "English", "Japanese", "UnknownLang"]
    if VOICE_CONFIG:
        for lang in test_langs:
            settings = get_voice_settings(lang, VOICE_CONFIG)
            print(f"Settings for '{lang}': {settings}")
    else:
        print("Skipping voice settings test - VOICE_CONFIG not loaded.")

    # 2. Test concatenate_audio (cần tạo file audio giả)
    print("\n--- Testing concatenate_audio ---")
    if AudioSegment:
        test_concat_dir = Path("./temp_concat_test")
        test_concat_dir.mkdir(exist_ok=True)
        test_files: List[Path] = []
        try:
            # Tạo file mp3 giả nhỏ (im lặng)
            silence = AudioSegment.silent(duration=500) # 0.5 giây im lặng
            for i in range(3):
                f_path = test_concat_dir / f"test_segment_{i}.mp3"
                silence.export(str(f_path), format="mp3")
                # Tạo file lỗi (0 byte)
                if i == 1:
                     f_path.write_bytes(b'')
                if f_path.exists(): # Chỉ thêm nếu file được tạo
                    test_files.append(f_path)

            output_concat_file = test_concat_dir / "combined_test.mp3"
            success = concatenate_audio(test_files, output_concat_file)
            print(f"Concatenation successful: {success}")
            if success and output_concat_file.exists():
                print(f"Output file created: {output_concat_file} (Size: {output_concat_file.stat().st_size})")
            else:
                print("Concatenation failed or output file not created.")

        except Exception as e_concat_test:
            print(f"Error during concatenation test: {e_concat_test}")
        finally:
             # Dọn dẹp file test
             if test_concat_dir.exists():
                 shutil.rmtree(test_concat_dir)
                 print(f"Cleaned up temp directory: {test_concat_dir}")
    else:
        print("Skipping concatenation test - Pydub not available.")


    # 3. Test create_audio_for_chunk (Rất phức tạp - cần mock DB, API, ...)
    # print("\n--- Testing create_audio_for_chunk (Mocked) ---")
    # print("Requires extensive mocking of DB, APIs, and file system.")

    # 4. Test combine_audio_from_db (Cần mock DB và file audio đã tồn tại)
    # print("\n--- Testing combine_audio_from_db (Mocked) ---")
    # print("Requires mocking DB and existing audio files.")