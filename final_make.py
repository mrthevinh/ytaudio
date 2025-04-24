#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script tự động tạo video từ MongoDB - Phiên bản tự động nhận diện HĐH.

- Tự động dịch đường dẫn Windows/Linux lấy từ DB cho phù hợp với HĐH đang chạy.
- Sử dụng CPU encoder (libx264) làm mặc định cho ổn định.
- Có thể chọn NVENC encoder qua file .env nếu chạy trên Windows có GPU Nvidia.

*** Yêu cầu: ***
- Python 3.
- FFmpeg đã được cài đặt và có trong PATH.
  (Nếu dùng NVENC, FFmpeg cần hỗ trợ --enable-nvenc, --enable-cuda...).
- Driver phù hợp (Nếu dùng NVENC, cần driver Nvidia mới nhất).
- Đã cài đặt thư viện: pip install ffmpeg-python python-dotenv pymongo
- File cấu hình `.env` nằm cùng thư mục với script.
- File `db_handler.py` (đã cung cấp) nằm cùng thư mục với script.
"""

# Standard library imports
import concurrent.futures
import datetime
import json
import math
import os
import platform  # Để nhận diện HĐH
import random
import re  # Để xử lý path mapping
import subprocess
import time
import traceback

# Third-party libraries
try:
    import ffmpeg
except ImportError:
    print("!!! Lỗi: Thư viện ffmpeg-python chưa được cài đặt.")
    print("Chạy lệnh: pip install ffmpeg-python")
    exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("!!! Lỗi: Thư viện python-dotenv chưa được cài đặt.")
    print("Chạy lệnh: pip install python-dotenv")
    exit(1)
try:
    from pymongo import MongoClient, ReturnDocument  # type: ignore
    from pymongo.errors import ConnectionFailure, OperationFailure  # type: ignore
    from bson import ObjectId  # type: ignore
except ImportError:
    print("!!! Lỗi: Thư viện pymongo chưa được cài đặt. Chạy: pip install pymongo")
    exit(1)

# Local application/library specific imports
try:
    import db_handler  # File db_handler.py phải nằm cùng thư mục
except ImportError:
    print("!!! Lỗi: Không tìm thấy file db_handler.py cùng thư mục.")
    exit(1)


# --- Tải biến môi trường ---
print("Đang tải cấu hình từ file .env...")
if load_dotenv(override=True):  # Thêm verbose để xem file .env nào được load
    print("-> Đã tải thành công file .env.")
else:
    print("Cảnh báo: Không tìm thấy file .env. Sử dụng giá trị mặc định nếu có.")


# ==============================================================================
# SECTION 1: CONFIG, PATH TRANSLATION & UTILITIES
# ==============================================================================


class ConfigError(Exception):
    """Lỗi tùy chỉnh cho các vấn đề về cấu hình ứng dụng."""

    pass


def get_env_var(var_name, default=None, required=False, var_type=str):
    """
    Lấy biến môi trường một cách an toàn, chuyển đổi kiểu dữ liệu và
    xử lý trường hợp thiếu hoặc giá trị không hợp lệ.

    Args:
        var_name (str): Tên biến môi trường (key trong file .env).
        default (any, optional): Giá trị trả về nếu biến không có hoặc rỗng
                                 và không phải là bắt buộc.
        required (bool, optional): Nếu True, sẽ raise ConfigError nếu biến
                                   thiếu hoặc rỗng.
        var_type (type, optional): Kiểu dữ liệu mong muốn (str, int, float, bool).

    Raises:
        ConfigError: Nếu biến bắt buộc thiếu/rỗng hoặc giá trị không hợp lệ.

    Returns:
        any: Giá trị cấu hình đã được chuyển đổi kiểu hoặc giá trị default.
    """
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        if required:
            raise ConfigError(
                f"Cấu hình bắt buộc '{var_name}' bị thiếu hoặc rỗng trong .env!"
            )
        return default

    value = value.strip().strip("'\"")
    if var_type == str:
        return value
    try:
        if var_type == bool:
            return value.lower() in ["true", "1", "t", "y", "yes"]
        # Xử lý int/float khi value là None (sau khi getenv)
        if value is None and var_type in [int, float]:
            if required:
                raise ValueError()  # Gây ValueError để bắt ở dưới
            else:
                return default
        return var_type(value)
    except ValueError:
        raise ConfigError(
            f"Giá trị '{value}' của '{var_name}' không hợp lệ cho kiểu {var_type.__name__}!"
        )
    except Exception as e:
        raise ConfigError(f"Lỗi khi xử lý '{var_name}': {e}") from e


def load_app_config():
    """
    Đọc tất cả cấu hình từ .env, bao gồm cả hai loại path mapping.
    Tính toán các giá trị phụ thuộc.
    """
    print("\n--- Đọc cấu hình ứng dụng (Auto OS - CPU/NVENC) ---")
    config = {}
    try:
        # --- Đọc Đường dẫn & MongoDB ---
        print("-> Đọc đường dẫn và MongoDB...")
        config["mongo_uri"] = get_env_var("MONGO_URI", required=True)
        config["mongo_db_name"] = get_env_var("MONGO_DB_NAME", required=True)
        config["mongo_collection"] = get_env_var("MONGO_COLLECTION_NAME", required=True)
        config["default_main_audio"] = get_env_var("DEFAULT_MAIN_AUDIO_FILE")
        config["default_overlay_image"] = get_env_var("DEFAULT_OVERLAY_IMAGE_FILE")
        config["default_text_image"] = get_env_var("DEFAULT_TEXT_IMAGE_FILE")
        config["bg_music_file"] = get_env_var("BG_MUSIC_FILE", required=True)
        config["overlay_video_file"] = get_env_var("OVERLAY_VIDEO_FILE", required=True)
        config["video_folder"] = get_env_var("VIDEO_FOLDER", required=True)
        config["output_dir"] = get_env_var("OUTPUT_DIR", required=True)

        # --- Đọc Tham số Chung & FFmpeg ---
        print("-> Đọc tham số chung và FFmpeg...")
        config["output_filename_pattern"] = get_env_var(
            "OUTPUT_FILENAME_PATTERN", "video_auto_{id}_{timestamp}.mp4"
        )
        config["overlay_opacity"] = get_env_var(
            "OVERLAY_VIDEO_OPACITY", 0.25, var_type=float
        )
        config["margin_right"] = get_env_var("OVERLAY_RIGHT_MARGIN", 15, var_type=int)
        config["margin_left"] = get_env_var("OVERLAY_LEFT_MARGIN", 15, var_type=int)
        config["bg_music_volume"] = get_env_var("BG_MUSIC_VOLUME", 0.3, var_type=float)
        default_workers = os.cpu_count() or 8
        config["max_workers"] = get_env_var(
            "MAX_PROBE_WORKERS", default_workers, var_type=int
        )
        config["resolution"] = get_env_var("TARGET_RESOLUTION", "1920x1080")
        config["framerate"] = get_env_var("VIDEO_FRAMERATE", 30, var_type=int)
        config["audio_bitrate"] = get_env_var("OUTPUT_AUDIO_BITRATE", "192k")

        # --- Đọc Cấu hình Encoder ---
        config["encoder_choice"] = get_env_var("ENCODER_CHOICE", "libx264").lower()
        # CPU Options
        config["cpu_preset"] = get_env_var("CPU_ENCODER_PRESET", "veryfast")
        config["cpu_crf"] = get_env_var("CPU_ENCODER_CRF", None)  # None nếu không đặt
        config["cpu_bitrate"] = get_env_var(
            "CPU_ENCODER_BITRATE", "6000k"
        )  # Default bitrate CPU
        # NVENC Options
        config["hw_nvenc_preset"] = get_env_var("HW_NVENC_PRESET", "p5")
        config["hw_nvenc_rc"] = get_env_var("HW_NVENC_RC", "constqp")
        config["hw_nvenc_qp"] = get_env_var("HW_NVENC_QP", "23")  # Đọc string
        config["hw_nvenc_bitrate"] = get_env_var("HW_NVENC_BITRATE", "8000k")
        config["cuda_device_id"] = get_env_var("CUDA_DEVICE_ID", 0, var_type=int)
        print(f"-> Encoder được chọn: {config['encoder_choice']}")

        # --- Đọc Tham số Waveform ---
        print("-> Đọc tham số waveform...")
        config["waveform_h"] = get_env_var("WAVEFORM_HEIGHT", 120, var_type=int)
        config["waveform_color"] = get_env_var("WAVEFORM_COLOR", "white")
        config["waveform_margin_bottom"] = get_env_var(
            "WAVEFORM_BOTTOM_MARGIN", 50, var_type=int
        )
        config["waveform_mode"] = get_env_var("WAVEFORM_MODE", "bar")
        config["waveform_ascale"] = get_env_var("WAVEFORM_ASCALE", "log")
        config["waveform_fscale"] = get_env_var("WAVEFORM_FSCALE", "log")
        config["waveform_win_size"] = get_env_var(
            "WAVEFORM_WIN_SIZE", 2048, var_type=int
        )
        config["waveform_win_func"] = get_env_var("WAVEFORM_WIN_FUNC", "hann")

        # --- Xử lý Path Mappings ---
        print("-> Đọc Path Mappings từ .env...")
        config["win_to_linux_mappings"] = {}
        config["linux_to_win_mappings"] = {}
        linux_prefixes_temp = {}
        win_targets_temp = {}
        win_map_count = 0
        linux_map_count = 0

        # Duyệt qua biến môi trường để tìm mapping
        for key, value in os.environ.items():
            value_clean = value.strip().strip("'\"")
            if not value_clean:
                continue

            # Win -> Linux (PATH_MAP_X=...)
            if key.startswith("PATH_MAP_"):
                drive_key = key.replace("PATH_MAP_", "").upper()
                if drive_key and len(drive_key) == 1 and drive_key.isalpha():
                    win_prefix = drive_key + ":"
                    linux_target = value_clean.replace("\\", "/").rstrip("/")
                    if linux_target:
                        config["win_to_linux_mappings"][win_prefix] = linux_target
                        win_map_count += 1
                        print(f"DEBUG: Đã thêm mapping Win->Linux: {win_prefix} -> {linux_target}") # Thêm dòng này

            # Linux -> Win (MAP_LINUX_PREFIX_n + MAP_LINUX_TARGET_n)
            elif key.startswith("MAP_LINUX_PREFIX_"):
                index = key.replace("MAP_LINUX_PREFIX_", "")
                linux_prefixes_temp[index] = value_clean.replace("\\", "/").rstrip("/")
            elif key.startswith("MAP_LINUX_TARGET_"):
                index = key.replace("MAP_LINUX_TARGET_", "")
                win_targets_temp[index] = value_clean

        # Ghép cặp mapping Linux -> Win
        for index, linux_prefix in linux_prefixes_temp.items():
            if index in win_targets_temp:
                win_target = win_targets_temp[index]
                if len(win_target) == 1 and win_target.isalpha():
                    win_target += ":"
                elif not win_target.startswith("\\\\"):
                    win_target = win_target.rstrip("\\/")
                if linux_prefix and win_target:
                    config["linux_to_win_mappings"][linux_prefix] = win_target
                    linux_map_count += 1
            elif linux_prefix:
                print(
                    f"Cảnh báo: Thiếu MAP_LINUX_TARGET_{index} cho prefix '{linux_prefix}'"
                )

        print(f"-> Win->Linux Mappings: {len(config['win_to_linux_mappings'])} rules")
        print(f"-> Linux->Win Mappings: {len(config['linux_to_win_mappings'])} rules")
        print("DEBUG: Nội dung config['win_to_linux_mappings']:", config.get('win_to_linux_mappings'))

        # --- Tính toán Kích thước Phụ thuộc ---
        print("-> Tính toán kích thước phụ thuộc...")
        try:
            res = config["resolution"]
            width_str, height_str = res.split("x")
            target_w, target_h = int(width_str), int(height_str)
            if target_w <= 0 or target_h <= 0:
                raise ValueError("Resolution > 0")
            config["target_width"] = target_w
            config["target_height"] = target_h
            config["overlay_char_width"] = int(target_w * 0.4)
            config["overlay_text_width"] = int(target_w * 0.6)
            config["waveform_w"] = int(target_w * 0.5)
            print(
                f"  - Overlay W: NV={config['overlay_char_width']}px, Chữ={config['overlay_text_width']}px"
            )
            print(f"  - Waveform W: {config['waveform_w']}px")
        except Exception as e:
            raise ConfigError(f"TARGET_RESOLUTION ('{res}') không hợp lệ: {e}")

        # --- File tạm/Cache ---
        print("-> Xử lý đường dẫn tạm và cache...")
        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir_val = config.get("output_dir", ".")
        default_temp_dir = os.path.join(output_dir_val, "temp_video_files_auto")
        config["temp_dir"] = get_env_var("TEMP_DIR", default_temp_dir)
        config["cache_file"] = get_env_var(
            "CACHE_FILE_PATH", os.path.join(base_dir, "video_metadata_cache.json")
        )
        config["concat_dir"] = config["temp_dir"]  # Đặt concat_dir vào temp_dir
        print(f"  - Thư mục tạm/concat: {config['temp_dir']}")
        print(f"  - File cache: {config['cache_file']}")

        # --- Hoàn thành ---
        print("-> Đã đọc xong toàn bộ cấu hình.")
        return config

    except ConfigError as e:
        print(f"\n!!! Lỗi Đọc Cấu Hình !!!\nLỗi: {e}\nVui lòng kiểm tra file .env.")
        exit(1)
    except Exception as e:
        print(f"\n!!! Lỗi không xác định khi đọc cấu hình !!!\nLỗi: {e}")
        traceback.print_exc()
        exit(1)


def translate_path(path_str, config):
    """
    Tự động dịch đường dẫn cho phù hợp với HĐH đang chạy dựa vào config.

    Args:
        path_str (str): Đường dẫn gốc (từ DB hoặc config).
        config (dict): Dictionary cấu hình chứa mapping dictionaries.

    Returns:
        str: Đường dẫn đã được dịch (hoặc gốc nếu không cần/không có mapping).
    """
    if not path_str or not isinstance(path_str, str):
        return path_str

    current_os = platform.system()
    path_norm = path_str.replace("\\", "/")  # Luôn chuẩn hóa slash trước

    translated_path = path_norm  # Mặc định là path đã chuẩn hóa

    try:
        # Nếu chạy trên Linux, thử dịch path Windows
        if current_os == "Linux":
            mappings = config.get("win_to_linux_mappings", {})
            if mappings:
                sorted_win_prefixes = sorted(mappings.keys(), key=len, reverse=True)
                for win_prefix in sorted_win_prefixes:
                    if re.match(f"^{re.escape(win_prefix)}/", path_norm, re.IGNORECASE):
                        linux_prefix = mappings[win_prefix]
                        rest_of_path = path_norm[len(win_prefix) :].lstrip("/")
                        translated_path = os.path.join(linux_prefix, rest_of_path)
                        # print(f"DEBUG Translate Win->Linux: '{path_str}' -> '{translated_path}'")
                        break  # Dừng sau khi tìm thấy mapping đầu tiên (dài nhất)

        # Nếu chạy trên Windows, thử dịch path Linux
        elif current_os == "Windows":
            mappings = config.get("linux_to_win_mappings", {})
            if mappings:
                # Kiểm tra xem có phải là đường dẫn Windows hợp lệ rồi không
                if (
                    re.match(r"^[a-zA-Z]:[/\\]", path_norm)
                    or path_norm.startswith("//")
                    or path_norm.startswith("\\\\")
                ):
                    translated_path = os.path.normpath(
                        path_str
                    )  # Chuẩn hóa lại nếu cần
                else:  # Nếu không phải path Win, thử dịch từ Linux
                    sorted_linux_prefixes = sorted(
                        mappings.keys(), key=len, reverse=True
                    )
                    for linux_prefix in sorted_linux_prefixes:
                        if path_norm.startswith(linux_prefix):
                            win_prefix = mappings[
                                linux_prefix
                            ]  # Z: hoặc \\server\share
                            rest_of_path = path_norm[len(linux_prefix) :].lstrip("/")
                            # Ghép lại cho Windows dùng os.path.join
                            if len(win_prefix) == 2 and win_prefix.endswith(":"):
                                win_prefix_for_join = win_prefix + "\\"
                            else:
                                win_prefix_for_join = win_prefix
                            translated_path = os.path.join(
                                win_prefix_for_join, rest_of_path
                            )
                            # print(f"DEBUG Translate Linux->Win: '{path_str}' -> '{translated_path}'")
                            break  # Dừng sau khi tìm thấy mapping

        # Cảnh báo nếu không có thay đổi và path không tuyệt đối theo HĐH hiện tại
        # (Hơi phức tạp, tạm thời bỏ qua để tránh log thừa)
        # if translated_path == path_norm and not os.path.isabs(translated_path):
        #      print(f"CB: Path '{path_str}' không được dịch và không tuyệt đối.")

    except Exception as e:
        print(f"Cảnh báo: Lỗi khi dịch đường dẫn '{path_str}': {e}")
        return path_norm  # Trả về path gốc nếu có lỗi dịch

    return translated_path


def validate_paths(paths_to_check):
    """Kiểm tra các đường dẫn đã được dịch (nếu cần)."""
    # print("\n--- Kiểm tra đường dẫn ---") # Giảm log
    validated = {}
    try:
        for name, (item_path, item_type) in paths_to_check.items():
            if not item_path:
                raise ValueError(f"Đường dẫn '{name}' bắt buộc.")
            # Đường dẫn vào đây NÊN đã được translate_path xử lý nếu cần
            # Chỉ cần chuẩn hóa và kiểm tra tồn tại
            abs_path = os.path.abspath(os.path.expanduser(str(item_path)))
            # print(f"DEBUG validate: '{name}' -> '{abs_path}'") # Debug

            if item_type == "file":
                if not os.path.exists(abs_path):
                    raise FileNotFoundError(f"File '{name}' không thấy: {abs_path}")
                if not os.path.isfile(abs_path):
                    raise ValueError(f"'{name}' không phải file: {abs_path}")
                validated[name] = abs_path
            elif item_type in ["dir", "dir_create"]:
                if not os.path.exists(abs_path):
                    if item_type == "dir_create":
                        print(f"Thư mục '{name}' không tồn tại. Đang tạo: {abs_path}")
                        os.makedirs(
                            abs_path, exist_ok=True
                        )  # exist_ok=True là quan trọng
                    else:
                        raise FileNotFoundError(
                            f"Thư mục '{name}' không thấy: {abs_path}"
                        )
                # Kiểm tra lại sau khi tạo (hoặc nếu đã tồn tại)
                if not os.path.isdir(abs_path):
                    raise NotADirectoryError(f"'{name}' không phải thư mục: {abs_path}")
                validated[name] = abs_path  # Hợp lệ
            else:
                raise ValueError(f"Kiểu '{item_type}' không hợp lệ cho '{name}'")
        # print("-> Các đường dẫn đã kiểm tra hợp lệ.") # Giảm log
        return validated
    except Exception as e:
        print(f"\n!!! Lỗi Kiểm Tra Đường Dẫn !!!\nLỗi: {e}")
        raise e  # Ném lại lỗi


# --- Các hàm xử lý cache và metadata ---
# (Copy đầy đủ các hàm load_cache, save_cache, get_duration, probe_file_metadata,
#  get_video_metadata_batch từ phiên bản trước, đảm bảo đã sửa lỗi Pylance)


def load_cache(cache_path):
    """Tải dữ liệu cache từ file JSON, bỏ qua lỗi."""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache_path, data):
    """Lưu dữ liệu cache vào file JSON, bỏ qua lỗi."""
    try:
        cache_dir = os.path.dirname(cache_path)
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"CB: Lỗi ghi cache {cache_path}: {e}")


def get_duration(filename):
    """Lấy thời lượng media (s), thử ffmpeg.probe rồi ffprobe."""
    duration = 0.0
    duration_str = None
    base_filename = os.path.basename(filename) # Lấy tên file để báo lỗi
    # print(f"DEBUG: Getting duration for {filename}") # Debug
    try:
        # Thử ffmpeg.probe trước
        try:
            probe = ffmpeg.probe(filename, timeout=20) # Timeout 20s
            # Ưu tiên format duration
            if 'format' in probe and probe['format'].get('duration'):
                format_duration_str = probe['format']['duration']
                if format_duration_str and isinstance(format_duration_str, str) and format_duration_str.upper() != 'N/A':
                    try: duration = float(format_duration_str)
                    except ValueError: pass
            # Nếu không có hoặc <= 0, thử stream duration
            if duration <= 0:
                stream = next((s for s in probe['streams'] if s.get('duration')), None)
                if not stream: stream = next((s for s in probe['streams'] if s['codec_type'] in ['video', 'audio']), None)
                if stream and stream.get('duration'):
                    stream_duration_str = stream['duration']
                    if stream_duration_str and isinstance(stream_duration_str, str) and stream_duration_str.upper() != 'N/A':
                         try: duration = float(stream_duration_str)
                         except ValueError: pass
        except Exception as probe_err:
             print(f"DEBUG: ffmpeg.probe failed for {base_filename}: {probe_err}")
             # Không raise lỗi ở đây, để thử ffprobe

        # Nếu vẫn <= 0, thử ffprobe
        if duration <= 0:
            print(f"DEBUG: Probing with ffprobe for {base_filename}...") # Debug
            try:
                # Tăng timeout cho ffprobe nếu file trên mạng
                ffprobe_timeout = 30
                result = subprocess.run(
                    ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', filename],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                    check=True, timeout=ffprobe_timeout, encoding='utf-8', errors='replace'
                 )
                duration_str_ff = result.stdout.strip()
                if duration_str_ff and duration_str_ff.upper() != 'N/A':
                    duration = float(duration_str_ff)
                else:
                    # ffprobe chạy thành công nhưng không trả về duration
                     raise ValueError("ffprobe completed but returned no valid duration.")
            except subprocess.TimeoutExpired:
                 raise ValueError(f"ffprobe timed out after {ffprobe_timeout}s.") from None
            except subprocess.CalledProcessError as cpe:
                 # ffprobe chạy nhưng trả về lỗi (exit code != 0)
                 raise ValueError(f"ffprobe failed with exit code {cpe.returncode}. Stderr: {cpe.stderr.strip()}") from cpe
            except FileNotFoundError:
                 raise ValueError("ffprobe command not found in PATH.") from None
            except Exception as ffprobe_err: # Các lỗi khác (vd: ValueError khi float())
                 raise ValueError(f"Error processing ffprobe output: {ffprobe_err}") from ffprobe_err

        # Kiểm tra lần cuối
        if duration <= 0:
            raise ValueError("Duration could not be determined or is invalid (<= 0).")

        return duration

    except Exception as e:
        # Ném lại lỗi cuối cùng với thông tin rõ ràng hơn
        raise ValueError(f"Lỗi get_duration cho file '{base_filename}'") from e


def probe_file_metadata(filepath):
    """Worker function để lấy duration và mtime cho một file."""
    try:
        if not os.path.exists(filepath):
            return filepath, None
        duration = get_duration(filepath)
        mtime = os.path.getmtime(filepath)
        return filepath, {"duration": duration, "mtime": mtime}
    except Exception as e:
        return filepath, None  # Không in lỗi


def get_video_metadata_batch(file_paths, cache_path, max_workers):
    """Lấy metadata cho danh sách video, dùng cache và đa luồng."""
    cache = load_cache(cache_path)
    results = {}
    files_to_probe = []
    cache_needs_saving = False
    processed_files = set()
    start_check_time = time.time()
    print(f"--- Kiểm tra cache video nền ({len(file_paths)} files) ---")
    for filepath in file_paths:
        try:
            abs_filepath = os.path.abspath(filepath)
            if abs_filepath in processed_files:
                continue
            processed_files.add(abs_filepath)
            if not os.path.exists(filepath):
                if abs_filepath in cache:
                    del cache[abs_filepath]
                    cache_needs_saving = True
                    continue
            current_mtime = os.path.getmtime(filepath)
            if abs_filepath in cache:
                cached_data = cache[abs_filepath]
                if (
                    cached_data.get("mtime") == current_mtime
                    and isinstance(cached_data.get("duration"), (int, float))
                    and cached_data.get("duration") > 0
                ):
                    results[filepath] = cached_data["duration"]
                else:
                    files_to_probe.append(filepath)
            else:
                files_to_probe.append(filepath)
        except Exception as e:
            print(f"CB: Lỗi check cache {os.path.basename(filepath)}: {e}")
            files_to_probe.append(filepath) if filepath not in files_to_probe else None
    end_check_time = time.time()
    print(
        f"-> Check cache xong ({end_check_time - start_check_time:.2f}s). Cần probe {len(files_to_probe)} file(s)."
    )
    if files_to_probe:
        print(f"-> Bắt đầu probe đa luồng (max_workers={max_workers})...")
        start_probe_time = time.time()
        successful_probes, failed_probes = 0, 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(probe_file_metadata, fp): fp for fp in files_to_probe
            }
            for future in concurrent.futures.as_completed(future_to_file):
                original_filepath, abs_filepath = future_to_file[
                    future
                ], os.path.abspath(future_to_file[future])
                try:
                    _, data = future.result()
                    current_mtime = os.path.getmtime(original_filepath)
                    if (
                        data
                        and isinstance(data.get("duration"), (int, float))
                        and data["duration"] > 0
                    ):
                        results[original_filepath] = data["duration"]
                        cache[abs_filepath] = {
                            "duration": data["duration"],
                            "mtime": current_mtime,
                        }
                        cache_needs_saving = True
                        successful_probes += 1  # Đã sửa lỗi Pylance 1
                    else:
                        failed_probes += 1
                        results[original_filepath] = 0.0
                        cache[abs_filepath] = {"duration": None, "mtime": current_mtime}
                        cache_needs_saving = True
                except FileNotFoundError:
                    failed_probes += 1
                    if abs_filepath in cache:
                        del cache[abs_filepath]
                        cache_needs_saving = True
                except Exception as exc:
                    failed_probes += 1
                    print(f"-> Lỗi probe {os.path.basename(original_filepath)}: {exc}")
                    results[original_filepath] = 0.0
                    try:
                        current_mtime = os.path.getmtime(original_filepath)
                        cache[abs_filepath] = {"duration": None, "mtime": current_mtime}
                        cache_needs_saving = True
                    except Exception:
                        pass
        end_probe_time = time.time()
        print(
            f"-> Probe xong: {successful_probes} OK, {failed_probes} lỗi ({end_probe_time - start_probe_time:.2f}s)."
        )
    if cache_needs_saving:
        save_cache(cache_path, cache)
    final_results = {
        fp: results.get(fp)
        for fp in file_paths
        if os.path.exists(fp)
        and isinstance(results.get(fp), (int, float))
        and results.get(fp) > 0
    }
    return final_results


# ==============================================================================
# SECTION 4: LOGIC CHUẨN BỊ VIDEO NỀN
# ==============================================================================
def prepare_background_videos(
    video_folder_path, target_duration, cache_file_path, max_workers
):
    """Lấy metadata, chọn ngẫu nhiên video nền đủ thời lượng yêu cầu."""
    print(f"\n--- Chuẩn bị video nền từ: '{os.path.basename(video_folder_path)}' ---")
    available_videos = []
    try:
        print("-> Liệt kê file video...")
        available_videos = [
            os.path.join(video_folder_path, f)
            for f in os.listdir(video_folder_path)
            if f.lower().endswith((".mp4", ".mov", ".avi", ".mkv", ".webm"))
        ]
    except Exception as e:
        print(f"!!! Lỗi liệt kê file: {e}")
        raise
    if not available_videos:
        print(f"!!! Lỗi: Không tìm thấy video nào trong '{video_folder_path}'")
        raise FileNotFoundError()

    video_metadata = get_video_metadata_batch(
        available_videos, cache_file_path, max_workers
    )
    if not video_metadata:
        print("!!! Lỗi: Không có video nền hợp lệ.")
        raise ValueError("No valid background videos.")

    valid_video_paths = list(video_metadata.keys())
    print(f"-> Tìm thấy {len(valid_video_paths)} video hợp lệ.")
    print(f"--- Chọn video nền (cần {target_duration:.2f}s) ---")
    selected_video_files, current_duration, attempts, max_attempts = (
        [],
        0.0,
        0,
        len(valid_video_paths) * 30 + 10,
    )

    while current_duration < target_duration and attempts < max_attempts:
        # Đã sửa lỗi Pylance 2 & 3
        video_to_add = random.choice(valid_video_paths)
        video_duration = video_metadata.get(video_to_add, 0)
        if video_duration > 0:
            selected_video_files.append(video_to_add)
            current_duration += video_duration
        attempts += 1

    print(
        f"-> Đã chọn {len(selected_video_files)} video (lặp) tổng ~{current_duration:.2f}s."
    )
    if not selected_video_files:
        raise ValueError("Không chọn được video nền nào.")
    if current_duration < target_duration:
        print(
            f"CB: Tổng duration nền ({current_duration:.2f}s) ngắn hơn yêu cầu ({target_duration:.2f}s)."
        )
    return selected_video_files


# ==============================================================================
# SECTION 5: HÀM TẠO VIDEO (Tự động chọn CPU/NVENC)
# ==============================================================================


# def generate_video(video_inputs, bg_video_list, output_path, config):
def generate_single_video_2step(video_inputs, bg_video_list, output_path, config):
    """
    Tạo video theo quy trình 2 bước:
    1. CPU Filter + CPU Encode (chất lượng cao) -> Intermediate File.
    2. Transcode Intermediate -> Final Output dùng Encoder đã chọn (CPU/NVENC/VAAPI).

    Args:
        video_inputs (dict): Chứa đường dẫn input chính đã validate.
        bg_video_list (list): Danh sách video nền đã chọn.
        output_path (str): Đường dẫn file output cuối cùng.
        config (dict): Dictionary cấu hình chung đã load và tính toán.

    Returns:
        bool: True nếu cả 2 bước thành công, False nếu có lỗi.
    """
    func_start_time = time.time()
    base_output_name = os.path.basename(output_path)
    print(f"\n--- Bắt đầu tạo video 2 BƯỚC: {base_output_name} ---")

    concat_file_path = None
    intermediate_file_path = None
    success_step1 = False
    success_step2 = False
    compiled_cmd_list_step1 = None
    compiled_cmd_list_step2 = None

    try:
        # --- Chuẩn bị file concat và lấy duration ---
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        temp_dir = os.path.abspath(config.get('temp_dir', '.'))
        os.makedirs(temp_dir, exist_ok=True)
        if not os.access(temp_dir, os.W_OK):
            raise OSError(f"Không quyền ghi thư mục tạm: {temp_dir}")

        concat_filename = f"concat_{base_output_name}_{timestamp}.txt"
        concat_file_path = os.path.join(temp_dir, concat_filename)
        intermediate_filename = f"intermediate_{base_output_name}_{timestamp}.mkv"
        intermediate_file_path = os.path.join(temp_dir, intermediate_filename)

        print(f"-> File concat tạm: {concat_file_path}")
        print(f"-> File trung gian sẽ tạo: {intermediate_file_path}")

        if not isinstance(bg_video_list, list) or not bg_video_list:
            raise ValueError("bg_video_list rỗng hoặc không hợp lệ!")

        # Ghi file concat
        with open(concat_file_path, 'w', encoding='utf-8') as f:
            print("-> Đang ghi file concat...")
            for video_file in bg_video_list:
                if not isinstance(video_file, str): continue
                if "'" in video_file: print(f"!!! CB: Tên file chứa dấu nháy đơn: {video_file}")
                safe_path = video_file.replace('\\', '/')
                line_to_write = f"file '{safe_path}'\n"
                f.write(line_to_write)
        print(f"-> Đã ghi xong file concat.")

        # Lấy thời lượng
        print("-> Lấy thời lượng audio chính...")
        target_duration = get_duration(video_inputs['main_audio'])
        if target_duration <= 0: raise ValueError("Duration audio chính <= 0")
        print(f"-> Thời lượng đích: {target_duration:.2f}s")

        # ==================== BƯỚC 1: TẠO FILE TRUNG GIAN (CPU) ====================
        print("\n--- Bước 1: Tạo file trung gian (CPU Encode) ---")
        step1_process_start_time = time.time()
        try:
            # --- Xây dựng Filter Graph (Bước 1 - CPU) ---
            print("-> Chuẩn bị filter graph (Bước 1)...")
            # Inputs
            input_videos = ffmpeg.input(concat_file_path, format='concat', safe=0, itsoffset=0)
            input_main_audio = ffmpeg.input(video_inputs['main_audio'])
            try:
                split_audio = input_main_audio.asplit()
                main_audio_stream, waveform_audio_input = split_audio[0], split_audio[1]
            except Exception:
                main_audio_stream = waveform_audio_input = input_main_audio
            input_bg_music = ffmpeg.input(video_inputs['bg_music'], stream_loop=-1)
            input_overlay_vid = ffmpeg.input(video_inputs['overlay_video'], stream_loop=-1)
            input_overlay_img_raw = ffmpeg.input(video_inputs['overlay_image'])
            input_text_img_raw = ffmpeg.input(video_inputs['text_image'])

            # CPU Filters
            input_overlay_img = input_overlay_img_raw.filter('scale', h=config['target_height'], w=-1) # Full Height
            input_text_img = input_text_img_raw.filter('scale', w=config['overlay_text_width'], h=-1)
            waveform_video = waveform_audio_input.filter(
                'showfreqs', s=f"{config['waveform_w']}x{config['waveform_h']}",
                mode=config.get('waveform_mode', 'bar'), ascale=config.get('waveform_ascale', 'log'),
                fscale=config.get('waveform_fscale', 'log'), win_size=config.get('waveform_win_size', 2048),
                win_func=config.get('waveform_win_func', 'hann'), colors=config.get('waveform_color', 'white'),
                rate=config.get('framerate', 30)
            ).filter('format', pix_fmts='yuva420p')
            processed_video = input_videos.trim(duration=target_duration).filter('setpts', 'PTS-STARTPTS')
            processed_overlay_vid = input_overlay_vid.trim(duration=target_duration).filter('setpts', 'PTS-STARTPTS')
            overlay_vid_with_opacity = processed_overlay_vid.filter('format', pix_fmts='yuva420p') \
                                                          .filter('colorchannelmixer', aa=config.get('overlay_opacity', 0.25))
            processed_bg_music_trimmed = input_bg_music.filter('atrim', duration=target_duration).filter('asetpts', 'PTS-STARTPTS')
            processed_bg_music = processed_bg_music_trimmed.filter('volume', volume=config.get('bg_music_volume', 0.3))
            mixed_audio = ffmpeg.filter([main_audio_stream, processed_bg_music], 'amix', inputs=2, duration='first')

            # Ghép các lớp video (BG -> CharImg -> OverlayVid -> TextImg -> Waveform)
            print("-> Applying overlays...")
            char_img_x = f"main_w-overlay_w-{config.get('margin_right', 15)}"; char_img_y = '0'
            merged_layer1 = ffmpeg.overlay(processed_video, input_overlay_img, x=char_img_x, y=char_img_y, shortest=False)
            merged_layer2 = ffmpeg.overlay(merged_layer1, overlay_vid_with_opacity, x=0, y=0, shortest=False)
            text_img_x = f"{config.get('margin_left', 15)}"; text_img_y = '(main_h-overlay_h)/2'
            merged_layer3 = ffmpeg.overlay(merged_layer2, input_text_img, x=text_img_x, y=text_img_y, shortest=False)
            waveform_x = '(main_w-overlay_w)/2'; waveform_y = f"main_h-overlay_h-{config.get('waveform_margin_bottom', 50)}"
            final_video = ffmpeg.overlay(merged_layer3, waveform_video, x=waveform_x, y=waveform_y, shortest=False)

            # --- Output Args (Bước 1 - CPU Intermediate) ---
            # Sử dụng cài đặt CPU từ config cho file trung gian
            intermediate_output_args = {
                'vcodec': 'libx264',
                'acodec': 'aac', # Dùng AAC để bước sau copy được
                'audio_bitrate': config.get('audio_bitrate', '192k'),
                's': config.get('resolution', '1920x1080'),
                'pix_fmt': 'yuv420p',
                'r': config.get('framerate', 30),
                't': target_duration,
                'preset': config.get('cpu_preset', 'veryfast'),
                'strict': 'experimental',
            }
            # Ưu tiên CRF cho chất lượng trung gian
            cpu_crf_val_str = config.get('cpu_crf') # Lấy giá trị CRF từ config
            if cpu_crf_val_str is not None:
                try: intermediate_output_args['crf'] = int(cpu_crf_val_str)
                except Exception: intermediate_output_args['b:v'] = config.get('cpu_bitrate', '6000k')
            else: intermediate_output_args['b:v'] = config.get('cpu_bitrate', '6000k')
            # Xóa key không dùng
            if 'crf' in intermediate_output_args: intermediate_output_args.pop('b:v', None)
            else: intermediate_output_args.pop('crf', None)


            print(f"-> Chuẩn bị tạo file trung gian: {intermediate_file_path}")
            print(f"-> CPU Encode Options (Intermediate): { {k:v for k,v in intermediate_output_args.items() if k in ['preset','crf','b:v']} }")

            # --- Thực thi FFmpeg (Bước 1) ---
            step1_stream = ffmpeg.output(
                final_video, mixed_audio, intermediate_file_path, **intermediate_output_args
            )
            step1_stream = step1_stream.overwrite_output()
            compiled_cmd_list_step1 = ffmpeg.compile(step1_stream, cmd='ffmpeg')
            print("-> Lệnh FFmpeg (Bước 1 - CPU Intermediate):")
            print(subprocess.list2cmdline(compiled_cmd_list_step1))

            print("\n-> Bắt đầu encode Bước 1 (CPU)...")
            step1_sp_result = subprocess.run(
                compiled_cmd_list_step1, capture_output=True, text=True,
                encoding='utf-8', errors='replace', check=False
            )
            print(f"-> Hoàn thành encode Bước 1. Exit code: {step1_sp_result.returncode}")

            # Kiểm tra lỗi Bước 1
            if step1_sp_result.returncode != 0 or not os.path.exists(intermediate_file_path) or os.path.getsize(intermediate_file_path) <= 100:
                print(f"!!! LỖI TẠO FILE TRUNG GIAN (Exit Code: {step1_sp_result.returncode}) !!!")
                stderr_step1 = step1_sp_result.stderr if step1_sp_result.stderr else "(Không có stderr)"
                print(f"--- FFmpeg stderr (Bước 1) ---\n{stderr_step1}\n--- End stderr ---")
                raise RuntimeError("Tạo file trung gian thất bại.")
            else:
                 print(f"-> Đã tạo file trung gian thành công: {intermediate_file_path}")
                 success_step1 = True

        except Exception as e_step1:
            print(f"!!! Lỗi trong Bước 1: {e_step1}")
            if compiled_cmd_list_step1:
                try: print("-> Lệnh FFmpeg lỗi (Bước 1):\n", subprocess.list2cmdline(compiled_cmd_list_step1))
                except Exception: pass
            if not isinstance(e_step1, RuntimeError): traceback.print_exc()
            raise # Ném lại lỗi

        step1_end_time = time.time()
        print(f"--- Hoàn thành Bước 1 trong {step1_end_time - step1_process_start_time:.2f} giây ---")


        # ==================== BƯỚC 2: CHUYỂN MÃ SANG FILE CUỐI (GPU/CPU) ====================
        if success_step1:
            step2_start_time = time.time()
            compiled_cmd_list_step2 = None
            encoder_name_step2 = 'libx264' # Mặc định
            global_args_list_step2 = []
            final_output_args = {}

            try:
                # --- Xác định Encoder và chuẩn bị Args cho Bước 2 ---
                encoder_choice = config.get('encoder_choice', 'libx264').lower()
                current_os = platform.system()
                use_nvenc = (encoder_choice in ['h264_nvenc', 'hevc_nvenc'] and current_os == "Windows")
                use_vaapi = (encoder_choice in ['h264_vaapi', 'hevc_vaapi'] and current_os == "Linux")

                input_intermediate = ffmpeg.input(intermediate_file_path)

                # Args chung cho Bước 2
                final_output_args = {
                    'acodec': 'copy', # Copy audio từ file trung gian
                    's': config.get('resolution', '1920x1080'),
                    'r': config.get('framerate', 30),
                    't': target_duration,
                    'movflags': '+faststart',
                    'strict': 'experimental', # Có thể cần cho một số muxer/codec
                }

                # --- Cấu hình cho NVENC ---
                if use_nvenc:
                    encoder_name_step2 = config.get('hw_encoder', 'h264_nvenc')
                    print(f"\n--- Bước 2: Chuyển mã sang file cuối cùng (NVENC Transcode) ---")
                    final_output_args['vcodec'] = encoder_name_step2
                    final_output_args['preset'] = config.get('hw_nvenc_preset', 'p5')
                    nvenc_rc = config.get('hw_nvenc_rc', 'constqp')
                    nvenc_qp = config.get('hw_nvenc_qp') # Giữ là string hoặc None
                    nvenc_bitrate = config.get('hw_nvenc_bitrate', '8000k')
                    final_output_args['rc'] = nvenc_rc
                    if nvenc_rc == 'constqp':
                        final_output_args['qp'] = nvenc_qp if nvenc_qp is not None else '23' # Truyền string qp
                    elif nvenc_rc in ['vbr', 'cbr']:
                        final_output_args['b:v'] = nvenc_bitrate
                    print(f"-> NVENC Options: { {k:v for k,v in final_output_args.items() if k in ['preset','rc','qp','b:v']} }")
                    # Thử không dùng global args cho NVENC transcode đơn giản
                    global_args_list_step2 = []
                    print(f"-> Using Global Args (Step 2 - NVENC): {global_args_list_step2}")

                # --- Cấu hình cho VAAPI ---
                elif use_vaapi:
                    encoder_name_step2 = 'h264_vaapi' # Mặc định H.264 cho VAAPI
                    print(f"\n--- Bước 2: Chuyển mã sang file cuối cùng (VAAPI Transcode) ---")
                    final_output_args['vcodec'] = encoder_name_step2
                    final_output_args['b:v'] = config.get('hw_bitrate', '8000k')
                    # Thêm tùy chọn VAAPI khác nếu có
                    if config.get('hw_vaapi_rc'): final_output_args['rc_mode'] = config['hw_vaapi_rc']
                    if config.get('hw_vaapi_qp'): final_output_args['qp'] = config['hw_vaapi_qp'] # Truyền string qp
                    print(f"-> VAAPI Options: { {k:v for k,v in final_output_args.items() if k in ['b:v','rc_mode','qp']} }")
                    # Global Args cho VAAPI
                    vaapi_device = config.get('vaapi_device_path')
                    if not vaapi_device: raise ConfigError("Thiếu VAAPI_DEVICE")
                    global_args_list_step2 = ['-vaapi_device', vaapi_device]
                    print(f"-> Using Global Args (Step 2 - VAAPI): {global_args_list_step2}")

                # --- Cấu hình cho CPU (Fallback) ---
                else:
                    encoder_name_step2 = 'libx264'
                    print(f"\n--- Bước 2: Chuyển mã sang file cuối cùng (CPU Transcode) ---")
                    final_output_args['vcodec'] = encoder_name_step2
                    final_output_args['preset'] = config.get('cpu_preset', 'veryfast')
                    final_output_args['pix_fmt'] = 'yuv420p'
                    # Dùng CRF/Bitrate CPU
                    cpu_crf_val = config.get('cpu_crf')
                    cpu_bitrate_val = config.get('cpu_bitrate')
                    if cpu_crf_val is not None:
                         try: final_output_args['crf'] = int(cpu_crf_val)
                         except Exception: final_output_args['b:v'] = cpu_bitrate_val if cpu_bitrate_val else '6000k'
                    else: final_output_args['b:v'] = cpu_bitrate_val if cpu_bitrate_val else '6000k'
                    print(f"-> CPU Options: { {k:v for k,v in final_output_args.items() if k in ['preset','crf','b:v']} }")
                    global_args_list_step2 = []

                # --- Thực thi FFmpeg (Bước 2) ---
                step2_stream = ffmpeg.output(
                    input_intermediate['v'], input_intermediate['a'], # Chọn luồng video/audio
                    output_path, **final_output_args
                ).overwrite_output()

                compiled_cmd_list_step2 = ffmpeg.compile(step2_stream)
                if not compiled_cmd_list_step2: raise ValueError("compile() Bước 2 rỗng.")
                if compiled_cmd_list_step2[0].lower().endswith('ffmpeg'):
                    step2_full_cmd = ( compiled_cmd_list_step2[:1] + global_args_list_step2 + compiled_cmd_list_step2[1:] )
                else: step2_full_cmd = ['ffmpeg'] + global_args_list_step2 + compiled_cmd_list_step2

                print(f"-> Lệnh FFmpeg (Bước 2 - {encoder_name_step2}):")
                print(subprocess.list2cmdline(step2_full_cmd))
                print(f"\n-> Bắt đầu encode Bước 2 ({encoder_name_step2})...")
                step2_sp_result = subprocess.run(step2_full_cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=False)
                print(f"-> Hoàn thành encode Bước 2. Exit code: {step2_sp_result.returncode}")

                # Kiểm tra lỗi Bước 2
                if step2_sp_result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) <= 100:
                    print(f"!!! LỖI TRANSCODE BƯỚC 2 (Exit Code: {step2_sp_result.returncode}) !!!")
                    stderr_step2 = step2_sp_result.stderr if step2_sp_result.stderr else "(Không có stderr)"
                    print(f"--- FFmpeg stderr (Bước 2) ---\n{stderr_step2}\n--- End stderr ---")
                    raise RuntimeError(f"Transcode {encoder_name_step2} thất bại.")
                else:
                    print(f"-> Video cuối cùng đã tạo thành công: {output_path}")
                    success_step2 = True

            except Exception as e_step2:
                print(f"!!! Lỗi trong Bước 2 ({encoder_name_step2}): {e_step2}")
                if compiled_cmd_list_step2:
                     try: print("-> Lệnh FFmpeg lỗi (Bước 2):\n", subprocess.list2cmdline(compiled_cmd_list_step2))
                     except Exception: pass
                # Không ném lại lỗi ở đây để cleanup file trung gian
                success_step2 = False # Đánh dấu bước 2 thất bại

            step2_end_time = time.time()
            print(f"--- Hoàn thành Bước 2 trong {step2_end_time - step2_start_time:.2f} giây ---")

        # Thành công cuối cùng nếu cả 2 bước OK
        success = success_step1 and success_step2

    # Xử lý lỗi chung của toàn bộ hàm
    except Exception as e_main:
        print(f"\n!!! Lỗi trong quá trình tạo video '{os.path.basename(output_path)}' (2-Step) !!!")
        print(f"Lỗi: {e_main}")
        if not isinstance(e_main, (FileNotFoundError, NotADirectoryError, OSError, ValueError, ConfigError, RuntimeError)):
             traceback.print_exc()

    finally:
        # --- Dọn dẹp file tạm ---
        print("-> Bắt đầu dọn dẹp file tạm...")
        if concat_file_path and os.path.exists(concat_file_path):
            try: os.remove(concat_file_path)
            except OSError as e: print(f"CB: Lỗi xóa {concat_file_path}: {e}")
        if intermediate_file_path and os.path.exists(intermediate_file_path):
            try: os.remove(intermediate_file_path); print(f"-> Đã xóa file trung gian: {intermediate_file_path}")
            except OSError as e: print(f"CB: Lỗi xóa {intermediate_file_path}: {e}")

        end_time = time.time()
        total_duration_func = end_time - func_start_time
        print(f"--- Kết thúc tạo video {os.path.basename(output_path)} (Thời gian hàm: {total_duration_func:.2f} giây) ---")
        return success



# ==============================================================================
# SECTION 6: HÀM MAIN ĐIỀU PHỐI
# ==============================================================================
def generate_cpu_video(video_inputs, bg_video_list, output_path, config):
    """
    Tạo một video duy nhất dựa trên các input và config được cung cấp.

    Bao gồm tạo file concat tạm, xây dựng lệnh ffmpeg (với waveform 'showfreqs'),
    chạy lệnh và dọn dẹp file tạm.

    Args:
        video_inputs (dict): Chứa đường dẫn tuyệt đối các file input chính:
                             'main_audio', 'bg_music', 'overlay_image',
                             'text_image', 'overlay_video'.
        bg_video_list (list): Danh sách đường dẫn tuyệt đối video nền đã chọn.
        output_path (str): Đường dẫn tuyệt đối để lưu video kết quả.
        config (dict): Dictionary chứa các cấu hình chung (margins, volume,
                       ffmpeg params, waveform settings...).

    Returns:
        bool: True nếu tạo video thành công, False nếu thất bại.
    """
    print(f"\n--- Bắt đầu tạo video: {os.path.basename(output_path)} ---")
    start_time = time.time()
    concat_file_path = None # Khởi tạo để dùng trong finally
    success = False
    process = None # Khởi tạo để dùng trong except ffmpeg.Error

    try:
        # --- 1. Tạo và kiểm tra file concat tạm thời ---
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        concat_filename = f"concat_{os.path.basename(output_path)}_{timestamp}.txt"
        concat_dir = os.path.abspath(config.get('concat_dir', '.'))
        os.makedirs(concat_dir, exist_ok=True)

        if not os.access(concat_dir, os.W_OK):
             raise OSError(f"Không có quyền ghi vào thư mục concat: {concat_dir}")

        concat_file_path = os.path.join(concat_dir, concat_filename)
        print(f"-> Tạo file concat tạm: {concat_file_path}")

        if not isinstance(bg_video_list, list) or not bg_video_list:
             raise ValueError("bg_video_list không phải là danh sách hợp lệ hoặc rỗng!")

        # Mở và ghi file concat
        with open(concat_file_path, 'w', encoding='utf-8') as f:
            for i, video_file in enumerate(bg_video_list):
                 if not isinstance(video_file, str):
                      print(f"Cảnh báo: Bỏ qua mục không phải string trong bg_video_list ở index {i}")
                      continue
                 if "'" in video_file: # Kiểm tra dấu nháy đơn
                      print(f"!!! CẢNH BÁO: Tên file chứa dấu nháy đơn: {video_file}")
                 safe_path = video_file.replace('\\', '/')
                 line_to_write = f"file '{safe_path}'\n"
                 f.write(line_to_write)
        print(f"-> Đã ghi xong file concat.")

        # --- 2. Lấy thời lượng audio chính ---
        print("-> Lấy thời lượng audio chính...")
        try:
            target_duration = get_duration(video_inputs['main_audio'])
            if target_duration <= 0: raise ValueError("<= 0")
            print(f"-> Thời lượng đích: {target_duration:.2f}s")
        except Exception as e:
            raise ValueError(f"Lỗi lấy duration audio chính: {e}") from e

        # --- 3. Xây dựng lệnh FFmpeg ---
        print("-> Chuẩn bị filter graph...")

        # Inputs FFmpeg
        input_videos = ffmpeg.input(concat_file_path, format='concat', safe=0, itsoffset=0)
        input_main_audio = ffmpeg.input(video_inputs['main_audio'])
        try: # Tách luồng audio
             split_audio = input_main_audio.asplit()
             main_audio_stream = split_audio[0]
             waveform_audio_input = split_audio[1]
        except Exception as e:
             print(f"Cảnh báo: Không tách được luồng audio: {e}. Dùng luồng gốc.")
             main_audio_stream = waveform_audio_input = input_main_audio
        input_bg_music = ffmpeg.input(video_inputs['bg_music'], stream_loop=-1)
        input_overlay_vid = ffmpeg.input(video_inputs['overlay_video'], stream_loop=-1)
        input_overlay_img_raw = ffmpeg.input(video_inputs['overlay_image'])
        input_text_img_raw = ffmpeg.input(video_inputs['text_image'])

        # Scale Ảnh Lớp phủ
        print(f"-> Scaling ảnh nhân vật to width: {config['overlay_char_width']}px (~40%)")
        input_overlay_img = input_overlay_img_raw.filter('scale', h=config['target_height'], w=-1) # <-- Sửa lại h

        print(f"-> Scaling ảnh chữ to width: {config['overlay_text_width']}px (~60%)")
        input_text_img = input_text_img_raw.filter('scale', w=config['overlay_text_width'], h=-1)

        # Generate Waveform Video using showfreqs
        print(f"-> Generating frequency bars (showfreqs - WxH: {config['waveform_w']}x{config['waveform_h']}, Mode: {config.get('waveform_mode','bar')})")
        waveform_video = waveform_audio_input.filter(
            'showfreqs',
            s=f"{config['waveform_w']}x{config['waveform_h']}",
            mode=config.get('waveform_mode', 'bar'),
            ascale=config.get('waveform_ascale', 'log'),
            fscale=config.get('waveform_fscale', 'log'),
            win_size=config.get('waveform_win_size', 2048),
            win_func=config.get('waveform_win_func', 'hann'),
            colors=config.get('waveform_color', 'lime'),
            rate=config.get('framerate', 30)
        ).filter('format', pix_fmts='yuva420p') # Đảm bảo alpha cho overlay

        # Processing Video Streams
        processed_video = input_videos.trim(duration=target_duration).filter('setpts', 'PTS-STARTPTS')
        processed_overlay_vid = input_overlay_vid.trim(duration=target_duration).filter('setpts', 'PTS-STARTPTS')
        overlay_vid_with_opacity = processed_overlay_vid.filter('format', pix_fmts='yuva420p') \
                                                      .filter('colorchannelmixer', aa=config.get('overlay_opacity', 0.25))

        # Processing Audio Streams
        processed_bg_music_trimmed = input_bg_music.filter('atrim', duration=target_duration).filter('asetpts', 'PTS-STARTPTS')
        bg_vol = config.get('bg_music_volume', 0.20)
        print(f"-> Reducing background music volume to {bg_vol*100:.0f}%.")
        processed_bg_music = processed_bg_music_trimmed.filter('volume', volume=bg_vol)
        mixed_audio = ffmpeg.filter([main_audio_stream, processed_bg_music], 'amix', inputs=2, duration='first')

                # Thứ tự Z (từ dưới lên): BG -> Ảnh NV -> Video Mờ -> Ảnh Chữ -> Waveform
        print("-> Applying overlays (Order: BG->CharImg->OverlayVid->TextImg->Waveform)...")

        # Lớp 1: Video nền (processed_video) + Ảnh Nhân Vật (input_overlay_img)
        # Ảnh NV ở bên phải, giữa dọc (hoặc full height tùy bạn chọn ở bước scale)
        char_img_x = f"main_w-overlay_w-{config.get('margin_right', 15)}"
        char_img_y = '(main_h-overlay_h)/2'
        merged_layer1 = ffmpeg.overlay(
            processed_video,     # Input chính (nền)
            input_overlay_img,   # Lớp phủ 1 (Ảnh NV đã scale)
            x=char_img_x,
            y=char_img_y,
            shortest=False
        )
        print("DEBUG: Applied CharImg")

        # Lớp 2: Lớp 1 (BG+Char) + Video Lớp Phủ Mờ (overlay_vid_with_opacity)
        # Video này phủ toàn khung hình
        merged_layer2 = ffmpeg.overlay(
            merged_layer1,            # Input chính (BG + Char)
            overlay_vid_with_opacity, # Lớp phủ 2 (Video mờ)
            x=0,
            y=0,
            shortest=False
        )
        print("DEBUG: Applied OverlayVid")

        # Lớp 3: Lớp 2 (BG+Char+Vid) + Ảnh Chữ (input_text_img)
        # Ảnh chữ ở bên trái, giữa dọc
        text_img_x = f"{config.get('margin_left', 15)}"
        text_img_y = '(main_h-overlay_h)/2'
        final_video = ffmpeg.overlay(
            merged_layer2,       # Input chính (BG + Char + Vid)
            input_text_img,      # Lớp phủ 3 (Ảnh chữ đã scale)
            x=text_img_x,
            y=text_img_y,
            shortest=False
         )
        print("DEBUG: Applied TextImg")

        # Lớp 4 (Cuối cùng): Lớp 3 (Mọi thứ trước đó) + Waveform (waveform_video)
        # Vị trí waveform vẫn giữ nguyên (giữa ngang, gần đáy)
        # waveform_x = '(main_w-overlay_w)/2'
        # waveform_y = f"main_h-overlay_h-{config.get('waveform_margin_bottom', 50)}"
        # final_video = ffmpeg.overlay(
        #     merged_layer3,    # Input chính (Mọi thứ đã ghép)
        #     waveform_video,   # Lớp phủ trên cùng (Waveform)
        #     x=waveform_x,
        #     y=waveform_y,
        #     shortest=False
        # )
        # print("DEBUG: Applied Waveform (Topmost)")

        # --- 4. Output Arguments ---
        print(f"-> Chuẩn bị tạo video output tại: {output_path}")
        output_args = {
            'vcodec': 'libx264', 'acodec': 'aac',
            'audio_bitrate': config.get('audio_bitrate', '192k'),
            'preset': config.get('preset', 'medium'),
            's': config.get('resolution', '1920x1080'),
            'pix_fmt': 'yuv420p',
            'r': config.get('framerate', 30), # Lấy framerate từ config
            't': target_duration,
            'strict': 'experimental',
            'movflags': '+faststart'
        }
        # Xử lý bitrate vs CRF
        crf_val = config.get('crf')
        if crf_val is not None:
            try: output_args['crf'] = int(crf_val); print(f"-> Using CRF: {output_args['crf']}")
            except (ValueError, TypeError):
                print(f"CB: CRF ('{crf_val}') không hợp lệ, dùng Bitrate.")
                output_args['video_bitrate'] = config.get('video_bitrate', '6000k')
                print(f"-> Using Video Bitrate: {output_args['video_bitrate']}")
        else:
            output_args['video_bitrate'] = config.get('video_bitrate', '6000k')
            print(f"-> Using Video Bitrate: {output_args['video_bitrate']}")

        # --- 5. Thực thi FFmpeg ---
        process = ffmpeg.output(final_video, mixed_audio, output_path, **output_args).overwrite_output()

        print("\n-> Lệnh FFmpeg sẽ chạy (ước lượng):")
        try: print(' '.join(process.compile()))
        except Exception as compile_err: print(f"(Không thể hiển thị lệnh compile: {compile_err})")

        print("\n-> Bắt đầu encode...")
        start_encode_time = time.time()
        stdout, stderr = process.run(capture_stdout=True, capture_stderr=True)
        end_encode_time = time.time()
        print(f"-> Hoàn thành encode ({end_encode_time - start_encode_time:.2f}s).")

        # Kiểm tra kết quả
        if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
             print(f"-> Video đã được tạo thành công tại: {output_path}")
             success = True
        else:
             print(f"!!! Cảnh báo: File output không được tạo hoặc bị trống.")
             print("--- FFmpeg stderr (có thể chứa lỗi) ---\n", stderr.decode('utf-8', errors='ignore'), "\n--- End stderr ---")

    # Xử lý lỗi cụ thể của FFmpeg
    except ffmpeg.Error as e:
        print('\n!!! LỖI FFmpeg !!!')
        print('--- FFmpeg stderr ---\n', e.stderr.decode('utf-8', errors='ignore'), '\n--- End stderr ---')
        if process:
            try: print("-> Lệnh FFmpeg lỗi:\n", ' '.join(process.compile()))
            except Exception: pass
    # Xử lý các lỗi khác
    except (ValueError, IOError, OSError, Exception) as e:
        print(f"\n!!! Lỗi trong quá trình tạo video '{os.path.basename(output_path)}' !!!")
        print(f"Lỗi: {e}")
        # In traceback nếu không phải lỗi IO đã biết
        if not isinstance(e, (IOError, OSError)):
             traceback.print_exc()

    finally:
        # --- Dọn dẹp file concat tạm ---
        if concat_file_path and os.path.exists(concat_file_path):
            try:
                os.remove(concat_file_path)
                print(f"-> Đã xóa file tạm: {concat_file_path}")
            except OSError as e:
                print(f"Cảnh báo: Lỗi xóa file tạm {concat_file_path}: {e}")
        end_time = time.time()
        print(f"--- Kết thúc tạo video {os.path.basename(output_path)} (Thời gian hàm: {end_time - start_time:.2f} giây) ---")
        return success


def main():
    """Hàm chính: Kết nối DB, lấy task, dịch path, gọi tạo video, cập nhật status."""
    script_start_time = time.time()
    print("=" * 60)
    print(" BẮT ĐẦU QUÁ TRÌNH XỬ LÝ VIDEO TỪ MONGODB (Auto OS - CPU/NVENC)")
    print("=" * 60)
    config = None

    try:
        # 1. Tải cấu hình chung từ .env
        config = load_app_config()

        # 2. Kiểm tra các đường dẫn chung từ config
        common_paths_to_check = {
            "Nhạc nền": (config["bg_music_file"], "file"),
            "Video Overlay": (config["overlay_video_file"], "file"),
            "Thư mục video nền": (config["video_folder"], "dir"),
            "Thư mục output": (config["output_dir"], "dir_create"),
            "Thư mục tạm": (config["temp_dir"], "dir_create"),
        }
        # Dịch path chung nếu cần
        common_paths_translated = {
            name: (translate_path(path, config), type)
            for name, (path, type) in common_paths_to_check.items()
        }
        validated_common_paths = validate_paths(common_paths_translated)
        # Cập nhật config với đường dẫn tuyệt đối đã check và dịch
        config.update(
            {
                "bg_music_file": validated_common_paths["Nhạc nền"],
                "overlay_video_file": validated_common_paths["Video Overlay"],
                "video_folder": validated_common_paths["Thư mục video nền"],
                "output_dir": validated_common_paths["Thư mục output"],
                "temp_dir": validated_common_paths["Thư mục tạm"],
                "concat_dir": validated_common_paths[
                    "Thư mục tạm"
                ],  # Cập nhật concat_dir
            }
        )

        # 3. Vòng lặp xử lý các task từ MongoDB
        processed_count, successful_videos, failed_count = 0, 0, 0
        max_process = 1  # Giới hạn số task

        while processed_count < max_process:
            print("-" * 40)
            task_doc = db_handler.get_next_pending_task()  # Lấy và khóa task

            if not task_doc:
                print("-> Không tìm thấy task nào đang chờ xử lý.")
                break
            processed_count += 1
            doc_id = task_doc["_id"]
            print(f"--- Bắt đầu Task {processed_count} cho Doc ID: {doc_id} ---")
            task_start_time = time.time()
            final_output_path = None
            task_success = False
            error_message = None

            try:
                # --- Lấy và Dịch/Chuẩn hóa đường dẫn từ document ---
                print("-> Lấy và chuẩn hóa/dịch đường dẫn từ MongoDB...")
                main_audio_doc = task_doc.get("final_audio_path")
                overlay_image_doc = task_doc.get("thumbnail_character_path")
                text_image_doc = task_doc.get("thumbnail_text_path")

                main_audio_task = translate_path(main_audio_doc, config)
                overlay_image_task = translate_path(overlay_image_doc, config)
                text_image_task = translate_path(text_image_doc, config)

                # --- Kiểm tra các đường dẫn của task ---
                task_paths_to_check = {
                    "Audio chính (Task)": (main_audio_task, "file"),
                    "Ảnh Overlay (Task)": (overlay_image_task, "file"),
                    "Ảnh chữ (Task)": (text_image_task, "file"),
                }
                validated_task_paths = validate_paths(task_paths_to_check)

                # --- Chuẩn bị inputs và dữ liệu ---
                current_video_inputs = {
                    "main_audio": validated_task_paths["Audio chính (Task)"],
                    "bg_music": config["bg_music_file"],
                    "overlay_image": validated_task_paths["Ảnh Overlay (Task)"],
                    "text_image": validated_task_paths["Ảnh chữ (Task)"],
                    "overlay_video": config["overlay_video_file"],
                }
                current_target_duration = get_duration(
                    current_video_inputs["main_audio"]
                )
                selected_bg_videos = prepare_background_videos(
                    config["video_folder"],
                    current_target_duration,
                    config["cache_file"],
                    config["max_workers"],
                )

                # --- Tạo tên file output ---
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                output_suffix = (
                    task_doc.get("output_suffix") or f"_{timestamp}"
                )  # Thử đọc suffix từ DB
                base_filename = os.path.splitext(
                    config["output_filename_pattern"].format(
                        id=str(doc_id), timestamp=timestamp
                    )
                )[0]
                final_output_filename = f"{base_filename}{output_suffix}.mp4"
                final_output_path = os.path.join(
                    config["output_dir"], final_output_filename
                )

                # *** Gọi hàm tạo video tự động chọn encoder ***
                task_success = generate_cpu_video(
                    current_video_inputs, selected_bg_videos, final_output_path, config
                )

            except Exception as task_err:
                print(f"!!! Lỗi chuẩn bị/chạy generate_video cho {doc_id}: {task_err}")
                if not isinstance(
                    task_err,
                    (
                        FileNotFoundError,
                        ValueError,
                        NotADirectoryError,
                        OSError,
                        ConfigError,
                    ),
                ):
                    traceback.print_exc()
                task_success = False
                error_message = f"{type(task_err).__name__}: {str(task_err)[:500]}"

            # Cập nhật status DB
            final_status = "finish" if task_success else "failed"
            if db_handler.update_task_status(
                doc_id,
                final_status,
                output_path=final_output_path if task_success else None,
                error_message=error_message,
            ):
                if task_success:
                    successful_videos += 1
                else:
                    failed_count += 1
            else:
                print(f"!!! CB: Không cập nhật được status cuối cùng cho {doc_id}")
                failed_count += 1

            task_end_time = time.time()
            print(
                f"--- Kết thúc Task {processed_count} ({task_end_time - task_start_time:.2f}s) ---"
            )

        # Kết thúc vòng lặp
        print("\n" + "=" * 60)
        print(f" HOÀN THÀNH ({processed_count}) TASK")
        print(f" -> Thành công: {successful_videos}")
        print(f" -> Thất bại: {failed_count}")
        print("=" * 60)

    except ConfigError as cfg_err:
        print(f"\n!!! Lỗi Cấu Hình !!!\n{cfg_err}")
    except Exception as main_err:
        print(f"\n!!! Lỗi nghiêm trọng hàm main !!!\nLỗi: {main_err}")
        traceback.print_exc()
    finally:
        db_handler.close_db_connection()  # Đóng kết nối DB
        script_end_time = time.time()
        print(
            f"Tổng thời gian chạy script: {script_end_time - script_start_time:.2f} giây"
        )


# --- Chạy hàm main ---
if __name__ == "__main__":
    while True:
        main()
        print("ngu 120s....")
        time.sleep(120)
