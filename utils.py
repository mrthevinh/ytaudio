# utils.py
import logging
from collections import Counter
import tiktoken
import re
import nltk # <<< Import nltk
from nltk.tokenize import sent_tokenize


# --- Logging Setup ---
logger = logging.getLogger(__name__)

# --- Đảm bảo dữ liệu 'punkt' của NLTK đã được tải ---
try:
    nltk.data.find('tokenizers/punkt')
except (LookupError, nltk.downloader.DownloadError):
    logger.info("NLTK 'punkt' tokenizer not found. Attempting to download...")
    try:
        nltk.download('punkt', quiet=True)
        logger.info("'punkt' downloaded successfully.")
    except Exception as e:
        logger.error(f"Failed to download NLTK 'punkt' data automatically: {e}")
        logger.error("Please run 'python -m nltk.downloader punkt' manually if issues persist.")
except Exception as e:
     logger.error(f"Error checking/downloading NLTK data: {e}")

# --- Token Counter ---
def count_tokens(text, model_name="gpt-4o-mini"):
    """Đếm số token."""
    if not text or not isinstance(text, str): return 0
    try: encoding = tiktoken.encoding_for_model(model_name)
    except KeyError: encoding = tiktoken.get_encoding("cl100k_base")
    try: return len(encoding.encode(text))
    except Exception as e: logger.error(f"Err counting tokens: {e}"); return len(text.split()) + 1

# --- Indentation Helpers (Có thể giữ lại nếu parser cũ còn dùng) ---
def calculate_indent_level(line, base_indent, indent_unit):
    if not line or not line.strip(): return -1
    stripped = line.lstrip(' '); indent = len(line) - len(stripped)
    return max(0, round((indent - base_indent) / indent_unit)) if indent_unit > 0 else 0

def detect_indent_settings(lines):
    base_indent = 0; indent_unit = 4; indents = sorted(list(set(len(l)-len(l.lstrip(' ')) for l in lines if l.strip())))
    if indents: base_indent = indents[0]
    if len(indents) > 1:
        diffs = [indents[i]-indents[i-1] for i in range(1,len(indents)) if indents[i]>indents[i-1]]
        if diffs: count = Counter(d for d in diffs if d > 0); unit = count.most_common(1)[0][0] if count else 0; indent_unit = unit if unit > 0 else 4
    if indent_unit <= 0: indent_unit = 4
    logger.debug(f"Detected Base indent: {base_indent}, Indent unit: {indent_unit}")
    return base_indent, indent_unit

# --- Ước lượng tham số ---
def estimate_num_quotes_stories(duration_minutes, language="Vietnamese"):
    """
    Ước lượng số quote/story VÀ số ký tự mục tiêu (target_chars)
    dựa trên thời lượng và tốc độ đọc ước tính theo ngôn ngữ (CPM).
    """
    if not isinstance(duration_minutes, (int, float)) or duration_minutes <= 0:
         duration_minutes = 120 # Default
         logging.warning(f"Invalid duration_minutes, defaulting to {duration_minutes} min.")

    # Ước lượng Tốc độ đọc Ký tự/Phút (CPM) - Cần tinh chỉnh!
    cpm_map = {
        'vietnamese': 1500, # Tiếng Việt đọc khá nhanh
        'english': 800,    # Tiếng Anh cũng khá nhanh (nhiều từ ngắn)
        'chinese': 400,    # Tiếng Trung đọc chậm hơn đáng kể
        'japanese': 450,   # Tiếng Nhật
        'korean': 500,     # Tiếng Hàn
    }
    # Lấy CPM hoặc dùng default (ví dụ: tiếng Anh)
    lang_key = language.lower()
    # Tìm key khớp hoặc chứa tên ngôn ngữ
    cpm = default_cpm = cpm_map.get('english', 750) # Default CPM
    if lang_key in cpm_map:
         cpm = cpm_map[lang_key]
    else:
         for key, value in cpm_map.items():
              if lang_key in key:
                   cpm = value
                   logging.warning(f"Using CPM estimate for '{key}' for language '{language}'.")
                   break
         else:
              logging.warning(f"CPM estimate not found for '{language}'. Using default {default_cpm}.")
              cpm = default_cpm

    # Tính số ký tự mục tiêu
    target_chars = int(duration_minutes * cpm)
    target_chars = max(target_chars, 4000) # Đảm bảo tối thiểu
    logging.info(f"Estimated Target Chars for {duration_minutes} min ({language}, CPM:{cpm}): {target_chars}")

    # Ước lượng số items (quotes + stories) dựa trên target_chars
    # Giả sử mỗi item chính (quote/story + phân tích) cần ~300-500 ký tự? -> Rất khó đoán
    # Có thể dùng cách đơn giản: số item tỷ lệ với thời gian
    items_per_hour = 30 # Tổng số quote+story cho 60 phút (15+15)
    num_items = max(4, int(items_per_hour * (duration_minutes / 60.0)))

    # Chia số items ra quote và story (có thể bỏ giới hạn 15)
    num_quotes = (num_items + 1) // 2
    num_stories = num_items - num_quotes
    # Giờ không cần giới hạn 15 nữa
    # num_quotes = min(num_quotes, 15)
    # num_stories = min(num_stories, 15)

    logging.info(f"Estimated Items: {num_quotes} quotes, {num_stories} stories.")
    # Trả về target_chars thay vì target_words
    return num_quotes, num_stories, target_chars

# --- HÀM CHIA CHUNK THEO CÂU/ĐOẠN (Phiên bản đúng) ---
def split_script_into_chunks(text, max_chars=3800, language='english'): # <<< CÓ tham số language
    """Chia text dài thành các chunk nhỏ hơn, ưu tiên ngắt tại cuối câu/đoạn."""
    if not text or not isinstance(text, str): return []
    logger.info(f"Splitting text ({len(text)} chars) by sentence/paragraph (max ~{max_chars} chars)...")

    nltk_language_map = {'vietnamese': 'vietnamese', 'english': 'english', 'chinese': 'chinese', 'japanese': 'japanese', 'korean': 'korean', 'french': 'french', 'spanish': 'spanish'}
    nltk_lang = nltk_language_map.get(language.lower(), 'english') # Default English
    logging.debug(f"Using NLTK language '{nltk_lang}' for sentence tokenization.")

    chunks = []; current_chunk = ""
    paragraphs = text.split('\n\n')

    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if not paragraph: continue

        if len(current_chunk) == 0 and len(paragraph) <= max_chars:
            current_chunk = paragraph; continue

        try: sentences = sent_tokenize(paragraph, language=nltk_lang)
        except LookupError:
             logger.error(f"NLTK 'punkt' for '{nltk_lang}' not found. Download it."); sentences = re.split(r'([.?!]+)', paragraph); sentences = [s.strip() for s in sentences if s.strip()]
        except Exception as e:
             logging.error(f"NLTK sent_tokenize failed: {e}. Falling back."); sentences = re.split(r'([.?!]+)', paragraph); sentences = [s.strip() for s in sentences if s.strip()]

        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence: continue
            if len(sentence) < 2:
                continue
            if len(sentence) > max_chars:
                logging.warning(f"Single sentence exceeds max_chars: '{sentence[:100]}...'. Force splitting.")
                if current_chunk: chunks.append(current_chunk)
                start = 0
                while start < len(sentence):
                    end = start + max_chars; split_pos = sentence.rfind(' ', start, end)
                    if split_pos != -1 and end < len(sentence): end = split_pos + 1
                    part = sentence[start:end].strip();
                    if part: chunks.append(part)
                    start = end
                current_chunk = ""
            elif len(current_chunk) + len(sentence) + 1 <= max_chars:
                current_chunk += (" " + sentence) if current_chunk else sentence
            else:
                if current_chunk: chunks.append(current_chunk)
                current_chunk = sentence

    if current_chunk: chunks.append(current_chunk)
    logging.info(f"Split into {len(chunks)} chunks.")
    return chunks