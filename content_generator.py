# content_generator.py
import logging
import time
import openai
import tenacity
from bson.objectid import ObjectId
import concurrent.futures
import random
import os
import re
import pymongo

# Import các hàm/biến cần thiết từ các module khác
try:
    from utils import count_tokens
    # Import các hàm get collection và save_chunk
    from db_manager import (get_script_chunks_collection,
                           get_content_generations_collection,
                           get_text_from_db,
                           save_chunk_to_db) # Import hàm lưu chunk
except ImportError as e:
    logging.critical(f"Content Generator Failed Imports: {e}")
    exit(1) # Cần thiết nên thoát nếu lỗi
# --- HÀM DỊCH (Thêm vào đây) ---
@tenacity.retry(
    stop=tenacity.stop_after_attempt(3), # Thử lại tối đa 3 lần
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=10), # Chờ lâu hơn giữa các lần thử
    reraise=True # Ném lại lỗi sau khi hết lần thử
)
def translate_text(text, target_language="Vietnamese", source_language="auto", model="gpt-4o-mini", max_retries=2): # max_retries này không còn dùng trực tiếp nếu dùng tenacity
    """Dịch văn bản sử dụng OpenAI API và làm sạch kết quả."""
    global oai_client # Sử dụng client đã khởi tạo
    if oai_client is None:
        logging.error("translate_text: OpenAI client is not available.")
        # Trả về lỗi để tenacity thử lại hoặc báo lỗi ra ngoài
        raise ConnectionError("OpenAI client unavailable for translation.")
    if not text: return ""

    # Tạo prompt yêu cầu chỉ trả về bản dịch
    prompt = f"Translate the following text strictly to {target_language}. Output ONLY the translated text, without any extra explanation, formatting, or quotation marks:\n\n{text}"
    if source_language != "auto":
         prompt = f"Translate the following text strictly from {source_language} to {target_language}. Output ONLY the translated text, without any extra explanation, formatting, or quotation marks:\n\n{text}"

    messages = [
        {"role": "system", "content": f"You are a highly precise translation engine. Respond ONLY with the translation to {target_language}."},
        {"role": "user", "content": prompt}
    ]
    try:
        response = oai_client.chat.completions.create(
            model=model, messages=messages,
            # Ước lượng token output, cộng thêm buffer lớn hơn chút cho dịch thuật
            max_tokens=int(len(text.split()) * 4 + 100),
            temperature=0.1 # Nhiệt độ thấp cho dịch thuật chính xác
        )
        translation_raw = response.choices[0].message.content.strip()
        # Dọn dẹp các ký tự không mong muốn ở đầu/cuối
        translation_clean = translation_raw.strip().strip('"\'“”‘’()[]{}*-\t ')
        logging.debug(f"Translate Raw: '{translation_raw}' | Cleaned: '{translation_clean}'")
        # Kiểm tra xem có phải là câu trả lời từ chối hoặc lỗi không
        if len(translation_clean) < 2 and len(text) > 5:
             logging.warning(f"Translation for '{text[:50]}...' resulted in very short output: '{translation_clean}'.")
             # Có thể coi đây là lỗi và raise để tenacity retry
             # raise ValueError("Translation result too short")
        return translation_clean
    except openai.APIError as e:
        logging.error(f"OpenAI API Error during translation of '{text[:50]}...': {e}")
        raise # Để tenacity retry
    except Exception as e:
        logging.error(f"Unexpected error during translation of '{text[:50]}...': {e}", exc_info=True)
        raise # Ném lại lỗi để tenacity retry hoặc để hàm gọi xử lý

@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(min=2, max=10), reraise=True)
def generate_seo_title(script_snippet, language, model="gpt-4o-mini"): # Đổi tên tham số
    """Tạo tiêu đề SEO từ một đoạn script, có retry."""
    check_openai_ready()
    if not script_snippet: return None

    logging.info(f"Generating SEO title from script snippet ({language})... Snippet length: {len(script_snippet)}")

    prompt = f"""Analyze the following script snippet and generate ONE compelling, SEO-friendly YouTube video title that accurately reflects the main topic and encourages clicks.
    Language for the title: {language}
    Requirements: Concise (under 70 characters if possible), include main keywords, evoke curiosity. Output ONLY the title itself.

    Script Snippet:
    --- START SNIPPET ---
    {script_snippet}
    --- END SNIPPET ---

    Generated Title ({language}):"""

    messages = [
        {"role": "system", "content": f"You are an SEO expert creating YouTube titles from script content. Output language is {language}."},
        {"role": "user", "content": prompt}
    ]
    try:
        response = oai_client.chat.completions.create(model=model, messages=messages, max_tokens=100, temperature=0.7)
        title = response.choices[0].message.content.strip().replace('"', '')
        # Có thể thêm kiểm tra hậu kỳ cho title (ví dụ: không quá ngắn)
        if len(title) < 5:
             logging.warning(f"Generated SEO title seems too short: '{title}'")
             return None # Coi như không thành công nếu quá ngắn
        logging.info(f"Generated SEO Title ({language}) from snippet: {title}")
        return title
    except Exception as e:
        logging.error(f"Error generating SEO title from snippet ({language}): {e}")
        raise # Ném lỗi để retry

# Khởi tạo OpenAI client
oai_client = None
try:
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        logging.error("CRITICAL: OPENAI_API_KEY is not set.")
    else:
        openai_base_url = os.getenv("OPENAI_BASE_URL")
        oai_client = openai.OpenAI(api_key=openai_api_key, base_url=openai_base_url) if openai_base_url else openai.OpenAI(api_key=openai_api_key)
        logging.info("OpenAI client initialized in content_generator.")
except Exception as e:
    logging.critical(f"CRITICAL: Failed to initialize OpenAI client in content_generator: {e}")
    oai_client = None

def check_openai_ready():
    """Kiểm tra client và raise lỗi nếu chưa sẵn sàng."""
    if oai_client is None: raise ConnectionError("OpenAI client not available.")

# --- Hàm tạo Outline Markdown TỪ TOPIC ---
@tenacity.retry(stop=tenacity.stop_after_attempt(2), wait=tenacity.wait_exponential(min=5, max=20), reraise=True) # Ít retry hơn cho outline
def generate_outline_markdown(topic, language, model="gpt-4o", num_quotes=5, num_stories=5):
    check_openai_ready()
    logging.info(f"Generating Markdown outline for topic '{topic}' ({language})...")
    # Sử dụng prompt đã được cải thiện để yêu cầu Markdown
    prompt = f"""
Hãy tạo một dàn ý chi tiết và có cấu trúc cho video YouTube về chủ đề "{topic}" bằng ngôn ngữ {language}.

**Yêu cầu Định dạng:**
- Output PHẢI là **Markdown** hợp lệ.
- Dùng `#` cho Mở đầu, Kết luận.
- Dùng `##` cho các phần lớn trong Thân bài.
- Dùng `###` cho từng Danh ngôn / Câu chuyện.
- Dùng `####` hoặc list (`*`, `-`) cho các mục chi tiết.
- Mỗi mục trên một dòng mới.

**Yêu cầu Nội dung:**
1.  **# Mở đầu:** (lời dẫN cho chủ để đua vào lôi cuốn nhé nhang, không dẫN kiểu chào mừng).
2.  **## Thân bài - Phần 1: Phân tích Danh ngôn ({num_quotes} mục):**
    * ### [Danh ngôn 1]:
        * Bối cảnh/Tác giả: ...
        * Phân tích ý nghĩa: ...
        * Bài học/Ứng dụng: ...
    * (Lặp lại cấu trúc cho đủ {num_quotes} danh ngôn)
3.  **## Thân bài - Phần 2: Kể Chuyện ({num_stories} mục):**
    * ### [Câu chuyện 1]:
        * Kể chi tiết: ...
        * Phân tích ý nghĩa: ...
        * Bài học rút ra: ...
    * (Lặp lại cấu trúc cho đủ {num_stories} câu chuyện)
4.  **# Kết luận:** (Tóm tắt, nhấn mạnh, CTA, cảm ơn...).

**QUAN TRỌNG: Toàn bộ dàn ý PHẢI viết bằng ngôn ngữ: {language}. Chỉ trả về nội dung Markdown.**
"""
    messages = [{"role": "system", "content": f"You create detailed Markdown outlines in {language}."}, {"role": "user", "content": prompt}]
    try:
        response = oai_client.chat.completions.create(model=model, messages=messages, max_tokens=3500, temperature=0.5) # Giảm temp cho cấu trúc
        outline_text = response.choices[0].message.content.strip()
        # Làm sạch cơ bản output Markdown (xóa ```markdown nếu có)
        outline_text = re.sub(r"^```markdown\s*|\s*```$", "", outline_text, flags=re.MULTILINE).strip()
        logging.info(f"Markdown outline generated successfully ({language}). Length: {len(outline_text)} chars.")
        return outline_text
    except Exception as e: logging.error(f"Error generating outline for topic '{topic}' ({language}): {e}", exc_info=True); raise

# --- Hàm tạo Outline Markdown TỪ SCRIPT GỐC ---
@tenacity.retry(stop=tenacity.stop_after_attempt(2), wait=tenacity.wait_exponential(min=5, max=20), reraise=True)
def generate_outline_from_script(source_script, language, model="gpt-4o"):
    check_openai_ready()
    logging.info(f"Generating outline from script ({language}). Script length: {len(source_script)} chars")
    # Rút gọn script nếu quá dài
    max_source_tokens_for_outline = 30000
    source_script_tokens = count_tokens(source_script, model)
    if source_script_tokens > max_source_tokens_for_outline:
        cutoff_ratio = max(0.1, max_source_tokens_for_outline / source_script_tokens * 0.9)
        cutoff_index = int(len(source_script) * cutoff_ratio)
        source_script_shortened = source_script[:cutoff_index] + "\n...[SCRIPT TRUNCATED]..."
        logging.warning(f"Source script truncated for outline generation.")
    else:
        source_script_shortened = source_script

    prompt = f"""Analyze the script below and generate a detailed outline in MARKDOWN format. Capture main sections (Intro, Body, Conclusion), key points, quotes, stories. Use #, ##, ###, #### for hierarchy. Use lists (* or -) for details. The outline MUST be in {language}.\n\nSCRIPT:\n{source_script_shortened}\n\nOutput ONLY the Markdown outline."""
    messages = [{"role": "system", "content": f"You are an expert script analyzer creating Markdown outlines in {language}."}, {"role": "user", "content": prompt}]
    try:
        response = oai_client.chat.completions.create(model=model, messages=messages, max_tokens=3000, temperature=0.4)
        outline_text = response.choices[0].message.content.strip()
        outline_text = re.sub(r"^```markdown\s*|\s*```$", "", outline_text, flags=re.MULTILINE).strip()
        logging.info("Outline derived from script successfully.")
        return outline_text
    except Exception as e: logging.error(f"Error generating outline from script ({language}): {e}", exc_info=True); raise

# --- Hàm viết lại TOÀN BỘ Script (cho task 'rewrite_script') ---
@tenacity.retry(stop=tenacity.stop_after_attempt(2), wait=tenacity.wait_exponential(multiplier=2, min=15, max=90), reraise=True) # Chờ lâu hơn
def rewrite_entire_script(source_script, derived_outline, language, model, target_chars):
    check_openai_ready()
    logging.info(f"Starting full script rewrite ({language}). Target ~{target_chars} chars.")
    # Rút gọn input nếu cần
    max_input_tokens = 100000 # Tùy model
    source_tokens = count_tokens(source_script, model)
    outline_tokens = count_tokens(derived_outline, model)
    prompt_instruction_tokens = 600 # Tăng buffer cho prompt rewrite
    total_input_tokens = source_tokens + outline_tokens + prompt_instruction_tokens
    source_script_for_prompt = source_script
    if total_input_tokens > max_input_tokens:
        available_for_source = max(1000, max_input_tokens - outline_tokens - prompt_instruction_tokens - 1000)
        if source_tokens > available_for_source:
             cutoff_ratio = available_for_source / source_tokens * 0.95
             cutoff_index = int(len(source_script) * cutoff_ratio)
             source_script_for_prompt = source_script[:cutoff_index] + "\n...[SOURCE TRUNCATED]..."
             logging.warning(f"Source script truncated for rewrite task. Input tokens ~{available_for_source+outline_tokens+prompt_instruction_tokens}")
        else:
             logging.info("Source script fits within context window for rewrite.")


    prompt = f"""Rewrite the 'Original Script' below into a new, engaging video script, strictly following the 'Guiding Outline'.

**Instructions:**
1.  **Language:** The final script MUST be entirely in **{language}**.
2.  **Length:** Aim for a total length of approximately **{target_chars} characters**. Adjust detail per section to meet this target.
3.  **Style:** Use a fresh, natural, conversational style suitable for audio/video narration.
4.  **Content:** Retain the core ideas, information, essential quotes, and stories from the Original Script, but rephrase and re-express them. Use the Guiding Outline to structure the flow and ensure all key points are covered.
5.  **Output:** Generate ONLY the final rewritten script text. Exclude any meta-commentary, section labels (like 'Introduction:', 'Body:', etc.), or explanations about the rewriting process. Start directly with the introduction content.

**Guiding Outline:**
--- OUTLINE START ---
{derived_outline}
--- OUTLINE END ---

**Original Script (Reference):**
--- ORIGINAL SCRIPT START ---
{source_script_for_prompt}
--- ORIGINAL SCRIPT END ---

**Rewritten Script (in {language}, ~{target_chars} characters):**
"""
    messages = [{"role": "system", "content": f"You are a professional scriptwriter rewriting video content in {language}, following provided outlines and length targets."}, {"role": "user", "content": prompt}]

    try:
        # Ước lượng output tokens dựa trên target_chars
        # Tỉ lệ token/char thay đổi theo ngôn ngữ, ví dụ ~0.5-0.8 cho tiếng Việt/Anh, ~1.0-1.5 cho CJK
        char_to_token_ratio = 0.8 if language in ["Vietnamese", "English"] else 1.3 # Ước lượng thô
        estimated_output_tokens = int(target_chars * char_to_token_ratio)
        # Giới hạn max_tokens, cộng thêm buffer, không vượt quá khả năng của model
        max_output_tokens = min(max(3000, estimated_output_tokens + 500), 8000 if "gpt-4o-mini" in model else 16000) # Giới hạn an toàn

        logging.info(f"Calling LLM for full rewrite. Model: {model}, Target Chars: {target_chars}, Est Output Tokens: {estimated_output_tokens}, Max Output Tokens: {max_output_tokens}")
        response = oai_client.chat.completions.create(
            model=model, messages=messages, max_tokens=max_output_tokens,
            temperature=0.7, # Giữ nguyên
        )
        rewritten_script = response.choices[0].message.content.strip()
        generated_chars = len(rewritten_script)
        logging.info(f"Full script rewrite completed. Generated length: {generated_chars} chars.")
        if abs(generated_chars - target_chars) / target_chars > 0.3:
             logging.warning(f"Rewritten script length ({generated_chars}) differs >30% from target ({target_chars}).")

        return rewritten_script

    except Exception as e: logging.error(f"Error rewriting script: {e}", exc_info=True); raise


# --- Hàm Tạo Nội Dung Chi Tiết TỪ OUTLINE (cho task 'from_topic') ---
@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(min=4, max=10), reraise=True)
def generate_section_content_from_outline(topic, section, language, script_name, index, chunk_size, model, flat_outline_data):
    """Tạo nội dung cho một mục từ outline phẳng (dùng cho task 'from_topic')."""
    check_openai_ready()
    level = section.get('level', 0)
    title_or_content = section.get('title') if level <= 1 else section.get('content', section.get('title', ''))
    current_item_type = section.get('type', 'point')

    style_instruction = f"Viết tự nhiên, mạch lạc, như đang trò chuyện, phù hợp script audio/video. Bắt đầu trực tiếp. **Ngôn ngữ: {language}**."
    negative_constraint = """QUAN TRỌNG: KHÔNG dùng câu dẫn nhập, KHÔNG lặp lại tiêu đề mục. KHÔNG thêm chào hỏi/kết đoạn.
    Không đƯợc chứa nộI dung sau bắt buộc:
    Hate and Fairness	Hate and fairness-related harms refer to any content that attacks or uses discriminatory language with reference to a person or Identity group based on certain differentiating attributes of these groups.

This includes, but is not limited to:
Race, ethnicity, nationality
Gender identity groups and expression
Sexual orientation
Religion
Personal appearance and body size
Disability status
Harassment and bullying
Sexual	Sexual describes language related to anatomical organs and genitals, romantic relationships and sexual acts, acts portrayed in erotic or affectionate terms, including those portrayed as an assault or a forced sexual violent act against one’s will. 

 This includes but is not limited to:
Vulgar content
Prostitution
Nudity and Pornography
Abuse
Child exploitation, child abuse, child grooming
Violence	Violence describes language related to physical actions intended to hurt, injure, damage, or kill someone or something; describes weapons, guns and related entities.

This includes, but isn't limited to:
Weapons
Bullying and intimidation
Terrorist and violent extremism
Stalking
Self-Harm	Self-harm describes language related to physical actions intended to purposely hurt, injure, damage one’s body or kill oneself.

This includes, but isn't limited to:
Eating Disorders
Bullying and intimidation"""

    prompt = ""
    parent_title = topic # Default
    # Logic tìm Parent Title
    parent_level = level - 1
    if level > 0 and index > 0:
        for i in range(index - 1, -1, -1):
            parent_candidate = flat_outline_data[i]
            if parent_candidate.get('level') == parent_level:
                parent_title = parent_candidate.get('title') if parent_candidate.get('level',0) <= 1 else parent_candidate.get('content', topic)
                break
        else: logging.warning(f"Could not find parent for item idx {index}.")

    # Xác định prompt
    if current_item_type == 'intro': prompt = f"""Viết NỘI DUNG phần mở đầu hấp dẫn video về "{topic}". Gợi ý: "{title_or_content}". {style_instruction} {negative_constraint}"""
    elif current_item_type == 'outro': prompt = f"""Viết NỘI DUNG phần kết luận và CTA video về "{topic}". Gợi ý: "{title_or_content}". {style_instruction} {negative_constraint}"""
    elif current_item_type == 'section_header': prompt = f"""Viết đoạn chuyển tiếp NGẮN (1-2 câu) giới thiệu phần "{title_or_content}" trong video về "{topic}". {style_instruction} {negative_constraint}"""
    elif current_item_type == 'quote_suggestion': prompt = f"""Chủ đề: "{topic}". Ngữ cảnh: "{parent_title}". Câu nói/Ý: "{title_or_content}". Phân tích sâu sắc: ý nghĩa, liên hệ, bài học. {style_instruction} {negative_constraint}"""
    elif current_item_type == 'story_suggestion': prompt = f"""Chủ đề: "{topic}". Ngữ cảnh: "{parent_title}". Câu chuyện/Ví dụ: "{title_or_content}". Kể chi tiết, phân tích ý nghĩa, bài học. {style_instruction} {negative_constraint}"""
    else: prompt = f"""Chủ đề: "{topic}". Ngữ cảnh: "{parent_title}". Luận điểm: "{title_or_content}" (Cấp {level}). Viết nội dung chi tiết, ví dụ. {style_instruction} {negative_constraint}"""

    if not prompt: return index, section.get('title', title_or_content), level, f"Lỗi: Prompt rỗng.", current_item_type

    prompt_tokens = count_tokens(prompt, model)
    model_context_window = 128000 if "gpt-4o" in model else 8192
    available_tokens = model_context_window - prompt_tokens - 200
    target_tokens = int(chunk_size * 1.6) # chunk_size là số từ ước tính
    max_tokens = max(200, min(target_tokens, available_tokens)) # Ít nhất 200 tokens

    if max_tokens <= 50: return index, section.get('title', title_or_content), level, f"Lỗi: Prompt quá dài.", current_item_type

    logging.info(f"Generating: Idx:{index}, Lv:{level}, Type:{current_item_type}, Lang:{language}, MaxTk:{max_tokens}")
    messages = [{"role": "system", "content": f"Viết kịch bản video, giọng kể chuyện, ngôn ngữ {language}. Chỉ viết nội dung chính."}, {"role": "user", "content": prompt}]

    try:
        response = oai_client.chat.completions.create(model=model, messages=messages, max_tokens=max_tokens, temperature=0.7, frequency_penalty=0.1, presence_penalty=0.1)
        new_text = response.choices[0].message.content.strip()
        gen_tokens = count_tokens(new_text, model)
        logging.info(f"Generated {gen_tokens} tokens for Idx:{index} ({language})")
        if gen_tokens < 30 and level > 1 and current_item_type not in ['intro','outro','section_header']: logging.warning(f"Content for Idx:{index} too short?")
        return index, section.get('title', title_or_content), level, new_text, current_item_type
    except openai.APIError as e: logging.error(f"OpenAI Error Idx:{index}: {e}"); raise
    except Exception as e: logging.exception(f"Unexpected Error Idx:{index}"); return index, section.get('title', title_or_content), level, f"Lỗi bất ngờ: {e}", current_item_type

# --- Hàm Thêm Quote/Story (cho task 'from_topic') ---
@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
def add_new_quote_or_story(topic, language, script_name, generation_id, chunk_size, model, flat_outline_data, type_to_add):
    check_openai_ready()
    if not isinstance(generation_id, ObjectId): generation_id = ObjectId(generation_id)
    script_chunks_coll = get_script_chunks_collection()
    if script_chunks_coll is None: return False

    last_index_doc = script_chunks_coll.find_one({"generation_id": generation_id}, sort=[("section_index", pymongo.DESCENDING)])
    next_index = last_index_doc["section_index"] + 1 if last_index_doc else (len(flat_outline_data) if flat_outline_data else 0)

    style_instruction = f"Viết tự nhiên, mạch lạc, phù hợp script audio/video. **Ngôn ngữ: {language}**."
    neg_constraint = "QUAN TRỌNG: KHÔNG dẫn nhập."
    existing_titles = [doc['section_title'] for doc in script_chunks_coll.find({"generation_id": generation_id, "level": {"$gte": 2}},{"section_title": 1, "_id": 0}).limit(30)]
    existing_str = "\n - ".join(filter(None, [t[:70] for t in existing_titles]))

    prompt = ""; title = ""; level = 3
    if type_to_add == "quote":
        prompt = f"""Chủ đề: {topic}\nCác câu nói đã có:\n - {existing_str}\nTạo câu nói MỚI và KHÁC BIỆT, liên quan chủ đề, kèm phân tích/bài học.\n{style_instruction} {neg_constraint}\nYêu cầu: 1. Câu nói. 2. Phân tích. 3. Liên hệ. 4. Bài học. Ngôn ngữ: {language}. Chỉ trả về nội dung."""
        title = f"Added Quote #{next_index}"
    elif type_to_add == "story":
        prompt = f"""Chủ đề: {topic}\nCác câu chuyện đã có:\n - {existing_str}\nTạo câu chuyện MỚI và KHÁC BIỆT, liên quan chủ đề, kèm bài học.\n{style_instruction} {neg_constraint}\nYêu cầu: 1. Kể chuyện. 2. Bài học. Ngôn ngữ: {language}. Chỉ trả về nội dung."""
        title = f"Added Story #{next_index}"
    else: return False

    prompt_tokens = count_tokens(prompt, model)
    model_ctx = 128000 if "gpt-4o" in model else 8192
    avail_tokens = model_ctx - prompt_tokens - 200
    target_tokens = int(chunk_size * 1.6) # chunk_size là số từ
    max_tokens = max(400, min(target_tokens, avail_tokens))
    if max_tokens <= 50: return False

    messages = [{"role": "system", "content": f"Viết kịch bản video, {language}. Chỉ trả về nội dung."}, {"role": "user", "content": prompt}]
    try:
        response = oai_client.chat.completions.create(model=model, messages=messages, max_tokens=max_tokens, temperature=0.75)
        new_text = response.choices[0].message.content.strip()
        gen_tokens = count_tokens(new_text, model)
        logging.info(f"Added new {type_to_add} ({language}): '{title}', {gen_tokens} tokens for gen {generation_id}")
        save_chunk_to_db(generation_id, script_name, next_index, title, new_text, level, item_type=f"{type_to_add}_added")
        return True
    except Exception as e: logging.error(f"Error adding {type_to_add} ({language}): {e}", exc_info=True); raise

# --- Hàm Điều Phối Tạo Content Dài TỪ OUTLINE (cho task 'from_topic') ---
def generate_long_text(flat_outline_items, topic_input, language, script_name, gen_id_obj,
                       num_quotes, num_stories, min_chars, chunk_words, model):
    """Tạo nội dung dài từ list phẳng, đảm bảo min_chars bằng cách đếm ký tự."""
    if not flat_outline_items:
        logging.error(f"Gen {gen_id_obj}: Flattened outline is empty. Cannot generate text.")
        return False

    # Lấy collections
    script_chunks_coll = get_script_chunks_collection()
    content_generations_coll = get_content_generations_collection()
    if script_chunks_coll is None or content_generations_coll is None:
         raise ConnectionError(f"DB Collections unavailable for gen:{gen_id_obj}")

    logging.info(f"Starting FROM_TOPIC generation loop gen:{gen_id_obj}. Target Chars: {min_chars}")
    generation_successful = True # Cờ theo dõi tổng thể
    total_chars_generated_this_run = 0 # Chỉ đếm ký tự mới tạo trong lần chạy này

    # Xác định index bắt đầu (để có thể chạy lại nếu bị dừng giữa chừng)
    try:
        last_section_doc = script_chunks_coll.find_one({"generation_id": gen_id_obj}, sort=[("section_index", pymongo.DESCENDING)])
        start_outline_index = last_section_doc["section_index"] + 1 if last_section_doc else 0
    except Exception as e_find: start_outline_index = 0; logging.error(f"Error finding last index: {e_find}")
    logging.info(f"Resuming FROM_TOPIC generation from outline index {start_outline_index}.")

    items_to_generate = flat_outline_items[start_outline_index:]
    logging.info(f"Found {len(items_to_generate)} items remaining in outline.")

    # --- Tạo content song song cho các mục outline còn lại ---
    if items_to_generate:
        max_chunk_workers = int(os.getenv("AUDIO_MAX_CONCURRENT_CHUNKS", 4)) # Dùng chung biến env
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_chunk_workers) as executor:
            futures = [
                executor.submit(generate_section_content_from_outline, # Gọi hàm tạo content từ outline
                                 topic_input, item_data, language, script_name,
                                 item_data['index'], # Index gốc của item
                                 chunk_words, model, flat_outline_items)
                for item_data in items_to_generate
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    index, title, level, content, item_type = future.result()
                    if content and not content.startswith("Lỗi:"):
                        saved_id = save_chunk_to_db(gen_id_obj, script_name, index, title, content, level, item_type=item_type)
                        if saved_id: total_chars_generated_this_run += len(content)
                        else: generation_successful = False; logging.error(f"Failed save chunk Idx:{index}")
                    else: generation_successful = False; logging.error(f"Generation failed Idx:{index}")
                except Exception as e: generation_successful = False; logging.error(f"Thread error: {e}", exc_info=True)

    # --- Đảm bảo min_chars bằng cách thêm quote/story ---
    if generation_successful: # Chỉ chạy nếu bước trên không lỗi nặng
        logging.info(f"Checking total length vs min_chars ({min_chars})...")
        iteration_count = 0
        max_iterations_add = num_quotes + num_stories + 20 # Giới hạn số lần thêm

        while iteration_count < max_iterations_add:
            iteration_count += 1
            # Kiểm tra status task
            current_status_doc = content_generations_coll.find_one({"_id": gen_id_obj}, {"status": 1})
            if not current_status_doc or current_status_doc.get('status') in ['content_failed', 'deleted', 'reset']:
                logging.warning(f"Stopping length check as gen {gen_id_obj} status changed.")
                generation_successful = False; break

            current_text = get_text_from_db(gen_id_obj)

            # ***** LUÔN ĐẾM KÝ TỰ *****
            current_char_count = len(current_text)
            count_unit = "characters"
            # ***************************

            # --- ĐIỀU KIỆN DỪNG CHÍNH ---
            if current_char_count >= min_chars:
                logging.info(f"Target length reached ({current_char_count}/{min_chars} {count_unit}).")
                break # Dừng khi đủ độ dài KÝ TỰ

            logging.info(f"Current {count_unit}: {current_char_count}, need {min_chars - current_char_count} more. Iteration {iteration_count}/{max_iterations_add}")

            # --- Logic chọn thêm quote/story (vẫn dựa vào số lượng ước tính) ---
            q_regex = "^(Câu nói|Quote|名言|Added Quote)"; s_regex = "^(Câu chuyện|Story|Ví dụ|Example|故事|Added Story)"
            try:
                q_created = script_chunks_coll.count_documents({"generation_id": gen_id_obj, "section_title": {"$regex": q_regex, "$options": "i"}})
                s_created = script_chunks_coll.count_documents({"generation_id": gen_id_obj, "section_title": {"$regex": s_regex, "$options": "i"}})
            except Exception: q_created = 0; s_created = 0 # Mặc định nếu lỗi DB
            logging.debug(f"Counts check: Q_target={num_quotes}, Q_created={q_created}, S_target={num_stories}, S_created={s_created}")

            type_to_add = None
            # Ưu tiên thêm cái nào còn thiếu so với ước tính
            if q_created < num_quotes: type_to_add = "quote"
            elif s_created < num_stories: type_to_add = "story"
            else: type_to_add = "story" if iteration_count % 2 == 0 else "quote" # Thêm luân phiên nếu đã đủ số lượng ước tính

            logging.info(f"Attempting to add new {type_to_add}...")
            try:
                added = add_new_quote_or_story(topic_input, language, script_name, gen_id_obj, chunk_words, model, flat_outline_items, type_to_add)
                if not added: logging.warning(f"Failed add {type_to_add}. Stop."); break
                time.sleep(random.uniform(4, 7)) # Delay
            except Exception as add_err: logging.error(f"Error calling add_new_quote_or_story: {add_err}", exc_info=True); generation_successful = False; break
        else: # Kết thúc vòng lặp while
             if iteration_count >= max_iterations_add:
                 logging.warning(f"Stopped adding content after {max_iterations_add} iterations. Final length: {current_char_count}/{min_chars} {count_unit}.")

    else: logging.error("Skipping min_chars check due to errors.")

    logging.info(f"Finished content generation loop. Success flag: {generation_successful}")
    return generation_successful