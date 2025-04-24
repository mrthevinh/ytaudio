# tao_thumbnail_doc.py
import openai
import requests
import os
import uuid
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from dotenv import load_dotenv
import json
import re
import math
import sys # Để kiểm tra lỗi và thoát

# --- 1. CẤU HÌNH ---------------------------------------------------
print("Đang tải cấu hình...")
load_dotenv()

# --- API Keys (Lấy từ file .env) ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
RUNWARE_API_KEY = os.getenv("RUNWARE_API_KEY")

# --- API Endpoints & Models ---
RUNWARE_API_URL = 'https://api.runware.ai/v1'
RUNWARE_MODEL = "rundiffusion:110@101" # Hoặc model Runware khác
OPENAI_TEXT_MODEL = "gpt-4o-mini"      # Model tạo text thumbnail
OPENAI_PROMPT_MODEL = "gpt-4o-mini"  # Model tạo prompt ảnh (có thể dùng 4o-mini)

# --- Kích thước Thumbnail Dọc ---
THUMBNAIL_WIDTH = 704
THUMBNAIL_HEIGHT = 1280

# --- Đường dẫn Font ---
# Quan trọng: Đảm bảo các file font này tồn tại đúng đường dẫn
# Ví dụ: tạo thư mục 'font' cùng cấp file script và đặt font vào đó
FONT_PATH = "font/Merriweather-BoldItalic.ttf" # Font cho dòng 1 và 3
FONT_PATH_EMPHASIS = "font/Bangers-Regular.ttf" # Font cho dòng 2 (NHỚ THAY ĐỔI!)

# --- Cấu hình Font Size ---
INITIAL_FONT_SIZE = 100 # Kích thước thử ban đầu (có thể cần điều chỉnh)
MIN_FONT_SIZE = 24     # Kích thước nhỏ nhất

# --- Cấu hình Layout Text (Cho ảnh dọc, text ở dưới) ---
TEXT_HORIZONTAL_PADDING = 40 # Padding trái/phải cho text (tăng lên chút cho thoáng)
TEXT_BOTTOM_MARGIN = 120     # Khoảng cách từ đáy ảnh lên dưới cùng của khối text

# --- Cấu hình Style Chữ ---
STYLE_NORMAL_FILL_COLOR = (255, 255, 255) # Trắng
STYLE_NORMAL_STROKE_COLOR = (0, 0, 0)     # Đen
STYLE_NORMAL_STROKE_WIDTH = 3
STYLE_EMPHASIS_FILL_COLOR = (255, 255, 0) # Vàng (dòng 2)
STYLE_EMPHASIS_STROKE_COLOR = (0, 0, 0)   # Đen
STYLE_EMPHASIS_STROKE_WIDTH = 4           # Dày hơn
MAX_STROKE_WIDTH_FOR_SIZING = max(STYLE_NORMAL_STROKE_WIDTH, STYLE_EMPHASIS_STROKE_WIDTH)

# --- Cấu hình Khoảng cách dòng (Tỉ lệ) ---
LINE_SPACING_FACTOR_NORMAL = 0.2  # 20% chiều cao dòng trên (sau dòng 1)
LINE_SPACING_FACTOR_AFTER_LINE_2 = 0.35 # 35% chiều cao dòng trên (sau dòng 2)

# --- Cấu hình Nền Chữ (Tùy chọn) ---
ADD_TEXT_BACKGROUND = False # Đặt True để bật nền màu sau chữ
TEXT_BACKGROUND_COLOR_SOLID = (0, 0, 0) # Ví dụ: nền đen solid
BACKGROUND_PADDING_X = 20
BACKGROUND_PADDING_Y = 15
# --------------------------------------------------------------------

# --- 2. KIỂM TRA BAN ĐẦU --------------------------------------------
if not OPENAI_API_KEY:
    print("Lỗi: OPENAI_API_KEY chưa được thiết lập trong file .env hoặc biến môi trường.")
    sys.exit(1) # Thoát nếu thiếu key
if not RUNWARE_API_KEY:
    print("Lỗi: RUNWARE_API_KEY chưa được thiết lập trong file .env hoặc biến môi trường.")
    sys.exit(1)

# Kiểm tra sự tồn tại của file font
if not os.path.exists(FONT_PATH):
    print(f"Lỗi: Không tìm thấy file font '{FONT_PATH}'.")
    sys.exit(1)
if not os.path.exists(FONT_PATH_EMPHASIS):
    print(f"Lỗi: Không tìm thấy file font nhấn mạnh '{FONT_PATH_EMPHASIS}'.")
    print("Vui lòng đặt file font vào đúng đường dẫn hoặc sửa biến FONT_PATH_EMPHASIS.")
    sys.exit(1)

openai.api_key = OPENAI_API_KEY
print("Cấu hình và kiểm tra hoàn tất.")
# --------------------------------------------------------------------

# --- 3. CÁC HÀM HELPER -----------------------------------------------

def generate_image_prompt(video_topic_vi):
    """Sử dụng GPT để tạo prompt TIẾNG ANH cho ảnh dọc, không chữ."""
    print(f"\nĐang tạo prompt ảnh (tiếng Anh, dọc, không chữ) cho chủ đề: {video_topic_vi}...")
    try:
        response = openai.chat.completions.create(
            model=OPENAI_PROMPT_MODEL,
            messages=[
                {"role": "system", "content": f"You are a creative assistant generating prompts for an AI image generator to create YouTube thumbnails ({THUMBNAIL_WIDTH}x{THUMBNAIL_HEIGHT}px). The prompt MUST be in ENGLISH. The main subject MUST be clearly positioned on the LEFT HALF, leaving the RIGHT HALF relatively simple/empty for text overlay later. **Crucially, the generated image prompt MUST explicitly specify 'no text, no letters, no words' to prevent text rendering within the image itself.** The style should be engaging and clear."},
                {"role": "user", "content": f"Generate a detailed ENGLISH image prompt for a YouTube thumbnail about this topic: '{video_topic_vi}'. Ensure the main subject (lão tử or trang tử or không tử or gia cát lượng)  is on the 1/3 LEFT side,  **The image itself must contain absolutely NO text, letters, or words.**"}
            ],
            max_tokens=170,
            temperature=0.6
        )
        image_prompt_en = response.choices[0].message.content.strip()
        image_prompt_en = re.sub(r'^"|"$', '', image_prompt_en)
        if "no text" not in image_prompt_en.lower() and "no words" not in image_prompt_en.lower():
             image_prompt_en += ", no text, no words, no letters"
             print("-> Đã tự động thêm 'no text, no words'.")
        print(f"-> Prompt ảnh tiếng Anh: {image_prompt_en}")
        return image_prompt_en
    except Exception as e:
        print(f"Lỗi khi tạo prompt ảnh: {e}")
        print("-> Sử dụng prompt tiếng Anh mặc định.")
        return f"Vertical {THUMBNAIL_WIDTH}x{THUMBNAIL_HEIGHT} visual for '{video_topic_vi}', subject centered or upper half, simple background, youtube thumbnail style, no text, no words, no letters."

def generate_thumbnail_text(video_topic):
    """Sử dụng gpt-4o-mini để tạo 3 dòng text thumbnail LIỀN MẠCH."""
    print(f"\nĐang dùng {OPENAI_TEXT_MODEL} tạo 3 dòng text thumbnail (liền mạch)...")
    try:
        response = openai.chat.completions.create(
            model=OPENAI_TEXT_MODEL,
            messages=[
                 {"role": "system", "content": "Bạn là chuyên gia viết nội dung thumbnail YouTube siêu thu hút. Tạo **chính xác 3 dòng** văn bản tiếng Việt, **VIẾT HOA**. Ba dòng phải **kể chuyện siêu ngắn** hoặc tạo **luồng lập luận liền mạch, KHÔNG RỜI RẠC**. Chúng phải xây dựng sự tò mò qua từng dòng, đỉnh điểm là sự thôi thúc bấm xem. Gợi ý cấu trúc: [Vấn đề/Bối cảnh] -> [Diễn biến/Hậu quả] -> [Câu hỏi/Giải pháp/Gây sốc]. **Tránh** 3 ý riêng biệt. Phân chia ý hợp lý, liên kết chặt, kích thích tột độ. Mỗi dòng ngắn gọn."},
                 {"role": "user", "content": f"Chủ đề video: '{video_topic}'. Tạo 3 dòng text thumbnail liền mạch, liên kết chặt chẽ."}
            ],
            max_tokens=100, temperature=0.75
        )
        raw_text = response.choices[0].message.content.strip()
        print(f"  Text thô từ AI:\n---\n{raw_text}\n---")
        lines = raw_text.split('\n'); text_lines = [line.strip().upper() for line in lines if line.strip()]
        # Đảm bảo 3 dòng
        if len(text_lines) >= 3: final_lines = text_lines[:3]
        elif len(text_lines) == 2 : final_lines = text_lines + [""]; print("Cảnh báo: AI tạo 2 dòng.")
        elif len(text_lines) == 1: final_lines = text_lines + ["", ""]; print("Cảnh báo: AI tạo 1 dòng.")
        else: final_lines = ["LỖI TẠO TEXT", video_topic[:20].upper()+"...", "XEM NGAY!"]; print("Lỗi: AI không tạo text.")
        print(f"-> 3 dòng text cuối cùng: {final_lines}")
        return final_lines
    except Exception as e:
        print(f"Lỗi khi tạo text thumbnail: {e}")
        print("-> Sử dụng text mặc định do lỗi.")
        return ["LỖI API", video_topic[:20].upper()+"...", "THỬ LẠI!"]

def generate_image_with_runware(prompt):
    """Sử dụng Runware AI để tạo ảnh từ prompt tiếng Anh."""
    print(f"\nĐang gọi API Runware AI (Model: {RUNWARE_MODEL})...")
    try:
        task_id = str(uuid.uuid4())
        payload = [
            {"taskType": "authentication", "apiKey": RUNWARE_API_KEY},
            {"taskType": "imageInference", "taskUUID": task_id,
             "positivePrompt": prompt, "width": THUMBNAIL_WIDTH, "height": THUMBNAIL_HEIGHT,
             "model": RUNWARE_MODEL, "numberResults": 1}
             # Có thể thêm "negativePrompt": "text, words, letters, signature, watermark" ở đây nếu API hỗ trợ
        ]
        headers = {'Content-Type': 'application/json'}
        response = requests.post(RUNWARE_API_URL, headers=headers, json=payload, timeout=120) # Tăng timeout
        response.raise_for_status()
        response_data = response.json()
        # print("Phản hồi Runware:", json.dumps(response_data, indent=2)) # Bỏ comment để debug

        image_url = None
        if isinstance(response_data, dict) and 'data' in response_data and isinstance(response_data['data'], list) and len(response_data['data']) > 0:
            first_result = response_data['data'][0]
            if isinstance(first_result, dict) and 'imageURL' in first_result:
                image_url = first_result['imageURL']
        elif isinstance(response_data, dict) and 'errors' in response_data and response_data['errors']:
             print(f"Lỗi từ Runware API: {response_data['errors'][0].get('message', 'Không rõ lỗi')}")
             return None

        if image_url:
            print(f"-> Ảnh đã tạo: {image_url}")
            return image_url
        else:
            print("Lỗi: Không tìm thấy imageURL trong phản hồi Runware.")
            print("Response:", json.dumps(response_data, indent=2))
            return None
    except requests.exceptions.Timeout:
        print("Lỗi: Yêu cầu Runware API bị timeout.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Lỗi kết nối/HTTP khi gọi Runware: {e}")
        if e.response is not None: print("Body:", e.response.text)
        return None
    except Exception as e: print(f"Lỗi không xác định khi tạo ảnh Runware: {e}"); return None

def add_text_to_image(image_url, text_lines, output_path="thumbnail_vertical.jpg"):
    """
    Hàm chính: Tải ảnh, auto-size (2 fonts), canh giữa ngang, đặt dưới,
    style khác nhau, khoảng cách tỉ lệ, (tùy chọn) nền, thêm chữ và lưu.
    """
    print("\nBắt đầu xử lý ảnh và thêm chữ...")
    # --- Bước 1: Chuẩn bị text_lines (Giữ nguyên) ---
    if not isinstance(text_lines, list): print("Lỗi: text_lines phải là list."); return False
    if len(text_lines) < 3: text_lines.extend([""] * (3 - len(text_lines)))
    elif len(text_lines) > 3: text_lines = text_lines[:3]
    line_0 = text_lines[0].strip() if text_lines[0] else None
    line_1 = text_lines[1].strip() if text_lines[1] else None
    line_2 = text_lines[2].strip() if text_lines[2] else None
    valid_lines_normal = [line for line in [line_0, line_2] if line]
    valid_line_emphasis = line_1
    if not valid_lines_normal and not valid_line_emphasis:
        print("Không có text hợp lệ. Lưu ảnh gốc."); # ... (lưu ảnh gốc) ...
        # Thực hiện lưu ảnh gốc nếu cần
        try:
            response=requests.get(image_url); response.raise_for_status()
            img=Image.open(BytesIO(response.content)).convert("RGB")
            img.save(output_path,"JPEG",quality=90); print(f"Ảnh gốc lưu tại: {output_path}")
        except Exception as e: print(f"Lỗi lưu ảnh gốc: {e}")
        return False # Trả về False để báo hiệu không thành công

    # --- Bước 2: Tải ảnh (Giữ nguyên) ---
    try:
        print("Đang tải ảnh...")
        response = requests.get(image_url); response.raise_for_status()
        img = Image.open(BytesIO(response.content)).convert("RGB"); draw = ImageDraw.Draw(img)
    except Exception as e: print(f"Lỗi tải/mở ảnh: {e}"); return False

    # --- Bước 3: Tìm kích thước font (Logic giữ nguyên) ---
    print("Đang tìm kích thước font...")
    final_font_normal = None; final_font_emphasis = None
    final_size_normal = MIN_FONT_SIZE; final_size_emphasis = MIN_FONT_SIZE
    max_allowed_width = THUMBNAIL_WIDTH - (TEXT_HORIZONTAL_PADDING * 2)
    try:
        # Font thường
        if valid_lines_normal:
            current_size_normal = INITIAL_FONT_SIZE; found_normal_size = False
            while current_size_normal >= MIN_FONT_SIZE:
                 font_normal = ImageFont.truetype(FONT_PATH, current_size_normal)
                 max_width_normal = 0
                 for line in valid_lines_normal: bbox = draw.textbbox((0,0),line,font=font_normal,stroke_width=MAX_STROKE_WIDTH_FOR_SIZING); max_width_normal = max(max_width_normal, bbox[2]-bbox[0])
                 if max_width_normal <= max_allowed_width: final_font_normal = font_normal; final_size_normal = current_size_normal; print(f"-> Size font thường: {final_size_normal}"); found_normal_size = True; break
                 current_size_normal -= 2
            if not found_normal_size: final_font_normal = ImageFont.truetype(FONT_PATH, MIN_FONT_SIZE); print(f"Cảnh báo: Font thường dùng size min {MIN_FONT_SIZE}")
        else: final_font_normal = ImageFont.truetype(FONT_PATH, MIN_FONT_SIZE)
        # Font nhấn mạnh
        if valid_line_emphasis:
            current_size_emphasis = INITIAL_FONT_SIZE; found_emphasis_size = False
            while current_size_emphasis >= MIN_FONT_SIZE:
                font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, current_size_emphasis)
                bbox = draw.textbbox((0,0),valid_line_emphasis,font=font_emphasis,stroke_width=MAX_STROKE_WIDTH_FOR_SIZING); width_emphasis = bbox[2]-bbox[0]
                if width_emphasis <= max_allowed_width: final_font_emphasis = font_emphasis; final_size_emphasis = current_size_emphasis; print(f"-> Size font nhấn mạnh: {final_size_emphasis}"); found_emphasis_size = True; break
                current_size_emphasis -= 2
            if not found_emphasis_size: final_font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, MIN_FONT_SIZE); print(f"Cảnh báo: Font nhấn mạnh dùng size min {MIN_FONT_SIZE}")
        else: final_font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, MIN_FONT_SIZE)
    except FileNotFoundError as e: print(f"Lỗi font: {e}."); return False
    except Exception as e: print(f"Lỗi tìm size font: {e}"); return False

    # --- Bước 4: Tính toán chiều cao và vị trí Y (Logic giữ nguyên) ---
    total_text_actual_height = 0; total_gap_height = 0; line_metrics = {}; max_actual_width_final = 0
    if line_0: bbox_0=draw.textbbox((0,0),line_0,font=final_font_normal,stroke_width=STYLE_NORMAL_STROKE_WIDTH); h0=bbox_0[3]-bbox_0[1]; w0=bbox_0[2]-bbox_0[0]; line_metrics[0]={'font':final_font_normal,'height':h0,'width':w0,'text':line_0,'style':'normal'}; total_text_actual_height+=h0; max_actual_width_final=max(max_actual_width_final,w0)
    if line_1: bbox_1=draw.textbbox((0,0),line_1,font=final_font_emphasis,stroke_width=STYLE_EMPHASIS_STROKE_WIDTH); h1=bbox_1[3]-bbox_1[1]; w1=bbox_1[2]-bbox_1[0]; line_metrics[1]={'font':final_font_emphasis,'height':h1,'width':w1,'text':line_1,'style':'emphasis'}; total_text_actual_height+=h1; max_actual_width_final=max(max_actual_width_final,w1)
    if line_2: bbox_2=draw.textbbox((0,0),line_2,font=final_font_normal,stroke_width=STYLE_NORMAL_STROKE_WIDTH); h2=bbox_2[3]-bbox_2[1]; w2=bbox_2[2]-bbox_2[0]; line_metrics[2]={'font':final_font_normal,'height':h2,'width':w2,'text':line_2,'style':'normal'}; total_text_actual_height+=h2; max_actual_width_final=max(max_actual_width_final,w2)
    gap12 = int(line_metrics[0]['height'] * LINE_SPACING_FACTOR_NORMAL) if line_metrics.get(0) and line_metrics.get(1) else 0
    gap23 = int(line_metrics[1]['height'] * LINE_SPACING_FACTOR_AFTER_LINE_2) if line_metrics.get(1) and line_metrics.get(2) else 0
    total_gap_height = gap12 + gap23
    total_text_block_height = total_text_actual_height + total_gap_height
    start_y_block = THUMBNAIL_HEIGHT - TEXT_BOTTOM_MARGIN - total_text_block_height
    if start_y_block < 10: start_y_block = 10 # Đảm bảo không bị âm

    # --- Bước 5: (TÙY CHỌN) Vẽ nền (Logic giữ nguyên) ---
    if ADD_TEXT_BACKGROUND:
        print("Đang vẽ nền chữ...")
        center_x_block=THUMBNAIL_WIDTH//2;start_x_block=center_x_block-(max_actual_width_final/2);end_x_block=center_x_block+(max_actual_width_final/2)
        min_start_x_block=TEXT_HORIZONTAL_PADDING;max_end_x_block=THUMBNAIL_WIDTH-TEXT_HORIZONTAL_PADDING
        if start_x_block<min_start_x_block: start_x_block=min_start_x_block
        if end_x_block>max_end_x_block: end_x_block=max_end_x_block
        start_x_block=max(min_start_x_block, end_x_block-max_actual_width_final)
        bg_x0=max(0,int(start_x_block-BACKGROUND_PADDING_X)); bg_y0=max(0,int(start_y_block-BACKGROUND_PADDING_Y))
        bg_x1=min(THUMBNAIL_WIDTH,int(end_x_block+BACKGROUND_PADDING_X)); bg_y1=min(THUMBNAIL_HEIGHT,int(start_y_block+total_text_block_height+BACKGROUND_PADDING_Y))
        try: draw.rectangle([(bg_x0, bg_y0), (bg_x1, bg_y1)], fill=TEXT_BACKGROUND_COLOR_SOLID); print("-> Đã vẽ nền.")
        except Exception as e: print(f"Lỗi vẽ nền: {e}")

    # --- Bước 6: Vẽ từng dòng chữ (Logic giữ nguyên) ---
    print("Đang vẽ chữ...")
    current_y = start_y_block
    center_x_to_align = THUMBNAIL_WIDTH // 2
    min_start_x_line = TEXT_HORIZONTAL_PADDING
    for i in range(3):
        metrics = line_metrics.get(i)
        if metrics:
            line_text=metrics['text']; line_font=metrics['font']; line_height=metrics['height']; is_emphasis=(metrics['style']=='emphasis')
            fill_color=STYLE_EMPHASIS_FILL_COLOR if is_emphasis else STYLE_NORMAL_FILL_COLOR
            stroke_color=STYLE_EMPHASIS_STROKE_COLOR if is_emphasis else STYLE_NORMAL_STROKE_COLOR
            stroke_width=STYLE_EMPHASIS_STROKE_WIDTH if is_emphasis else STYLE_NORMAL_STROKE_WIDTH
            bbox_draw = draw.textbbox((0,0), line_text, font=line_font, stroke_width=stroke_width); text_width_for_draw = bbox_draw[2]-bbox_draw[0]
            start_x_line = max(min_start_x_line, center_x_to_align - (text_width_for_draw / 2)); start_x_line = int(start_x_line)
            try:
                draw.text((start_x_line, current_y), line_text, font=line_font, fill=stroke_color, stroke_width=stroke_width)
                draw.text((start_x_line, current_y), line_text, font=line_font, fill=fill_color)
            except Exception as e: print(f"Lỗi vẽ dòng {i}: {e}"); continue
            current_y += line_height
            if i == 0 and line_metrics.get(1): current_y += gap12
            elif i == 1 and line_metrics.get(2): current_y += gap23

    # --- Bước 7: Lưu ảnh (Giữ nguyên) ---
    try:
        img.save(output_path, "JPEG", quality=90)
        print(f"-> Thumbnail dọc đã tạo thành công: {output_path}")
        return True # Trả về True nếu thành công
    except Exception as e: print(f"Lỗi khi lưu ảnh cuối cùng: {e}"); return False

# --------------------------------------------------------------------

# --- 4. HÀM MAIN ĐỂ CHẠY SCRIPT --------------------------------------
def main():
    print("--- Bắt đầu tạo Thumbnail Video Dọc ---")
    # 1. Nhập chủ đề video
    video_chu_de = input("Nhập chủ đề video (tiếng Việt): ")
    if not video_chu_de:
        print("Lỗi: Chủ đề không được để trống.")
        return

    # 2. Tạo 3 dòng text thumbnail
    cac_dong_chu = generate_thumbnail_text(video_chu_de)
    if not cac_dong_chu or not any(cac_dong_chu): # Kiểm tra nếu list rỗng hoặc toàn chuỗi rỗng
        print("Không thể tạo text thumbnail, dừng lại.")
        return

    # 3. Tạo prompt ảnh tiếng Anh
    prompt_anh = generate_image_prompt(video_chu_de)
    if not prompt_anh:
        print("Không thể tạo prompt ảnh, dừng lại.")
        return

    # 4. Tạo ảnh bằng Runware AI
    url_anh = generate_image_with_runware(prompt_anh)
    if not url_anh:
        print("Không thể tạo ảnh từ Runware, dừng lại.")
        return

    # 5. Thêm chữ vào ảnh và lưu
    # Tạo tên file output động
    safe_topic = re.sub(r'[^\w\-]+', '_', video_chu_de.lower().strip()) # Làm sạch tên file
    output_filename = f"thumbnail_doc_{safe_topic[:30]}.jpg"

    success = add_text_to_image(url_anh, cac_dong_chu, output_path=output_filename)

    if success:
        print("--- Hoàn thành! ---")
    else:
        print("--- Có lỗi xảy ra trong quá trình tạo thumbnail. ---")

if __name__ == "__main__":
    main()
# --------------------------------------------------------------------