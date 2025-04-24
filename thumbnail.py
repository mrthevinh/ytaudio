import openai
import datetime
import requests
import os
import uuid
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from dotenv import load_dotenv
import json
import re # Thêm thư viện re để xử lý text từ AI



# --- Cấu hình (Giữ nguyên) ---
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
RUNWARE_API_KEY = os.getenv("RUNWARE_API_KEY")

if not OPENAI_API_KEY:
    print("Lỗi: Vui lòng đặt biến môi trường OPENAI_API_KEY")
    exit()
if not RUNWARE_API_KEY:
    print("Lỗi: Vui lòng đặt biến môi trường RUNWARE_API_KEY")
    exit()

openai.api_key = OPENAI_API_KEY

RUNWARE_API_URL = 'https://api.runware.ai/v1'
# RUNWARE_MODEL = "rundiffusion:110@101"
RUNWARE_MODEL = "rundiffusion:130@100"

THUMBNAIL_WIDTH = 1280
THUMBNAIL_HEIGHT = 704 # Giữ nguyên 704 để tương thích Runware (bội số 64)

# --- CẬP NHẬT FONT PATH ---
# Đảm bảo bạn đã tạo thư mục 'font' cùng cấp với file script
# và đặt file 'Merriweather-BoldItalic.ttf' vào trong đó.


# --- CÀI ĐẶT CHO VIỆC TỰ ĐỘNG CHỈNH SIZE CHỮ ---
THUMBNAIL_WIDTH = 1280
THUMBNAIL_HEIGHT = 704

FONT_PATH = "font/chinese/NotoSansTC-VariableFont_wght.ttf" # Font cho dòng 1 và 3
FONT_PATH_EMPHASIS = "font/chinese/NotoSansTC-VariableFont_wght.ttf" # Ví dụ: một font khác

INITIAL_FONT_SIZE = 80
MIN_FONT_SIZE = 30
TEXT_RIGHT_MARGIN = 20
TEXT_AREA_PADDING = 20
STYLE_NORMAL_FILL_COLOR = (255, 255, 255)
STYLE_NORMAL_STROKE_COLOR = (0, 0, 0)
STYLE_NORMAL_STROKE_WIDTH = 5
STYLE_EMPHASIS_FILL_COLOR = (255, 255, 0)
STYLE_EMPHASIS_STROKE_COLOR = (0, 0, 0)
STYLE_EMPHASIS_STROKE_WIDTH = 5
MAX_STROKE_WIDTH_FOR_SIZING = max(STYLE_NORMAL_STROKE_WIDTH, STYLE_EMPHASIS_STROKE_WIDTH)

ADD_TEXT_BACKGROUND = False
TEXT_BACKGROUND_COLOR_SOLID = (230, 230, 230)
BACKGROUND_PADDING_X = 10
BACKGROUND_PADDING_Y = 20
# Hệ số khoảng cách sau dòng 1 (so với chiều cao dòng 1)
# Ví dụ: 0.2 nghĩa là khoảng cách = 20% chiều cao dòng 1
LINE_SPACING_FACTOR_NORMAL = 0.2
# Hệ số khoảng cách sau dòng 2 (so với chiều cao dòng 2)
# Ví dụ: 0.3 nghĩa là khoảng cách = 30% chiều cao dòng 2 (để giãn dòng 2&3 ra)
LINE_SPACING_FACTOR_AFTER_LINE_2 = 0.35 # Tăng lên 35% thử xem

                                 # Tổng khoảng cách sau dòng 2 sẽ là STANDARD + EXTRA
# --- Hàm tạo prompt ảnh (Cập nhật: Yêu cầu KHÔNG CÓ CHỮ trong ảnh) ---
def generate_image_prompt(video_topic_vi):
    """Sử dụng GPT để tạo prompt TIẾNG ANH cho mô hình tạo ảnh, yêu cầu KHÔNG CÓ CHỮ trong ảnh."""
    try:
        print(f"Đang tạo prompt ảnh (tiếng Anh, không chữ) cho chủ đề: {video_topic_vi}...")
        response = openai.chat.completions.create(
            model="gpt-4o-mini", # Hoặc gpt-4o-mini nếu cần
            messages=[
                {"role": "system", "content": f"You are a creative assistant generating prompts for an AI image generator to create YouTube thumbnails ({THUMBNAIL_WIDTH}x{THUMBNAIL_HEIGHT}px). The prompt MUST be in ENGLISH. The main subject MUST be clearly positioned on the LEFT HALF, leaving the RIGHT HALF relatively simple/empty for text overlay later. **Crucially, the generated image prompt MUST explicitly specify 'no text, no letters, no words' to prevent text rendering within the image itself.** The style should be engaging and clear."},
                {"role": "user", "content": f"Generate a detailed ENGLISH image prompt for a YouTube thumbnail about this topic: '{video_topic_vi}'. Ensure the main subject (lão tử or trang tử or không tử or gia cát lượng)  is on the 1/3 LEFT side,  **The image itself must contain absolutely NO text, letters, or words.**"}
            ],
            max_tokens=170 # Tăng nhẹ token để chứa yêu cầu negative
        )
        image_prompt_en = response.choices[0].message.content.strip()
        image_prompt_en = re.sub(r'^"|"$', '', image_prompt_en)

        # Đảm bảo yêu cầu "no text" có trong prompt cuối cùng gửi cho Runware
        if "no text" not in image_prompt_en.lower() and "no words" not in image_prompt_en.lower():
             image_prompt_en += ", no text, no words, no letters" # Thêm vào nếu GPT quên
             print("Đã tự động thêm 'no text, no words, no letters' vào prompt ảnh.")

        print(f"Prompt ảnh tiếng Anh đã tạo: {image_prompt_en}")
        return image_prompt_en
    except Exception as e:
        print(f"Lỗi khi tạo prompt ảnh tiếng Anh bằng OpenAI: {e}")
        print("Sử dụng prompt tiếng Anh mặc định.")
        # Prompt mặc định cũng cần thêm yêu cầu no text
        return f"A compelling visual related to '{video_topic_vi}', main subject on the left, simple background on the right, youtube thumbnail style, no text, no words, no letters."

def generate_thumbnail_text(video_topic, language="Tiếng việT"):
    """Sử dụng gpt-4o-mini để tạo 3 dòng text thumbnail LIỀN MẠCH, liên kết, hấp dẫn."""
    try:
        print(f"Đang dùng gpt-4o-mini tạo 3 dòng text thumbnail (liền mạch) cho chủ đề: {video_topic}...")
        prompt = f"""Tạo 1 tiêu đề ngắn dưới 10 từ, siêu hấp dẫn, gây sốc hoặc tò mò mạnh cho hình thumbnail YouTube về chủ đề: "{video_topic}". QUAN TRỌNG chia tiêu đề này thanh 3 dòng cho hop ly với thumnail
    Ngôn ngữ: {language}
    Yêu cầu: Mỗi tiêu đề trên một dòng. Dùng từ ngữ mạnh, gợi cảm xúc. Không đánh số, không gạch đầu dòng.
    """
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a YouTube thumbnail title expert. Output language must be {language}. Each title on a new line."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100,
            temperature=0.75
        )
        raw_text = response.choices[0].message.content.strip()
        print(f"Text thô từ gpt-4o-mini:\n---\n{raw_text}\n---")

        lines = raw_text.split('\n')
        text_lines = [line.strip().upper() for line in lines if line.strip()]

        # Đảm bảo có đúng 3 dòng (logic giữ nguyên)
        if len(text_lines) >= 3:
            final_lines = text_lines[:3]
        # ... (phần xử lý nếu AI trả về ít hơn 3 dòng giữ nguyên như trước) ...
        elif len(text_lines) == 2 :
             final_lines = text_lines + [""] # Thêm dòng trống nếu chỉ có 2
             print("Cảnh báo: AI chỉ tạo được 2 dòng, đã thêm 1 dòng trống.")
        elif len(text_lines) == 1:
             final_lines = text_lines + ["", ""] # Thêm 2 dòng trống nếu chỉ có 1
             print("Cảnh báo: AI chỉ tạo được 1 dòng, đã thêm 2 dòng trống.")        

        print(f"3 dòng text thumbnail cuối cùng: {final_lines}")
        return final_lines

    except Exception as e:
        print(f"Lỗi khi tạo text thumbnail bằng OpenAI (gpt-4o-mini): {e}")
        print("Sử dụng text mặc định do lỗi.")
        return ["CHUYỆN GÌ ĐÂY?", "BÍ MẬT BỊ TIẾT LỘ!", video_topic[:15].upper()+"..."]

# --- Hàm tạo ảnh bằng Runware AI (Giữ nguyên như phiên bản trước) ---
def generate_image_with_runware(prompt):
    # ... (Code hàm này giữ nguyên như phiên bản cập nhật gần nhất) ...
    try:
        print(f"Đang gọi API Runware AI để tạo ảnh (Model: {RUNWARE_MODEL})...")
        task_id = str(uuid.uuid4())

        payload = [
            {"taskType": "authentication", "apiKey": RUNWARE_API_KEY},
            {
                "taskType": "imageInference",
                "taskUUID": task_id,
                "positivePrompt": prompt,
                "width": THUMBNAIL_WIDTH,
                "height": THUMBNAIL_HEIGHT,
                "model": RUNWARE_MODEL,
                "numberResults": 1
            }
        ]
        headers = {'Content-Type': 'application/json'}

        response = requests.post(RUNWARE_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        response_data = response.json()
        # print("Phản hồi từ Runware API:", json.dumps(response_data, indent=2))

        image_url = None
        if isinstance(response_data, dict) and 'data' in response_data and isinstance(response_data['data'], list) and len(response_data['data']) > 0:
            first_result = response_data['data'][0]
            if isinstance(first_result, dict) and 'imageURL' in first_result:
                image_url = first_result['imageURL']

        if image_url:
            print(f"Ảnh đã được tạo bởi Runware: {image_url}")
            return image_url
        else:
            print("Lỗi: Không tìm thấy 'imageURL' trong cấu trúc phản hồi từ Runware API.")
            print("Cấu trúc response nhận được:", json.dumps(response_data, indent=2))
            return None
    # ... (Phần except giữ nguyên) ...
    except requests.exceptions.RequestException as e:
        print(f"Lỗi kết nối hoặc HTTP khi gọi Runware API: {e}")
        if e.response is not None:
            print("Response status code:", e.response.status_code)
            try:
                print("Response body:", e.response.json())
            except json.JSONDecodeError:
                print("Response body:", e.response.text)
        return None
    except json.JSONDecodeError:
         print("Lỗi: Không thể phân tích JSON từ phản hồi của Runware API.")
         # print("Response text:", response.text) # Bỏ comment nếu cần debug
         return None
    except Exception as e:
        print(f"Lỗi không xác định khi tạo ảnh bằng Runware: {e}")
        return None



def add_text_to_image(image_url, text_lines, output_path="thumbnail_final.jpg"):
    """
    Tải ảnh, auto-size (2 fonts), canh giữa 2/3 phải, style khác nhau,
    khoảng cách dòng tỉ lệ, (tùy chọn) nền, thêm chữ và lưu.
    """
    # --- Bước 1: Chuẩn bị text_lines (Giữ nguyên) ---
    if not isinstance(text_lines, list): print("Lỗi: text_lines phải là list."); return
    if len(text_lines) < 3: text_lines.extend([""] * (3 - len(text_lines)))
    elif len(text_lines) > 3: text_lines = text_lines[:3]
    line_0 = text_lines[0].strip() if text_lines[0] else None
    line_1 = text_lines[1].strip() if text_lines[1] else None
    line_2 = text_lines[2].strip() if text_lines[2] else None
    valid_lines_normal = [line for line in [line_0, line_2] if line]
    valid_line_emphasis = line_1
    if not valid_lines_normal and not valid_line_emphasis:
        print("Không có dòng text hợp lệ. Lưu ảnh gốc."); # ... (lưu ảnh gốc) ...
        return

    # --- Bước 2: Tải ảnh (Giữ nguyên) ---
    try:
        response = requests.get(image_url); response.raise_for_status()
        img = Image.open(BytesIO(response.content)).convert("RGB"); draw = ImageDraw.Draw(img)
    except Exception as e: print(f"Lỗi tải/mở ảnh: {e}"); return

    # --- Bước 3: Tìm kích thước font (Giữ nguyên logic auto-size 2 font) ---
    print("Đang tìm kích thước font phù hợp...")
    final_font_normal = None; final_font_emphasis = None
    final_size_normal = MIN_FONT_SIZE; final_size_emphasis = MIN_FONT_SIZE
    max_allowed_width = (THUMBNAIL_WIDTH * 2 // 3) - TEXT_AREA_PADDING - TEXT_RIGHT_MARGIN
    try:
        # Auto-size font thường
        if valid_lines_normal:
            # ... (vòng lặp while cho font thường) ...
            current_size_normal = INITIAL_FONT_SIZE; found_normal_size = False
            while current_size_normal >= MIN_FONT_SIZE:
                 font_normal = ImageFont.truetype(FONT_PATH, current_size_normal)
                 max_width_normal = 0
                 for line in valid_lines_normal:
                     bbox = draw.textbbox((0,0),line,font=font_normal,stroke_width=MAX_STROKE_WIDTH_FOR_SIZING)
                     max_width_normal = max(max_width_normal, bbox[2]-bbox[0])
                 if max_width_normal <= max_allowed_width:
                     final_font_normal = font_normal; final_size_normal = current_size_normal
                     print(f"==> Chọn size font thường: {final_size_normal}"); found_normal_size = True; break
                 current_size_normal -= 2
            if not found_normal_size: final_font_normal = ImageFont.truetype(FONT_PATH, MIN_FONT_SIZE)
        else: final_font_normal = ImageFont.truetype(FONT_PATH, MIN_FONT_SIZE) # Load size min nếu không dùng

        # Auto-size font nhấn mạnh
        if valid_line_emphasis:
            # ... (vòng lặp while cho font nhấn mạnh) ...
            current_size_emphasis = INITIAL_FONT_SIZE; found_emphasis_size = False
            while current_size_emphasis >= MIN_FONT_SIZE:
                 font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, current_size_emphasis)
                 bbox = draw.textbbox((0,0),valid_line_emphasis,font=font_emphasis,stroke_width=MAX_STROKE_WIDTH_FOR_SIZING)
                 width_emphasis = bbox[2]-bbox[0]
                 if width_emphasis <= max_allowed_width:
                     final_font_emphasis = font_emphasis; final_size_emphasis = current_size_emphasis
                     print(f"==> Chọn size font nhấn mạnh: {final_size_emphasis}"); found_emphasis_size = True; break
                 current_size_emphasis -= 2
            if not found_emphasis_size: final_font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, MIN_FONT_SIZE)
        else: final_font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, MIN_FONT_SIZE) # Load size min nếu không dùng

    except FileNotFoundError as e: print(f"Lỗi: Không tìm thấy font: {e}."); return
    except Exception as e: print(f"Lỗi tìm kích thước font: {e}"); return

    # --- Bước 4: Tính toán chiều cao và vị trí Y (Dùng khoảng cách tỉ lệ) ---
    total_text_actual_height = 0 # Tổng chiều cao chỉ của các dòng chữ
    total_gap_height = 0       # Tổng chiều cao các khoảng trống
    line_metrics = {}          # Lưu metrics các dòng có chữ
    max_actual_width_final = 0 # Width lớn nhất để vẽ nền

    # Tính metrics cho từng dòng có chữ
    if line_0:
        bbox_0 = draw.textbbox((0,0), line_0, font=final_font_normal, stroke_width=STYLE_NORMAL_STROKE_WIDTH)
        h0 = bbox_0[3]-bbox_0[1]; w0 = bbox_0[2]-bbox_0[0]
        line_metrics[0] = {'font': final_font_normal, 'height': h0, 'width': w0, 'text': line_0, 'style': 'normal'}
        total_text_actual_height += h0; max_actual_width_final = max(max_actual_width_final, w0)
    if line_1:
        bbox_1 = draw.textbbox((0,0), line_1, font=final_font_emphasis, stroke_width=STYLE_EMPHASIS_STROKE_WIDTH)
        h1 = bbox_1[3]-bbox_1[1]; w1 = bbox_1[2]-bbox_1[0]
        line_metrics[1] = {'font': final_font_emphasis, 'height': h1, 'width': w1, 'text': line_1, 'style': 'emphasis'}
        total_text_actual_height += h1; max_actual_width_final = max(max_actual_width_final, w1)
    if line_2:
        bbox_2 = draw.textbbox((0,0), line_2, font=final_font_normal, stroke_width=STYLE_NORMAL_STROKE_WIDTH)
        h2 = bbox_2[3]-bbox_2[1]; w2 = bbox_2[2]-bbox_2[0]
        line_metrics[2] = {'font': final_font_normal, 'height': h2, 'width': w2, 'text': line_2, 'style': 'normal'}
        total_text_actual_height += h2; max_actual_width_final = max(max_actual_width_final, w2)

    # --- THAY ĐỔI: Tính tổng khoảng cách tỉ lệ ---
    gap12 = 0 # Khoảng cách sau dòng 0 (nếu dòng 1 tồn tại)
    gap23 = 0 # Khoảng cách sau dòng 1 (nếu dòng 2 tồn tại)
    if line_metrics.get(0) and line_metrics.get(1): # Nếu có dòng 0 và 1
        gap12 = int(line_metrics[0]['height'] * LINE_SPACING_FACTOR_NORMAL)
        total_gap_height += gap12
    if line_metrics.get(1) and line_metrics.get(2): # Nếu có dòng 1 và 2
        gap23 = int(line_metrics[1]['height'] * LINE_SPACING_FACTOR_AFTER_LINE_2)
        total_gap_height += gap23

    total_text_block_height = total_text_actual_height + total_gap_height
    start_y_block = max(BACKGROUND_PADDING_Y, (THUMBNAIL_HEIGHT - total_text_block_height) // 2)
    # --- Kết thúc thay đổi tính khoảng cách ---

    # --- Bước 5: (TÙY CHỌN) Vẽ nền chữ nhật (Logic giữ nguyên) ---
    if ADD_TEXT_BACKGROUND:
        # ... (Code vẽ nền giữ nguyên như trước, sử dụng total_text_block_height đã tính) ...
        center_of_text_area_x=THUMBNAIL_WIDTH*2//3;start_x_block=center_of_text_area_x-(max_actual_width_final/2);end_x_block=center_of_text_area_x+(max_actual_width_final/2)
        min_start_x_block=THUMBNAIL_WIDTH//3+TEXT_AREA_PADDING;max_end_x_block=THUMBNAIL_WIDTH-TEXT_RIGHT_MARGIN
        if start_x_block<min_start_x_block: start_x_block=min_start_x_block
        if end_x_block>max_end_x_block: end_x_block=max_end_x_block
        start_x_block=max(min_start_x_block, end_x_block-max_actual_width_final)
        bg_x0=max(0,int(start_x_block-BACKGROUND_PADDING_X));bg_y0=max(0,int(start_y_block-BACKGROUND_PADDING_Y))
        bg_x1=min(THUMBNAIL_WIDTH,int(end_x_block+BACKGROUND_PADDING_X));bg_y1=min(THUMBNAIL_HEIGHT,int(start_y_block+total_text_block_height+BACKGROUND_PADDING_Y))
        try: draw.rectangle([(bg_x0, bg_y0), (bg_x1, bg_y1)], fill=TEXT_BACKGROUND_COLOR_SOLID); print(f"Đã vẽ nền.")
        except Exception as e: print(f"Lỗi vẽ nền: {e}")


    # --- Bước 6: Vẽ từng dòng chữ (Dùng khoảng cách tỉ lệ) ---
    print("Đang vẽ chữ (khoảng cách tỉ lệ)...")
    current_y = start_y_block
    center_x_to_align = THUMBNAIL_WIDTH * 2 // 3
    min_start_x_line = THUMBNAIL_WIDTH // 3 + TEXT_AREA_PADDING

    for i in range(3): # Duyệt qua 3 vị trí dòng
        metrics = line_metrics.get(i)
        if metrics:
            line_text = metrics['text']; line_font = metrics['font']; line_height = metrics['height']
            is_emphasis = (metrics['style'] == 'emphasis')
            fill_color=STYLE_EMPHASIS_FILL_COLOR if is_emphasis else STYLE_NORMAL_FILL_COLOR
            stroke_color=STYLE_EMPHASIS_STROKE_COLOR if is_emphasis else STYLE_NORMAL_STROKE_COLOR
            stroke_width=STYLE_EMPHASIS_STROKE_WIDTH if is_emphasis else STYLE_NORMAL_STROKE_WIDTH

            bbox_draw = draw.textbbox((0,0), line_text, font=line_font, stroke_width=stroke_width)
            text_width_for_draw = bbox_draw[2] - bbox_draw[0]
            start_x_line = max(min_start_x_line, center_x_to_align - (text_width_for_draw / 2))
            start_x_line = int(start_x_line)

            # Vẽ
            try:
                draw.text((start_x_line, current_y), line_text, font=line_font, fill=stroke_color, stroke_width=stroke_width)
                draw.text((start_x_line, current_y), line_text, font=line_font, fill=fill_color)
            except Exception as e: print(f"Lỗi vẽ dòng {i}: {e}"); continue

            # --- THAY ĐỔI: Cập nhật Y với khoảng cách tỉ lệ ---
            current_y += line_height # Cộng chiều cao dòng vừa vẽ
            # Cộng thêm khoảng cách tỉ lệ nếu đây không phải dòng cuối cùng có chữ
            if i == 0 and line_metrics.get(1):      # Nếu vừa vẽ dòng 0 và có dòng 1
                 current_y += gap12 # Cộng khoảng cách đã tính trước đó
            elif i == 1 and line_metrics.get(2):    # Nếu vừa vẽ dòng 1 và có dòng 2
                 current_y += gap23 # Cộng khoảng cách đã tính trước đó
            # --- Kết thúc thay đổi cập nhật Y ---

    # --- Bước 7: Lưu ảnh kết quả (Giữ nguyên) ---
    try:
        img.save(output_path, "JPEG", quality=90)
        print(f"Thumbnail đã được tạo và lưu thành công tại: {output_path}")
    except Exception as e: print(f"Lỗi khi lưu ảnh cuối cùng: {e}")



if __name__ == "__main__":
    # video_chu_de = input("Nhập chủ đề / câu chuyện của video: ")
    video_chu_de = "Sống Vì Chính Mình: Khám Phá Triết Lý Cổ Nhân Nửa Sau Cuộc Đời"

    if not video_chu_de:
        print("Lỗi: Chủ đề video là bắt buộc.")
    else:
        # 1. Tạo text thumbnail (Dùng OpenAI gpt-4o-mini) <<< THAY ĐỔI Ở ĐÂY
        cac_dong_chu = generate_thumbnail_text(video_chu_de)

        # 2. Tạo prompt ảnh (Dùng OpenAI model khác hoặc gpt-4o-mini)
        prompt_anh = generate_image_prompt(video_chu_de)

        if prompt_anh and cac_dong_chu: # Chỉ tiếp tục nếu có cả prompt ảnh và text
            # 3. Tạo ảnh (Dùng Runware AI)
            url_anh = generate_image_with_runware(prompt_anh)

            if url_anh:
                # 4. Thêm chữ (đã tạo tự động) vào ảnh và lưu
                now = datetime.datetime.now()
                ngay_thang_gio_phut_giay = now.strftime("%Y%m%d_%H%M%S")
                ten_file_output = f"thumbnail_{video_chu_de.replace(' ','_')[:20]}_{ngay_thang_gio_phut_giay}.jpg"
                output_path = os.path.join(os.getcwd(), "thumbnails", ten_file_output)
                
                # ten_file_output = f"thumbnail_{video_chu_de.replace(' ','_')[:20]}.jpg"
                add_text_to_image(url_anh, cac_dong_chu, output_path=output_path)