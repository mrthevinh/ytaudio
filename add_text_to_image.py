from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import requests
import os
import math

THUMBNAIL_WIDTH = 1280
THUMBNAIL_HEIGHT = 704

INITIAL_FONT_SIZE = 80
MIN_FONT_SIZE = 20
TEXT_RIGHT_MARGIN = 30
TEXT_AREA_PADDING = 30
STYLE_NORMAL_FILL_COLOR = (255, 255, 255)
STYLE_NORMAL_STROKE_COLOR = (0, 0, 0)
STYLE_NORMAL_STROKE_WIDTH = 3
STYLE_EMPHASIS_FILL_COLOR = (255, 255, 0)
STYLE_EMPHASIS_STROKE_COLOR = (0, 0, 0)
STYLE_EMPHASIS_STROKE_WIDTH = 4
MAX_STROKE_WIDTH_FOR_SIZING = max(STYLE_NORMAL_STROKE_WIDTH, STYLE_EMPHASIS_STROKE_WIDTH)

FONT_PATH = "font/chinese/KosugiMaru-Regular.ttf" # Font cho dòng 1 và 3
FONT_PATH_EMPHASIS = "ffont/chinese/NotoSansTC-VariableFont_wght.ttf" # Ví dụ: một font khác

# --- Cấu hình màu sắc/style (Giữ nguyên) ---
STYLE_NORMAL_FILL_COLOR = (255, 255, 255)
STYLE_NORMAL_STROKE_COLOR = (0, 0, 0)
STYLE_NORMAL_STROKE_WIDTH = 3
STYLE_EMPHASIS_FILL_COLOR = (255, 255, 0) # Màu vàng cho dòng 2
STYLE_EMPHASIS_STROKE_COLOR = (0, 0, 0)
STYLE_EMPHASIS_STROKE_WIDTH = 4
MAX_STROKE_WIDTH_FOR_SIZING = max(STYLE_NORMAL_STROKE_WIDTH, STYLE_EMPHASIS_STROKE_WIDTH)

# --- Cấu hình nền chữ (Giữ nguyên) ---
ADD_TEXT_BACKGROUND = False
TEXT_BACKGROUND_COLOR_SOLID = (230, 230, 230)
BACKGROUND_PADDING_X = 20
BACKGROUND_PADDING_Y = 10
def add_text_to_image(image_url, text_lines, output_path="thumbnail_final.jpg"):
    """
    Tải ảnh, tự động chỉnh size (với 2 font khác nhau), canh giữa 2/3 phải,
    áp dụng style khác nhau, (tùy chọn) vẽ nền, thêm chữ và lưu.
    """
    # --- Bước 1: Chuẩn bị text_lines (Giữ nguyên) ---
    if not isinstance(text_lines, list): print("Lỗi: text_lines phải là list."); return
    if len(text_lines) < 3: text_lines.extend([""] * (3 - len(text_lines)))
    elif len(text_lines) > 3: text_lines = text_lines[:3]

    # Tách các dòng ra để xử lý font riêng
    line_0 = text_lines[0].strip() if text_lines[0] else None
    line_1 = text_lines[1].strip() if text_lines[1] else None # Dòng nhấn mạnh
    line_2 = text_lines[2].strip() if text_lines[2] else None

    valid_lines_normal = [line for line in [line_0, line_2] if line]
    valid_line_emphasis = line_1

    if not valid_lines_normal and not valid_line_emphasis:
        print("Không có dòng text nào hợp lệ. Chỉ lưu ảnh gốc.")
        # ... (Code lưu ảnh gốc) ...
        return

    # --- Bước 2: Tải ảnh và chuẩn bị vẽ (Giữ nguyên) ---
    try:
        # ... (Code tải ảnh) ...
        response = requests.get(image_url); response.raise_for_status()
        img = Image.open(BytesIO(response.content)).convert("RGB"); draw = ImageDraw.Draw(img)
    except Exception as e: print(f"Lỗi tải/mở ảnh: {e}"); return

    # --- Bước 3: Tìm kích thước font phù hợp (RIÊNG BIỆT cho 2 font) ---
    print("Đang tìm kích thước font phù hợp...")
    final_font_normal = None
    final_font_emphasis = None
    final_size_normal = MIN_FONT_SIZE # Giá trị mặc định nếu không tìm thấy
    final_size_emphasis = MIN_FONT_SIZE # Giá trị mặc định

    # Chiều rộng tối đa cho phép trong khu vực 2/3 phải
    max_allowed_width = (THUMBNAIL_WIDTH * 2 // 3) - TEXT_AREA_PADDING - TEXT_RIGHT_MARGIN

    try:
        # --- Auto-size cho font THƯỜNG (dòng 0 và 2) ---
        if valid_lines_normal:
            current_size_normal = INITIAL_FONT_SIZE
            found_normal_size = False
            while current_size_normal >= MIN_FONT_SIZE:
                font_normal = ImageFont.truetype(FONT_PATH, current_size_normal)
                max_width_normal = 0
                for line in valid_lines_normal:
                    bbox = draw.textbbox((0, 0), line, font=font_normal, stroke_width=MAX_STROKE_WIDTH_FOR_SIZING)
                    max_width_normal = max(max_width_normal, bbox[2] - bbox[0])

                if max_width_normal <= max_allowed_width:
                    final_font_normal = font_normal
                    final_size_normal = current_size_normal
                    print(f"==> Chọn size font thường (dòng 1&3): {final_size_normal}")
                    found_normal_size = True
                    break
                current_size_normal -= 2
            if not found_normal_size:
                 print(f"Cảnh báo: Font thường quá rộng, dùng size nhỏ nhất: {MIN_FONT_SIZE}")
                 final_font_normal = ImageFont.truetype(FONT_PATH, MIN_FONT_SIZE)
        else:
             # Nếu không có dòng 0 và 2, vẫn cần load font thường với size nhỏ nhất để dùng nếu cần
             final_font_normal = ImageFont.truetype(FONT_PATH, MIN_FONT_SIZE)


        # --- Auto-size cho font NHẤN MẠNH (dòng 1) ---
        if valid_line_emphasis:
             current_size_emphasis = INITIAL_FONT_SIZE
             found_emphasis_size = False
             while current_size_emphasis >= MIN_FONT_SIZE:
                 font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, current_size_emphasis)
                 bbox = draw.textbbox((0, 0), valid_line_emphasis, font=font_emphasis, stroke_width=MAX_STROKE_WIDTH_FOR_SIZING)
                 width_emphasis = bbox[2] - bbox[0]

                 if width_emphasis <= max_allowed_width:
                     final_font_emphasis = font_emphasis
                     final_size_emphasis = current_size_emphasis
                     print(f"==> Chọn size font nhấn mạnh (dòng 2): {final_size_emphasis}")
                     found_emphasis_size = True
                     break
                 current_size_emphasis -= 2
             if not found_emphasis_size:
                 print(f"Cảnh báo: Font nhấn mạnh quá rộng, dùng size nhỏ nhất: {MIN_FONT_SIZE}")
                 final_font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, MIN_FONT_SIZE)
        else:
             # Nếu không có dòng 1, vẫn cần load font nhấn mạnh với size nhỏ nhất
             final_font_emphasis = ImageFont.truetype(FONT_PATH_EMPHASIS, MIN_FONT_SIZE)


    except FileNotFoundError as e: print(f"Lỗi: Không tìm thấy file font: {e}."); return
    except Exception as e: print(f"Lỗi tìm kích thước font: {e}"); return

    # --- Bước 4: Tính toán chiều cao và vị trí Y (Dùng font tương ứng) ---
    total_text_height = 0
    line_metrics = {}
    max_actual_width_final = 0 # Width lớn nhất của cả khối text

    # Tính metrics cho dòng 0 (nếu có)
    if line_0:
        bbox_0 = draw.textbbox((0,0), line_0, font=final_font_normal, stroke_width=STYLE_NORMAL_STROKE_WIDTH)
        h0 = bbox_0[3] - bbox_0[1]; w0 = bbox_0[2] - bbox_0[0]
        line_metrics[0] = {'font': final_font_normal, 'height': h0, 'width': w0, 'text': line_0, 'style': 'normal'}
        total_text_height += h0 + 10
        max_actual_width_final = max(max_actual_width_final, w0)

    # Tính metrics cho dòng 1 (nếu có)
    if line_1:
        bbox_1 = draw.textbbox((0,0), line_1, font=final_font_emphasis, stroke_width=STYLE_EMPHASIS_STROKE_WIDTH)
        h1 = bbox_1[3] - bbox_1[1]; w1 = bbox_1[2] - bbox_1[0]
        line_metrics[1] = {'font': final_font_emphasis, 'height': h1, 'width': w1, 'text': line_1, 'style': 'emphasis'}
        total_text_height += h1 + 10
        max_actual_width_final = max(max_actual_width_final, w1)

    # Tính metrics cho dòng 2 (nếu có)
    if line_2:
        bbox_2 = draw.textbbox((0,0), line_2, font=final_font_normal, stroke_width=STYLE_NORMAL_STROKE_WIDTH)
        h2 = bbox_2[3] - bbox_2[1]; w2 = bbox_2[2] - bbox_2[0]
        line_metrics[2] = {'font': final_font_normal, 'height': h2, 'width': w2, 'text': line_2, 'style': 'normal'}
        total_text_height += h2 + 10
        max_actual_width_final = max(max_actual_width_final, w2)

    total_text_block_height = total_text_height - 10 if total_text_height > 0 else 0
    start_y_block = max(BACKGROUND_PADDING_Y, (THUMBNAIL_HEIGHT - total_text_block_height) // 2)

    # --- Bước 5: (TÙY CHỌN) Vẽ nền chữ nhật (Logic giữ nguyên, dùng max_actual_width_final) ---
    if ADD_TEXT_BACKGROUND:
        # ... (Code vẽ nền giữ nguyên như trước, sử dụng max_actual_width_final đã tính ở trên) ...
        center_of_text_area_x = THUMBNAIL_WIDTH * 2 // 3
        start_x_block = center_of_text_area_x - (max_actual_width_final / 2)
        end_x_block = center_of_text_area_x + (max_actual_width_final / 2)
        min_start_x_block = THUMBNAIL_WIDTH // 3 + TEXT_AREA_PADDING
        max_end_x_block = THUMBNAIL_WIDTH - TEXT_RIGHT_MARGIN
        if start_x_block < min_start_x_block: start_x_block = min_start_x_block
        if end_x_block > max_end_x_block: end_x_block = max_end_x_block
        start_x_block = max(min_start_x_block, end_x_block - max_actual_width_final)
        # Tính tọa độ hộp nền bg_x0, bg_y0, bg_x1, bg_y1 và vẽ...
        bg_x0=int(start_x_block-BACKGROUND_PADDING_X); bg_y0=int(start_y_block-BACKGROUND_PADDING_Y)
        bg_x1=int(end_x_block+BACKGROUND_PADDING_X); bg_y1=int(start_y_block+total_text_block_height+BACKGROUND_PADDING_Y)
        bg_x0=max(0,bg_x0); bg_y0=max(0,bg_y0); bg_x1=min(THUMBNAIL_WIDTH,bg_x1); bg_y1=min(THUMBNAIL_HEIGHT,bg_y1)
        try:
            draw.rectangle([(bg_x0, bg_y0), (bg_x1, bg_y1)], fill=TEXT_BACKGROUND_COLOR_SOLID)
            print(f"Đã vẽ nền từ ({bg_x0},{bg_y0}) đến ({bg_x1},{bg_y1})")
        except Exception as e: print(f"Lỗi vẽ nền chữ nhật: {e}")


    # --- Bước 6: Vẽ từng dòng chữ (Sử dụng font và metrics tương ứng) ---
    print("Đang vẽ chữ (2 fonts, canh giữa 2/3 phải)...")
    current_y = start_y_block
    center_x_to_align = THUMBNAIL_WIDTH * 2 // 3
    min_start_x_line = THUMBNAIL_WIDTH // 3 + TEXT_AREA_PADDING

    for i in range(3): # Duyệt qua 3 vị trí dòng
        metrics = line_metrics.get(i)
        if metrics: # Nếu dòng này có metrics (tức là có chữ)
            line_text = metrics['text']
            line_font = metrics['font']
            line_height = metrics['height']
            # line_width = metrics['width'] # Width này là với stroke chuẩn, cần tính lại với stroke thực tế

            # Chọn style
            is_emphasis = (metrics['style'] == 'emphasis') # i == 1
            fill_color = STYLE_EMPHASIS_FILL_COLOR if is_emphasis else STYLE_NORMAL_FILL_COLOR
            stroke_color = STYLE_EMPHASIS_STROKE_COLOR if is_emphasis else STYLE_NORMAL_STROKE_COLOR
            stroke_width = STYLE_EMPHASIS_STROKE_WIDTH if is_emphasis else STYLE_NORMAL_STROKE_WIDTH

            # Tính lại width với stroke của dòng này để canh giữa
            bbox_draw = draw.textbbox((0,0), line_text, font=line_font, stroke_width=stroke_width)
            text_width_for_draw = bbox_draw[2] - bbox_draw[0]

            # Tính start_x canh giữa
            start_x_line = center_x_to_align - (text_width_for_draw / 2)
            if start_x_line < min_start_x_line: start_x_line = min_start_x_line
            start_x_line = int(start_x_line)

            # Vẽ
            try:
                draw.text((start_x_line, current_y), line_text, font=line_font, fill=stroke_color, stroke_width=stroke_width)
                draw.text((start_x_line, current_y), line_text, font=line_font, fill=fill_color)
            except Exception as e: print(f"Lỗi vẽ dòng {i}: {e}"); continue

            current_y += line_height + 10 # Di chuyển Y cho dòng tiếp theo (nếu có)


    # --- Bước 7: Lưu ảnh kết quả (Giữ nguyên) ---
    try:
        img.save(output_path, "JPEG", quality=90)
        print(f"Thumbnail đã được tạo và lưu thành công tại: {output_path}")
    except Exception as e: print(f"Lỗi khi lưu ảnh cuối cùng: {e}")

# --- Kết thúc hàm add_text_to_image ---