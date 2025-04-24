# outline_parser.py
import logging
import json
import re
from markdown_it import MarkdownIt
from collections import Counter
# Import utils nếu tách hàm detect_indent
# from utils import detect_indent_settings, calculate_indent_level

# --- Keywords (Có thể đưa vào file config) ---
INTRO_KEYWORDS = ['intro', 'mở đầu', 'giới thiệu', '引言']
OUTRO_KEYWORDS = ['outro', 'kết luận', 'conclusion', '结论', 'tổng kết', 'cta', 'call to action']
QUOTE_KEYWORDS = ['quote', 'trích dẫn', 'danh ngôn', '名言', '"']
STORY_KEYWORDS = ['story', 'câu chuyện', 'ví dụ', 'example', '故事', '例子']

def parse_outline_markdown(markdown_text):
    """
    Phân tích outline định dạng Markdown thành cấu trúc dữ liệu lồng nhau.
    Sử dụng thư viện markdown-it-py.
    """
    logging.info("--- Starting parse_outline_markdown ---")
    if not markdown_text or not isinstance(markdown_text, str):
        logging.error("Invalid or empty markdown_text provided.")
        return None

    try:
        md = MarkdownIt()
        tokens = md.parse(markdown_text)
    except Exception as e:
        logging.error(f"Markdown parsing failed: {e}", exc_info=True)
        return None

    # logging.debug(f"Markdown Tokens: {tokens}")

    # Cấu trúc cây trung gian
    tree = {'level': -1, 'items': []} # Nút gốc ảo
    stack = [tree] # Stack chứa các node cha (dictionary)

    i = 0
    while i < len(tokens):
        token = tokens[i]
        current_parent = stack[-1]
        current_parent_level = current_parent.get('level', -1)

        logging.debug(f"Token[{i}]: {token.type} / Tag:{token.tag} / Level:{token.level} / Content: {token.content[:50] if token.content else ''}")

        if token.type == 'heading_open':
            level = int(token.tag[1]) # h1=1, h2=2,...
            # Tìm cha phù hợp trên stack
            while stack and stack[-1].get('level', -1) >= level:
                stack.pop()
                logging.debug(f"Popped stack for heading {level}. Stack size: {len(stack)}")

            parent_node = stack[-1] if stack else tree # Nếu stack rỗng, cha là gốc ảo

            # Lấy nội dung heading
            title = ""
            if i + 1 < len(tokens) and tokens[i+1].type == 'inline':
                title = tokens[i+1].content.strip()
                i += 1
            if i + 2 < len(tokens) and tokens[i+2].type == 'heading_close':
                i += 1

            if title:
                # Tạo node mới
                new_node = {'level': level, 'title': title, 'content': title, 'items': [], 'type': 'point'}

                # Phân loại type
                lower_title = title.lower()
                if level <= 2: # H1, H2
                    if any(kw in lower_title for kw in INTRO_KEYWORDS): new_node['type'] = 'intro'
                    elif any(kw in lower_title for kw in OUTRO_KEYWORDS): new_node['type'] = 'outro'
                    else: new_node['type'] = 'section_header'
                else: # H3+
                    if any(kw in lower_title for kw in QUOTE_KEYWORDS): new_node['type'] = 'quote_suggestion'
                    elif any(kw in lower_title for kw in STORY_KEYWORDS): new_node['type'] = 'story_suggestion'

                parent_node.setdefault('items', []).append(new_node) # Thêm vào cha
                stack.append(new_node) # Đẩy node mới vào stack
                logging.debug(f"{'  '*level}Added Heading Node (L{level}, Type:{new_node['type']}): '{title[:50]}...'")

            else:
                 logging.warning(f"Heading token found at index {i} but no inline content followed.")


        elif token.type == 'bullet_list_open' or token.type == 'ordered_list_open':
            # Có thể dùng level của token để xử lý list lồng nhau, nhưng hiện tại bỏ qua
            logging.debug(f"List open at token level {token.level}")
            pass # Chỉ cần xử lý list_item_open

        elif token.type == 'list_item_open':
            # Tìm cha gần nhất trên stack
            while stack and stack[-1].get('level', -1) >= token.level: # Dùng token.level để tìm cha
                 stack.pop()
            parent_node = stack[-1] if stack else tree

            # Lấy nội dung list item (thường là inline bên trong paragraph)
            content = ""
            j = i + 1
            temp_level = token.level # Lưu level của list item
            while j < len(tokens) and tokens[j].type != 'list_item_close':
                if tokens[j].type == 'inline':
                    content += tokens[j].content.strip() + " "
                j += 1
            content = content.strip()
            i = j # Cập nhật index i

            if content:
                 # Tạo node mới
                 list_item_node = {'level': temp_level, 'title': content, 'content': content, 'items': [], 'type': 'point'}
                 # Phân loại type
                 lower_content = content.lower()
                 if any(kw in lower_content for kw in QUOTE_KEYWORDS): list_item_node['type'] = 'quote_suggestion'
                 elif any(kw in lower_content for kw in STORY_KEYWORDS): list_item_node['type'] = 'story_suggestion'

                 parent_node.setdefault('items', []).append(list_item_node)
                 # Không đẩy list item vào stack trừ khi nó có thể chứa heading/list con
                 logging.debug(f"{'  '*temp_level}Added List Item (L{temp_level}, Type:{list_item_node['type']}): '{content[:50]}...'")

            else:
                 logging.warning(f"List item open token found at index {i} but no inline content followed.")


        elif token.type == 'paragraph_open':
             # Xử lý paragraph độc lập (không nằm trong list item)
             if i + 1 < len(tokens) and tokens[i+1].type == 'inline':
                content = tokens[i+1].content.strip()
                i += 1 # Skip inline
                if i + 2 < len(tokens) and tokens[i+2].type == 'paragraph_close':
                    i += 1 # Skip close

                if content:
                    # Tìm cha gần nhất
                    while stack and stack[-1].get('level', -1) >= token.level:
                         stack.pop()
                    parent_node = stack[-1] if stack else tree

                    para_level = token.level
                    para_node = {'level': para_level, 'title': content, 'content': content, 'items':[], 'type':'point'}

                    parent_node.setdefault('items', []).append(para_node)
                    logging.debug(f"{'  '*para_level}Added Paragraph Item (L{para_level}, Type:{para_node['type']}): '{content[:50]}...'")
             else:
                  # Paragraph rỗng hoặc chỉ có thẻ mở/đóng
                  if i + 1 < len(tokens) and tokens[i+1].type == 'paragraph_close': i += 1


        i += 1 # Chuyển sang token tiếp theo

    # --- Trích xuất cấu trúc cuối cùng ---
    outline_data = {'intro': None, 'sections': [], 'outro': None}
    if tree['items']:
         # Kiểm tra item đầu tiên có phải intro không
         first_item = tree['items'][0]
         if first_item.get('type') == 'intro':
              outline_data['intro'] = first_item # Lưu cả cấu trúc con
              tree['items'].pop(0) # Xóa khỏi danh sách chính

         # Kiểm tra item cuối cùng có phải outro không
         if tree['items']: # Kiểm tra lại sau khi pop intro
              last_item = tree['items'][-1]
              if last_item.get('type') == 'outro':
                   outline_data['outro'] = last_item
                   tree['items'].pop(-1)

    # Phần còn lại là sections
    outline_data['sections'] = tree['items']

    logging.debug(f"--- Final Parsed Structure (Nested Dict) ---")
    logging.debug(json.dumps(outline_data, indent=2, ensure_ascii=False))
    logging.info("--- Finished parse_outline_markdown ---")

    if not outline_data['sections'] and not outline_data['intro'] and not outline_data['outro']:
        logging.warning("parse_outline_markdown: Parsing resulted in empty structure.")
        return None # Trả về None nếu rỗng

    return outline_data

def flatten_outline(parsed_outline):
    """Làm phẳng cấu trúc outline lồng nhau thành danh sách các mục."""
    flat_list = []
    item_id_counter = 0 # Biến đếm ID duy nhất, định nghĩa ở hàm cha

    def _flatten_recursive(node, current_level_num):
        nonlocal item_id_counter # <<< KHAI BÁO nonlocal Ở ĐÂY

        # Xử lý node hiện tại (là dict)
        item_id_counter += 1 # Bây giờ có thể sửa đổi biến của hàm cha
        item_level = node.get('level', current_level_num)
        item_type = node.get('type', 'point')
        # Lấy title hoặc content tùy theo level
        item_title = node.get('title', '')
        item_content = node.get('content', item_title) # Fallback content = title

        flat_item = {
            'id': item_id_counter, # ID duy nhất từ bộ đếm
            'level': item_level,
            'type': item_type,
            'title': item_title,   # Giữ lại title (thường là heading)
            'content': item_content # Content chính của mục này
        }

        # Bỏ qua item nếu cả title và content đều rỗng sau khi strip
        if not flat_item.get('content','').strip() and not flat_item.get('title','').strip():
            logging.debug(f"Skipping empty flattened item (ID {item_id_counter}): {node}")
            item_id_counter -= 1 # Hoàn lại ID nếu bỏ qua item
            return

        flat_list.append(flat_item)
        logging.debug(f"Flattened item: {flat_item}")

        # Đệ quy cho các mục con 'items'
        if 'items' in node and node['items']:
            for child_item in node['items']:
                 # Level con có thể lấy trực tiếp từ child_item['level'] nếu parse đúng
                 child_level = child_item.get('level', item_level + 1) # Lấy level con hoặc đoán
                 _flatten_recursive(child_item, child_level) # Truyền level của con vào đệ quy

    if parsed_outline:
        logging.info("--- Starting flatten_outline ---")
        # Intro
        intro_node = parsed_outline.get('intro')
        if intro_node:
            # Intro có thể là dict hoặc string (nếu parse đơn giản hóa)
            if isinstance(intro_node, dict):
                 _flatten_recursive(intro_node, 0) # Giả sử intro là level 0
            elif isinstance(intro_node, str): # Trường hợp parse chỉ trả về text
                  item_id_counter += 1
                  flat_list.append({'id': item_id_counter, 'level': 0, 'type': 'intro', 'title': 'Introduction', 'content': intro_node})
                  logging.debug("Flattened intro (as string).")

        # Sections
        if parsed_outline.get('sections'):
             _flatten_recursive({'items': parsed_outline['sections'], 'level': -1}, -1) # Bắt đầu từ root ảo

        # Outro
        outro_node = parsed_outline.get('outro')
        if outro_node:
            if isinstance(outro_node, dict):
                 _flatten_recursive(outro_node, 0)
            elif isinstance(outro_node, str):
                  item_id_counter += 1
                  flat_list.append({'id': item_id_counter, 'level': 0, 'type': 'outro', 'title': 'Conclusion', 'content': outro_node})
                  logging.debug("Flattened outro (as string).")

        logging.info(f"--- Flattening complete, {len(flat_list)} raw items found ---")
    else:
         logging.warning("flatten_outline received empty or None parsed_outline.")
         return [] # Trả về list rỗng

    # Gán lại index tuần tự cuối cùng (0, 1, 2, ...)
    for i, item in enumerate(flat_list):
         item['index'] = i # Thêm trường index tuần tự

    logging.info(f"--- Finished flatten_outline, final items: {len(flat_list)} ---")
    return flat_list