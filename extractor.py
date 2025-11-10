import os
import json
import re
from .utils.config import ATTACH_DIR, load_extraction_prompt, get_model

def extract_invoice(email_result: dict):
    """Nhận kết quả classify, extract file attachment nếu là invoice"""
    email_id = email_result["email_id"]
    is_invoice = email_result["isInvoice"]
    print(f"[extractor]: Process initializing: {email_result['email_id']}...")
    if not is_invoice:
        print("[extractor]: ℹ️ Email is not invoice, skipping process.")
        return None

    # 🔹 Lấy file XML từ ATTACH_DIR theo email_id
    file_path = os.path.join(ATTACH_DIR, f"{email_id}.xml")
    if not os.path.exists(file_path):
        print(f"[extractor]:❌Attachment not found in: {file_path}")
        return None

    with open(file_path, "r", encoding="utf-8") as f:
        file_content = f.read().strip()
        print(f"[extractor]: Attachments found in {file_path}")
    if not file_content:
        print(f"[extractor]: ❌ Attachment doesn't exist: {file_path}")
        return None

    # 🔹 Load prompt extraction
    instruction = load_extraction_prompt()
    prompt = f"{instruction}\n\nAttached File Content:\n{file_content}"
    print(f"[extractor]: Extracting process initializing: {prompt[:10]}")
    # 🔹 Gọi model để extract
    try:
        model = get_model()
        response = model.generate_content(prompt)
        raw_output = response.text.strip()
        # 🔹 Làm sạch output nếu Gemini trả ```json ... ```
        if raw_output.startswith("```"):
            raw_output = raw_output.strip("`")
            if raw_output.lower().startswith("json"):
                raw_output = raw_output[4:].strip()

        # 🔹 Regex bắt block JSON đầu tiên
        match = re.search(r"\{.*\}", raw_output, re.DOTALL)
        if not match:
            print(f"[extractor]: ❌ Can't extract data from output: {raw_output}")
            return None

        clean_json = match.group(0)

        extracted_data = json.loads(clean_json)
        print(f"[extractor]: ✅ Extracted Invoice:\n{json.dumps(extracted_data, ensure_ascii=False, indent=2)}")
        return extracted_data

    except Exception as e:
        print(f"[extractor]: ❌ Can't extract data from attachments: {e}")
        return None
