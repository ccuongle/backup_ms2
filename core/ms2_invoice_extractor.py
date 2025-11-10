import os
import json
import xmltodict
from utils.config import ATTACH_DIR, load_extraction_prompt, get_model
from pypdf import PdfReader
from ms2_extractor.utils.rabbitmq import RabbitMQConnection

# Ưu tiên trích xuất XML:
def _load_xml_content(email_id: str):
    """Tải nội dung file XML đính kèm từ ATTACH_DIR."""
    # Lấy file XML từ ATTACH_DIR theo email_id
    try:
        file_path = os.path.join(ATTACH_DIR, f"{email_id}.xml")
        with open(file_path, "rb") as f:
            file_content = f.read().strip()
            if not file_content:
                print(f"[ms3_invoiceExtraction]: Attachment is empty: {file_path}")
                return None
            # decode bytes -> str, bỏ BOM nếu có
            file_content = file_content.decode("utf-8-sig")
            print(f"[ms3_invoiceExtraction]: Content loaded, first 500 chars:\n{file_content[:500]}...\n")
            return file_content
    # Nếu không tìm thấy XML, chuyển sang tìm PDF:
    except Exception:
        file_path = os.path.join(ATTACH_DIR, f"{email_id}.pdf")
        if not file_path:
            print("[ms3_invoiceExtraction]: None attachment found")
        
#----------------------------------------Logic trích xuất PDF --------------------------------------------------------    
def _pdf_extraction_logic(file_path: str):
    print(f"[ms3_pdfOCR]: Running PDF/OCR logic for {file_path}")

    raw_data = None
    instruction = None

    try:
        parsed_invoice = PdfReader(file_path)
        page = parsed_invoice.pages[0]
        raw_data = page.extract_text()
    except Exception as e: 
        raise ValueError(f"[ms3_pdfParse]: Error during parsing PDF: {e}")

    try:
        instruction = load_extraction_prompt()
        print(f"[ms3_invoiceExtraction]: Extraction prompt loaded succesfully! {instruction[:50]}...")
    except Exception as e:
        raise ValueError(f"[ms3_invoiceExtraction]: Extraction prompt not found {e}!")

    if not raw_data or not instruction:
        print("[ms3_invoiceExtraction]: Missing raw_data or instruction, aborting.")
        return None

    prompt = f"{instruction}\n  Here's the invoice:\n{raw_data}"
    print("[ms3_invoiceExtraction]: Sending prompt to model...")

    try:
        model = get_model()
        if model is None:
            raise ValueError("Model is not loaded")
        respond = model.generate_content(prompt, generation_config={"temperature": 0.0}).text.strip()
        print("[ms3_invoiceExtraction]: Extraction completed.")
        clean_text = respond.strip('`')
        if clean_text.startswith('json'):
            clean_text = clean_text[4:].strip()  
        print(clean_text)
        return clean_text
    except Exception as e:
        print(f"Error during redefining: {e}")
        return None

#------------------------------------------------------------------------------------------------------------------------------

def map_invoice(file_content: str) -> dict:
    """Parse XML string thành dict invoice chuẩn hóa"""
    if not isinstance(file_content, str):
        raise ValueError(f"[ms3_xmlMapping]: map_invoice expects str, got {type(file_content)}")
    
    print("[xmltoDict]: ==== Parsing XML ====")
    data = xmltodict.parse(file_content)
    # print("[xmltoDict]: Top-level keys:", list(data.keys()))
    
    hdon = data.get("HDon", {})
    dl = hdon.get("DLHDon", {})
    ndhd = dl.get("NDHDon", {})
    
    ttchung = dl.get("TTChung", {})
    nban = ndhd.get("NBan", {})
    nmua = ndhd.get("NMua", {})
    dshhdvu = ndhd.get("DSHHDVu", {}).get("HHDVu", [])
    ttoan = ndhd.get("TToan", {})

    if isinstance(dshhdvu, dict):
        dshhdvu = [dshhdvu]

    # Khởi tạo cấu trúc hóa đơn trích xuất
    extractedInvoice = {
        "invoice_type": ttchung.get("THDon", ""),
        "vendor_tax_code": nban.get("MST", ""),
        "vendor_name": nban.get("Ten", ""),
        "vendor_address": nban.get("DChi", ""),
        "buyer_tax_code": nmua.get("MST", ""),
        "buyer_name": nmua.get("Ten", ""),
        "buyer_address": nmua.get("DChi", ""),
        "invoice_number": ttchung.get("SHDon", ""),
        "template_code": ttchung.get("KHMSHDon", ""),
        "invoice_series": ttchung.get("KHHDon", ""),
        "issued_date": ttchung.get("NLap", ""),
        "currency_code": ttchung.get("DVTTe", "VND"),
        "total_amount_before_vat": float(ttoan.get("TgTCThue", 0)),
        "total_vat_amount": float(ttoan.get("TgTThue", 0)),
        "total_amount_after_vat": float(ttoan.get("TgTTTBSo", 0)),
        "items": []
    }

    # Lặp qua các mặt hàng
    for hh in dshhdvu:
        ttin_list = hh.get("TTKhac", {}).get("TTin", [])
        if isinstance(ttin_list, dict):
            ttin_list = [ttin_list]
        
        vat_amount = 0
        amount_after_vat = 0
        promotion_flag = False
        
        for t in ttin_list:
            ttruong = t.get("TTruong", "")
            dl_val = t.get("DLieu", {})
            
            if ttruong == "Tiền thuế":
                if isinstance(dl_val, (str, int, float)):
                    vat_amount += float(dl_val)
            elif ttruong == "TTMR" and isinstance(dl_val, dict):
                amount_after_vat += float(dl_val.get("TTST", 0))
                # Gắn cờ khuyến mãi nếu có trường KM = 1
                km_value = dl_val.get("KM", "0")
                if str(km_value).strip() in ("1", "True", "true"):
                    promotion_flag = True

        extractedItem = {
            "product_code": hh.get("MHHDVu", ""),
            "product_name": hh.get("THHDVu", ""),
            "unit_name": hh.get("DVTinh", ""),
            "quantity": float(hh.get("SLuong", 0)),
            "unit_price": float(hh.get("DGia", 0)),
            "amount_before_vat": float(hh.get("ThTien", 0)),
            "vat_rate": float(hh.get("TSuat", "0").replace("%", "")),
            "vat_amount": vat_amount,
            "amount_after_vat": amount_after_vat,
            "promotion_flag": promotion_flag
        }
        
        extractedInvoice["items"].append(extractedItem)

    print("[xmltoDict]: ==== Extracted Invoice ====")
    print(json.dumps(extractedInvoice, indent=2, ensure_ascii=False))
    return extractedInvoice


def publish_invoice_data(invoice_data: dict):
    """Serializes and publishes invoice data to RabbitMQ."""
    if not invoice_data:
        print("[ms2_publisher]: No invoice data to publish.")
        return

    rmq = None
    try:
        print("[ms2_publisher]: Initializing RabbitMQ connection...")
        rmq = RabbitMQConnection()
        rmq.connect()

        message_body = json.dumps(invoice_data, ensure_ascii=False)
        
        print("[ms2_publisher]: Publishing message to exchange 'invoice_exchange' with routing key 'queue.for_persistence'...")
        rmq.publish(
            exchange='invoice_exchange',
            routing_key='queue.for_persistence',
            body=message_body
        )
        print("[ms2_publisher]: Message published successfully.")

    except Exception as e:
        print(f"[ms2_publisher]: Failed to publish message to RabbitMQ: {e}")
        # Optionally, re-raise the exception if the caller needs to handle it
        # raise
    finally:
        if rmq:
            rmq.close()


def extract_invoice_data(email_id: str):
    """Hàm điều phối trích xuất chung (XML, PDF, etc.)"""
    if not isinstance(email_id, str) or not email_id:
        raise ValueError(f"[ms3_invoiceExtraction]: Invalid email_id: {email_id}")
    
    extracted_data = None
    # 1. Thử trích xuất XML trước
    xml_content = _load_xml_content(email_id)
    if xml_content:
        extracted_data = map_invoice(xml_content)
    # 2. Tìm PDF nếu không có XML
    else:
        print(f"[ms3_invoiceExtraction]: No valid XML content found for {email_id}, trying PDF...")
        pdf_path = os.path.join(ATTACH_DIR, f"{email_id}.pdf")
        if os.path.exists(pdf_path):
            extracted_data = _pdf_extraction_logic(pdf_path)
        else:
            print(f"[ms3_invoiceExtraction]: No valid PDF attachment found for {email_id}")

    if extracted_data:
        # Publish the extracted data to RabbitMQ
        publish_invoice_data(extracted_data)
    else:
        print(f"[ms3_invoiceExtraction]: Extraction failed for {email_id}")

    return extracted_data
