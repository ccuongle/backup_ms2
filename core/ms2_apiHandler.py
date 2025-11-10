import logging
from flask import Flask, request, jsonify
import requests
from requests.exceptions import ConnectionError, RequestException
from utils.config import MS4_PERSISTENCE_BASE_URL
from ms2_extractor.core.ms2_invoice_extractor import extract_invoice_data

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ---------------- Helper Functions ----------------

def call_ms4_persistence(invoice_data):
    """Gọi API MS4 để lưu metadata (blocking)"""
    try:
        url = f"{MS4_PERSISTENCE_BASE_URL}/invoice"
        response = requests.post(url, json=invoice_data, timeout=10)

        if response.status_code == 201:
            return {"service": "MS4", "status": "success", "message": "SQL persistence successful"}
        else:
            return {
                "service": "MS4",
                "status": "error",
                "message": f"MS4 responded with {response.status_code}: {response.text}"
            }
    except ConnectionError:
        return {
            "service": "MS4",
            "status": "error",
            "message": "Failed to persist data due to connection error to MS4"
        }
    except RequestException as e:
        return {
            "service": "MS4",
            "status": "error",
            "message": f"Request to MS4 failed: {str(e)}"
        }

# ---------------- API Endpoint ----------------

@app.route("/extract", methods=["POST"])
def extract_invoice():
    """Endpoint chính nhận request từ MS1"""
    data = request.get_json(silent=True) or {}

    email_id = data.get("email_id")
    is_invoice = data.get("isInvoice")

    # 1. Kiểm tra input hợp lệ
    if not email_id:
        return jsonify({
            "status": "error",
            "message": "Missing required field: email_id"
        }), 400

    # 2. Bỏ qua nếu không phải hóa đơn (logic này có thể thay đổi)
    if not is_invoice:
        return jsonify({
            "status": "skipped",
            "message": "Email is not an invoice"
        }), 200

    # 3. Gọi hàm trích xuất dữ liệu thực tế
    try:
        invoice_data = extract_invoice_data(email_id)
        if not invoice_data:
            return jsonify({
                "status": "error",
                "message": f"Failed to extract invoice data for email_id: {email_id}"
            }), 500
    except Exception as e:
        logger.error(f"Extraction failed for {email_id} with error: {e}")
        return jsonify({
            "status": "error",
            "message": f"An exception occurred during extraction: {e}"
        }), 500

    # 4. Gọi MS4 để persist dữ liệu
    ms4_result = call_ms4_persistence(invoice_data)

    # 5. Xử lý phản hồi dựa trên kết quả từ MS4
    if ms4_result.get("status") == "error":
        return jsonify({
            "status": "error",
            "message": ms4_result.get("message", "Failed to persist data via MS4"),
        }), 500

    # Cả hai thành công
    return jsonify({
        "status": "success",
        "message": "Extraction and SQL persistence successful",
        "details": {
            "ms4": ms4_result.get("message")
        }
    }), 201


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5003, debug=True)