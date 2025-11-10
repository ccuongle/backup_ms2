import os
from dotenv import load_dotenv
import google.generativeai as genai
import yaml

# Load environment variables
load_dotenv()

# ============= RabbitMQ Settings =============
# RabbitMQ Connection Configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
RABBITMQ_VIRTUAL_HOST = os.getenv('RABBITMQ_VIRTUAL_HOST', '/')

# MS2 Queue Topology (Consumer & Producer)
RABBITMQ_CONSUME_QUEUE = os.getenv('RABBITMQ_CONSUME_QUEUE', 'queue.for_extraction')
RABBITMQ_EXCHANGE = os.getenv('RABBITMQ_EXCHANGE', 'invoice_exchange')
RABBITMQ_ROUTING_KEY = os.getenv('RABBITMQ_ROUTING_KEY', 'invoice.to.persistence')

# Service Settings
SERVICE_NAME = os.getenv('SERVICE_NAME', 'ms2_extractor')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# ============= MS4 Settings =============
MS4_PERSISTENCE_BASE_URL = os.getenv("MS4_PERSISTENCE_BASE_URL", "http://localhost:5004")

# ============= Validation =============
def validate_config():
    """Validate configuration"""
    errors = []
    
    if not RABBITMQ_HOST:
        errors.append("RABBITMQ_HOST is required")
    
    if not RABBITMQ_USERNAME:
        errors.append("RABBITMQ_USERNAME is required")
        
    if not RABBITMQ_PASSWORD:
        errors.append("RABBITMQ_PASSWORD is required")

    if errors:
        raise ValueError("Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))

# Validate on import
validate_config()

# Directories
BASE_DIR = os.path.dirname(__file__)
ATTACH_DIR = os.path.join(BASE_DIR, "..", "storage", "attachments")
EXTRACTED_DIR = os.path.join(BASE_DIR, "..", "storage", "extracted")

os.makedirs(ATTACH_DIR, exist_ok=True)
os.makedirs(EXTRACTED_DIR, exist_ok=True)

# Config Google GenAI
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_NAME = os.getenv("MODEL_NAME")
genai.configure(api_key=GEMINI_API_KEY)
try:
    available_models = [m.name for m in genai.list_models()]
except Exception:
    available_models = ["models/gemini-1.5-flash"] # Mock model for testing
def get_model(model_name: str = MODEL_NAME):
    if model_name not in available_models:
        raise ValueError(f"{model_name} is not available, please load other model")
    else:
        print(f"{model_name} is available")
        return genai.GenerativeModel(model_name)

# Load prompts:
def load_extraction_prompt():
    """Load extract_prompts.yaml from the prompts directory"""
    extract_prompt_path = os.path.join(os.path.dirname(__file__), 'prompts', 'extract_prompt.yaml')
    if not os.path.exists(extract_prompt_path):
        raise FileNotFoundError(f"❌ Can't find extract_prompt.yaml at {extract_prompt_path}")
    with open(extract_prompt_path, "r", encoding="utf-8") as f:
        extractor_prompts = yaml.safe_load(f)
    return extractor_prompts.get("extractor_instruction")
