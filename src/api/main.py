from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import logging

RULE_API_URL = "http://localhost:8000/update-rule"

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Fraud Rule Config Service")

# --- 1. Cấu hình Kafka Producer ---
# Trong thực tế, 'localhost:9092' nên được lấy từ file config hoặc env
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
except Exception as e:
    logger.error(f"Could not connect to Kafka: {e}")
    producer = None

# --- 2. Định nghĩa Schema cho Rule (Pydantic) ---
class RuleParams(BaseModel):
    field: str
    op: str
    value: float | int | str

class RuleModel(BaseModel):
    rule_id: str
    template: str
    params: RuleParams
    severity: str
    enabled: bool

# --- 3. Các Endpoints API ---

@app.get("/")
def read_root():
    return {"status": "Rule Config Service is running"}

@app.post("/update-rule")
async def update_rule(rule: RuleModel):
    """
    Endpoint nhận rule từ UI và đẩy vào Kafka topic 'rule_updates'
    """
    if producer is None:
        raise HTTPException(status_code=500, detail="Kafka Producer not initialized")
    
    try:
        # Chuyển đổi Pydantic model sang Dictionary
        rule_data = rule.dict()
        
        # Gửi dữ liệu vào Kafka
        future = producer.send('rule_updates', value=rule_data)
        
        # Đợi xác nhận gửi thành công (optional - tăng độ tin cậy)
        record_metadata = future.get(timeout=10)
        
        logger.info(f"Rule {rule.rule_id} sent to topic {record_metadata.topic} at offset {record_metadata.offset}")
        
        return {
            "message": "Rule updated successfully",
            "rule_id": rule.rule_id,
            "topic": record_metadata.topic
        }
    except Exception as e:
        logger.error(f"Error sending rule to Kafka: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    # Kiểm tra trạng thái Kafka
    kafka_status = "Connected" if producer and producer.bootstrap_connected() else "Disconnected"
    return {"status": "healthy", "kafka": kafka_status}

# --- 4. Chạy Service ---
if __name__ == "__main__":
    import uvicorn
    # Chạy lệnh: python src/api/main.py
    uvicorn.run(app, host="0.0.0.0", port=8000)