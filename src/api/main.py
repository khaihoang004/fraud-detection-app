from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime
import json
import logging

# --- CẤU HÌNH & LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Fraud Detection API Service")

# --- 1. KẾT NỐI KAFKA (Dành cho Rule Updates) ---
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )
    logger.info("✅ Kafka Producer initialized.")
except Exception as e:
    logger.error(f"❌ Could not connect to Kafka: {e}")
    producer = None

# --- 2. KẾT NỐI CASSANDRA (Dành cho Alerts & Stats) ---
try:
    cluster = Cluster(['localhost'])
    cassandra_session = cluster.connect('fraud_detection')
    # Trả về kết quả dạng Dictionary để dễ xử lý JSON
    cassandra_session.row_factory = dict_factory
    logger.info("✅ Cassandra Session initialized.")
except Exception as e:
    logger.error(f"❌ Could not connect to Cassandra: {e}")
    cassandra_session = None

# --- 3. ĐỊNH NGHĨA SCHEMAS (Pydantic) ---
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

# --- 4. ENDPOINTS CHO RULE ENGINE (GỬI ĐI) ---

@app.post("/update-rule")
async def update_rule(rule: RuleModel):
    if producer is None:
        raise HTTPException(status_code=500, detail="Kafka Producer not initialized")
    try:
        rule_data = rule.dict()
        future = producer.send('rule_updates', value=rule_data)
        record_metadata = future.get(timeout=10)
        return {
            "status": "success",
            "rule_id": rule.rule_id,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset
        }
    except Exception as e:
        logger.error(f"Error sending rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- 5. ENDPOINTS CHO DASHBOARD (LẤY VỀ) ---

@app.get("/recent-alerts")
async def get_recent_alerts(limit: int = 10):
    """Lấy danh sách các vụ gian lận mới nhất từ Cassandra"""
    if cassandra_session is None:
        raise HTTPException(status_code=500, detail="Cassandra not connected")
    
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        # Truy vấn dựa trên Partition Key là 'day' và Clustering Key là 'timestamp'
        query = "SELECT * FROM fraud_events WHERE day = %s LIMIT %s"
        rows = cassandra_session.execute(query, [today, limit])
        return list(rows)
    except Exception as e:
        logger.error(f"Cassandra query error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from database")

@app.get("/stats")
async def get_global_stats():
    """Lấy số liệu tổng hợp cho các thẻ Metrics trên UI"""
    if cassandra_session is None:
        raise HTTPException(status_code=500, detail="Cassandra not connected")
    
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        # Cách đơn giản: Đếm trực tiếp từ bảng events của ngày hôm nay
        query = "SELECT is_fraud, amount FROM fraud_events WHERE day = %s"
        rows = cassandra_session.execute(query, [today])
        
        data = list(rows)
        frauds = [r for r in data if r['is_fraud']]
        
        return {
            "total_transactions": len(data),
            "fraud_count": len(frauds),
            "total_fraud_amount": sum(f['amount'] for f in frauds)
        }
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return {"fraud_count": 0, "total_fraud_amount": 0.0}

# --- 6. HỆ THỐNG KIỂM TRA ---

@app.get("/health")
def health_check():
    return {
        "status": "online",
        "kafka": "Connected" if producer and producer.bootstrap_connected() else "Disconnected",
        "cassandra": "Connected" if cassandra_session else "Disconnected"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)