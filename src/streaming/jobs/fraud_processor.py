from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, struct, current_timestamp, date_format, size
from pyspark.sql.types import *
import uuid
import joblib
from src.streaming.rules.engine import evaluate_rule

# 1. Định nghĩa Schema cho Kafka (Amount, Time, V1...V28)
fields = [StructField("Time", DoubleType()), StructField("Amount", DoubleType())]
for i in range(1, 29):
    fields.append(StructField(f"V{i}", DoubleType()))
schema = StructType(fields)

# 2. Mock Rules (Trong thực tế bạn sẽ load từ Redis hoặc Broadcast từ Kafka)
current_rules = [
    {"rule_id": "HIGH_AMT", "params": {"field": "Amount", "op": ">", "value": 5000}, "enabled": True},
    {"rule_id": "V1_THRESHOLD", "params": {"field": "V1", "op": "<", "value": -2.5}, "enabled": True}
]

model = joblib.load("models/fraud_model.pkl")

def predict_fraud_score(features_dict):
    # Chuyển dict thành mảng/dataframe để predict
    # Ở đây tôi giả lập trả về một score ngẫu nhiên dựa trên amount
    return float(model.predict_proba([[features_dict['amount']]])[0][1])

predict_udf = udf(predict_fraud_score, DoubleType())

# --- 2. Dynamic Rule Engine (UDF) ---
# Giả sử ta có danh sách rules được fetch từ API/Cache
# Ở đây ta sẽ demo với 1 list rules cố định (trong thực tế dùng Broadcast)

def apply_rules(row_dict):
    triggered = []
    for r in current_rules:
        if evaluate_rule(row_dict, r):
            triggered.append(r["rule_id"])
    return triggered

apply_rules_udf = udf(apply_rules, ArrayType(StringType()))
gen_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("FraudProcessorV2") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

base_df = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

enriched_df = base_df \
    .withColumn("transaction_id", gen_uuid()) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("day", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .withColumn("fraud_score", predict_udf(struct([col(c) for c in base_df.columns]))) \
    .withColumn("triggered_rules", apply_rules_udf(struct([col(c) for c in base_df.columns])))

final_df = enriched_df.withColumn(
    "is_fraud", 
    (col("fraud_score") > 0.8) | (size(col("triggered_rules")) > 0)
)

def write_to_cassandra(batch_df, batch_id):
    # Ghi vào bảng fraud_events
    batch_df.write.format("org.apache.spark.sql.cassandra") \
        .options(table="fraud_events", keyspace="fraud_detection") \
        .mode("append").save()

query = final_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("update") \
    .start()

query.awaitTermination()