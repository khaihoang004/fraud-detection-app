import os
import time
import redis
import joblib
import pandas as pd
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement

# ---------- Redis ----------
STREAM_IN = os.getenv("STREAM_IN", "cc_stream")
GROUP = os.getenv("GROUP", "cc_group")
CONSUMER = os.getenv("CONSUMER", "infer-1")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "5000"))

r = redis.Redis(host="redis", port=6379, decode_responses=True)

# ---------- Cassandra ----------
CASS_HOST = os.getenv("CASS_HOST", "cassandra")
CASS_KEYSPACE = os.getenv("CASS_KEYSPACE", "fraud_detection")
CASS_TABLE = os.getenv("CASS_TABLE", "predictions_by_day_asc")  # table name from .cql

# ---------- Model ----------
MODEL_PATH = os.getenv("MODEL_PATH", "./models/LogisticRegression.pkl")
PASSTHROUGH = ["Time", "Amount", "Class"]  # must exist in producer

T_LOW = 0.15   # Example: Below 0.1 is Safe
T_HIGH = 0.8  # Example: Above 0.9 is Fraud

def classify(prob):
    if prob < T_LOW:
        return 'Safe'
    elif prob > T_HIGH:
        return 'Fraud'
    else:
        return 'Suspicious' # The "Warning" Zone


def wait_for_redis():
    while True:
        try:
            r.ping()
            return
        except redis.exceptions.ConnectionError:
            print("Waiting for Redis...", flush=True)
            time.sleep(0.5)


def ensure_group():
    try:
        r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
        print(f"✅ group ready: {GROUP} on {STREAM_IN}", flush=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise


def wait_for_cassandra():
    while True:
        try:
            cluster = Cluster([CASS_HOST])
            session = cluster.connect()
            return cluster, session
        except Exception as e:
            print(f"Waiting for Cassandra... ({e})", flush=True)
            time.sleep(2)


def parse_batch(messages, features):
    rows = []
    ids = []
    meta = []
    skipped = 0

    for msg_id, fields in messages:
        try:
            # model features
            xrow = {f: float(fields[f]) for f in features}

            # required DB fields
            t = fields.get("Time")
            amt = fields.get("Amount")
            cls = fields.get("Class")
            if t is None or amt is None or cls is None:
                raise KeyError("Missing Time/Amount/Class")

            rows.append(xrow)
            ids.append(msg_id)
            meta.append(
                {
                    "Time": float(t),
                    "Amount": float(amt),
                    "Class": int(float(cls)),  # sometimes "0.0"
                }
            )
        except Exception:
            skipped += 1

    if skipped:
        print(f"⚠️ skipped {skipped} records (missing/bad required fields)", flush=True)

    if not rows:
        return None, [], []

    X = pd.DataFrame(rows, columns=features)
    return X, ids, meta


def main():
    # --- Redis ---
    wait_for_redis()
    ensure_group()
    print("Redis ready ✅", flush=True)

    # --- Cassandra ---
    cluster, session = wait_for_cassandra()
    print("Cassandra ready ✅", flush=True)

    # Fully qualify table so we don't depend on USE/set_keyspace
    insert_stmt: PreparedStatement = session.prepare(
        f"""
        INSERT INTO {CASS_KEYSPACE}.{CASS_TABLE}
        (day, event_ts, event_id, time, amount, ground_truth, prediction_score, class)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    # --- Model ---
    print(f"Loading model from {MODEL_PATH}...", flush=True)
    model = joblib.load(MODEL_PATH)
    FEATURES = list(model.feature_names_in_)
    print(f"Model loaded ✅ ({type(model).__name__})", flush=True)

    while True:
        try:
            resp = r.xreadgroup(
                GROUP, CONSUMER, {STREAM_IN: ">"}, count=BATCH_SIZE, block=BLOCK_MS
            )
            if not resp:
                continue

            _, messages = resp[0]
            X, msg_ids, meta = parse_batch(messages, FEATURES)
            if X is None:
                continue

            # inference (use probability if available)
            if hasattr(model, "predict_proba"):
                probs = model.predict_proba(X)[:, 1]
            else:
                # fallback: treat predict() output as score
                probs = model.predict(X).astype(float)

            
            # write to cassandra + ack to redis
            now = datetime.now(timezone.utc)
            day = now.strftime("%Y%m%d")  # partition key like '20251224'

            pipe = r.pipeline()
            wrote = 0

            for i, msg_id in enumerate(msg_ids):
                event_id = msg_id
                prediction_score = float(probs[i])
                ruled_class = classify(prediction_score)
                # insert to cassandra first
                session.execute(
                    insert_stmt,
                    (
                        day,
                        now,
                        event_id,
                        float(meta[i]["Time"]),
                        float(meta[i]["Amount"]),
                        int(meta[i]["Class"]),
                        prediction_score,
                        ruled_class
                    ),
                )

                # ack only after insert success
                pipe.xack(STREAM_IN, GROUP, msg_id)
                wrote += 1

            pipe.execute()

            avgp = float(pd.Series([float(x) for x in probs]).mean())
            print(f"✅ wrote {wrote} rows | avg_score={avgp:.4f}", flush=True)

        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                print("⚠️ NOGROUP: recreating group...", flush=True)
                ensure_group()
                continue
            raise

        except Exception as e:
            # If Cassandra fails, DO NOT ack -> messages stay pending
            print(f"❌ pipeline error: {e}", flush=True)
            time.sleep(1)

    cluster.shutdown()


if __name__ == "__main__":
    main()
